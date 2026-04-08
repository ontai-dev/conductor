// compile.go contains the input type definitions and CR generation logic for
// all active Compiler subcommands. The Compiler is a CR compiler: it reads
// human-authored spec files and emits Kubernetes CR YAML ready to apply to the
// management cluster. conductor-schema.md §9, platform-schema.md.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	talosconfig "github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	platformv1alpha1 "github.com/ontai-dev/platform/api/v1alpha1"
	wrapperv1alpha1 "github.com/ontai-dev/wrapper/api/v1alpha1"
)

// BootstrapNode declares a single Talos node for management cluster bootstrap.
// Each node maps to one Talos machine configuration and one Kubernetes Secret.
type BootstrapNode struct {
	// Hostname is the node's hostname. Used as the node name in Secret naming
	// convention seam-mc-{cluster}-{hostname}. platform-schema.md §9.
	Hostname string `yaml:"hostname"`

	// IP is the node's IP address reachable on Talos port 50000.
	IP string `yaml:"ip"`

	// Role is the Talos machine type: "init" (first control plane node),
	// "controlplane" (additional control plane nodes), or "worker".
	// Exactly one node must have role "init".
	Role string `yaml:"role"`
}

// BootstrapSection holds management cluster bootstrap configuration.
// Required when compiling bootstrap artifacts (compiler bootstrap subcommand).
type BootstrapSection struct {
	// ControlPlaneEndpoint is the canonical HTTPS endpoint for the Kubernetes
	// API server (e.g., https://10.20.0.10:6443). Used as the cluster endpoint
	// in all generated machine configurations and the talosconfig.
	ControlPlaneEndpoint string `yaml:"controlPlaneEndpoint"`

	// TalosVersion is the Talos OS version to target (e.g., "v1.9.3").
	// Used to select the appropriate VersionContract for config generation.
	TalosVersion string `yaml:"talosVersion"`

	// KubernetesVersion is the Kubernetes version to install (e.g., "1.32.0").
	KubernetesVersion string `yaml:"kubernetesVersion"`

	// InstallerImage is the fully-qualified Talos installer image reference.
	// Defaults to ghcr.io/siderolabs/installer:{talosVersion} when empty.
	// +optional
	InstallerImage string `yaml:"installerImage,omitempty"`

	// InstallDisk is the default block device for Talos installation across
	// all nodes (e.g., "/dev/sda").
	// Defaults to "/dev/sda" when empty.
	// +optional
	InstallDisk string `yaml:"installDisk,omitempty"`

	// Nodes declares each node that will form the management cluster.
	// Exactly one node must have role "init". All others must be
	// "controlplane" or "worker".
	Nodes []BootstrapNode `yaml:"nodes"`
}

// ClusterInput is the human-authored spec format for bootstrap, launch, and enable
// subcommands. Humans write this file; Compiler reads it and emits a TalosCluster CR.
// Fields map directly to TalosClusterSpec. conductor-schema.md §9.
//
// Cilium native routing on Talos — required machine config patches:
//
// Every node's machine config produced by the bootstrap subcommand MUST include
// the following kernel modules and sysctls for Cilium native routing to function.
// These cannot be applied after boot; they must be present in the initial machine
// config delivered on port 50000.
//
//	machine.kernel.modules:
//	  - name: br_netfilter
//	  - name: xt_socket
//
//	machine.sysctls:
//	  net.ipv4.conf.all.rp_filter:     "0"
//	  net.ipv4.conf.default.rp_filter: "0"
//
// br_netfilter: required for bridge traffic to traverse netfilter rules (eBPF hooks).
// xt_socket:    required for transparent proxy / socket-based load balancing in Cilium.
// rp_filter=0:  disables reverse-path filtering — mandatory for native routing mode
//               because pod traffic arrives on an interface other than the default route.
//
// Without these, Cilium installs but pod-to-pod routing silently drops packets.
// The Compiler does not inject these automatically; the Lab Operator must include
// them as patches in the ClusterInput or apply them as talosctl machine config patches.
type ClusterInput struct {
	// Name is the TalosCluster resource name. Used as metadata.name.
	Name string `yaml:"name"`

	// Namespace is the target namespace for the TalosCluster CR.
	// Defaults to "seam-system" when empty.
	Namespace string `yaml:"namespace,omitempty"`

	// Mode declares whether this TalosCluster bootstraps a new cluster or imports
	// an existing one. Must be "bootstrap" or "import".
	// +required
	Mode string `yaml:"mode"`

	// CAPI holds CAPI-specific configuration. Set enabled=true for target clusters,
	// false for the management cluster. platform-schema.md §5.
	CAPI CAPIInput `yaml:"capi"`

	// Bootstrap holds management cluster bootstrap configuration. Required for
	// the compiler bootstrap subcommand.
	// +optional
	Bootstrap *BootstrapSection `yaml:"bootstrap,omitempty"`
}

// CAPIInput is the CAPI configuration section of ClusterInput.
type CAPIInput struct {
	// Enabled is true for all target (CAPI-managed) clusters, false for the
	// management cluster (direct bootstrap path via Conductor Job).
	Enabled bool `yaml:"enabled"`

	// TalosVersion is the Talos OS version for TalosConfigTemplate and CABPT
	// machineconfig generation. Required when Enabled=true.
	TalosVersion string `yaml:"talosVersion,omitempty"`

	// KubernetesVersion is the Kubernetes version for TalosControlPlane.
	// Required when Enabled=true.
	KubernetesVersion string `yaml:"kubernetesVersion,omitempty"`

	// ControlPlaneReplicas is the desired number of control plane nodes.
	// Required when Enabled=true.
	ControlPlaneReplicas int32 `yaml:"controlPlaneReplicas,omitempty"`
}

// PackBuildInput is the human-authored spec format for the packbuild subcommand.
// Humans write this file; Compiler reads it and emits a ClusterPack CR.
// conductor-schema.md §9, wrapper-schema.md §3.
type PackBuildInput struct {
	// Name is the ClusterPack resource name. Used as metadata.name.
	Name string `yaml:"name"`

	// Namespace is the target namespace for the ClusterPack CR.
	// Defaults to "seam-system" when empty.
	Namespace string `yaml:"namespace,omitempty"`

	// Version is the semantic version of this pack (e.g., v1.2.3).
	Version string `yaml:"version"`

	// RegistryURL is the OCI registry URL including image name.
	RegistryURL string `yaml:"registryUrl"`

	// Digest is the OCI image digest (e.g., sha256:abc123...).
	Digest string `yaml:"digest"`

	// Checksum is the SHA256 checksum of the pack artifact.
	Checksum string `yaml:"checksum"`

	// SourceBuildRef is an opaque reference to the build that produced this pack.
	// +optional
	SourceBuildRef string `yaml:"sourceBuildRef,omitempty"`
}

// readClusterInput reads and parses a ClusterInput spec file from the given path.
func readClusterInput(path string) (ClusterInput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return ClusterInput{}, fmt.Errorf("read input file %q: %w", path, err)
	}
	var in ClusterInput
	if err := yaml.Unmarshal(data, &in); err != nil {
		return ClusterInput{}, fmt.Errorf("parse input file %q: %w", path, err)
	}
	if in.Name == "" {
		return ClusterInput{}, fmt.Errorf("input file %q: name is required", path)
	}
	if in.Mode == "" {
		return ClusterInput{}, fmt.Errorf("input file %q: mode is required (bootstrap or import)", path)
	}
	return in, nil
}

// readPackBuildInput reads and parses a PackBuildInput spec file from the given path.
func readPackBuildInput(path string) (PackBuildInput, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return PackBuildInput{}, fmt.Errorf("read input file %q: %w", path, err)
	}
	var in PackBuildInput
	if err := yaml.Unmarshal(data, &in); err != nil {
		return PackBuildInput{}, fmt.Errorf("parse input file %q: %w", path, err)
	}
	if in.Name == "" {
		return PackBuildInput{}, fmt.Errorf("input file %q: name is required", path)
	}
	if in.Version == "" {
		return PackBuildInput{}, fmt.Errorf("input file %q: version is required", path)
	}
	if in.RegistryURL == "" {
		return PackBuildInput{}, fmt.Errorf("input file %q: registryUrl is required", path)
	}
	if in.Digest == "" {
		return PackBuildInput{}, fmt.Errorf("input file %q: digest is required", path)
	}
	return in, nil
}

// writeCRYAML marshals the given object to Kubernetes CR YAML and writes it to
// the output directory as {name}.yaml.
func writeCRYAML(outDir, name string, obj interface{}) error {
	data, err := yaml.Marshal(obj)
	if err != nil {
		return fmt.Errorf("marshal CR: %w", err)
	}
	if err := os.MkdirAll(outDir, 0755); err != nil {
		return fmt.Errorf("create output directory %q: %w", outDir, err)
	}
	outPath := filepath.Join(outDir, name+".yaml")
	if err := os.WriteFile(outPath, data, 0644); err != nil {
		return fmt.Errorf("write output file %q: %w", outPath, err)
	}
	return nil
}

// buildTalosCluster constructs a TalosCluster CR from a ClusterInput.
func buildTalosCluster(in ClusterInput) platformv1alpha1.TalosCluster {
	ns := in.Namespace
	if ns == "" {
		ns = "seam-system"
	}
	mode := platformv1alpha1.TalosClusterMode(in.Mode)
	return platformv1alpha1.TalosCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "platform.ontai.dev/v1alpha1",
			Kind:       "TalosCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
		},
		Spec: platformv1alpha1.TalosClusterSpec{
			Mode: mode,
			CAPI: platformv1alpha1.CAPIConfig{
				Enabled:           in.CAPI.Enabled,
				TalosVersion:      in.CAPI.TalosVersion,
				KubernetesVersion: in.CAPI.KubernetesVersion,
				ControlPlane: platformv1alpha1.CAPIControlPlaneConfig{
					Replicas: in.CAPI.ControlPlaneReplicas,
				},
			},
		},
	}
}

// validateBootstrapInput checks all required fields in the BootstrapSection.
func validateBootstrapInput(b *BootstrapSection) error {
	if b == nil {
		return fmt.Errorf("bootstrap section is required for the bootstrap subcommand")
	}
	if b.ControlPlaneEndpoint == "" {
		return fmt.Errorf("bootstrap.controlPlaneEndpoint is required")
	}
	if b.TalosVersion == "" {
		return fmt.Errorf("bootstrap.talosVersion is required")
	}
	if b.KubernetesVersion == "" {
		return fmt.Errorf("bootstrap.kubernetesVersion is required")
	}
	if len(b.Nodes) == 0 {
		return fmt.Errorf("bootstrap.nodes must contain at least one node")
	}
	initCount := 0
	for i, n := range b.Nodes {
		if n.Hostname == "" {
			return fmt.Errorf("bootstrap.nodes[%d]: hostname is required", i)
		}
		if n.IP == "" {
			return fmt.Errorf("bootstrap.nodes[%d]: ip is required", i)
		}
		switch n.Role {
		case "init":
			initCount++
		case "controlplane", "worker":
			// valid
		default:
			return fmt.Errorf("bootstrap.nodes[%d] %q: role must be init, controlplane, or worker", i, n.Hostname)
		}
	}
	if initCount != 1 {
		return fmt.Errorf("bootstrap.nodes: exactly one node must have role \"init\", found %d", initCount)
	}
	return nil
}

// compileBootstrap implements the bootstrap subcommand.
//
// Reads a ClusterInput spec (with a bootstrap section declaring node IPs, roles,
// and Talos version) and produces three output artifacts in --output:
//   - seam-mc-{cluster}-{hostname}.yaml — Kubernetes Secret YAML per node
//     containing the Talos machine configuration. platform-schema.md §9.
//   - {cluster-name}.yaml — TalosCluster CR with mode=bootstrap, capi.enabled=false.
//   - bootstrap-sequence.yaml — documents the apply order.
//
// Uses the Talos machinery library to generate machine configurations.
// No cluster connection required. conductor-schema.md §9.
func compileBootstrap(input, output string) error {
	in, err := readClusterInput(input)
	if err != nil {
		return err
	}
	if err := validateBootstrapInput(in.Bootstrap); err != nil {
		return fmt.Errorf("input %q: %w", input, err)
	}
	b := in.Bootstrap

	// Apply defaults.
	installDisk := b.InstallDisk
	if installDisk == "" {
		installDisk = "/dev/sda"
	}
	installerImage := b.InstallerImage
	if installerImage == "" {
		installerImage = "ghcr.io/siderolabs/installer:" + b.TalosVersion
	}

	// Parse version contract.
	versionContract, err := talosconfig.ParseContractFromVersion(b.TalosVersion)
	if err != nil {
		return fmt.Errorf("parse talos version %q: %w", b.TalosVersion, err)
	}

	// Collect control plane node IPs for the endpoint list.
	var cpIPs []string
	for _, n := range b.Nodes {
		if n.Role == "init" || n.Role == "controlplane" {
			cpIPs = append(cpIPs, n.IP)
		}
	}

	// Generate the cluster secrets bundle once. This is the source of all
	// cryptographic material (CA certs, bootstrap tokens, etc.).
	secretsBundle, err := secrets.NewBundle(
		secrets.NewFixedClock(time.Now()),
		versionContract,
	)
	if err != nil {
		return fmt.Errorf("generate secrets bundle: %w", err)
	}

	// Build the generate input with cluster-wide settings.
	genInput, err := generate.NewInput(
		in.Name,
		b.ControlPlaneEndpoint,
		b.KubernetesVersion,
		generate.WithVersionContract(versionContract),
		generate.WithSecretsBundle(secretsBundle),
		generate.WithInstallDisk(installDisk),
		generate.WithInstallImage(installerImage),
		generate.WithEndpointList(cpIPs),
	)
	if err != nil {
		return fmt.Errorf("build generate input: %w", err)
	}

	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	ns := in.Namespace
	if ns == "" {
		ns = "seam-system"
	}

	// Generate machine configuration for each node and write as a Secret.
	var secretNames []string
	for _, node := range b.Nodes {
		machineType, err := nodeRoleToMachineType(node.Role)
		if err != nil {
			return fmt.Errorf("node %q: %w", node.Hostname, err)
		}

		cfg, err := genInput.Config(machineType)
		if err != nil {
			return fmt.Errorf("generate config for node %q: %w", node.Hostname, err)
		}

		cfgBytes, err := cfg.Bytes()
		if err != nil {
			return fmt.Errorf("marshal config for node %q: %w", node.Hostname, err)
		}

		secretName := "seam-mc-" + in.Name + "-" + node.Hostname
		secret := corev1.Secret{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Secret",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: ns,
				Labels: map[string]string{
					"ontai.dev/cluster":    in.Name,
					"ontai.dev/node":       node.Hostname,
					"ontai.dev/node-role":  node.Role,
					"ontai.dev/managed-by": "compiler",
				},
			},
			Type: corev1.SecretTypeOpaque,
			StringData: map[string]string{
				"machineconfig.yaml": string(cfgBytes),
			},
		}

		if err := writeCRYAML(output, secretName, secret); err != nil {
			return fmt.Errorf("write machineconfig secret for node %q: %w", node.Hostname, err)
		}
		secretNames = append(secretNames, secretName+".yaml")
	}

	// Produce TalosCluster CR: mode=bootstrap, capi.enabled=false (management cluster).
	tc := platformv1alpha1.TalosCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "platform.ontai.dev/v1alpha1",
			Kind:       "TalosCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
		},
		Spec: platformv1alpha1.TalosClusterSpec{
			Mode: platformv1alpha1.TalosClusterModeBootstrap,
			CAPI: platformv1alpha1.CAPIConfig{Enabled: false},
		},
	}
	if err := writeCRYAML(output, in.Name, tc); err != nil {
		return fmt.Errorf("write TalosCluster CR: %w", err)
	}

	// Produce bootstrap-sequence.yaml documenting the apply order.
	return writeBootstrapSequence(output, in.Name, secretNames)
}

// nodeRoleToMachineType converts a bootstrap node role to the Talos machine.Type.
func nodeRoleToMachineType(role string) (machine.Type, error) {
	switch role {
	case "init":
		return machine.TypeInit, nil
	case "controlplane":
		return machine.TypeControlPlane, nil
	case "worker":
		return machine.TypeWorker, nil
	default:
		return machine.TypeUnknown, fmt.Errorf("unknown role %q", role)
	}
}

// BootstrapSequenceStep documents one step in the bootstrap apply sequence.
type BootstrapSequenceStep struct {
	Step        int      `json:"step" yaml:"step"`
	Description string   `json:"description" yaml:"description"`
	Resources   []string `json:"resources" yaml:"resources"`
}

// BootstrapSequence is the apply-order document produced by compiler bootstrap.
type BootstrapSequence struct {
	APIVersion  string                  `json:"apiVersion" yaml:"apiVersion"`
	Kind        string                  `json:"kind" yaml:"kind"`
	ClusterName string                  `json:"clusterName" yaml:"clusterName"`
	Steps       []BootstrapSequenceStep `json:"steps" yaml:"steps"`
}

// writeBootstrapSequence writes bootstrap-sequence.yaml documenting apply order.
func writeBootstrapSequence(output, clusterName string, secretFiles []string) error {
	seq := BootstrapSequence{
		APIVersion:  "ontai.dev/v1alpha1",
		Kind:        "BootstrapSequence",
		ClusterName: clusterName,
		Steps: []BootstrapSequenceStep{
			{
				Step: 1,
				Description: "Apply Talos machineconfig Secrets — one per node. " +
					"Apply before the TalosCluster CR so Platform can reference them at bootstrap time.",
				Resources: secretFiles,
			},
			{
				Step: 2,
				Description: "Apply TalosCluster CR with mode=bootstrap and capi.enabled=false. " +
					"Platform's TalosClusterReconciler watches this CR and submits the bootstrap Conductor Job.",
				Resources: []string{clusterName + ".yaml"},
			},
		},
	}

	data, err := yaml.Marshal(seq)
	if err != nil {
		return fmt.Errorf("marshal bootstrap sequence: %w", err)
	}
	outPath := filepath.Join(output, "bootstrap-sequence.yaml")
	return os.WriteFile(outPath, data, 0644)
}

// compilePackBuild implements the packbuild subcommand.
// Reads a PackBuildInput spec and emits a ClusterPack CR YAML.
// conductor-schema.md §6 (pack-compile), wrapper-schema.md §3.
func compilePackBuild(input, output string) error {
	in, err := readPackBuildInput(input)
	if err != nil {
		return err
	}

	ns := in.Namespace
	if ns == "" {
		ns = "seam-system"
	}

	cp := wrapperv1alpha1.ClusterPack{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infra.ontai.dev/v1alpha1",
			Kind:       "ClusterPack",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
		},
		Spec: wrapperv1alpha1.ClusterPackSpec{
			Version: in.Version,
			RegistryRef: wrapperv1alpha1.PackRegistryRef{
				URL:    in.RegistryURL,
				Digest: in.Digest,
			},
			Checksum:       in.Checksum,
			SourceBuildRef: in.SourceBuildRef,
		},
	}
	return writeCRYAML(output, in.Name, cp)
}
