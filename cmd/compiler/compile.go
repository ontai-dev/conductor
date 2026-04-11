// compile.go contains the input type definitions and CR generation logic for
// all active Compiler subcommands. The Compiler is a CR compiler: it reads
// human-authored spec files and emits Kubernetes CR YAML ready to apply to the
// management cluster. conductor-schema.md §9, platform-schema.md.
package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	talosconfig "github.com/siderolabs/talos/pkg/machinery/config"
	"github.com/siderolabs/talos/pkg/machinery/config/configloader"
	"github.com/siderolabs/talos/pkg/machinery/config/generate"
	"github.com/siderolabs/talos/pkg/machinery/config/generate/secrets"
	"github.com/siderolabs/talos/pkg/machinery/config/machine"

	platformv1alpha1 "github.com/ontai-dev/platform/api/v1alpha1"
	wrapperv1alpha1 "github.com/ontai-dev/wrapper/api/v1alpha1"
)

// RegistryMirror configures one registry mirror entry in Talos machine config
// registries.mirrors. The Registry field is the upstream registry hostname;
// Endpoints is the list of mirror URLs. The http:// prefix is preserved exactly
// as provided — do not add TLS config for http:// endpoints.
// lab/CLAUDE.md §Cilium, Talos machine config registries.mirrors spec.
type RegistryMirror struct {
	// Registry is the upstream registry hostname (e.g., "docker.io", "ghcr.io").
	Registry string `yaml:"registry"`
	// Endpoints is the list of mirror endpoint URLs.
	// The http:// prefix is preserved exactly — do not add insecureSkipVerify.
	Endpoints []string `yaml:"endpoints"`
}

// ciliumPrerequisitesPatch returns the Talos machine config YAML patch that enables
// Cilium native routing. Applied when ClusterInput.CiliumPrerequisites=true.
//
// Required kernel modules:
//   - br_netfilter: bridge traffic must traverse netfilter rules for eBPF hooks.
//   - xt_socket:    required for transparent proxy / socket-based load balancing.
//
// Required sysctls:
//   - net.ipv4.conf.all.rp_filter=0:     disable reverse-path filtering on all ifaces.
//   - net.ipv4.conf.default.rp_filter=0: disable rp_filter for newly created ifaces.
//
// Without these, Cilium installs but pod-to-pod routing silently drops packets.
// lab/CLAUDE.md §Cilium Deployment Invariants, compile.go ClusterInput comment.
func ciliumPrerequisitesPatch() string {
	return `machine:
  kernel:
    modules:
      - name: br_netfilter
      - name: xt_socket
  sysctls:
    net.ipv4.conf.all.rp_filter: "0"
    net.ipv4.conf.default.rp_filter: "0"
`
}

// buildRegistryMirrorsPatch builds the Talos machine config YAML patch that injects
// the provided registry mirrors into machine.registries.mirrors. The http:// prefix
// on endpoints is preserved exactly — no TLS config is added.
func buildRegistryMirrorsPatch(mirrors []RegistryMirror) (string, error) {
	type mirrorSpec struct {
		Endpoints []string `yaml:"endpoints"`
	}
	type registriesSpec struct {
		Mirrors map[string]mirrorSpec `yaml:"mirrors"`
	}
	type machineSpec struct {
		Registries registriesSpec `yaml:"registries"`
	}
	type patchSpec struct {
		Machine machineSpec `yaml:"machine"`
	}

	mirrorMap := make(map[string]mirrorSpec, len(mirrors))
	for _, m := range mirrors {
		mirrorMap[m.Registry] = mirrorSpec{Endpoints: m.Endpoints}
	}
	data, err := yaml.Marshal(patchSpec{
		Machine: machineSpec{
			Registries: registriesSpec{Mirrors: mirrorMap},
		},
	})
	if err != nil {
		return "", fmt.Errorf("marshal registry mirrors patch: %w", err)
	}
	return string(data), nil
}

// deepMerge recursively merges src into dst in-place.
//
//   - Map values are merged recursively.
//   - Slice values are appended (useful for kernel modules, DNS search lists, etc.).
//   - Scalar values override the dst value.
func deepMerge(dst, src map[string]interface{}) {
	for k, srcVal := range src {
		switch sv := srcVal.(type) {
		case map[string]interface{}:
			if dv, ok := dst[k].(map[string]interface{}); ok {
				deepMerge(dv, sv)
			} else {
				dst[k] = srcVal
			}
		case []interface{}:
			if dv, ok := dst[k].([]interface{}); ok {
				dst[k] = append(dv, sv...)
			} else {
				dst[k] = srcVal
			}
		default:
			dst[k] = srcVal
		}
	}
}

// applyYAMLPatch applies a YAML deep-merge patch to configBytes and returns the
// merged YAML. Maps are merged recursively; slices are appended; scalars override.
// Used to inject kernel modules, sysctls, registry mirrors, and custom patches
// into Talos machine configs without discarding existing fields.
func applyYAMLPatch(configBytes []byte, patch string) ([]byte, error) {
	var base map[string]interface{}
	if err := yaml.Unmarshal(configBytes, &base); err != nil {
		return nil, fmt.Errorf("unmarshal base config: %w", err)
	}
	var overlay map[string]interface{}
	if err := yaml.Unmarshal([]byte(patch), &overlay); err != nil {
		return nil, fmt.Errorf("unmarshal patch: %w", err)
	}
	if base == nil {
		base = make(map[string]interface{})
	}
	deepMerge(base, overlay)
	result, err := yaml.Marshal(base)
	if err != nil {
		return nil, fmt.Errorf("marshal patched config: %w", err)
	}
	return result, nil
}


// extractCAFromMachineConfig parses machineConfigBytes as a Talos machine config
// and derives a secrets.Bundle from the existing CA material. Used by both the
// local file path and the Kubernetes Secret path when importExistingCluster=true.
//
// Returns an error if parsing fails or the derived bundle fails validation.
// Validation failure indicates that the machine config lacks required PKI fields
// (e.g., machine.security.ca, cluster.ca, cluster.etcd.ca) — this would happen
// if the bytes are a worker config or a pre-PKI stub, not a control-plane config.
func extractCAFromMachineConfig(machineConfigBytes []byte) (*secrets.Bundle, error) {
	provider, err := configloader.NewFromBytes(machineConfigBytes)
	if err != nil {
		return nil, fmt.Errorf("parse machineconfig: %w", err)
	}
	bundle := secrets.NewBundleFromConfig(secrets.NewClock(), provider)
	if err := bundle.Validate(); err != nil {
		return nil, fmt.Errorf("secrets bundle derived from machineconfig is incomplete (init node config required): %w", err)
	}
	return bundle, nil
}

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

// stripScheme removes a URI scheme prefix (e.g. "https://") from an endpoint
// string, returning only host:port. TalosClusterSpec.ClusterEndpoint is host:port
// format only — no scheme, no trailing slash.
func stripScheme(endpoint string) string {
	if i := strings.Index(endpoint, "://"); i >= 0 {
		return endpoint[i+3:]
	}
	return endpoint
}

// clusterRole returns the TalosClusterRole for a ClusterInput. When the Role
// field is explicitly set, it is used directly. Otherwise the default is
// TalosClusterRoleManagement, which covers all bootstrap and talosconfig-only
// import scenarios. Fix 3.
func clusterRole(in ClusterInput) platformv1alpha1.TalosClusterRole {
	switch in.Role {
	case "tenant":
		return platformv1alpha1.TalosClusterRoleTenant
	default:
		return platformv1alpha1.TalosClusterRoleManagement
	}
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
// Set ciliumPrerequisites: true to have the Compiler inject these automatically,
// or include them explicitly in the patches list.
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

	// Role declares whether this cluster is the management cluster or a tenant
	// (target) cluster. Valid values: "management", "tenant". Defaults to
	// "management" when empty. Fix 3.
	// +optional
	Role string `yaml:"role,omitempty"`

	// CAPI holds CAPI-specific configuration. Set enabled=true for target clusters,
	// false for the management cluster. platform-schema.md §5.
	CAPI CAPIInput `yaml:"capi"`

	// Bootstrap holds management cluster bootstrap configuration. Required for
	// the compiler bootstrap subcommand.
	// +optional
	Bootstrap *BootstrapSection `yaml:"bootstrap,omitempty"`

	// Patches is an optional list of Talos machine config patch YAML strings.
	// Each patch is applied to every node's generated machine config in order,
	// after CiliumPrerequisites and RegistryMirrors patches (if set). Patches use
	// deep-merge semantics: maps are merged recursively, slices are appended,
	// scalars override. Use for kernel modules, sysctls, and any other node-level
	// customization that must be present at first boot.
	// +optional
	Patches []string `yaml:"patches,omitempty"`

	// ImportExistingCluster, when true, extracts PKI material from the running
	// cluster rather than generating a fresh PKI bundle. Two extraction paths exist:
	//
	//   machineConfigPaths non-empty — local file path (pre-Seam clusters):
	//     Reads CA material from local Talos machine config YAML files. The init
	//     node entry must be present in the map. Use this for clusters bootstrapped
	//     before Seam, where no seam-mc Secrets exist in the management cluster.
	//
	//   machineConfigPaths absent or empty — Kubernetes API path (Seam clusters):
	//     Connects to the management cluster API (kubeconfig resolved via
	//     --kubeconfig flag → KUBECONFIG env → ~/.kube/config), reads the init-node
	//     Secret seam-mc-{cluster-name}-{init-node} from seam-system, and extracts
	//     CA material. Use for clusters previously bootstrapped via compiler bootstrap.
	//
	// In both paths, the same CA material extraction logic is used; only the source
	// of the machine config bytes differs.
	// +optional
	ImportExistingCluster bool `yaml:"importExistingCluster,omitempty"`

	// MachineConfigPaths is an optional map from node hostname to local file path of
	// the raw Talos machine config YAML for that node. When importExistingCluster=true
	// and this map is non-empty, Compiler reads CA material from local files instead
	// of querying the Kubernetes API. Only the init node entry is used for CA
	// extraction — the derived bundle is reused for all node configs in this run.
	//
	// The file must be the raw Talos machine config YAML (not a Kubernetes Secret).
	// If the map is non-empty but the init node hostname is absent, Compiler returns
	// an error — the init node entry is required for CA extraction.
	//
	// Example for ccs-mgmt (five-node management cluster):
	//   machineConfigPaths:
	//     ccs-mgmt-cp1: /path/to/controlplane.yaml
	//     ccs-mgmt-cp2: /path/to/controlplane.yaml
	//     ccs-mgmt-cp3: /path/to/controlplane.yaml
	//     ccs-mgmt-w1:  /path/to/worker.yaml
	//     ccs-mgmt-w2:  /path/to/worker.yaml
	//
	// Ignored when importExistingCluster=false.
	// +optional
	MachineConfigPaths map[string]string `yaml:"machineConfigPaths,omitempty"`

	// RegistryMirrors configures registry mirrors injected into every node's machine
	// config registries.mirrors section. Applied before user-provided Patches.
	// The http:// prefix on endpoints is preserved exactly — do not add TLS config.
	// Required in lab environments where containerd must use the local OCI registry.
	// +optional
	RegistryMirrors []RegistryMirror `yaml:"registryMirrors,omitempty"`

	// CiliumPrerequisites, when true, injects the kernel modules (br_netfilter,
	// xt_socket) and sysctls (net.ipv4.conf.all.rp_filter=0,
	// net.ipv4.conf.default.rp_filter=0) required for Cilium native routing on Talos.
	// Applied first, before RegistryMirrors and user Patches.
	// lab/CLAUDE.md §Cilium Deployment Invariants.
	// +optional
	CiliumPrerequisites bool `yaml:"ciliumPrerequisites,omitempty"`
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

	// TargetClusters is the list of cluster names to which this ClusterPack should
	// be delivered. Mapped directly to ClusterPackSpec.targetClusters.
	// Optional — if absent, the ClusterPack is global (all clusters on this pack).
	// wrapper-schema.md §4. WS3.
	// +optional
	TargetClusters []string `yaml:"targetClusters,omitempty"`
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
// kubeconfigPath is the optional path to a kubeconfig file, used only when
// in.ImportExistingCluster=true. Pass empty string to use the standard resolution
// chain (KUBECONFIG env → ~/.kube/config).
//
// When importExistingCluster=true, Compiler connects to the cluster Kubernetes API
// via kubeconfig, reads the init-node machine config Secret from seam-system, parses
// it, and derives the secrets bundle from existing CA material so new configs are
// signed with the same PKI. Fails fast if the kubeconfig is unreachable or the
// Secret or its machineconfig.yaml field is missing.
//
// Uses the Talos machinery library to generate machine configurations.
// No cluster connection is required in the default (fresh PKI) path.
// conductor-schema.md §9.
func compileBootstrap(input, output, kubeconfigPath, talosconfigPath string) error {
	in, err := readClusterInput(input)
	if err != nil {
		return err
	}

	// Talosconfig-only import: when importExistingCluster=true, no machineConfigPaths,
	// and no bootstrap nodes are declared, the operator only needs the talosconfig
	// Secret to connect to the cluster. Emit seam-mc-{cluster}-talosconfig.yaml and
	// return — no machineconfigs are generated and no PKI extraction is needed.
	if in.ImportExistingCluster && len(in.MachineConfigPaths) == 0 &&
		(in.Bootstrap == nil || len(in.Bootstrap.Nodes) == 0) {
		return compileImportTalosconfigSecret(in, output, talosconfigPath)
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

	// Resolve the secrets bundle. When importExistingCluster=true, extract PKI from
	// an existing cluster. Two paths are available:
	//
	//   machineConfigPaths non-empty — local file path (pre-Seam clusters):
	//     Read the init node entry from the map, load the raw machine config file,
	//     and extract CA material via extractCAFromMachineConfig.
	//
	//   machineConfigPaths absent — Kubernetes API path (Seam clusters):
	//     Connect to the cluster API via kubeconfig, read the seam-mc-{cluster}-{init}
	//     Secret from seam-system, extract machineconfig.yaml, and extract CA material.
	//
	// Both paths share extractCAFromMachineConfig for the final CA extraction step.
	var secretsBundle *secrets.Bundle
	if in.ImportExistingCluster {
		// Find the init node hostname (guaranteed present by validateBootstrapInput).
		var initHostname string
		for _, n := range b.Nodes {
			if n.Role == "init" {
				initHostname = n.Hostname
				break
			}
		}

		if len(in.MachineConfigPaths) > 0 {
			// Local file path: read CA from user-provided machine config file.
			// Only the init node entry is required; the same bundle is used for all nodes.
			mcPath, ok := in.MachineConfigPaths[initHostname]
			if !ok {
				return fmt.Errorf("importExistingCluster: machineConfigPaths is non-empty but init node %q has no entry", initHostname)
			}
			mcBytes, err := os.ReadFile(mcPath)
			if err != nil {
				return fmt.Errorf("importExistingCluster: read machineconfig for init node %q from %q: %w", initHostname, mcPath, err)
			}
			secretsBundle, err = extractCAFromMachineConfig(mcBytes)
			if err != nil {
				return fmt.Errorf("importExistingCluster: extract CA from local file %q: %w", mcPath, err)
			}
		} else {
			// Kubernetes API path: read CA from seam-mc Secret in seam-system.
			resolvedKubeconfig := resolveKubeconfigPath(kubeconfigPath)
			k8sClient, err := buildK8sClient(resolvedKubeconfig)
			if err != nil {
				return fmt.Errorf("importExistingCluster: connect to cluster via kubeconfig %q: %w", resolvedKubeconfig, err)
			}

			// Strip cluster-name prefix from hostname: Talos node names carry the
			// cluster prefix (e.g. "ccs-mgmt-cp1" for cluster "ccs-mgmt"), so the
			// Secret name would double the prefix without this strip. C-32.
			hostname := strings.TrimPrefix(initHostname, in.Name+"-")
			secretName := "seam-mc-" + in.Name + "-" + hostname
			mcSecret, err := k8sClient.CoreV1().Secrets("seam-system").Get(
				context.Background(), secretName, metav1.GetOptions{},
			)
			if err != nil {
				if apierrors.IsNotFound(err) {
					// seam-mc Secret absent — cluster was not bootstrapped via Seam.
					// Fall through to the talosconfig-only path: emit only the
					// talosconfig Secret and TalosCluster CR. No machineconfig
					// generation, no PKI extraction. C-32 Bug 2.
					return compileImportTalosconfigSecret(in, output, talosconfigPath)
				}
				return fmt.Errorf("importExistingCluster: read secret %q from seam-system: %w", secretName, err)
			}

			mcBytes, ok := mcSecret.Data["machineconfig.yaml"]
			if !ok {
				return fmt.Errorf("importExistingCluster: secret %q is missing machineconfig.yaml field", secretName)
			}

			secretsBundle, err = extractCAFromMachineConfig(mcBytes)
			if err != nil {
				return fmt.Errorf("importExistingCluster: extract CA from secret %q: %w", secretName, err)
			}
		}
	} else {
		secretsBundle, err = secrets.NewBundle(
			secrets.NewFixedClock(time.Now()),
			versionContract,
		)
		if err != nil {
			return fmt.Errorf("generate secrets bundle: %w", err)
		}
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

	// Build the ordered patch list:
	//   1. CiliumPrerequisites (built-in, applied first)
	//   2. RegistryMirrors (injected next)
	//   3. User Patches (applied last, in order)
	var patches []string
	if in.CiliumPrerequisites {
		patches = append(patches, ciliumPrerequisitesPatch())
	}
	if len(in.RegistryMirrors) > 0 {
		mirrorPatch, err := buildRegistryMirrorsPatch(in.RegistryMirrors)
		if err != nil {
			return fmt.Errorf("build registry mirrors patch: %w", err)
		}
		patches = append(patches, mirrorPatch)
	}
	patches = append(patches, in.Patches...)

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

		// Apply all patches in order (CiliumPrerequisites → RegistryMirrors → user Patches).
		for i, patch := range patches {
			cfgBytes, err = applyYAMLPatch(cfgBytes, patch)
			if err != nil {
				return fmt.Errorf("apply patch %d to node %q: %w", i, node.Hostname, err)
			}
		}

		// Strip cluster-name prefix from hostname before constructing the secret
		// name so the prefix is not doubled (e.g. ccs-mgmt-cp1 → cp1). C-32.
		bareHostname := strings.TrimPrefix(node.Hostname, in.Name+"-")
		secretName := "seam-mc-" + in.Name + "-" + bareHostname
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

	// Fix 1: importExistingCluster=true always emits mode=import. The
	// machineConfigPaths field only controls where PKI is read from, not the
	// cluster lifecycle mode. A re-imported cluster is always mode=import.
	tcMode := platformv1alpha1.TalosClusterModeBootstrap
	if in.ImportExistingCluster {
		tcMode = platformv1alpha1.TalosClusterModeImport
	}

	// Produce TalosCluster CR. ontai.dev/owns-runnerconfig signals Platform to add
	// a finalizer and clean up the RunnerConfig in ont-system on deletion. Bug 3.
	tc := platformv1alpha1.TalosCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "platform.ontai.dev/v1alpha1",
			Kind:       "TalosCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
			Annotations: map[string]string{
				"ontai.dev/owns-runnerconfig": "true",
			},
		},
		Spec: platformv1alpha1.TalosClusterSpec{
			Mode:            tcMode,
			Role:            clusterRole(in),
			TalosVersion:    b.TalosVersion,
			ClusterEndpoint: stripScheme(b.ControlPlaneEndpoint),
			CAPI:            platformv1alpha1.CAPIConfig{Enabled: false},
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
			TargetClusters: in.TargetClusters,
		},
	}
	return writeCRYAML(output, in.Name, cp)
}

// compileImportTalosconfigSecret handles the talosconfig-only import path.
// When importExistingCluster=true with no machineConfigPaths and no bootstrap nodes,
// reads the talosconfig using resolution order: flagValue → TALOSCONFIG env →
// ~/.talos/config. Emits only the seam-mc-{cluster}-talosconfig Secret and a
// TalosCluster CR. No machineconfigs are generated. conductor-schema.md §9 Step 1.
func compileImportTalosconfigSecret(in ClusterInput, output, flagValue string) error {
	talosConfigPath := flagValue
	if talosConfigPath == "" {
		talosConfigPath = os.Getenv("TALOSCONFIG")
	}
	if talosConfigPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("compileImportTalosconfigSecret: resolve home dir: %w", err)
		}
		talosConfigPath = filepath.Join(home, ".talos", "config")
	}

	data, err := os.ReadFile(talosConfigPath)
	if err != nil {
		return fmt.Errorf("compileImportTalosconfigSecret: read talosconfig %q: %w", talosConfigPath, err)
	}

	ns := in.Namespace
	if ns == "" {
		ns = "seam-system"
	}

	secretName := "seam-mc-" + in.Name + "-talosconfig"
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
				"ontai.dev/managed-by": "compiler",
			},
		},
		Type: corev1.SecretTypeOpaque,
		StringData: map[string]string{
			"talosconfig": string(data),
		},
	}

	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("compileImportTalosconfigSecret: create output dir: %w", err)
	}

	if err := writeCRYAML(output, secretName, secret); err != nil {
		return fmt.Errorf("compileImportTalosconfigSecret: write talosconfig secret: %w", err)
	}

	// Emit TalosCluster CR with mode=import so the operator can adopt the cluster
	// without a bootstrap Job. conductor-schema.md §9.
	// Fix 1: populate TalosVersion and ClusterEndpoint when bootstrap section present.
	// Fix 3: Role derived from in.Role (defaults to management).
	var talosVersion, clusterEndpoint string
	if in.Bootstrap != nil {
		talosVersion = in.Bootstrap.TalosVersion
		clusterEndpoint = stripScheme(in.Bootstrap.ControlPlaneEndpoint)
	}
	tc := platformv1alpha1.TalosCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "platform.ontai.dev/v1alpha1",
			Kind:       "TalosCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
			Annotations: map[string]string{
				"ontai.dev/owns-runnerconfig": "true",
			},
		},
		Spec: platformv1alpha1.TalosClusterSpec{
			Mode:            platformv1alpha1.TalosClusterModeImport,
			Role:            clusterRole(in),
			TalosVersion:    talosVersion,
			ClusterEndpoint: clusterEndpoint,
			CAPI:            platformv1alpha1.CAPIConfig{Enabled: false},
		},
	}
	return writeCRYAML(output, in.Name, tc)
}
