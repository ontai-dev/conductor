// compile.go contains the input type definitions and CR generation logic for
// all active Compiler subcommands. The Compiler is a CR compiler: it reads
// human-authored spec files and emits Kubernetes CR YAML ready to apply to the
// management cluster. conductor-schema.md §9, platform-schema.md.
package main

import (
	"context"
	"fmt"
	"log/slog"
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

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"

	platformv1alpha1 "github.com/ontai-dev/platform/api/v1alpha1"
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
	// Optional when machineConfigPaths is provided — the compiler extracts the
	// endpoint automatically from the init node's machine config YAML.
	// +optional
	ControlPlaneEndpoint string `yaml:"controlPlaneEndpoint,omitempty"`

	// TalosVersion is the Talos OS version to target (e.g., "v1.9.3").
	// Used to select the appropriate VersionContract for config generation.
	TalosVersion string `yaml:"talosVersion"`

	// KubernetesVersion is the Kubernetes version to install (e.g., "1.32.3").
	// Optional — defaults to the highest supported version for the given TalosVersion
	// per the official Talos support matrix when absent. Set bootstrap.kubernetesVersion
	// explicitly to pin a specific patch version.
	// +optional
	KubernetesVersion string `yaml:"kubernetesVersion,omitempty"`

	// InstallerImage is the fully-qualified Talos installer image reference.
	// Defaults to ghcr.io/siderolabs/installer:{talosVersion} when empty.
	// +optional
	InstallerImage string `yaml:"installerImage,omitempty"`

	// InstallDisk is the default block device for Talos installation across
	// all nodes (e.g., "/dev/sda", "/dev/vda").
	// Optional when machineConfigPaths is provided — the compiler extracts the
	// install disk from the init node's machine config. Defaults to "/dev/sda".
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

// talosK8sVersionMatrix maps Talos minor version keys (e.g., "v1.9") to the highest
// Kubernetes patch version officially supported by that Talos release series.
// Source: https://www.talos.dev/latest/introduction/support-matrix/
// Update this map when new Talos releases extend the support matrix.
var talosK8sVersionMatrix = map[string]string{
	"v1.9": "1.32.3",
	"v1.8": "1.31.5",
	"v1.7": "1.30.9",
	"v1.6": "1.29.14",
	"v1.5": "1.28.15",
}

// defaultKubernetesVersion returns the highest Kubernetes version officially
// supported by the given Talos version per the support matrix. talosVersion must
// be "vMAJOR.MINOR.PATCH" format. Returns an error when the Talos minor version
// is absent from the matrix — in that case set bootstrap.kubernetesVersion explicitly.
func defaultKubernetesVersion(talosVersion string) (string, error) {
	bare := strings.TrimPrefix(talosVersion, "v")
	parts := strings.SplitN(bare, ".", 3)
	if len(parts) < 2 {
		return "", fmt.Errorf("invalid talosVersion %q: expected vMAJOR.MINOR.PATCH", talosVersion)
	}
	key := "v" + parts[0] + "." + parts[1]
	k8sVer, ok := talosK8sVersionMatrix[key]
	if !ok {
		return "", fmt.Errorf("no default Kubernetes version for Talos %q: set bootstrap.kubernetesVersion explicitly or add an entry to talosK8sVersionMatrix", talosVersion)
	}
	return k8sVer, nil
}

// extractEndpointFromMachineConfig parses machineConfigBytes as a Talos machine
// config and returns the cluster.controlPlane.endpoint value.
func extractEndpointFromMachineConfig(machineConfigBytes []byte) (string, error) {
	var raw map[string]interface{}
	if err := yaml.Unmarshal(machineConfigBytes, &raw); err != nil {
		return "", fmt.Errorf("parse machineconfig: %w", err)
	}
	cluster, ok := raw["cluster"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("machineconfig: missing cluster section")
	}
	cp, ok := cluster["controlPlane"].(map[string]interface{})
	if !ok {
		return "", fmt.Errorf("machineconfig: missing cluster.controlPlane section")
	}
	endpoint, ok := cp["endpoint"].(string)
	if !ok || endpoint == "" {
		return "", fmt.Errorf("machineconfig: cluster.controlPlane.endpoint is empty or missing")
	}
	return endpoint, nil
}

// extractInstallDiskFromMachineConfig parses machineConfigBytes and returns the
// machine.install.disk value. Returns empty string when absent or unparseable
// (callers should fall back to "/dev/sda").
func extractInstallDiskFromMachineConfig(machineConfigBytes []byte) string {
	var raw map[string]interface{}
	if err := yaml.Unmarshal(machineConfigBytes, &raw); err != nil {
		return ""
	}
	machine, ok := raw["machine"].(map[string]interface{})
	if !ok {
		return ""
	}
	install, ok := machine["install"].(map[string]interface{})
	if !ok {
		return ""
	}
	disk, _ := install["disk"].(string)
	return disk
}

// extractFromInitNode reads the init or first cp node entry from mcPaths, reads
// that file, and calls the supplied extractor. Returns ("", nil) when mcPaths is
// empty (callers must handle the missing case themselves).
func extractFromInitNode(mcPaths map[string]string, nodes []BootstrapNode, extractor func([]byte) (string, error)) (string, error) {
	if len(mcPaths) == 0 {
		return "", nil
	}
	for _, preferredRole := range []string{"init", "controlplane"} {
		for _, n := range nodes {
			if n.Role != preferredRole {
				continue
			}
			mcPath, ok := mcPaths[n.Hostname]
			if !ok {
				continue
			}
			mcBytes, err := os.ReadFile(mcPath)
			if err != nil {
				return "", fmt.Errorf("read machineconfig for %q from %q: %w", n.Hostname, mcPath, err)
			}
			return extractor(mcBytes)
		}
	}
	return "", nil
}

// clusterRole returns the TalosClusterRole for a ClusterInput.
// An absent or empty role defaults to management. An explicitly set role must be
// "management" or "tenant"; any other value returns an error.
// Only called on import paths -- bootstrap callers must not invoke this.
func clusterRole(in ClusterInput) (platformv1alpha1.TalosClusterRole, error) {
	switch in.Role {
	case "", "management":
		return platformv1alpha1.TalosClusterRoleManagement, nil
	case "tenant":
		return platformv1alpha1.TalosClusterRoleTenant, nil
	default:
		return "", fmt.Errorf("mode=import role must be \"management\" or \"tenant\", got %q", in.Role)
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

// CAPIControlPlaneInput holds control plane configuration within a CAPIInput.
type CAPIControlPlaneInput struct {
	// Replicas is the desired number of control plane nodes.
	Replicas int32 `yaml:"replicas,omitempty"`
	// EndpointVIP is the VIP address for the control plane endpoint. When set,
	// it is used as the cluster's control plane endpoint in CAPI objects.
	EndpointVIP string `yaml:"endpointVIP,omitempty"`
}

// CAPIWorkerInput declares a worker node pool within a CAPIInput.
type CAPIWorkerInput struct {
	// Name is the pool identifier, used as the MachineDeployment name suffix.
	Name string `yaml:"name"`
	// Replicas is the desired number of worker nodes in this pool.
	Replicas int32 `yaml:"replicas,omitempty"`
	// Machines lists the SeamInfrastructureMachine CR names pre-provisioned for
	// this pool. Maps to CAPIWorkerPool.SeamInfrastructureMachineNames.
	Machines []string `yaml:"machines,omitempty"`
}

// CAPICiliumPackRefInput is the human-authored Cilium pack reference in CAPIInput.
type CAPICiliumPackRefInput struct {
	// Name is the ClusterPack CR name for the cluster-specific Cilium pack.
	Name string `yaml:"name"`
	// Version is the ClusterPack version string.
	Version string `yaml:"version"`
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

	// ControlPlane holds control plane configuration. Required when Enabled=true.
	ControlPlane *CAPIControlPlaneInput `yaml:"controlPlane,omitempty"`

	// Workers is the list of worker node pools. Each pool maps to a
	// MachineDeployment and a set of pre-provisioned SeamInfrastructureMachine CRs.
	Workers []CAPIWorkerInput `yaml:"workers,omitempty"`

	// CiliumPackRef references the cluster-specific Cilium ClusterPack.
	// Required when Enabled=true. Applied as the first pack after the CAPI cluster
	// reaches Running state. platform-schema.md §2.3.
	CiliumPackRef *CAPICiliumPackRefInput `yaml:"ciliumPackRef,omitempty"`
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

	// RBACDigest is the OCI digest of the RBAC layer (SA, Role, CR, RB, CRB).
	// Set by the human or pipeline after running the three-layer OCI push.
	// Absent for pre-split artifacts. wrapper-schema.md §4.
	// +optional
	RBACDigest string `yaml:"rbacDigest,omitempty"`

	// ClusterScopedDigest is the OCI digest of the cluster-scoped non-RBAC layer
	// (CRDs, ValidatingWebhookConfigurations, Namespaces, etc.).
	// Set by the human or pipeline after running the three-layer OCI push.
	// Absent when the pack has no cluster-scoped resources. wrapper-schema.md §4.
	// +optional
	ClusterScopedDigest string `yaml:"clusterScopedDigest,omitempty"`

	// WorkloadDigest is the OCI digest of the workload layer (all non-RBAC, non-cluster-scoped manifests).
	// Set by the human or pipeline after running the three-layer OCI push.
	// Absent for pre-split artifacts. wrapper-schema.md §4.
	// +optional
	WorkloadDigest string `yaml:"workloadDigest,omitempty"`

	// BasePackName is the logical pack name shared across versions (e.g., "nginx-ingress").
	// When set, PackInstances are named {basePackName}-{clusterName} enabling version
	// supersession. Decision 11.
	// +optional
	BasePackName string `yaml:"basePackName,omitempty"`

	// ValuesFile is the path to the values/overlay file used during pack compilation.
	// For Helm packs: relative to the PackBuildInput file location. Merged with chart
	// defaults at render time and recorded in the output ClusterPack CR for traceability.
	// For kustomize/raw packs: the overlay or patch file used in the external build.
	// +optional
	ValuesFile string `yaml:"valuesFile,omitempty"`

	// Category declares the pack type discriminator. Accepted values: helm, kustomize, raw.
	// Required when helmSource, kustomizeSource, or rawSource is set.
	// When absent, the compiler falls back to nil-check dispatch for backward compatibility.
	// T-05, T-11.
	// +optional
	Category string `yaml:"category,omitempty"`

	// HelmSource defines the Helm chart source for automated packbuild.
	// When present, the compiler fetches, renders, and pushes the chart
	// automatically instead of requiring pre-built OCI digests.
	// conductor-schema.md §9, wrapper-schema.md §4, INV-001.
	// +optional
	HelmSource *HelmSource `yaml:"helmSource,omitempty"`

	// RawSource defines a directory of raw YAML manifest files for automated packbuild.
	// When present, the compiler reads all .yaml/.yml files from the directory,
	// splits them into RBAC, cluster-scoped, and workload OCI layers, and pushes
	// them to the registry. Mutually exclusive with HelmSource.
	// conductor-schema.md §9, wrapper-schema.md §4.
	// +optional
	RawSource *RawSource `yaml:"rawSource,omitempty"`

	// KustomizeSource defines a kustomize overlay directory for automated packbuild.
	// When present, the compiler runs krusty.Kustomizer on the directory, renders
	// all resources, splits into RBAC/cluster-scoped/workload OCI layers, and pushes
	// them to the registry. Mutually exclusive with HelmSource and RawSource.
	// conductor-schema.md §9, wrapper-schema.md §4, T-12.
	// +optional
	KustomizeSource *KustomizeSource `yaml:"kustomizeSource,omitempty"`
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
	if in.Mode == "import" && in.Role != "" && in.Role != "management" && in.Role != "tenant" {
		return ClusterInput{}, fmt.Errorf("input file %q: mode=import role must be \"management\" or \"tenant\", got %q", path, in.Role)
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
	// Category validation: when set, must be one of the known enum values. T-05, T-11.
	if in.Category != "" {
		switch in.Category {
		case "helm", "kustomize", "raw":
		default:
			return PackBuildInput{}, fmt.Errorf("input file %q: category must be helm, kustomize, or raw; got %q", path, in.Category)
		}
		// Cross-contamination check: source field must match declared category.
		if in.Category == "helm" && in.HelmSource == nil {
			return PackBuildInput{}, fmt.Errorf("input file %q: category=helm requires helmSource to be set", path)
		}
		if in.Category == "raw" && in.RawSource == nil {
			return PackBuildInput{}, fmt.Errorf("input file %q: category=raw requires rawSource to be set", path)
		}
		if in.Category == "kustomize" && in.KustomizeSource == nil {
			return PackBuildInput{}, fmt.Errorf("input file %q: category=kustomize requires kustomizeSource to be set", path)
		}
		if in.Category != "helm" && in.HelmSource != nil {
			return PackBuildInput{}, fmt.Errorf("input file %q: helmSource is set but category=%q; remove helmSource or set category=helm", path, in.Category)
		}
		if in.Category != "raw" && in.RawSource != nil {
			return PackBuildInput{}, fmt.Errorf("input file %q: rawSource is set but category=%q; remove rawSource or set category=raw", path, in.Category)
		}
		if in.Category != "kustomize" && in.KustomizeSource != nil {
			return PackBuildInput{}, fmt.Errorf("input file %q: kustomizeSource is set but category=%q; remove kustomizeSource or set category=kustomize", path, in.Category)
		}
	}

	// helmSource path: registryUrl required, digests computed by compiler.
	if in.HelmSource != nil {
		if in.RegistryURL == "" {
			return PackBuildInput{}, fmt.Errorf("input file %q: registryUrl is required when helmSource is set", path)
		}
		return in, nil
	}
	// rawSource path: registryUrl required, digests computed by compiler.
	if in.RawSource != nil {
		if in.RegistryURL == "" {
			return PackBuildInput{}, fmt.Errorf("input file %q: registryUrl is required when rawSource is set", path)
		}
		return in, nil
	}
	// kustomizeSource path: registryUrl required, digests computed by compiler.
	if in.KustomizeSource != nil {
		if in.RegistryURL == "" {
			return PackBuildInput{}, fmt.Errorf("input file %q: registryUrl is required when kustomizeSource is set", path)
		}
		return in, nil
	}
	if in.RegistryURL == "" {
		return PackBuildInput{}, fmt.Errorf("input file %q: registryUrl is required", path)
	}
	hasSplitDigests := in.RBACDigest != "" && in.WorkloadDigest != ""
	if in.Digest == "" && !hasSplitDigests {
		return PackBuildInput{}, fmt.Errorf("input file %q: digest is required (or both rbacDigest and workloadDigest for two-layer packs)", path)
	}
	return in, nil
}

// writeSeamTenantNamespaceManifest writes a Namespace manifest for
// seam-tenant-{clusterName} to the output directory. The manifest carries the
// ontai.dev/tenant and ontai.dev/cluster labels required by the namespace model.
// For mode=import clusters: the bootstrap bundle must include this manifest so
// the admin can apply it before the Secrets (which live in seam-tenant-{cluster}).
// For mode=bootstrap/CAPI: Platform creates the namespace via ensureTenantNamespace.
// Returns the filename. Governor ruling 2026-04-21 (session/13-namespace-model-fix).
func writeSeamTenantNamespaceManifest(clusterName, output string) (string, error) {
	ns := corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "seam-tenant-" + clusterName,
			Labels: map[string]string{
				"ontai.dev/tenant":  "true",
				"ontai.dev/cluster": clusterName,
			},
		},
	}
	const filename = "seam-tenant-namespace.yaml"
	if err := writeCRYAML(output, "seam-tenant-namespace", ns); err != nil {
		return "", fmt.Errorf("write seam-tenant namespace manifest: %w", err)
	}
	return filename, nil
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

// buildCAPIConfig maps a CAPIInput to a platformv1alpha1.CAPIConfig pointer.
// Returns a fully populated CAPIConfig with all provided fields set.
// The caller is responsible for only calling this when CAPIInput.Enabled=true.
func buildCAPIConfig(c CAPIInput) *platformv1alpha1.CAPIConfig {
	cfg := &platformv1alpha1.CAPIConfig{
		Enabled:           true,
		TalosVersion:      c.TalosVersion,
		KubernetesVersion: c.KubernetesVersion,
	}
	if c.ControlPlane != nil {
		cfg.ControlPlane = &platformv1alpha1.CAPIControlPlaneConfig{
			Replicas: c.ControlPlane.Replicas,
		}
	}
	for _, w := range c.Workers {
		cfg.Workers = append(cfg.Workers, platformv1alpha1.CAPIWorkerPool{
			Name:                           w.Name,
			Replicas:                       w.Replicas,
			SeamInfrastructureMachineNames: w.Machines,
		})
	}
	if c.CiliumPackRef != nil {
		cfg.CiliumPackRef = &platformv1alpha1.CAPICiliumPackRef{
			Name:    c.CiliumPackRef.Name,
			Version: c.CiliumPackRef.Version,
		}
	}
	return cfg
}

// validateBootstrapInput checks required fields in the BootstrapSection.
// controlPlaneEndpoint, kubernetesVersion, and installDisk are optional:
// they are resolved after validation via machineConfigPaths extraction or
// the Talos support matrix.
func validateBootstrapInput(b *BootstrapSection) error {
	if b == nil {
		return fmt.Errorf("bootstrap section is required for the bootstrap subcommand")
	}
	if b.TalosVersion == "" {
		return fmt.Errorf("bootstrap.talosVersion is required")
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

	// Resolve controlPlaneEndpoint: explicit > extracted from machineConfigPaths.
	controlPlaneEndpoint := b.ControlPlaneEndpoint
	if controlPlaneEndpoint == "" {
		ep, err := extractFromInitNode(in.MachineConfigPaths, b.Nodes, extractEndpointFromMachineConfig)
		if err != nil {
			return fmt.Errorf("input %q: extract controlPlaneEndpoint: %w", input, err)
		}
		if ep == "" {
			return fmt.Errorf("input %q: bootstrap.controlPlaneEndpoint is required when machineConfigPaths is not provided", input)
		}
		controlPlaneEndpoint = ep
	}

	// Resolve kubernetesVersion: explicit > support matrix.
	kubernetesVersion := b.KubernetesVersion
	if kubernetesVersion == "" {
		k8sVer, err := defaultKubernetesVersion(b.TalosVersion)
		if err != nil {
			return fmt.Errorf("input %q: resolve kubernetesVersion: %w", input, err)
		}
		kubernetesVersion = k8sVer
	}

	// Resolve installDisk: explicit > extracted from machineConfigPaths > default.
	installDisk := b.InstallDisk
	if installDisk == "" {
		extracted, err := extractFromInitNode(in.MachineConfigPaths, b.Nodes,
			func(mcBytes []byte) (string, error) {
				return extractInstallDiskFromMachineConfig(mcBytes), nil
			})
		if err != nil {
			return fmt.Errorf("input %q: extract installDisk: %w", input, err)
		}
		if extracted != "" {
			installDisk = extracted
		} else {
			installDisk = "/dev/sda"
		}
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
		controlPlaneEndpoint,
		kubernetesVersion,
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
		// Machine config secrets always live in seam-tenant-{cluster}, not in the
		// TalosCluster CR namespace (seam-system). Platform reads them from there.
		bareHostname := strings.TrimPrefix(node.Hostname, in.Name+"-")
		secretName := "seam-mc-" + in.Name + "-" + bareHostname
		secret := corev1.Secret{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "Secret",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      secretName,
				Namespace: "seam-tenant-" + in.Name,
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

	// C-35: When importExistingCluster=true, also emit the talosconfig Secret so
	// Platform can generate the kubeconfig via ensureKubeconfigSecret. Applies to
	// both the machineConfigPaths path (local file PKI) and the Kubernetes API path
	// (Seam clusters). Failure is a warning -- the operator can apply manually.
	// Also emit the seam-tenant namespace manifest so the admin can apply it before
	// the Secrets (which live in seam-tenant-{cluster}). platform-schema.md §9.
	if in.ImportExistingCluster {
		nsFile, err := writeSeamTenantNamespaceManifest(in.Name, output)
		if err != nil {
			return err
		}
		secretNames = append([]string{nsFile}, secretNames...)
		if tcfgFile, err := writeTalosconfigSecret(in, talosconfigPath, output); err != nil {
			return err
		} else if tcfgFile != "" {
			secretNames = append(secretNames, tcfgFile)
		}
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
	//
	// Role is set when: (a) import path -- clusterRole defaults empty to management;
	// (b) bootstrap path with explicit role field (e.g. role: tenant in fixture).
	// Bootstrap paths with no role field omit the field (omitempty suppresses "").
	tcSpec := platformv1alpha1.TalosClusterSpec{
		Mode:              tcMode,
		TalosVersion:      b.TalosVersion,
		KubernetesVersion: kubernetesVersion,
		ClusterEndpoint:   stripScheme(controlPlaneEndpoint),
	}
	if in.ImportExistingCluster || in.Role != "" {
		role, err := clusterRole(in)
		if err != nil {
			return fmt.Errorf("compileBootstrap: %w", err)
		}
		tcSpec.Role = role
	}
	if in.CAPI.Enabled {
		// CAPI target cluster: populate the full CAPI block using buildCAPIConfig.
		// CAPI nil when disabled -- pointer suppresses the capi block from YAML (C-34).
		tcSpec.CAPI = buildCAPIConfig(in.CAPI)
	}
	tc := platformv1alpha1.TalosCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infrastructure.ontai.dev/v1alpha1",
			Kind:       "InfrastructureTalosCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
			Annotations: map[string]string{
				"ontai.dev/owns-runnerconfig": "true",
			},
		},
		Spec: tcSpec,
	}
	if err := writeCRYAML(output, in.Name, tc); err != nil {
		return fmt.Errorf("write TalosCluster CR: %w", err)
	}

	// Produce bootstrap-sequence.yaml documenting the apply order.
	return writeBootstrapSequence(output, in.Name, secretNames, tcMode)
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

// writeTalosconfigSecret reads the talosconfig from talosconfigPath (falling back to
// TALOSCONFIG env var and ~/.talos/config) and writes seam-mc-{cluster}-talosconfig.yaml
// to the output directory. Returns the filename ("seam-mc-{cluster}-talosconfig.yaml")
// on success so callers can add it to secretNames. Returns ("", nil) if the talosconfig
// cannot be read or written — failure is a warning, not fatal; the operator can apply
// the Secret manually. platform-schema.md §9, conductor-schema.md §9.
func writeTalosconfigSecret(in ClusterInput, talosconfigPath, output string) (string, error) {
	// talosconfig Secret lives in seam-tenant-{cluster} per the namespace model.
	// Governor ruling 2026-04-21: seam-tenant-{cluster} holds talosconfig Secret.
	ns := "seam-tenant-" + in.Name

	tcfgPath := talosconfigPath
	if tcfgPath == "" {
		tcfgPath = os.Getenv("TALOSCONFIG")
	}
	if tcfgPath == "" {
		if home, err := os.UserHomeDir(); err == nil {
			tcfgPath = filepath.Join(home, ".talos", "config")
		}
	}
	if tcfgPath == "" {
		slog.Warn("writeTalosconfigSecret: cannot resolve talosconfig path — Secret will not be emitted; apply manually",
			"cluster", in.Name)
		return "", nil
	}

	tcfgData, err := os.ReadFile(tcfgPath)
	if err != nil {
		slog.Warn("writeTalosconfigSecret: cannot read talosconfig — Secret will not be emitted; apply manually",
			"path", tcfgPath, "error", err)
		return "", nil
	}

	secretName := "seam-mc-" + in.Name + "-talosconfig"
	secret := corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: ns,
			Labels: map[string]string{
				"ontai.dev/cluster":    in.Name,
				"ontai.dev/managed-by": "compiler",
			},
		},
		Type:       corev1.SecretTypeOpaque,
		StringData: map[string]string{"talosconfig": string(tcfgData)},
	}
	if err := writeCRYAML(output, secretName, secret); err != nil {
		slog.Warn("writeTalosconfigSecret: cannot write talosconfig Secret YAML — apply manually", "error", err)
		return "", nil
	}
	return secretName + ".yaml", nil
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

// writeBootstrapSequence writes bootstrap-sequence.yaml as a Kubernetes ConfigMap.
//
// The ConfigMap wraps the BootstrapSequence so that `kubectl apply -f compiled/bootstrap/`
// succeeds without errors — `kind: BootstrapSequence` is not a CRD and kubectl would
// reject it. The sequence content is stored in ConfigMap.data["sequence.yaml"] and can
// be inspected with: kubectl get cm seam-bootstrap-sequence-{cluster} -n seam-system -o yaml
//
// The mode parameter controls step descriptions:
//   - bootstrap: standard fresh-PKI path; step 1 lists machineconfig Secrets only.
//   - import: existing cluster path; step 1 lists machineconfig and talosconfig Secrets;
//     step 2 reminds the operator that the talosconfig Secret must be applied first.
//
// C-36: previously used kind: BootstrapSequence (not a valid CRD). platform-schema.md §9.
func writeBootstrapSequence(output, clusterName string, secretFiles []string, mode platformv1alpha1.TalosClusterMode) error {
	step1Desc := "Apply Talos machineconfig Secrets — one per node. " +
		"Apply ALL before the TalosCluster CR."
	step2Desc := "Apply TalosCluster CR with mode=bootstrap and capi.enabled=false. " +
		"Platform's TalosClusterReconciler watches this CR and submits the bootstrap Conductor Job."

	if mode == platformv1alpha1.TalosClusterModeImport {
		step1Desc = "Apply ALL Secrets: machineconfig Secrets (one per node) AND the talosconfig Secret " +
			"(seam-mc-" + clusterName + "-talosconfig.yaml). " +
			"The talosconfig Secret is required for Platform to generate the kubeconfig. " +
			"Apply ALL before TalosCluster CR."
		step2Desc = "Apply TalosCluster CR with mode=import. " +
			"Apply AFTER all Secrets in step 1 are present in the cluster — " +
			"Platform reads the talosconfig Secret during TalosCluster reconciliation " +
			"to generate and store the cluster kubeconfig."
	}

	seq := BootstrapSequence{
		APIVersion:  "ontai.dev/v1alpha1",
		Kind:        "BootstrapSequence",
		ClusterName: clusterName,
		Steps: []BootstrapSequenceStep{
			{
				Step:        1,
				Description: step1Desc,
				Resources:   secretFiles,
			},
			{
				Step:        2,
				Description: step2Desc,
				Resources:   []string{clusterName + ".yaml"},
			},
		},
	}

	seqData, err := yaml.Marshal(seq)
	if err != nil {
		return fmt.Errorf("marshal bootstrap sequence: %w", err)
	}

	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "seam-bootstrap-sequence-" + clusterName,
			Namespace: "seam-system",
			Labels: map[string]string{
				"ontai.dev/managed-by": "compiler",
				"ontai.dev/cluster":    clusterName,
			},
		},
		Data: map[string]string{
			"sequence.yaml": string(seqData),
		},
	}

	cmData, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal bootstrap-sequence ConfigMap: %w", err)
	}
	outPath := filepath.Join(output, "bootstrap-sequence.yaml")
	return os.WriteFile(outPath, cmData, 0644)
}

// compilePackBuild implements the packbuild subcommand.
// Reads a PackBuildInput spec and emits a ClusterPack CR YAML.
// Dispatch is category-driven when category is set; falls back to source nil-check
// for backward compatibility when category is absent. T-05, T-13.
// conductor-schema.md §6 (pack-compile), wrapper-schema.md §3.
func compilePackBuild(input, output string) error {
	in, err := readPackBuildInput(input)
	if err != nil {
		return err
	}

	// Category-driven dispatch (T-13): explicit category takes precedence.
	switch in.Category {
	case "helm":
		inputDir := filepath.Dir(input)
		return helmCompilePackBuild(context.Background(), in, inputDir, output)
	case "raw":
		inputDir := filepath.Dir(input)
		return rawCompilePackBuild(context.Background(), in, inputDir, output)
	case "kustomize":
		inputDir := filepath.Dir(input)
		return kustomizeCompilePackBuild(context.Background(), in, inputDir, output)
	}

	// Backward-compatible nil-check dispatch when category is absent.
	if in.HelmSource != nil {
		inputDir := filepath.Dir(input)
		return helmCompilePackBuild(context.Background(), in, inputDir, output)
	}

	if in.RawSource != nil {
		inputDir := filepath.Dir(input)
		return rawCompilePackBuild(context.Background(), in, inputDir, output)
	}

	if in.KustomizeSource != nil {
		inputDir := filepath.Dir(input)
		return kustomizeCompilePackBuild(context.Background(), in, inputDir, output)
	}

	ns := in.Namespace
	if ns == "" {
		ns = "seam-system"
	}

	cp := seamcorev1alpha1.InfrastructureClusterPack{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infrastructure.ontai.dev/v1alpha1",
			Kind:       "InfrastructureClusterPack",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
		},
		Spec: seamcorev1alpha1.InfrastructureClusterPackSpec{
			Version: in.Version,
			RegistryRef: seamcorev1alpha1.InfrastructurePackRegistryRef{
				URL:    in.RegistryURL,
				Digest: in.Digest,
			},
			Checksum:            in.Checksum,
			SourceBuildRef:      in.SourceBuildRef,
			TargetClusters:      in.TargetClusters,
			RBACDigest:          in.RBACDigest,
			ClusterScopedDigest: in.ClusterScopedDigest,
			WorkloadDigest:      in.WorkloadDigest,
			BasePackName:        in.BasePackName,
			ValuesFile:          in.ValuesFile,
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

	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("compileImportTalosconfigSecret: create output dir: %w", err)
	}

	// Emit the seam-tenant namespace manifest so the admin can apply it before the
	// talosconfig Secret (which lives in seam-tenant-{cluster}). For mode=import,
	// the bootstrap bundle must include this manifest for one-shot kubectl apply.
	// Governor ruling 2026-04-21 (session/13-namespace-model-fix). platform-schema.md §9.
	if _, err := writeSeamTenantNamespaceManifest(in.Name, output); err != nil {
		return err
	}

	// talosconfig Secret lives in seam-tenant-{cluster} per the namespace model.
	// Governor ruling 2026-04-21: seam-tenant-{cluster} holds talosconfig Secret.
	tenantNS := "seam-tenant-" + in.Name
	// TalosCluster CR namespace is seam-system (operator-facing CR, not tenant-scoped).
	crNS := in.Namespace
	if crNS == "" {
		crNS = "seam-system"
	}

	secretName := "seam-mc-" + in.Name + "-talosconfig"
	secret := corev1.Secret{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Secret",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: tenantNS,
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

	if err := writeCRYAML(output, secretName, secret); err != nil {
		return fmt.Errorf("compileImportTalosconfigSecret: write talosconfig secret: %w", err)
	}

	// Emit TalosCluster CR with mode=import so the operator can adopt the cluster
	// without a bootstrap Job. conductor-schema.md §9.
	var talosVersion, clusterEndpoint, kubernetesVersion string
	if in.Bootstrap != nil {
		talosVersion = in.Bootstrap.TalosVersion
		clusterEndpoint = stripScheme(in.Bootstrap.ControlPlaneEndpoint)
		kubernetesVersion = in.Bootstrap.KubernetesVersion
		if kubernetesVersion == "" && talosVersion != "" {
			kubernetesVersion, _ = defaultKubernetesVersion(talosVersion)
		}
	}
	role, err := clusterRole(in)
	if err != nil {
		return fmt.Errorf("compileImportTalosconfigSecret: %w", err)
	}
	tc := platformv1alpha1.TalosCluster{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infrastructure.ontai.dev/v1alpha1",
			Kind:       "InfrastructureTalosCluster",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: crNS,
			Annotations: map[string]string{
				"ontai.dev/owns-runnerconfig": "true",
			},
		},
		Spec: platformv1alpha1.TalosClusterSpec{
			Mode:              platformv1alpha1.TalosClusterModeImport,
			Role:              role,
			TalosVersion:      talosVersion,
			KubernetesVersion: kubernetesVersion,
			ClusterEndpoint:   clusterEndpoint,
			// CAPI nil -- management cluster import path; nil suppresses capi block (C-34).
		},
	}
	return writeCRYAML(output, in.Name, tc)
}
