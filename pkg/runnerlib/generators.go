package runnerlib

import seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"

// TalosClusterSpec is the minimal representation of a TalosCluster CR spec used
// by GenerateFromTalosCluster to produce a RunnerConfigSpec. Fields derived from
// platform-schema.md Section 2 (TalosCluster key spec fields).
//
// This type exists in the shared library to decouple the generator from the
// platform CRD Go types. Operators populate this from their CRD types and
// pass it to the generator.
type TalosClusterSpec struct {
	// ClusterEndpoint is the VIP or first control plane IP. Embedded in all
	// generated configs. Used as the cluster identity in RunnerConfigSpec.ClusterRef.
	//
	// TODO: a formal cluster name field should be added to TalosCluster when the
	// Schema Engineer defines the CRD API types. ClusterEndpoint is used as cluster
	// identity here as an interim measure. platform-schema.md §2 TalosCluster.
	ClusterEndpoint string

	// TalosVersion is the Talos OS version. Determines runner compatibility.
	// INV-012: the runner image tag must be compatible with this version.
	TalosVersion string

	// KubernetesVersion is the Kubernetes version to pin on this cluster.
	KubernetesVersion string

	// InstallDisk is the block device path for Talos installation.
	InstallDisk string

	// ControlPlaneNodes is the list of control plane node IPs.
	ControlPlaneNodes []string

	// WorkerNodes is the list of worker node IPs. May be empty for control-plane-only
	// clusters.
	WorkerNodes []string

	// SeedNodes is the list of node IPs reachable on port 50000 before config is
	// applied. Used by the bootstrap capability to apply initial machine configuration.
	SeedNodes []string
}

// PackBuildSpec is the minimal representation of a PackBuild CR spec used by
// GenerateFromPackBuild to produce a RunnerConfigSpec. Fields derived from
// wrapper-schema.md Section 3 (PackBuild key spec fields).
//
// This type exists in the shared library to decouple the generator from the
// wrapper CRD Go types. Operators populate this from their CRD types and pass
// it to the generator.
type PackBuildSpec struct {
	// SourceHelm is the Helm chart source configuration. Nil if not a Helm-based pack.
	SourceHelm *HelmSource

	// SourceKustomize is the Kustomize overlay source. Nil if not Kustomize-based.
	SourceKustomize *KustomizeSource

	// SourceRaw is the list of raw manifest references. May be empty.
	SourceRaw []RawManifestSource

	// TargetVersion is the version string to assign to the produced ClusterPack.
	TargetVersion string
}

// HelmSource describes a Helm chart source for a PackBuild.
// wrapper-schema.md §3 PackBuild source.helm fields.
type HelmSource struct {
	// RepoURL is the Helm chart repository URL.
	RepoURL string

	// ChartName is the name of the chart within the repository.
	ChartName string

	// ChartVersion is the pinned chart version to render.
	ChartVersion string

	// Values is the structured values map passed to Helm at render time.
	// Corresponds to values.yaml overrides declared in the PackBuild spec.
	Values map[string]interface{}
}

// KustomizeSource describes a Kustomize overlay source for a PackBuild.
// wrapper-schema.md §3 PackBuild source.kustomize fields.
type KustomizeSource struct {
	// OverlayPath is the path reference to the Kustomize overlay directory.
	OverlayPath string
}

// RawManifestSource describes a single raw manifest reference for a PackBuild.
// wrapper-schema.md §3 PackBuild source.raw fields.
type RawManifestSource struct {
	// Path is the path reference to the raw Kubernetes manifest file.
	Path string
}

// GenerateFromTalosCluster produces a RunnerConfigSpec from a TalosClusterSpec.
// Called by platform when a TalosCluster CR lands on the management cluster.
// The returned RunnerConfigSpec has ClusterRef populated from the spec endpoint,
// RunnerImage left empty (the operator sets this from the cluster's desired runner
// version), and Phases initialized with a "launch" phase.
//
// INV-009: RunnerConfig is operator-generated at runtime. This function is the
// generation path. INV-010: this shared library is the single source.
func GenerateFromTalosCluster(spec TalosClusterSpec) (seamcorev1alpha1.InfrastructureRunnerConfigSpec, error) {
	return seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		// TODO: replace ClusterEndpoint-as-identity with a formal cluster name field
		// once TalosCluster CRD API types are defined by the Schema Engineer.
		// See platform-schema.md §2 TalosCluster.
		ClusterRef:  spec.ClusterEndpoint,
		RunnerImage: "", // Caller must set this from the cluster's desired runner version.
		Phases: []seamcorev1alpha1.RunnerPhaseConfig{
			{
				Name:       "launch",
				Parameters: map[string]string{},
			},
		},
		OperationalHistory: []seamcorev1alpha1.RunnerOperationalHistoryEntry{},
	}, nil
}

// GenerateFromPackBuild produces a RunnerConfigSpec from a PackBuildSpec.
// Called by wrapper when a PackBuild CR lands on the management cluster.
// Pack compilation is not cluster-specific — ClusterRef is empty. RunnerImage
// must be set by the caller.
//
// INV-009: RunnerConfig is operator-generated at runtime. INV-010: this shared
// library is the single source.
func GenerateFromPackBuild(spec PackBuildSpec) (seamcorev1alpha1.InfrastructureRunnerConfigSpec, error) {
	return seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		ClusterRef:  "", // Pack compilation is not cluster-specific.
		RunnerImage: "", // Caller must set this.
		Phases: []seamcorev1alpha1.RunnerPhaseConfig{
			{
				Name:       "compile",
				Parameters: map[string]string{},
			},
		},
		OperationalHistory: []seamcorev1alpha1.RunnerOperationalHistoryEntry{},
	}, nil
}
