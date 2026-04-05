// compile.go contains the input type definitions and CR generation logic for
// all active Compiler subcommands. The Compiler is a CR compiler: it reads
// human-authored spec files and emits Kubernetes CR YAML ready to apply to the
// management cluster. conductor-schema.md §9, platform-schema.md.
package main

import (
	"fmt"
	"os"
	"path/filepath"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	platformv1alpha1 "github.com/ontai-dev/platform/api/v1alpha1"
	wrapperv1alpha1 "github.com/ontai-dev/wrapper/api/v1alpha1"
)

// ClusterInput is the human-authored spec format for bootstrap, launch, and enable
// subcommands. Humans write this file; Compiler reads it and emits a TalosCluster CR.
// Fields map directly to TalosClusterSpec. conductor-schema.md §9.
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
// Common logic shared by bootstrap, launch, and enable subcommands.
func buildTalosCluster(in ClusterInput) platformv1alpha1.TalosCluster {
	ns := in.Namespace
	if ns == "" {
		ns = "seam-system"
	}
	mode := platformv1alpha1.TalosClusterMode(in.Mode)
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
	return tc
}

// compileBootstrap implements the bootstrap subcommand.
// Reads a ClusterInput spec and emits a TalosCluster CR YAML in bootstrap mode.
// The bootstrap phase provisions tenant cluster agents and is declared in
// RunnerConfig.phases. conductor-schema.md §9, platform-schema.md.
func compileBootstrap(input, output string) error {
	in, err := readClusterInput(input)
	if err != nil {
		return err
	}
	// Bootstrap phase: target cluster (capi.enabled=true) or management cluster
	// (capi.enabled=false) bootstrap via Conductor Job.
	tc := buildTalosCluster(in)
	return writeCRYAML(output, in.Name, tc)
}

// compileLaunch implements the launch subcommand.
// Reads a ClusterInput spec and emits a TalosCluster CR YAML for the launch phase.
// The launch phase is declared in RunnerConfig.phases for both management and
// tenant clusters. conductor-schema.md §9, platform-schema.md.
func compileLaunch(input, output string) error {
	in, err := readClusterInput(input)
	if err != nil {
		return err
	}
	tc := buildTalosCluster(in)
	return writeCRYAML(output, in.Name, tc)
}

// compileEnable implements the enable subcommand.
// Reads a ClusterInput spec and emits a TalosCluster CR YAML for the enable phase.
// The enable phase installs operators on the management cluster and is declared in
// RunnerConfig.phases for management clusters only (capi.enabled=false).
// conductor-schema.md §9, platform-schema.md.
func compileEnable(input, output string) error {
	in, err := readClusterInput(input)
	if err != nil {
		return err
	}
	tc := buildTalosCluster(in)
	return writeCRYAML(output, in.Name, tc)
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
