package runnerlib_test

import (
	"testing"

	"github.com/ontai-dev/ont-runner/pkg/runnerlib"
)

// TestGenerateFromTalosClusterZeroValueSpec verifies that GenerateFromTalosCluster
// with a zero-value TalosClusterSpec returns a non-nil (zero-value struct is valid)
// RunnerConfigSpec and no error. This is stub behavior — real validation is added
// when the generator implementation is completed.
func TestGenerateFromTalosClusterZeroValueSpec(t *testing.T) {
	spec, err := runnerlib.GenerateFromTalosCluster(runnerlib.TalosClusterSpec{})
	if err != nil {
		t.Fatalf("GenerateFromTalosCluster with zero-value spec returned unexpected error: %v", err)
	}

	// The returned struct must be valid (accessible without panic).
	_ = spec.ClusterRef
	_ = spec.RunnerImage
	_ = spec.LicenseSecretRef
	_ = spec.Phases
	_ = spec.OperationalHistory

	// OperationalHistory must be initialized, not nil.
	if spec.OperationalHistory == nil {
		t.Error("GenerateFromTalosCluster: OperationalHistory must be initialized (not nil)")
	}

	// Phases must be initialized, not nil.
	if spec.Phases == nil {
		t.Error("GenerateFromTalosCluster: Phases must be initialized (not nil)")
	}
}

// TestGenerateFromPackBuildZeroValueSpec verifies that GenerateFromPackBuild with
// a zero-value PackBuildSpec returns a valid RunnerConfigSpec and no error.
func TestGenerateFromPackBuildZeroValueSpec(t *testing.T) {
	spec, err := runnerlib.GenerateFromPackBuild(runnerlib.PackBuildSpec{})
	if err != nil {
		t.Fatalf("GenerateFromPackBuild with zero-value spec returned unexpected error: %v", err)
	}

	_ = spec.ClusterRef
	_ = spec.RunnerImage
	_ = spec.LicenseSecretRef
	_ = spec.Phases
	_ = spec.OperationalHistory

	if spec.OperationalHistory == nil {
		t.Error("GenerateFromPackBuild: OperationalHistory must be initialized (not nil)")
	}
	if spec.Phases == nil {
		t.Error("GenerateFromPackBuild: Phases must be initialized (not nil)")
	}
}

// TestTalosClusterSpecFieldsAccessible verifies that all TalosClusterSpec fields
// are accessible without panic on a zero value.
func TestTalosClusterSpecFieldsAccessible(t *testing.T) {
	var s runnerlib.TalosClusterSpec

	_ = s.ClusterEndpoint
	_ = s.TalosVersion
	_ = s.KubernetesVersion
	_ = s.InstallDisk
	_ = s.ControlPlaneNodes
	_ = s.WorkerNodes
	_ = s.SeedNodes

	// Nil slices are valid — range must not panic.
	for range s.ControlPlaneNodes {
	}
	for range s.WorkerNodes {
	}
	for range s.SeedNodes {
	}
}

// TestPackBuildSpecFieldsAccessible verifies that all PackBuildSpec fields are
// accessible without panic on a zero value.
func TestPackBuildSpecFieldsAccessible(t *testing.T) {
	var s runnerlib.PackBuildSpec

	_ = s.SourceHelm
	_ = s.SourceKustomize
	_ = s.SourceRaw
	_ = s.TargetVersion

	// SourceHelm and SourceKustomize are pointers — nil check must not panic.
	if s.SourceHelm != nil {
		t.Error("zero-value SourceHelm should be nil")
	}
	if s.SourceKustomize != nil {
		t.Error("zero-value SourceKustomize should be nil")
	}

	// Nil slice is valid — range must not panic.
	for range s.SourceRaw {
	}
}

// TestGenerateFromTalosClusterClusterRefFromEndpoint verifies that the generated
// RunnerConfigSpec.ClusterRef is populated from TalosClusterSpec.ClusterEndpoint.
func TestGenerateFromTalosClusterClusterRefFromEndpoint(t *testing.T) {
	spec, err := runnerlib.GenerateFromTalosCluster(runnerlib.TalosClusterSpec{
		ClusterEndpoint: "10.20.0.10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if spec.ClusterRef != "10.20.0.10" {
		t.Errorf("ClusterRef: got %q, want %q", spec.ClusterRef, "10.20.0.10")
	}
}

// TestGenerateFromTalosClusterRunnerImageEmpty verifies that the generator leaves
// RunnerImage empty — the caller (operator) is responsible for setting it.
func TestGenerateFromTalosClusterRunnerImageEmpty(t *testing.T) {
	spec, err := runnerlib.GenerateFromTalosCluster(runnerlib.TalosClusterSpec{
		ClusterEndpoint: "10.20.0.10",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if spec.RunnerImage != "" {
		t.Errorf("RunnerImage should be empty (caller sets it): got %q", spec.RunnerImage)
	}
}
