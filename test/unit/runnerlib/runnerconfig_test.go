package runnerlib_test

import (
	"testing"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// TestResolveNodeExclusions_SelfOperationTrue verifies that when SelfOperation=true,
// ResolveNodeExclusionsFromRunnerConfig merges MaintenanceTargetNodes and
// OperatorLeaderNode into a single exclusion list. conductor-schema.md §13.
func TestResolveNodeExclusions_SelfOperationTrue(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		SelfOperation:          true,
		MaintenanceTargetNodes: []string{"worker-1", "worker-2"},
		OperatorLeaderNode:     "cp-0",
	}

	got := runnerlib.ResolveNodeExclusionsFromRunnerConfig(spec)

	wantLen := 3
	if len(got) != wantLen {
		t.Fatalf("expected %d exclusions, got %d: %v", wantLen, len(got), got)
	}

	wantNodes := map[string]bool{"worker-1": true, "worker-2": true, "cp-0": true}
	for _, n := range got {
		if !wantNodes[n] {
			t.Errorf("unexpected node in exclusions: %q", n)
		}
	}
}

// TestResolveNodeExclusions_SelfOperationFalse verifies that when SelfOperation=false,
// ResolveNodeExclusionsFromRunnerConfig returns nil regardless of other fields.
// Tenant-targeted operations are exempt from node exclusion. conductor-schema.md §13.
func TestResolveNodeExclusions_SelfOperationFalse(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		SelfOperation:          false,
		MaintenanceTargetNodes: []string{"worker-1", "worker-2"},
		OperatorLeaderNode:     "cp-0",
	}

	got := runnerlib.ResolveNodeExclusionsFromRunnerConfig(spec)

	if got != nil {
		t.Errorf("expected nil for SelfOperation=false, got %v", got)
	}
}

// TestResolveNodeExclusions_LeaderNodeOnly verifies that when only OperatorLeaderNode
// is set (no MaintenanceTargetNodes), the single leader node is returned.
func TestResolveNodeExclusions_LeaderNodeOnly(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		SelfOperation:      true,
		OperatorLeaderNode: "cp-0",
	}

	got := runnerlib.ResolveNodeExclusionsFromRunnerConfig(spec)

	if len(got) != 1 || got[0] != "cp-0" {
		t.Errorf("expected [cp-0], got %v", got)
	}
}

// TestJobSpecBuilderWithNodeExclusions_SelfOperation verifies end-to-end that
// a JobSpec produced with WithNodeExclusions carries the correct exclusion list.
func TestJobSpecBuilderWithNodeExclusions_SelfOperation(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		SelfOperation:          true,
		MaintenanceTargetNodes: []string{"worker-1"},
		OperatorLeaderNode:     "cp-0",
	}

	exclusions := runnerlib.ResolveNodeExclusionsFromRunnerConfig(spec)

	jobSpec, err := runnerlib.NewJobSpecBuilder().
		WithCapability(runnerlib.CapabilityEtcdBackup).
		WithRunnerImage("registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1").
		WithNodeExclusions(exclusions).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(jobSpec.NodeExclusions) != 2 {
		t.Fatalf("expected 2 NodeExclusions, got %d: %v", len(jobSpec.NodeExclusions), jobSpec.NodeExclusions)
	}
	wantNodes := map[string]bool{"worker-1": true, "cp-0": true}
	for _, n := range jobSpec.NodeExclusions {
		if !wantNodes[n] {
			t.Errorf("unexpected node in NodeExclusions: %q", n)
		}
	}
}

// TestJobSpecBuilderWithNodeExclusions_TenantOperation verifies that a JobSpec
// produced without node exclusions (selfOperation=false path) has nil NodeExclusions.
func TestJobSpecBuilderWithNodeExclusions_TenantOperation(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructureRunnerConfigSpec{
		SelfOperation:          false,
		MaintenanceTargetNodes: []string{"worker-1"},
		OperatorLeaderNode:     "cp-0",
	}

	exclusions := runnerlib.ResolveNodeExclusionsFromRunnerConfig(spec)
	// exclusions is nil for SelfOperation=false -- do not call WithNodeExclusions.

	jobSpec, err := runnerlib.NewJobSpecBuilder().
		WithCapability(runnerlib.CapabilityPackDeploy).
		WithRunnerImage("registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1").
		WithNodeExclusions(exclusions).
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if len(jobSpec.NodeExclusions) != 0 {
		t.Errorf("expected no NodeExclusions for tenant operation, got %v", jobSpec.NodeExclusions)
	}
}
