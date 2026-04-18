package runnerlib_test

import (
	"testing"
	"time"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestRunnerConfigSpecZeroValue verifies that a zero-value RunnerConfigSpec is
// valid: all fields are accessible and no panics occur.
func TestRunnerConfigSpecZeroValue(t *testing.T) {
	var s runnerlib.RunnerConfigSpec

	// All field accesses must not panic.
	_ = s.ClusterRef
	_ = s.RunnerImage
	_ = s.Phases
	_ = s.OperationalHistory
	_ = s.MaintenanceTargetNodes
	_ = s.OperatorLeaderNode
	_ = s.SelfOperation

	// Nil slices are valid zero values — no panic on range.
	for range s.Phases {
	}
	for range s.OperationalHistory {
	}
	for range s.MaintenanceTargetNodes {
	}
}

// TestPhaseConfigEmptyParameters verifies that a PhaseConfig with an empty
// Parameters map does not panic on access.
func TestPhaseConfigEmptyParameters(t *testing.T) {
	p := runnerlib.PhaseConfig{
		Name:       "launch",
		Parameters: map[string]string{},
	}

	_ = p.Name
	_ = p.Parameters

	// Key lookup on empty map must not panic.
	v := p.Parameters["nonexistent"]
	_ = v
}

// TestOperationalHistoryEntryPreservesFields verifies that all fields of an
// OperationalHistoryEntry are preserved correctly when constructed.
func TestOperationalHistoryEntryPreservesFields(t *testing.T) {
	now := time.Now()
	entry := runnerlib.OperationalHistoryEntry{
		AppliedAt:     now,
		Concern:       "RunnerImage",
		PreviousValue: "old-image:v1",
		NewValue:      "new-image:v2",
		AppliedBy:     "bootstrap-job-abc123",
	}

	if !entry.AppliedAt.Equal(now) {
		t.Errorf("AppliedAt not preserved: got %v, want %v", entry.AppliedAt, now)
	}
	if entry.Concern != "RunnerImage" {
		t.Errorf("Concern not preserved: got %q", entry.Concern)
	}
	if entry.PreviousValue != "old-image:v1" {
		t.Errorf("PreviousValue not preserved: got %q", entry.PreviousValue)
	}
	if entry.NewValue != "new-image:v2" {
		t.Errorf("NewValue not preserved: got %q", entry.NewValue)
	}
	if entry.AppliedBy != "bootstrap-job-abc123" {
		t.Errorf("AppliedBy not preserved: got %q", entry.AppliedBy)
	}
}

// TestResolveNodeExclusions_SelfOperationTrue verifies that when SelfOperation=true,
// ResolveNodeExclusionsFromRunnerConfig merges MaintenanceTargetNodes and
// OperatorLeaderNode into a single exclusion list. conductor-schema.md §13.
func TestResolveNodeExclusions_SelfOperationTrue(t *testing.T) {
	spec := runnerlib.RunnerConfigSpec{
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
	spec := runnerlib.RunnerConfigSpec{
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
	spec := runnerlib.RunnerConfigSpec{
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
	spec := runnerlib.RunnerConfigSpec{
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
	spec := runnerlib.RunnerConfigSpec{
		SelfOperation:          false,
		MaintenanceTargetNodes: []string{"worker-1"},
		OperatorLeaderNode:     "cp-0",
	}

	exclusions := runnerlib.ResolveNodeExclusionsFromRunnerConfig(spec)
	// exclusions is nil for SelfOperation=false — do not call WithNodeExclusions.

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
