package runnerlib_test

import (
	"testing"
	"time"

	"github.com/ontai-dev/ont-runner/pkg/runnerlib"
)

// TestRunnerConfigSpecZeroValue verifies that a zero-value RunnerConfigSpec is
// valid: all fields are accessible and no panics occur.
func TestRunnerConfigSpecZeroValue(t *testing.T) {
	var s runnerlib.RunnerConfigSpec

	// All field accesses must not panic.
	_ = s.ClusterRef
	_ = s.RunnerImage
	_ = s.LicenseSecretRef
	_ = s.Phases
	_ = s.OperationalHistory

	// Nil slices are valid zero values — no panic on range.
	for range s.Phases {
	}
	for range s.OperationalHistory {
	}
}

// TestLicenseStatusConstantsNonEmptyAndDistinct verifies that all LicenseStatus
// constants are non-empty strings and that no two constants share the same value.
func TestLicenseStatusConstantsNonEmptyAndDistinct(t *testing.T) {
	constants := []runnerlib.LicenseStatus{
		runnerlib.LicenseStatusCommunity,
		runnerlib.LicenseStatusLicensed,
		runnerlib.LicenseStatusExpired,
		runnerlib.LicenseStatusConstraint,
	}

	seen := make(map[runnerlib.LicenseStatus]bool)
	for _, c := range constants {
		if c == "" {
			t.Errorf("LicenseStatus constant is empty string")
		}
		if seen[c] {
			t.Errorf("LicenseStatus constant %q is duplicated", c)
		}
		seen[c] = true
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
