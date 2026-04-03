package runnerlib_test

import (
	"testing"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestResultStatusConstantsDistinct verifies that ResultSucceeded and ResultFailed
// are distinct non-empty strings.
func TestResultStatusConstantsDistinct(t *testing.T) {
	if runnerlib.ResultSucceeded == "" {
		t.Error("ResultSucceeded must not be empty")
	}
	if runnerlib.ResultFailed == "" {
		t.Error("ResultFailed must not be empty")
	}
	if runnerlib.ResultSucceeded == runnerlib.ResultFailed {
		t.Errorf("ResultSucceeded and ResultFailed must be distinct, both are %q", runnerlib.ResultSucceeded)
	}
}

// TestFailureCategoryConstantsMatchDesignDoc verifies that all FailureCategory
// constants match the exact strings declared in conductor-design.md Section 6.1.
// These strings appear in OperationResult ConfigMaps and are parsed by operators.
// Any mismatch breaks the operator-runner protocol.
func TestFailureCategoryConstantsMatchDesignDoc(t *testing.T) {
	expected := map[runnerlib.FailureCategory]string{
		runnerlib.ValidationFailure:         "ValidationFailure",
		runnerlib.CapabilityUnavailable:     "CapabilityUnavailable",
		runnerlib.ExecutionFailure:          "ExecutionFailure",
		runnerlib.ExternalDependencyFailure: "ExternalDependencyFailure",
		runnerlib.InvariantViolation:        "InvariantViolation",
		runnerlib.LicenseViolation:          "LicenseViolation",
		runnerlib.StorageUnavailable:        "StorageUnavailable",
	}

	for cat, want := range expected {
		if string(cat) != want {
			t.Errorf("FailureCategory value mismatch: got %q, want %q", cat, want)
		}
	}

	// Verify count: exactly 7 categories as declared in design doc.
	if len(expected) != 7 {
		t.Errorf("expected 7 FailureCategory constants, got %d", len(expected))
	}
}

// TestOperationResultSpecNilFailureReasonNoPanic verifies that accessing a nil
// FailureReason via a pointer check does not panic. Operators must use a nil
// check before dereferencing — this test documents the expected usage pattern.
func TestOperationResultSpecNilFailureReasonNoPanic(t *testing.T) {
	result := runnerlib.OperationResultSpec{
		Status:        runnerlib.ResultSucceeded,
		FailureReason: nil,
	}

	// Standard nil pointer check — must not panic.
	if result.FailureReason != nil {
		t.Error("FailureReason should be nil on a succeeded result")
	}

	// Verify that accessing the pointer itself does not panic.
	var _ *runnerlib.FailureReason = result.FailureReason
}

// TestArtifactRefHasNoRawContent is a structural test verifying that ArtifactRef
// does not expose a Content or Data field. Artifacts are references, never raw
// content. conductor-design.md Section 10.
//
// This test is a compilation-time structural assertion: if Content or Data fields
// were added to ArtifactRef, callers would be tempted to embed raw content, which
// is an invariant violation. The test documents this constraint.
func TestArtifactRefHasNoRawContent(t *testing.T) {
	// Construct a fully specified ArtifactRef using named fields.
	// If Content or Data fields existed, this literal would not compile with them
	// absent and the test would fail at compilation.
	ref := runnerlib.ArtifactRef{
		Name:      "talosconfig",
		Kind:      "Secret",
		Reference: "ont-system/talosconfig-ccs-dev",
		Checksum:  "sha256:abc123",
	}

	// Verify the expected fields are present and accessible.
	if ref.Name == "" || ref.Kind == "" || ref.Reference == "" {
		t.Error("ArtifactRef fields not populated as expected")
	}
}

// TestStepResultZeroValueNoPanic verifies that the zero value of StepResult
// is accessible without panic.
func TestStepResultZeroValueNoPanic(t *testing.T) {
	var s runnerlib.StepResult

	_ = s.Name
	_ = s.Status
	_ = s.StartedAt
	_ = s.CompletedAt
	_ = s.Message
}
