package runnerlib_test

import (
	"testing"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestCapabilityManifestEntriesNonNil verifies that a CapabilityManifest with
// zero entries has a non-nil Entries slice. This is a structural guarantee:
// operators range over Entries and must not encounter nil.
//
// Note: the zero value of CapabilityManifest has Entries=nil (Go zero value).
// This test documents the expectation that callers initialize Entries before
// publishing to RunnerConfig status. It is not enforced by the type itself.
func TestCapabilityManifestEntriesNonNil(t *testing.T) {
	// A properly constructed manifest must have initialized Entries.
	m := runnerlib.CapabilityManifest{
		RunnerVersion: "v1.9.3-r1",
		Entries:       []runnerlib.CapabilityEntry{},
	}

	if m.Entries == nil {
		t.Error("CapabilityManifest.Entries must not be nil on a constructed manifest")
	}

	// Ranging over a non-nil empty slice must not panic.
	for range m.Entries {
	}
}

// TestCapabilityEntryNameNotNormalized verifies that the Name field of a
// CapabilityEntry is not transformed or normalized. The capability name is
// permanent and immutable — what the caller sets is what is stored. CR-INV-004.
func TestCapabilityEntryNameNotNormalized(t *testing.T) {
	entry := runnerlib.CapabilityEntry{
		Name: "bootstrap",
	}

	if entry.Name != "bootstrap" {
		t.Errorf("CapabilityEntry.Name was transformed: got %q, want %q", entry.Name, "bootstrap")
	}

	// Dash-separated names must be preserved exactly.
	entry2 := runnerlib.CapabilityEntry{
		Name: "talos-upgrade",
	}
	if entry2.Name != "talos-upgrade" {
		t.Errorf("CapabilityEntry.Name was transformed: got %q, want %q", entry2.Name, "talos-upgrade")
	}
}

// TestCapabilityModeConstantsDistinct verifies that the three CapabilityMode
// constants are distinct from each other. The three-mode boundary is absolute.
// INV-014, CR-INV-001.
func TestCapabilityModeConstantsDistinct(t *testing.T) {
	modes := []runnerlib.CapabilityMode{
		runnerlib.ExecutorMode,
		runnerlib.AgentMode,
		runnerlib.CompileMode,
	}

	seen := make(map[runnerlib.CapabilityMode]bool)
	for _, m := range modes {
		if m == "" {
			t.Errorf("CapabilityMode constant is empty string")
		}
		if seen[m] {
			t.Errorf("CapabilityMode constant %q is duplicated", m)
		}
		seen[m] = true
	}
}

// TestParameterDefRequiredDefaultsFalse verifies that the Required field of a
// zero-value ParameterDef is false. Go zero value for bool is false — this test
// documents the expected default behavior.
func TestParameterDefRequiredDefaultsFalse(t *testing.T) {
	var p runnerlib.ParameterDef

	if p.Required {
		t.Error("ParameterDef.Required zero value must be false (optional by default)")
	}
}
