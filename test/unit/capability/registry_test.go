package capability_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// fixedHandler is a test Handler that returns a pre-set result.
type fixedHandler struct {
	result runnerlib.OperationResultSpec
}

func (h *fixedHandler) Execute(_ context.Context, _ capability.ExecuteParams) (runnerlib.OperationResultSpec, error) {
	return h.result, nil
}

// TestRegistry_ResolveRegisteredCapability verifies that a registered capability
// can be resolved by name.
func TestRegistry_ResolveRegisteredCapability(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register("bootstrap", &fixedHandler{result: runnerlib.OperationResultSpec{
		Capability: "bootstrap",
		Status:     runnerlib.ResultSucceeded,
	}})

	h, err := reg.Resolve("bootstrap")
	if err != nil {
		t.Fatalf("expected no error; got %v", err)
	}
	if h == nil {
		t.Fatal("expected non-nil handler")
	}
}

// TestRegistry_ResolveUnknownCapabilityReturnsError verifies that resolving an
// unregistered capability returns ErrCapabilityUnavailable.
func TestRegistry_ResolveUnknownCapabilityReturnsError(t *testing.T) {
	reg := capability.NewRegistry()

	_, err := reg.Resolve("does-not-exist")
	if err == nil {
		t.Fatal("expected error for unregistered capability; got nil")
	}
	if !errors.Is(err, capability.ErrCapabilityUnavailable) {
		t.Errorf("expected ErrCapabilityUnavailable; got %v", err)
	}
}

// TestRegistry_DuplicateRegistrationPanics verifies that registering two handlers
// under the same name panics immediately (programming error). CR-INV-004.
func TestRegistry_DuplicateRegistrationPanics(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register("bootstrap", &fixedHandler{})

	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for duplicate capability registration; did not panic")
		}
	}()
	reg.Register("bootstrap", &fixedHandler{}) // must panic
}

// TestRegistry_RegisterAllPopulatesAllExecuteCapabilities verifies that
// RegisterAll registers all 17 named execute-mode capabilities and does NOT
// register pack-compile (which is Compiler compile-mode only).
func TestRegistry_RegisterAllPopulatesAllExecuteCapabilities(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	// All execute-mode capabilities must be resolvable.
	executeCapabilities := []string{
		runnerlib.CapabilityBootstrap,
		runnerlib.CapabilityTalosUpgrade,
		runnerlib.CapabilityKubeUpgrade,
		runnerlib.CapabilityStackUpgrade,
		runnerlib.CapabilityNodePatch,
		runnerlib.CapabilityNodeScaleUp,
		runnerlib.CapabilityNodeDecommission,
		runnerlib.CapabilityNodeReboot,
		runnerlib.CapabilityEtcdBackup,
		runnerlib.CapabilityEtcdDefrag,
		runnerlib.CapabilityEtcdRestore,
		runnerlib.CapabilityPKIRotate,
		runnerlib.CapabilityCredentialRotate,
		runnerlib.CapabilityHardeningApply,
		runnerlib.CapabilityClusterReset,
		runnerlib.CapabilityPackDeploy,
		runnerlib.CapabilityRBACProvision,
	}
	for _, name := range executeCapabilities {
		if _, err := reg.Resolve(name); err != nil {
			t.Errorf("expected capability %q to be registered; got error: %v", name, err)
		}
	}

	// pack-compile must NOT be registered — it is a Compiler compile-mode
	// invocation only. Registering it in the Conductor binary would be a
	// schema violation. conductor-schema.md §6.
	if _, err := reg.Resolve(runnerlib.CapabilityPackCompile); err == nil {
		t.Errorf("pack-compile must not be registered in the Conductor execute-mode registry")
	}
}

// TestRegistry_RegisteredNamesMatchRegistered verifies that RegisteredNames
// returns exactly the names that were registered.
func TestRegistry_RegisteredNamesMatchRegistered(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register("bootstrap", &fixedHandler{})
	reg.Register("node-patch", &fixedHandler{})

	names := reg.RegisteredNames()
	if len(names) != 2 {
		t.Errorf("expected 2 registered names; got %d: %v", len(names), names)
	}
}
