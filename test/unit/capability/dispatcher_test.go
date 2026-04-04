package capability_test

import (
	"context"
	"testing"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestDispatcher_ExecuteKnownCapabilityReturnsResult verifies that dispatching
// a known capability produces an OperationResultSpec from the handler.
func TestDispatcher_ExecuteKnownCapabilityReturnsResult(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register(runnerlib.CapabilityNodePatch, &fixedHandler{
		result: runnerlib.OperationResultSpec{
			Capability: runnerlib.CapabilityNodePatch,
			Status:     runnerlib.ResultSucceeded,
		},
	})

	h, err := reg.Resolve(runnerlib.CapabilityNodePatch)
	if err != nil {
		t.Fatalf("resolve failed: %v", err)
	}

	params := capability.ExecuteParams{
		Capability:        runnerlib.CapabilityNodePatch,
		ClusterRef:        "ccs-dev",
		OperationResultCM: "node-patch-result",
	}
	result, err := h.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q", result.Status)
	}
	if result.Capability != runnerlib.CapabilityNodePatch {
		t.Errorf("expected capability %q; got %q", runnerlib.CapabilityNodePatch, result.Capability)
	}
}

// TestDispatcher_StubHandlerReturnsFailedResult verifies that stub handlers
// (used for all unimplemented capabilities) return ResultFailed with
// ExecutionFailure category and a "not yet implemented" reason.
func TestDispatcher_StubHandlerReturnsFailedResult(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	h, err := reg.Resolve(runnerlib.CapabilityBootstrap)
	if err != nil {
		t.Fatalf("resolve bootstrap failed: %v", err)
	}

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityBootstrap,
		ClusterRef: "ccs-test",
	})
	if err != nil {
		t.Fatalf("stub execute returned unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("stub: expected ResultFailed; got %q", result.Status)
	}
	if result.FailureReason == nil {
		t.Fatal("stub: expected non-nil FailureReason")
	}
	if result.FailureReason.Category != runnerlib.ExecutionFailure {
		t.Errorf("stub: expected ExecutionFailure category; got %q", result.FailureReason.Category)
	}
}

// TestDispatcher_StubHandlerArtifactsNonNil verifies that stub handlers return
// a non-nil Artifacts slice (operators range over this).
func TestDispatcher_StubHandlerArtifactsNonNil(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)
	result, _ := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
	})
	if result.Artifacts == nil {
		t.Error("stub: Artifacts must not be nil — operators range over this slice")
	}
}

// TestDispatcher_CompileModeGuard_PackCompileNotInRegistry verifies the compile-mode
// guard: CapabilityPackCompile must not be registered in the Conductor execute-mode
// registry. This is the structural enforcement that prevents compile-mode
// capability invocation from the Conductor binary. conductor-schema.md §6.
func TestDispatcher_CompileModeGuard_PackCompileNotInRegistry(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	_, err := reg.Resolve(runnerlib.CapabilityPackCompile)
	if err == nil {
		t.Error("compile-mode guard violation: CapabilityPackCompile must not be " +
			"in the execute-mode registry — it is a Compiler invocation only")
	}
}
