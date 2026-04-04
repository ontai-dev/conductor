package kernel_test

import (
	"context"
	"testing"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestRunExecute_UnknownCapabilityReturnsError verifies that RunExecute returns
// a non-nil error when the capability is not in the registry.
// conductor-design.md §4.2 Phase 2.
func TestRunExecute_UnknownCapabilityReturnsError(t *testing.T) {
	reg := capability.NewRegistry()
	// Intentionally empty registry.

	ctx := config.ExecutionContext{
		Mode:              config.ModeExecute,
		Capability:        "does-not-exist",
		ClusterRef:        "ccs-dev",
		OperationResultCM: "result-cm",
	}

	err := kernel.RunExecute(ctx, reg)
	if err == nil {
		t.Fatal("expected error for unknown capability; got nil")
	}
}

// TestRunExecute_KnownCapabilityDispatchesSuccessfully verifies that RunExecute
// dispatches to the registered handler and completes without error.
// conductor-design.md §4.2 Phase 3.
func TestRunExecute_KnownCapabilityDispatchesSuccessfully(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register(runnerlib.CapabilityNodePatch, &alwaysSucceedHandler{})

	ctx := config.ExecutionContext{
		Mode:              config.ModeExecute,
		Capability:        runnerlib.CapabilityNodePatch,
		ClusterRef:        "ccs-dev",
		OperationResultCM: "node-patch-result",
	}

	if err := kernel.RunExecute(ctx, reg); err != nil {
		t.Errorf("expected no error for known capability; got %v", err)
	}
}

// TestRunExecute_AllStubCapabilitiesDispatchWithoutPanic verifies that every
// stub capability registered via RegisterAll can be dispatched through RunExecute
// without panic or Go error. Stubs return ResultFailed but that is not a Go error.
func TestRunExecute_AllStubCapabilitiesDispatchWithoutPanic(t *testing.T) {
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
		runnerlib.CapabilityEtcdMaintenance,
		runnerlib.CapabilityEtcdRestore,
		runnerlib.CapabilityPKIRotate,
		runnerlib.CapabilityCredentialRotate,
		runnerlib.CapabilityHardeningApply,
		runnerlib.CapabilityClusterReset,
		runnerlib.CapabilityPackDeploy,
		runnerlib.CapabilityRBACProvision,
	}

	for _, name := range executeCapabilities {
		t.Run(name, func(t *testing.T) {
			reg := capability.NewRegistry()
			capability.RegisterAll(reg)

			ctx := config.ExecutionContext{
				Mode:              config.ModeExecute,
				Capability:        name,
				ClusterRef:        "ccs-test",
				OperationResultCM: "result-cm",
			}

			if err := kernel.RunExecute(ctx, reg); err != nil {
				t.Errorf("unexpected error for stub capability %q: %v", name, err)
			}
		})
	}
}

// alwaysSucceedHandler is a test Handler that returns ResultSucceeded.
type alwaysSucceedHandler struct{}

func (h *alwaysSucceedHandler) Execute(_ context.Context, params capability.ExecuteParams) (runnerlib.OperationResultSpec, error) {
	return runnerlib.OperationResultSpec{
		Capability: params.Capability,
		Status:     runnerlib.ResultSucceeded,
		Artifacts:  []runnerlib.ArtifactRef{},
		Steps:      []runnerlib.StepResult{},
	}, nil
}
