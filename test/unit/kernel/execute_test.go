package kernel_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/internal/persistence"
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
		Namespace:         "ont-system",
	}

	err := kernel.RunExecute(ctx, reg, persistence.NoopConfigMapWriter{}, capability.ExecuteClients{})
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
		Namespace:         "ont-system",
	}

	if err := kernel.RunExecute(ctx, reg, persistence.NoopConfigMapWriter{}, capability.ExecuteClients{}); err != nil {
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
				Namespace:         "ont-system",
			}

			if err := kernel.RunExecute(ctx, reg, persistence.NoopConfigMapWriter{}, capability.ExecuteClients{}); err != nil {
				t.Errorf("unexpected error for stub capability %q: %v", name, err)
			}
		})
	}
}

// TestRunExecute_WritesOperationResultToConfigMap verifies that RunExecute
// writes the OperationResultSpec to the named ConfigMap via the writer.
// conductor-schema.md §8, conductor-design.md §2.8.
func TestRunExecute_WritesOperationResultToConfigMap(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register(runnerlib.CapabilityNodePatch, &alwaysSucceedHandler{})

	ctx := config.ExecutionContext{
		Mode:              config.ModeExecute,
		Capability:        runnerlib.CapabilityNodePatch,
		ClusterRef:        "ccs-dev",
		OperationResultCM: "node-patch-result",
		Namespace:         "ont-system",
	}

	rec := &recordingConfigMapWriter{}
	if err := kernel.RunExecute(ctx, reg, rec, capability.ExecuteClients{}); err != nil {
		t.Fatalf("expected no error; got %v", err)
	}

	if rec.writeCount != 1 {
		t.Errorf("expected exactly 1 ConfigMap write; got %d", rec.writeCount)
	}
	if rec.lastNamespace != "ont-system" {
		t.Errorf("expected namespace %q; got %q", "ont-system", rec.lastNamespace)
	}
	if rec.lastName != "node-patch-result" {
		t.Errorf("expected ConfigMap name %q; got %q", "node-patch-result", rec.lastName)
	}
	if rec.lastResult.Capability != runnerlib.CapabilityNodePatch {
		t.Errorf("expected capability %q in result; got %q",
			runnerlib.CapabilityNodePatch, rec.lastResult.Capability)
	}
	if rec.lastResult.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected status %q in result; got %q",
			runnerlib.ResultSucceeded, rec.lastResult.Status)
	}
}

// TestRunExecute_ConfigMapWriteFailureReturnsError verifies that a write failure
// from the ConfigMapWriter is propagated as an error from RunExecute.
// conductor-design.md §2.8.
func TestRunExecute_ConfigMapWriteFailureReturnsError(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register(runnerlib.CapabilityNodePatch, &alwaysSucceedHandler{})

	ctx := config.ExecutionContext{
		Mode:              config.ModeExecute,
		Capability:        runnerlib.CapabilityNodePatch,
		ClusterRef:        "ccs-dev",
		OperationResultCM: "node-patch-result",
		Namespace:         "ont-system",
	}

	err := kernel.RunExecute(ctx, reg, &failingConfigMapWriter{}, capability.ExecuteClients{})
	if err == nil {
		t.Fatal("expected error from ConfigMap write failure; got nil")
	}
}

// TestRunExecute_DefaultsNamespaceToOntSystem verifies that when Namespace is
// empty in ExecutionContext, the ConfigMap is written to ont-system.
// conductor-schema.md §5 (Namespace Conventions).
func TestRunExecute_DefaultsNamespaceToOntSystem(t *testing.T) {
	reg := capability.NewRegistry()
	reg.Register(runnerlib.CapabilityNodePatch, &alwaysSucceedHandler{})

	ctx := config.ExecutionContext{
		Mode:              config.ModeExecute,
		Capability:        runnerlib.CapabilityNodePatch,
		ClusterRef:        "ccs-dev",
		OperationResultCM: "result",
		Namespace:         "", // intentionally empty
	}

	rec := &recordingConfigMapWriter{}
	if err := kernel.RunExecute(ctx, reg, rec, capability.ExecuteClients{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rec.lastNamespace != config.DefaultNamespace {
		t.Errorf("expected namespace %q; got %q", config.DefaultNamespace, rec.lastNamespace)
	}
}

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

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

// recordingConfigMapWriter captures the most recent WriteResult call for assertions.
type recordingConfigMapWriter struct {
	writeCount    int
	lastNamespace string
	lastName      string
	lastResult    runnerlib.OperationResultSpec
}

func (w *recordingConfigMapWriter) WriteResult(_ context.Context, namespace, name string, result runnerlib.OperationResultSpec) error {
	w.writeCount++
	w.lastNamespace = namespace
	w.lastName = name
	w.lastResult = result
	return nil
}

// failingConfigMapWriter always returns an error from WriteResult.
type failingConfigMapWriter struct{}

func (failingConfigMapWriter) WriteResult(_ context.Context, _, _ string, _ runnerlib.OperationResultSpec) error {
	return fmt.Errorf("simulated ConfigMap write failure")
}
