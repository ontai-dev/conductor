package kernel_test

import (
	"context"
	"testing"

	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// WS3 -- Execute mode unit tests.
// These tests verify how the step sequencer propagates capability parameters
// to the StepExecutor, and how the sequencer handles failure conditions that
// arise when a step's required input (e.g., a ConfigMap reference) is absent.

// capturingExecutor is a StepExecutor that records the exact parameters it
// receives from RunExecute. Used to verify that clusterRef, namespace, and
// step fields are propagated correctly through the sequencer.
type capturingExecutor struct {
	fixedPhase      seamcorev1alpha1.RunnerStepResultPhase
	capturedStep    seamcorev1alpha1.RunnerConfigStep
	capturedCluster string
	capturedNS      string
}

func (e *capturingExecutor) Execute(
	_ context.Context,
	step seamcorev1alpha1.RunnerConfigStep,
	clusterRef, namespace string,
) (seamcorev1alpha1.RunnerConfigStepResult, error) {
	e.capturedStep = step
	e.capturedCluster = clusterRef
	e.capturedNS = namespace
	return seamcorev1alpha1.RunnerConfigStepResult{
		Name:   step.Name,
		Status: e.fixedPhase,
	}, nil
}

// executeCtxWithCluster constructs an ExecutionContext with an explicit clusterRef
// and namespace. Complements executeCtx (defined in execute_test.go) for tests
// that need to assert parameter propagation.
func executeCtxWithCluster(steps []seamcorev1alpha1.RunnerConfigStep, clusterRef, namespace string) config.ExecutionContext {
	ctx := executeCtx(steps)
	ctx.ClusterRef = clusterRef
	ctx.Namespace = namespace
	return ctx
}

// TestRunExecute_NodeDecommission_ExecutorReceivesCorrectParameters verifies that
// when a RunnerConfig step declares capability=node-decommission with a clusterRef
// target, RunExecute propagates the capability name, clusterRef, and namespace
// to the StepExecutor without modification.
//
// "No shell execution" is enforced structurally: the StepExecutor interface
// accepts only pure Go parameters (context, step struct, strings). There is no
// command string, no argv slice, no os/exec path -- the interface makes shell
// invocation impossible. INV-001, conductor-schema.md §17.
func TestRunExecute_NodeDecommission_ExecutorReceivesCorrectParameters(t *testing.T) {
	const (
		targetCluster = "ccs-test"
		targetNS      = "ont-system"
	)

	steps := []seamcorev1alpha1.RunnerConfigStep{
		{
			Name:       "drain-worker-1",
			Capability: runnerlib.CapabilityNodeDecommission,
			Parameters: map[string]string{
				"targetNode": "worker-1",
			},
		},
	}

	cap := &capturingExecutor{fixedPhase: seamcorev1alpha1.RunnerStepSucceeded}
	writer := &recordingStepStatusWriter{}

	ctx := executeCtxWithCluster(steps, targetCluster, targetNS)
	err := kernel.RunExecute(ctx, cap, writer)
	if err != nil {
		t.Fatalf("RunExecute: unexpected error: %v", err)
	}

	// Verify the executor received the correct capability name.
	if cap.capturedStep.Capability != runnerlib.CapabilityNodeDecommission {
		t.Errorf("Capability: got %q; want %q",
			cap.capturedStep.Capability, runnerlib.CapabilityNodeDecommission)
	}
	// Verify the cluster target was propagated.
	if cap.capturedCluster != targetCluster {
		t.Errorf("clusterRef: got %q; want %q", cap.capturedCluster, targetCluster)
	}
	// Verify the namespace was propagated.
	if cap.capturedNS != targetNS {
		t.Errorf("namespace: got %q; want %q", cap.capturedNS, targetNS)
	}
	// Verify Parameters passed through unmodified.
	if cap.capturedStep.Parameters["targetNode"] != "worker-1" {
		t.Errorf("Parameters[targetNode]: got %q; want %q",
			cap.capturedStep.Parameters["targetNode"], "worker-1")
	}
	// Terminal condition: Completed.
	if writer.completedCount != 1 {
		t.Errorf("expected 1 Completed write; got %d", writer.completedCount)
	}
}

// TestRunExecute_StepFailed_ConfigMapRefAbsent_TerminalFailed models the
// production failure path where a capability step cannot locate its required
// input ConfigMap (e.g., the OperationResult ConfigMap from a prior step no
// longer exists). The StepExecutor returns Status=Failed. The sequencer must
// write the terminal Failed condition and stop processing.
//
// conductor-schema.md §17: "On Failed + HaltOnFailure=true: write terminal
// Failed condition and stop."
func TestRunExecute_StepFailed_ConfigMapRefAbsent_TerminalFailed(t *testing.T) {
	steps := []seamcorev1alpha1.RunnerConfigStep{
		{
			Name:          "deploy-step",
			Capability:    runnerlib.CapabilityPackDeploy,
			HaltOnFailure: true,
			// Parameters would carry configMapRef in production; executor
			// returns Failed when the ConfigMap cannot be found.
			Parameters: map[string]string{
				"configMapRef": "missing-operation-result-cm",
			},
		},
	}

	// Executor simulates ConfigMap-not-found by returning Failed.
	failExec := &capturingExecutor{fixedPhase: seamcorev1alpha1.RunnerStepFailed}
	writer := &recordingStepStatusWriter{}

	err := kernel.RunExecute(executeCtx(steps), failExec, writer)
	if err != nil {
		t.Fatalf("RunExecute: unexpected error: %v", err)
	}

	// StepResult must record the failure.
	if len(writer.stepResults) != 1 {
		t.Fatalf("expected 1 StepResult; got %d", len(writer.stepResults))
	}
	if writer.stepResults[0].Status != seamcorev1alpha1.RunnerStepFailed {
		t.Errorf("StepResult.Status: got %q; want Failed", writer.stepResults[0].Status)
	}

	// Terminal condition must be Failed -- no Completed written.
	if writer.failedCount != 1 {
		t.Errorf("expected 1 Failed terminal write; got %d", writer.failedCount)
	}
	if writer.completedCount != 0 {
		t.Errorf("expected 0 Completed writes; got %d", writer.completedCount)
	}
	if writer.lastFailedStep != "deploy-step" {
		t.Errorf("lastFailedStep: got %q; want %q", writer.lastFailedStep, "deploy-step")
	}
}
