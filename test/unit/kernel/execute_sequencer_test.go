package kernel_test

import (
	"sync"
	"testing"

	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// WS2 -- RunnerConfig step sequencer unit tests (additions).
// These tests complement the existing suite in execute_test.go.
// fixedStepExecutor, recordingStepStatusWriter, and executeCtx are defined in
// execute_test.go (same package kernel_test).

// TestRunExecute_TwoSimultaneous_NoCrossContamination verifies that two
// concurrent RunExecute calls maintain fully independent state. Each call uses
// its own StepExecutor and StepStatusWriter -- no result from one RunnerConfig
// must appear in the other's writer.
//
// This tests that RunExecute carries no package-level shared mutable state.
// conductor-schema.md §17.
func TestRunExecute_TwoSimultaneous_NoCrossContamination(t *testing.T) {
	steps1 := []seamcorev1alpha1.RunnerConfigStep{
		{Name: "alpha", Capability: runnerlib.CapabilityBootstrap},
	}
	steps2 := []seamcorev1alpha1.RunnerConfigStep{
		{Name: "beta", Capability: runnerlib.CapabilityPackDeploy},
	}

	writer1 := &recordingStepStatusWriter{}
	writer2 := &recordingStepStatusWriter{}
	exec1 := &fixedStepExecutor{defaultPhase: seamcorev1alpha1.RunnerStepSucceeded}
	exec2 := &fixedStepExecutor{defaultPhase: seamcorev1alpha1.RunnerStepSucceeded}

	var wg sync.WaitGroup
	var err1, err2 error

	wg.Add(2)
	go func() {
		defer wg.Done()
		err1 = kernel.RunExecute(executeCtx(steps1), exec1, writer1)
	}()
	go func() {
		defer wg.Done()
		err2 = kernel.RunExecute(executeCtx(steps2), exec2, writer2)
	}()
	wg.Wait()

	if err1 != nil {
		t.Errorf("RunExecute(steps1): unexpected error: %v", err1)
	}
	if err2 != nil {
		t.Errorf("RunExecute(steps2): unexpected error: %v", err2)
	}

	// writer1 must contain exactly step "alpha" -- no contamination from "beta".
	if len(writer1.stepResults) != 1 {
		t.Fatalf("writer1: expected 1 StepResult; got %d", len(writer1.stepResults))
	}
	if writer1.stepResults[0].Name != "alpha" {
		t.Errorf("writer1: expected Name=%q; got %q", "alpha", writer1.stepResults[0].Name)
	}

	// writer2 must contain exactly step "beta" -- no contamination from "alpha".
	if len(writer2.stepResults) != 1 {
		t.Fatalf("writer2: expected 1 StepResult; got %d", len(writer2.stepResults))
	}
	if writer2.stepResults[0].Name != "beta" {
		t.Errorf("writer2: expected Name=%q; got %q", "beta", writer2.stepResults[0].Name)
	}

	// Each writer must have received exactly one Completed terminal condition.
	if writer1.completedCount != 1 {
		t.Errorf("writer1: expected 1 Completed write; got %d", writer1.completedCount)
	}
	if writer2.completedCount != 1 {
		t.Errorf("writer2: expected 1 Completed write; got %d", writer2.completedCount)
	}
}

// TestRunExecute_PlatformOwnerRef_RoutingUnaffected verifies that the step
// sequencer processes steps identically regardless of which operator generated
// the RunnerConfig. The ownerReference is a Kubernetes metadata concern that
// the sequencer never inspects -- it operates solely on ExecutionContext.RunnerConfig.Steps.
//
// This test models a RunnerConfig produced by the Platform operator (e.g., from a
// TalosCluster bootstrap intent) and asserts the sequencer reaches the terminal
// Completed condition correctly. conductor-schema.md §17.
func TestRunExecute_PlatformOwnerRef_RoutingUnaffected(t *testing.T) {
	// A Platform-originated RunnerConfig: single bootstrap step.
	// ownerRef would be set to TalosCluster in production, but the sequencer
	// never reads it -- ExecutionContext carries only the steps.
	steps := []seamcorev1alpha1.RunnerConfigStep{
		{Name: "platform-bootstrap", Capability: runnerlib.CapabilityBootstrap},
	}

	writer := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{defaultPhase: seamcorev1alpha1.RunnerStepSucceeded}

	err := kernel.RunExecute(executeCtx(steps), exec, writer)
	if err != nil {
		t.Fatalf("RunExecute: unexpected error: %v", err)
	}

	// Terminal condition must be Completed -- sequencer is owner-agnostic.
	if writer.completedCount != 1 {
		t.Errorf("expected 1 Completed write; got %d", writer.completedCount)
	}
	if writer.failedCount != 0 {
		t.Errorf("expected 0 Failed writes; got %d", writer.failedCount)
	}
	if len(writer.stepResults) != 1 {
		t.Fatalf("expected 1 StepResult; got %d", len(writer.stepResults))
	}
	if writer.stepResults[0].Name != "platform-bootstrap" {
		t.Errorf("Name: got %q; want %q", writer.stepResults[0].Name, "platform-bootstrap")
	}
}
