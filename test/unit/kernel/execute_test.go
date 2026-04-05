package kernel_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// fixedStepExecutor returns a pre-configured phase for every step it executes.
// Callers can override per-step behaviour by name via the overrides map.
type fixedStepExecutor struct {
	defaultPhase runnerlib.StepPhase
	overrides    map[string]runnerlib.StepPhase // stepName → phase
	execOrder    []string                       // records execution order
	failWithErr  string                         // non-empty: return a Go error for this step name
}

func (e *fixedStepExecutor) Execute(_ context.Context, step runnerlib.RunnerConfigStep, _, _ string) (runnerlib.RunnerConfigStepResult, error) {
	if e.failWithErr != "" && step.Name == e.failWithErr {
		return runnerlib.RunnerConfigStepResult{}, fmt.Errorf("simulated executor error for step %q", step.Name)
	}
	e.execOrder = append(e.execOrder, step.Name)
	phase := e.defaultPhase
	if p, ok := e.overrides[step.Name]; ok {
		phase = p
	}
	return runnerlib.RunnerConfigStepResult{
		StepName: step.Name,
		Phase:    phase,
	}, nil
}

// recordingStepStatusWriter captures WriteStepResult, WriteCompleted, and WriteFailed calls.
type recordingStepStatusWriter struct {
	stepResults     []runnerlib.RunnerConfigStepResult
	completedCount  int
	failedCount     int
	lastFailedStep  string
	failWriteResult bool // when true, WriteStepResult returns an error
}

func (w *recordingStepStatusWriter) WriteStepResult(_ context.Context, r runnerlib.RunnerConfigStepResult) error {
	if w.failWriteResult {
		return fmt.Errorf("simulated WriteStepResult failure")
	}
	w.stepResults = append(w.stepResults, r)
	return nil
}

func (w *recordingStepStatusWriter) WriteCompleted(_ context.Context) error {
	w.completedCount++
	return nil
}

func (w *recordingStepStatusWriter) WriteFailed(_ context.Context, failedStep string) error {
	w.failedCount++
	w.lastFailedStep = failedStep
	return nil
}

// executeCtx builds a minimal ExecutionContext for execute-mode tests.
// Populates RunnerConfig.Steps from the provided steps list.
func executeCtx(steps []runnerlib.RunnerConfigStep) config.ExecutionContext {
	return config.ExecutionContext{
		Mode:       config.ModeExecute,
		ClusterRef: "ccs-test",
		Namespace:  "ont-system",
		RunnerConfig: runnerlib.RunnerConfigSpec{
			ClusterRef:  "ccs-test",
			RunnerImage: "conductor:dev",
			Steps:       steps,
		},
	}
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestRunExecute_EmptyStepsReturnsError verifies that RunExecute fails when
// no steps are declared in the RunnerConfig.
func TestRunExecute_EmptyStepsReturnsError(t *testing.T) {
	ctx := executeCtx(nil)
	err := kernel.RunExecute(ctx, &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseSucceeded}, &recordingStepStatusWriter{})
	if err == nil {
		t.Fatal("expected error for empty steps list; got nil")
	}
}

// TestRunExecute_SingleStepSucceededWritesCompleted verifies the happy path:
// a single step succeeds and the terminal Completed condition is written.
func TestRunExecute_SingleStepSucceededWritesCompleted(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "step-1", Capability: "node-patch", HaltOnFailure: true},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseSucceeded}

	if err := kernel.RunExecute(ctx, exec, sw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sw.stepResults) != 1 {
		t.Errorf("expected 1 StepResult; got %d", len(sw.stepResults))
	}
	if sw.stepResults[0].Phase != runnerlib.StepPhaseSucceeded {
		t.Errorf("expected step phase Succeeded; got %q", sw.stepResults[0].Phase)
	}
	if sw.completedCount != 1 {
		t.Errorf("expected WriteCompleted called once; got %d", sw.completedCount)
	}
	if sw.failedCount != 0 {
		t.Errorf("expected WriteFailed not called; got %d", sw.failedCount)
	}
}

// TestRunExecute_SingleStepFailedWithHaltWritesFailed verifies that a single
// failing step with HaltOnFailure=true causes WriteFailed to be called.
func TestRunExecute_SingleStepFailedWithHaltWritesFailed(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "step-1", Capability: "node-patch", HaltOnFailure: true},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseFailed}

	if err := kernel.RunExecute(ctx, exec, sw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sw.failedCount != 1 {
		t.Errorf("expected WriteFailed called once; got %d", sw.failedCount)
	}
	if sw.lastFailedStep != "step-1" {
		t.Errorf("expected failedStep %q; got %q", "step-1", sw.lastFailedStep)
	}
	if sw.completedCount != 0 {
		t.Errorf("expected WriteCompleted not called; got %d", sw.completedCount)
	}
}

// TestRunExecute_MultiStepSequentialAllSucceeded verifies that all steps are
// executed in declared order and Completed is written after all succeed.
func TestRunExecute_MultiStepSequentialAllSucceeded(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "prepare", Capability: "node-patch"},
		{Name: "execute", Capability: "talos-upgrade", DependsOn: "prepare"},
		{Name: "verify", Capability: "node-patch", DependsOn: "execute"},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseSucceeded}

	if err := kernel.RunExecute(ctx, exec, sw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sw.stepResults) != 3 {
		t.Fatalf("expected 3 StepResults; got %d", len(sw.stepResults))
	}
	wantOrder := []string{"prepare", "execute", "verify"}
	for i, name := range wantOrder {
		if exec.execOrder[i] != name {
			t.Errorf("step %d: expected %q; got %q", i, name, exec.execOrder[i])
		}
		if sw.stepResults[i].Phase != runnerlib.StepPhaseSucceeded {
			t.Errorf("step %d (%q): expected Succeeded; got %q", i, name, sw.stepResults[i].Phase)
		}
	}
	if sw.completedCount != 1 {
		t.Errorf("expected WriteCompleted once; got %d", sw.completedCount)
	}
}

// TestRunExecute_HaltOnFailureStopsSequence verifies that when a step fails
// with HaltOnFailure=true, the sequencer stops and subsequent steps are not
// executed.
func TestRunExecute_HaltOnFailureStopsSequence(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "prepare", Capability: "node-patch", HaltOnFailure: true},
		{Name: "execute", Capability: "talos-upgrade", DependsOn: "prepare", HaltOnFailure: true},
		{Name: "verify", Capability: "node-patch", DependsOn: "execute"},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{
		defaultPhase: runnerlib.StepPhaseSucceeded,
		overrides:    map[string]runnerlib.StepPhase{"prepare": runnerlib.StepPhaseFailed},
	}

	if err := kernel.RunExecute(ctx, exec, sw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Only "prepare" should have been executed.
	if len(exec.execOrder) != 1 || exec.execOrder[0] != "prepare" {
		t.Errorf("expected only %q executed; got %v", "prepare", exec.execOrder)
	}
	if sw.failedCount != 1 {
		t.Errorf("expected WriteFailed once; got %d", sw.failedCount)
	}
	if sw.lastFailedStep != "prepare" {
		t.Errorf("expected failedStep %q; got %q", "prepare", sw.lastFailedStep)
	}
	if sw.completedCount != 0 {
		t.Errorf("expected WriteCompleted not called; got %d", sw.completedCount)
	}
}

// TestRunExecute_PartialCompletionWithoutHalt verifies that when a step fails
// without HaltOnFailure, the sequencer continues to non-dependent successor
// steps and writes Failed as the terminal condition (not Completed).
func TestRunExecute_PartialCompletionWithoutHalt(t *testing.T) {
	// step-a fails, step-b has no dependency (runs anyway), step-c depends on step-a (skipped as failed).
	steps := []runnerlib.RunnerConfigStep{
		{Name: "step-a", Capability: "node-patch", HaltOnFailure: false},
		{Name: "step-b", Capability: "node-patch", HaltOnFailure: false},
		{Name: "step-c", Capability: "node-patch", DependsOn: "step-a", HaltOnFailure: false},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{
		defaultPhase: runnerlib.StepPhaseSucceeded,
		overrides:    map[string]runnerlib.StepPhase{"step-a": runnerlib.StepPhaseFailed},
	}

	if err := kernel.RunExecute(ctx, exec, sw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// step-b should be executed; step-a fails but no halt.
	// step-c depends on step-a (failed), so it is skipped with Failed phase.
	if len(sw.stepResults) != 3 {
		t.Fatalf("expected 3 StepResults (a=Failed, b=Succeeded, c=Failed-skipped); got %d", len(sw.stepResults))
	}

	phaseByName := make(map[string]runnerlib.StepPhase, 3)
	for _, r := range sw.stepResults {
		phaseByName[r.StepName] = r.Phase
	}

	if phaseByName["step-a"] != runnerlib.StepPhaseFailed {
		t.Errorf("step-a: expected Failed; got %q", phaseByName["step-a"])
	}
	if phaseByName["step-b"] != runnerlib.StepPhaseSucceeded {
		t.Errorf("step-b: expected Succeeded; got %q", phaseByName["step-b"])
	}
	if phaseByName["step-c"] != runnerlib.StepPhaseFailed {
		t.Errorf("step-c: expected Failed (skipped); got %q", phaseByName["step-c"])
	}

	// Terminal condition: at least one step failed → Failed.
	if sw.failedCount != 1 {
		t.Errorf("expected WriteFailed once; got %d", sw.failedCount)
	}
	if sw.completedCount != 0 {
		t.Errorf("expected WriteCompleted not called; got %d", sw.completedCount)
	}

	// step-b must have been executed; step-a and step-c must not have been
	// dispatched via executor (step-a was dispatched, step-c was skipped).
	foundB := false
	for _, name := range exec.execOrder {
		if name == "step-b" {
			foundB = true
		}
		if name == "step-c" {
			t.Errorf("step-c should not have been dispatched via executor (skipped due to failed dep)")
		}
	}
	if !foundB {
		t.Error("step-b should have been dispatched via executor")
	}
}

// TestRunExecute_ExecutorErrorPropagates verifies that a Go error from the
// StepExecutor is returned as an error from RunExecute.
func TestRunExecute_ExecutorErrorPropagates(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "boom", Capability: "node-patch"},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{
		defaultPhase: runnerlib.StepPhaseSucceeded,
		failWithErr:  "boom",
	}

	err := kernel.RunExecute(ctx, exec, sw)
	if err == nil {
		t.Fatal("expected error from executor; got nil")
	}
}

// TestRunExecute_StepResultWriteFailurePropagates verifies that a write failure
// from StepStatusWriter.WriteStepResult is returned as an error.
func TestRunExecute_StepResultWriteFailurePropagates(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "step-1", Capability: "node-patch"},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{failWriteResult: true}
	exec := &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseSucceeded}

	err := kernel.RunExecute(ctx, exec, sw)
	if err == nil {
		t.Fatal("expected error from WriteStepResult failure; got nil")
	}
}

// TestRunExecute_DependsOnUndeclaredStepReturnsError verifies that referencing
// a step name that does not appear before the current step in the declared order
// produces an error.
func TestRunExecute_DependsOnUndeclaredStepReturnsError(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		// "step-b" comes before "step-a" in the list but step-b depends on step-a.
		{Name: "step-b", Capability: "node-patch", DependsOn: "step-a"},
		{Name: "step-a", Capability: "node-patch"},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseSucceeded}

	err := kernel.RunExecute(ctx, exec, sw)
	if err == nil {
		t.Fatal("expected error for dependency on undeclared step; got nil")
	}
}

// TestRunExecute_StepResultCarriesStepName verifies that each StepResult written
// to the status writer carries the correct StepName field.
func TestRunExecute_StepResultCarriesStepName(t *testing.T) {
	steps := []runnerlib.RunnerConfigStep{
		{Name: "alpha", Capability: "node-patch"},
		{Name: "beta", Capability: "node-patch"},
	}
	ctx := executeCtx(steps)
	sw := &recordingStepStatusWriter{}
	exec := &fixedStepExecutor{defaultPhase: runnerlib.StepPhaseSucceeded}

	if err := kernel.RunExecute(ctx, exec, sw); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(sw.stepResults) != 2 {
		t.Fatalf("expected 2 StepResults; got %d", len(sw.stepResults))
	}
	if sw.stepResults[0].StepName != "alpha" {
		t.Errorf("result[0].StepName: got %q, want %q", sw.stepResults[0].StepName, "alpha")
	}
	if sw.stepResults[1].StepName != "beta" {
		t.Errorf("result[1].StepName: got %q, want %q", sw.stepResults[1].StepName, "beta")
	}
}
