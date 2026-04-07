package integration_test

// Conductor WS3 execute-mode integration tests.
//
// These tests validate that RunExecute correctly persists StepResults and
// terminal conditions to RunnerConfig status in the real Kubernetes API server.
// They use the k8sStepStatusWriter from suite_test.go to write to envtest.
//
// Scenario 1 — All steps succeed → Completed condition persists in etcd.
// Scenario 2 — Tenant-labelled RunnerConfig → same step sequencer path; no
//              signing fields appear in status (signing is agent-mode only).
// Scenario 3 — Two concurrent RunnerConfigs → no StepResult cross-contamination.
// Scenario 4 — HaltOnFailure step failure → terminal Failed condition in API
//              server; steps after the halting step have no StepResult entry.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// fakeStepExecutor is a StepExecutor that returns pre-configured results by
// step name. Steps not found in the map return Succeeded by default.
// This replaces the real capability dispatcher in integration tests — the goal
// is to verify that RunExecute writes the correct status to the API server, not
// to test capability execution (which belongs to capability unit tests).
type fakeStepExecutor struct {
	// results maps step name → phase to return. If absent, returns Succeeded.
	results map[string]runnerlib.StepPhase
}

func (f *fakeStepExecutor) Execute(
	_ context.Context,
	step runnerlib.RunnerConfigStep,
	_, _ string,
) (runnerlib.RunnerConfigStepResult, error) {
	phase := runnerlib.StepPhaseSucceeded
	if f.results != nil {
		if p, ok := f.results[step.Name]; ok {
			phase = p
		}
	}
	return runnerlib.RunnerConfigStepResult{
		StepName: step.Name,
		Phase:    phase,
	}, nil
}

// makeExecuteCtx builds a minimal execute-mode ExecutionContext.
// The RunnerConfig spec is injected directly — no env vars required.
// This mirrors how the real binary populates ExecutionContext at startup.
func makeExecuteCtx(clusterRef, ns string, spec runnerlib.RunnerConfigSpec) config.ExecutionContext {
	return config.ExecutionContext{
		Mode:       config.ModeExecute,
		ClusterRef: clusterRef,
		Namespace:  ns,
		RunnerConfig: spec,
	}
}

// TestRunExecute_ManagementRoleSteps_StepResultsPersistedInAPIServer verifies
// that RunExecute, when wired with the k8sStepStatusWriter, correctly persists
// StepResults and the Completed terminal condition to the real Kubernetes API
// server. This validates the status write path that NoopStepStatusWriter (the
// current production placeholder) cannot validate.
//
// The test uses a management-labelled RunnerConfig (clusterRef=ccs-mgmt) with
// three steps that all succeed. Signing fields are absent from status because
// signing is an agent-mode concern, not an execute-mode concern. INV-026.
//
// Scenario 1 — WS3.
func TestRunExecute_ManagementRoleSteps_StepResultsPersistedInAPIServer(t *testing.T) {
	ctx := context.Background()
	ns := "default"
	name := "ws3-mgmt-runnerconfig"
	clusterRef := "ccs-mgmt"

	spec := runnerlib.RunnerConfigSpec{
		Steps: []runnerlib.RunnerConfigStep{
			{Name: "validate-bootstrap", Capability: "cluster-validate"},
			{Name: "apply-rbac", Capability: "rbac-provision", DependsOn: "validate-bootstrap"},
			{Name: "sign-packinstance", Capability: "pack-sign", DependsOn: "apply-rbac"},
		},
	}

	createRunnerConfig(ctx, t, ns, name, spec)

	writer := &k8sStepStatusWriter{
		client:    dynamicClient,
		namespace: ns,
		name:      name,
	}
	executor := &fakeStepExecutor{} // all steps return Succeeded

	execCtx := makeExecuteCtx(clusterRef, ns, spec)
	if err := kernel.RunExecute(execCtx, executor, writer); err != nil {
		t.Fatalf("RunExecute: %v", err)
	}

	// Poll until Completed condition appears in etcd — confirms the status write
	// path persisted the terminal condition through the real API server.
	ok := poll(t, 10*time.Second, func() bool {
		obj := getRunnerConfig(ctx, t, ns, name)
		return hasCondition(obj, "Completed", "True")
	})
	if !ok {
		obj := getRunnerConfig(ctx, t, ns, name)
		t.Errorf("timed out waiting for Completed=True; status: %v", obj.Object["status"])
	}

	// Verify all three StepResults are present in etcd with Succeeded phase.
	obj := getRunnerConfig(ctx, t, ns, name)
	results := getStepResults(obj)
	if len(results) != 3 {
		t.Errorf("expected 3 StepResults in API server; got %d: %v", len(results), results)
	}
	wantSteps := []string{"validate-bootstrap", "apply-rbac", "sign-packinstance"}
	for i, want := range wantSteps {
		if i >= len(results) {
			break
		}
		if results[i]["stepName"] != want {
			t.Errorf("StepResult[%d]: got stepName=%q; want %q", i, results[i]["stepName"], want)
		}
		if results[i]["phase"] != string(runnerlib.StepPhaseSucceeded) {
			t.Errorf("StepResult[%d] %q: got phase=%q; want Succeeded", i, want, results[i]["phase"])
		}
	}
}

// TestRunExecute_TenantRoleSteps_NoSigningFieldsInStatus verifies that a
// tenant-cluster RunnerConfig (clusterRef=ccs-test) that runs through the step
// sequencer produces StepResults in the API server with no signing-authority
// fields in status. Signing is performed exclusively by the management cluster
// Conductor in agent mode. The execute-mode sequencer is role-agnostic and never
// writes signing fields — they are populated by the agent control loop. INV-026.
//
// Scenario 2 — WS3.
func TestRunExecute_TenantRoleSteps_NoSigningFieldsInStatus(t *testing.T) {
	ctx := context.Background()
	ns := "default"
	name := "ws3-tenant-runnerconfig"
	clusterRef := "ccs-test"

	spec := runnerlib.RunnerConfigSpec{
		Steps: []runnerlib.RunnerConfigStep{
			{Name: "verify-packreceipt", Capability: "pack-verify"},
			{Name: "apply-local-rbac", Capability: "rbac-provision", DependsOn: "verify-packreceipt"},
		},
	}

	createRunnerConfig(ctx, t, ns, name, spec)

	writer := &k8sStepStatusWriter{
		client:    dynamicClient,
		namespace: ns,
		name:      name,
	}
	executor := &fakeStepExecutor{}

	execCtx := makeExecuteCtx(clusterRef, ns, spec)
	if err := kernel.RunExecute(execCtx, executor, writer); err != nil {
		t.Fatalf("RunExecute: %v", err)
	}

	// Poll for Completed condition.
	ok := poll(t, 10*time.Second, func() bool {
		obj := getRunnerConfig(ctx, t, ns, name)
		return hasCondition(obj, "Completed", "True")
	})
	if !ok {
		obj := getRunnerConfig(ctx, t, ns, name)
		t.Errorf("timed out waiting for Completed=True on tenant RunnerConfig; status: %v", obj.Object["status"])
	}

	// Verify no signing-authority fields in status — signing is agent-mode only.
	obj := getRunnerConfig(ctx, t, ns, name)
	status := getRunnerConfigStatus(obj)
	if _, ok := status["packInstanceSignature"]; ok {
		t.Error("status.packInstanceSignature present in execute-mode status; expected absent (signing is agent-mode only)")
	}
	if _, ok := status["signingAuthority"]; ok {
		t.Error("status.signingAuthority present in execute-mode status; expected absent (signing is agent-mode only)")
	}

	// Verify the two tenant steps persisted.
	results := getStepResults(obj)
	if len(results) != 2 {
		t.Errorf("expected 2 StepResults in API server; got %d: %v", len(results), results)
	}
}

// TestRunExecute_TwoConcurrentRunnerConfigs_NoStepResultCrossContamination
// verifies that two RunExecute calls running concurrently against separate
// RunnerConfig objects do not contaminate each other's status in the API server.
//
// This cannot be validated with fake clients — fake clients use in-memory maps
// that share mutable state across calls within the same test run, making
// cross-contamination invisible. The real API server enforces per-object isolation
// because each status patch targets a specific resource by namespace/name.
//
// Scenario 3 — WS3.
func TestRunExecute_TwoConcurrentRunnerConfigs_NoStepResultCrossContamination(t *testing.T) {
	ctx := context.Background()
	ns := "default"

	specA := runnerlib.RunnerConfigSpec{
		Steps: []runnerlib.RunnerConfigStep{
			{Name: "step-alpha-1", Capability: "cluster-validate"},
			{Name: "step-alpha-2", Capability: "rbac-provision", DependsOn: "step-alpha-1"},
		},
	}
	specB := runnerlib.RunnerConfigSpec{
		Steps: []runnerlib.RunnerConfigStep{
			{Name: "step-beta-1", Capability: "pack-verify"},
			{Name: "step-beta-2", Capability: "pack-sign", DependsOn: "step-beta-1"},
			{Name: "step-beta-3", Capability: "cluster-validate", DependsOn: "step-beta-2"},
		},
	}

	nameA := "ws3-concurrent-alpha"
	nameB := "ws3-concurrent-beta"

	createRunnerConfig(ctx, t, ns, nameA, specA)
	createRunnerConfig(ctx, t, ns, nameB, specB)

	writerA := &k8sStepStatusWriter{client: dynamicClient, namespace: ns, name: nameA}
	writerB := &k8sStepStatusWriter{client: dynamicClient, namespace: ns, name: nameB}
	executorA := &fakeStepExecutor{}
	executorB := &fakeStepExecutor{}

	ctxA := makeExecuteCtx("ccs-alpha", ns, specA)
	ctxB := makeExecuteCtx("ccs-beta", ns, specB)

	var wg sync.WaitGroup
	errs := make([]error, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		errs[0] = kernel.RunExecute(ctxA, executorA, writerA)
	}()
	go func() {
		defer wg.Done()
		errs[1] = kernel.RunExecute(ctxB, executorB, writerB)
	}()
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("RunExecute[%d]: %v", i, err)
		}
	}

	// Poll for both Completed conditions.
	ok := poll(t, 15*time.Second, func() bool {
		objA := getRunnerConfig(ctx, t, ns, nameA)
		objB := getRunnerConfig(ctx, t, ns, nameB)
		return hasCondition(objA, "Completed", "True") && hasCondition(objB, "Completed", "True")
	})
	if !ok {
		objA := getRunnerConfig(ctx, t, ns, nameA)
		objB := getRunnerConfig(ctx, t, ns, nameB)
		t.Errorf("timed out: alpha Completed=%v, beta Completed=%v",
			hasCondition(objA, "Completed", "True"),
			hasCondition(objB, "Completed", "True"))
	}

	// Verify step results are isolated: alpha has exactly its 2 steps, beta has exactly its 3.
	objA := getRunnerConfig(ctx, t, ns, nameA)
	objB := getRunnerConfig(ctx, t, ns, nameB)
	resultsA := getStepResults(objA)
	resultsB := getStepResults(objB)

	if len(resultsA) != 2 {
		t.Errorf("alpha RunnerConfig: expected 2 StepResults; got %d: %v", len(resultsA), resultsA)
	}
	if len(resultsB) != 3 {
		t.Errorf("beta RunnerConfig: expected 3 StepResults; got %d: %v", len(resultsB), resultsB)
	}

	// Confirm no beta step names appear in alpha's results and vice versa.
	alphaStepSet := map[string]bool{"step-alpha-1": true, "step-alpha-2": true}
	betaStepSet := map[string]bool{"step-beta-1": true, "step-beta-2": true, "step-beta-3": true}

	for _, r := range resultsA {
		name := fmt.Sprintf("%v", r["stepName"])
		if betaStepSet[name] {
			t.Errorf("alpha RunnerConfig status contains beta step %q — cross-contamination detected", name)
		}
	}
	for _, r := range resultsB {
		name := fmt.Sprintf("%v", r["stepName"])
		if alphaStepSet[name] {
			t.Errorf("beta RunnerConfig status contains alpha step %q — cross-contamination detected", name)
		}
	}
}

// TestRunExecute_HaltOnFailure_TerminalFailedConditionInAPIServer verifies that
// when a step with HaltOnFailure=true fails, RunExecute:
//  1. Writes a StepResult for the failed step with phase=Failed.
//  2. Writes the terminal Failed condition to the API server.
//  3. Does NOT write StepResult entries for steps that follow the halting step.
//
// This is the critical terminal-condition test that recording StepStatusWriters
// cannot validate — the recording writer captures calls but cannot confirm the
// terminal condition persists in etcd and that subsequent steps truly have no
// entries (no API race).
//
// Scenario 4 — WS3.
func TestRunExecute_HaltOnFailure_TerminalFailedConditionInAPIServer(t *testing.T) {
	ctx := context.Background()
	ns := "default"
	name := "ws3-halt-runnerconfig"
	clusterRef := "ccs-mgmt"

	spec := runnerlib.RunnerConfigSpec{
		Steps: []runnerlib.RunnerConfigStep{
			{Name: "pre-flight", Capability: "cluster-validate"},
			{Name: "critical-step", Capability: "rbac-provision", DependsOn: "pre-flight", HaltOnFailure: true},
			{Name: "post-step", Capability: "pack-sign", DependsOn: "critical-step"},
		},
	}

	createRunnerConfig(ctx, t, ns, name, spec)

	writer := &k8sStepStatusWriter{
		client:    dynamicClient,
		namespace: ns,
		name:      name,
	}

	// Executor returns Failed for "critical-step", Succeeded for all others.
	executor := &fakeStepExecutor{
		results: map[string]runnerlib.StepPhase{
			"critical-step": runnerlib.StepPhaseFailed,
		},
	}

	execCtx := makeExecuteCtx(clusterRef, ns, spec)
	if err := kernel.RunExecute(execCtx, executor, writer); err != nil {
		t.Fatalf("RunExecute: %v", err)
	}

	// Poll for Failed terminal condition in etcd.
	ok := poll(t, 10*time.Second, func() bool {
		obj := getRunnerConfig(ctx, t, ns, name)
		return hasCondition(obj, "Failed", "True")
	})
	if !ok {
		obj := getRunnerConfig(ctx, t, ns, name)
		t.Errorf("timed out waiting for Failed=True; status: %v", obj.Object["status"])
	}

	obj := getRunnerConfig(ctx, t, ns, name)

	// Completed must be absent — the run did not succeed.
	if hasCondition(obj, "Completed", "True") {
		t.Error("Completed=True present in status after HaltOnFailure; expected absent")
	}

	// Verify StepResults: pre-flight (Succeeded) + critical-step (Failed) only.
	// post-step must NOT have a StepResult entry — RunExecute halted before it ran.
	results := getStepResults(obj)
	if len(results) != 2 {
		t.Errorf("expected 2 StepResults (pre-flight + critical-step); got %d: %v", len(results), results)
	}

	for _, r := range results {
		if r["stepName"] == "post-step" {
			t.Error("post-step has a StepResult entry in status; expected absent after HaltOnFailure")
		}
	}

	// Confirm critical-step result is Failed.
	var criticalResult map[string]interface{}
	for _, r := range results {
		if r["stepName"] == "critical-step" {
			criticalResult = r
			break
		}
	}
	if criticalResult == nil {
		t.Error("critical-step StepResult not found in status")
	} else if criticalResult["phase"] != string(runnerlib.StepPhaseFailed) {
		t.Errorf("critical-step phase: got %q; want Failed", criticalResult["phase"])
	}
}
