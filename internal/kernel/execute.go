package kernel

import (
	"context"
	"fmt"

	"github.com/ontai-dev/conductor/internal/config"
	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// StepExecutor runs a single RunnerConfig step and returns the StepResult.
//
// In production: CapabilityStepExecutor dispatches to the capability registry,
// writes the OperationResult ConfigMap, and returns the harvested result.
// In unit tests: a fake implementation returns prepared results synchronously.
//
// conductor-schema.md §17.
type StepExecutor interface {
	Execute(ctx context.Context, step seamcorev1alpha1.RunnerConfigStep, clusterRef, namespace string) (seamcorev1alpha1.RunnerConfigStepResult, error)
}

// StepStatusWriter persists StepResults and terminal conditions to RunnerConfig status.
//
// In production: the real implementation writes to the RunnerConfig status
// subresource via the Kubernetes API.
// In unit tests: a recording implementation captures calls for assertions.
//
// conductor-schema.md §17.
type StepStatusWriter interface {
	WriteStepResult(ctx context.Context, result seamcorev1alpha1.RunnerConfigStepResult) error
	WriteCompleted(ctx context.Context) error
	WriteFailed(ctx context.Context, failedStep string) error
}

// NoopStepStatusWriter is a StepStatusWriter that discards all writes.
// Used in production until the full RunnerConfig status-write implementation
// lands. Satisfies the interface without requiring a Kubernetes client.
type NoopStepStatusWriter struct{}

func (NoopStepStatusWriter) WriteStepResult(_ context.Context, _ seamcorev1alpha1.RunnerConfigStepResult) error {
	return nil
}

func (NoopStepStatusWriter) WriteCompleted(_ context.Context) error { return nil }

func (NoopStepStatusWriter) WriteFailed(_ context.Context, _ string) error { return nil }

// RunExecute implements the execute-mode step sequencer.
//
// Phase 1 — Validate mode: ctx.Mode must be ModeExecute.
// Phase 2 — Validate steps: ctx.RunnerConfig.Steps must be non-empty.
// Phase 3 — Sequence: iterate steps in declared order. For each step:
//   - Check DependsOn is satisfied (referenced step reached Succeeded).
//   - Dispatch to executor.
//   - Write StepResult to status.
//   - On Failed + HaltOnFailure=true: write terminal Failed condition and stop.
//
// Phase 4 — Terminal: write Completed if all steps succeeded, Failed otherwise.
//
// The sequencer is the sole authority over step-to-step progression.
// The owning operator watches the terminal condition — it never drives steps.
// This boundary is permanent and locked. conductor-schema.md §17.
func RunExecute(ctx config.ExecutionContext, executor StepExecutor, statusWriter StepStatusWriter) error {
	// Phase 1 — Validate mode.
	if ctx.Mode != config.ModeExecute {
		ExitInvariantViolation(fmt.Sprintf(
			"RunExecute called with mode %q; expected execute", ctx.Mode))
	}

	// Phase 2 — Validate steps list.
	steps := ctx.RunnerConfig.Steps
	if len(steps) == 0 {
		return fmt.Errorf("execute mode: RunnerConfig carries no steps — step list must be non-empty")
	}

	ns := ctx.Namespace
	if ns == "" {
		ns = config.DefaultNamespace
	}

	goCtx := context.Background()

	// completed tracks the terminal phase reached by each step, keyed by step name.
	completed := make(map[string]seamcorev1alpha1.RunnerStepResultPhase, len(steps))

	for _, step := range steps {
		// Check DependsOn constraint.
		if step.DependsOn != "" {
			depPhase, seen := completed[step.DependsOn]
			if !seen {
				// DependsOn references a step not yet processed — declaration order violation.
				return fmt.Errorf(
					"execute mode: step %q dependsOn %q which has not been processed yet — steps must be declared in dependency order",
					step.Name, step.DependsOn)
			}
			if depPhase != seamcorev1alpha1.RunnerStepSucceeded {
				// Dependency did not succeed — skip this step as Failed.
				skipped := seamcorev1alpha1.RunnerConfigStepResult{
					Name:   step.Name,
					Status: seamcorev1alpha1.RunnerStepFailed,
				}
				if writeErr := statusWriter.WriteStepResult(goCtx, skipped); writeErr != nil {
					return fmt.Errorf("execute mode: write skipped StepResult for %q: %w", step.Name, writeErr)
				}
				completed[step.Name] = seamcorev1alpha1.RunnerStepFailed
				if step.HaltOnFailure {
					if termErr := statusWriter.WriteFailed(goCtx, step.Name); termErr != nil {
						return fmt.Errorf("execute mode: write terminal Failed condition: %w", termErr)
					}
					return nil
				}
				continue
			}
		}

		// Dispatch step to executor.
		result, err := executor.Execute(goCtx, step, ctx.ClusterRef, ns)
		if err != nil {
			return fmt.Errorf("execute mode: step %q executor error: %w", step.Name, err)
		}

		// Write StepResult to status.
		if writeErr := statusWriter.WriteStepResult(goCtx, result); writeErr != nil {
			return fmt.Errorf("execute mode: write StepResult for step %q: %w", step.Name, writeErr)
		}

		completed[step.Name] = result.Status

		// Handle halt-on-failure.
		if result.Status == seamcorev1alpha1.RunnerStepFailed && step.HaltOnFailure {
			if termErr := statusWriter.WriteFailed(goCtx, step.Name); termErr != nil {
				return fmt.Errorf("execute mode: write terminal Failed condition: %w", termErr)
			}
			return nil
		}
	}

	// Determine terminal condition from completed results.
	allSucceeded := true
	lastFailed := ""
	for _, s := range steps {
		if completed[s.Name] != seamcorev1alpha1.RunnerStepSucceeded {
			allSucceeded = false
			lastFailed = s.Name
		}
	}

	if allSucceeded {
		if termErr := statusWriter.WriteCompleted(goCtx); termErr != nil {
			return fmt.Errorf("execute mode: write terminal Completed condition: %w", termErr)
		}
	} else {
		if termErr := statusWriter.WriteFailed(goCtx, lastFailed); termErr != nil {
			return fmt.Errorf("execute mode: write terminal Failed condition: %w", termErr)
		}
	}

	return nil
}
