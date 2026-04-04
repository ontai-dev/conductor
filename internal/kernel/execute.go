package kernel

import (
	"context"
	"errors"
	"fmt"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
)

// RunExecute implements the execute-mode pipeline.
//
// Phase 1 — Bootstrap: validate the ExecutionContext (mode, capability name).
// Phase 2 — Capability Resolution: resolve from registry. Unknown capability →
//
//	CapabilityUnavailable structured failure.
//
// Phase 3 — Execution: dispatch to the capability handler.
// Phase 4 — Finalization: (stub) write OperationResult to ConfigMap.
//
// conductor-design.md §4.2.
func RunExecute(ctx config.ExecutionContext, reg *capability.Registry) error {
	// Phase 1 — Validate.
	if ctx.Mode != config.ModeExecute {
		ExitInvariantViolation(fmt.Sprintf(
			"RunExecute called with mode %q; expected execute", ctx.Mode))
	}

	// Phase 2 — Resolve the named capability.
	handler, err := reg.Resolve(ctx.Capability)
	if err != nil {
		if errors.Is(err, capability.ErrCapabilityUnavailable) {
			return fmt.Errorf("execute mode: capability %q is not available in this Conductor image",
				ctx.Capability)
		}
		return fmt.Errorf("execute mode: capability resolution failed: %w", err)
	}

	// Phase 3 — Execute the capability.
	params := capability.ExecuteParams{
		Capability:        ctx.Capability,
		ClusterRef:        ctx.ClusterRef,
		OperationResultCM: ctx.OperationResultCM,
	}
	result, err := handler.Execute(context.Background(), params)
	if err != nil {
		return fmt.Errorf("execute mode: capability %q returned error: %w", ctx.Capability, err)
	}

	// Phase 4 — Write OperationResult to ConfigMap (stub).
	// TODO: implement ConfigMap write in a dedicated persistence session.
	// conductor-design.md §2.8.
	_ = result
	fmt.Printf("conductor execute: capability=%q cluster=%q status=%s [configmap write pending]\n",
		ctx.Capability, ctx.ClusterRef, result.Status)

	return nil
}
