package kernel

import (
	"context"
	"errors"
	"fmt"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/persistence"
)

// RunExecute implements the execute-mode pipeline.
//
// Phase 1 — Bootstrap: validate the ExecutionContext (mode, capability name).
// Phase 2 — Capability Resolution: resolve from registry. Unknown capability →
//
//	CapabilityUnavailable structured failure.
//
// Phase 3 — Execution: dispatch to the capability handler.
// Phase 4 — Finalization: write OperationResult JSON to the named ConfigMap.
//
// clients is injected by the caller; fields may be nil in tests, real in production.
// The writer parameter satisfies the ConfigMapWriter interface. Production callers
// pass persistence.NewKubeConfigMapWriter(client); unit tests pass
// persistence.NoopConfigMapWriter{} or a recording fake.
//
// conductor-design.md §4.2, conductor-schema.md §8.
func RunExecute(ctx config.ExecutionContext, reg *capability.Registry, writer persistence.ConfigMapWriter, clients capability.ExecuteClients) error {
	// Phase 1 — Validate mode.
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

	ns := ctx.Namespace
	if ns == "" {
		ns = config.DefaultNamespace
	}

	// Phase 3 — Execute the capability.
	params := capability.ExecuteParams{
		Capability:        ctx.Capability,
		ClusterRef:        ctx.ClusterRef,
		OperationResultCM: ctx.OperationResultCM,
		Namespace:         ns,
		ExecuteClients:    clients,
	}
	result, err := handler.Execute(context.Background(), params)
	if err != nil {
		return fmt.Errorf("execute mode: capability %q returned error: %w", ctx.Capability, err)
	}

	// Phase 4 — Write OperationResult to the named ConfigMap.
	// This is the only output channel between operator and Conductor Job.
	// conductor-schema.md §8, conductor-design.md §2.8.
	if writeErr := writer.WriteResult(context.Background(), ns, ctx.OperationResultCM, result); writeErr != nil {
		return fmt.Errorf("execute mode: write OperationResult to ConfigMap %q in %q: %w",
			ctx.OperationResultCM, ns, writeErr)
	}

	return nil
}
