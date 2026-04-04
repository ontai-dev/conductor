package capability

import (
	"context"
	"fmt"
	"time"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// stubHandler is a placeholder capability implementation returned for all named
// capabilities in the current build. Each stub returns a failed OperationResult
// with ExecutionFailure and a "not yet implemented" reason. This ensures that:
//  1. All capabilities are registered and resolvable (no CapabilityUnavailable).
//  2. The dispatcher, registry, and execute-mode pipeline are fully testable.
//  3. Actual capability implementations are added in future sessions without
//     modifying the registry or dispatcher.
//
// conductor-design.md §5 — capability authoring will replace stubs individually.
type stubHandler struct {
	name string
}

func (h *stubHandler) Execute(_ context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now()
	return runnerlib.OperationResultSpec{
		Capability: h.name,
		Status:     runnerlib.ResultFailed,
		StartedAt:   now,
		CompletedAt: now,
		Artifacts:   []runnerlib.ArtifactRef{},
		FailureReason: &runnerlib.FailureReason{
			Category: runnerlib.ExecutionFailure,
			Reason:   fmt.Sprintf("capability %q is not yet implemented in this build", h.name),
		},
		Steps: []runnerlib.StepResult{},
	}, nil
}

// stub returns a stubHandler for the given capability name.
func stub(name string) Handler {
	return &stubHandler{name: name}
}

// RegisterAll populates the registry with stub handlers for every named
// capability declared in conductor-schema.md §6. Registration is static at
// build time. CR-INV-004, conductor-design.md §2.3.
//
// When a capability is fully implemented, replace stub(name) with the real
// handler — no other change is required.
func RegisterAll(reg *Registry) {
	// Platform capabilities — cluster lifecycle and operations.
	reg.Register(runnerlib.CapabilityBootstrap, stub(runnerlib.CapabilityBootstrap))
	reg.Register(runnerlib.CapabilityTalosUpgrade, stub(runnerlib.CapabilityTalosUpgrade))
	reg.Register(runnerlib.CapabilityKubeUpgrade, stub(runnerlib.CapabilityKubeUpgrade))
	reg.Register(runnerlib.CapabilityStackUpgrade, stub(runnerlib.CapabilityStackUpgrade))
	reg.Register(runnerlib.CapabilityNodePatch, stub(runnerlib.CapabilityNodePatch))
	reg.Register(runnerlib.CapabilityNodeScaleUp, stub(runnerlib.CapabilityNodeScaleUp))
	reg.Register(runnerlib.CapabilityNodeDecommission, stub(runnerlib.CapabilityNodeDecommission))
	reg.Register(runnerlib.CapabilityNodeReboot, stub(runnerlib.CapabilityNodeReboot))
	reg.Register(runnerlib.CapabilityEtcdBackup, stub(runnerlib.CapabilityEtcdBackup))
	reg.Register(runnerlib.CapabilityEtcdMaintenance, stub(runnerlib.CapabilityEtcdMaintenance))
	reg.Register(runnerlib.CapabilityEtcdRestore, stub(runnerlib.CapabilityEtcdRestore))
	reg.Register(runnerlib.CapabilityPKIRotate, stub(runnerlib.CapabilityPKIRotate))
	reg.Register(runnerlib.CapabilityCredentialRotate, stub(runnerlib.CapabilityCredentialRotate))
	reg.Register(runnerlib.CapabilityHardeningApply, stub(runnerlib.CapabilityHardeningApply))
	reg.Register(runnerlib.CapabilityClusterReset, stub(runnerlib.CapabilityClusterReset))

	// Wrapper capabilities — pack delivery.
	reg.Register(runnerlib.CapabilityPackDeploy, stub(runnerlib.CapabilityPackDeploy))

	// Guardian capabilities — RBAC plane.
	reg.Register(runnerlib.CapabilityRBACProvision, stub(runnerlib.CapabilityRBACProvision))

	// Note: CapabilityPackCompile is NOT registered here. pack-compile is a
	// Compiler compile-mode invocation only — it never runs as a Conductor Job.
	// Registering it here would be a schema violation. conductor-schema.md §6.
}
