package kernel

import (
	"fmt"

	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// RunAgent implements the agent-mode pipeline.
//
// Phase 1 — Bootstrap: validate mode (refuse compile flag). INV-023.
// Phase 2 — Capability Declaration stub.
// Phase 3 — Service Initialization stub.
// Phase 4 — Continuous Operation stub (returns immediately in this build).
//
// conductor-design.md §4.3, conductor-schema.md §10.
//
// TODO: implement leader election, capability manifest publishing, receipt
// reconciliation, drift detection, admission webhook, and PermissionService in
// a dedicated Conductor Engineer session covering agent control loops.
func RunAgent(ctx config.ExecutionContext) error {
	// Phase 1 — Validate mode. compile flag is InvariantViolation. INV-023.
	GuardNotCompileMode(ctx.Mode)

	if ctx.Mode != config.ModeAgent {
		ExitInvariantViolation(fmt.Sprintf(
			"RunAgent called with mode %q; expected agent", ctx.Mode))
	}

	// Phase 2 — Capability Declaration stub.
	// TODO: acquire leader election lease, then publish capability manifest.
	manifest := runnerlib.CapabilityManifest{
		RunnerVersion: "dev",
		Entries:       []runnerlib.CapabilityEntry{},
	}
	fmt.Printf("conductor agent: cluster=%q version=%q capabilities=%d [leader election pending]\n",
		ctx.ClusterRef, manifest.RunnerVersion, len(manifest.Entries))

	// Phase 3 + 4 — Control loops stub.
	// TODO: start leader election, receipt reconciliation, drift detection,
	// admission webhook, and PermissionService gRPC server. conductor-schema.md §10.
	fmt.Printf("conductor agent: cluster=%q running [control loops not yet operational]\n",
		ctx.ClusterRef)

	// Stub: return immediately. Real implementation blocks on leader election
	// and OS signal handling. conductor-design.md §4.3 Phase 4.
	return nil
}
