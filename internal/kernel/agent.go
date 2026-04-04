package kernel

import (
	"context"
	"fmt"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/ontai-dev/conductor/internal/agent"
	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// agentVersion is the version string stamped into the capability manifest.
// Production builds override this via -ldflags -X.
const agentVersion = "dev"

// RunAgent implements the agent-mode pipeline.
//
// Phase 1 — Bootstrap: validate mode (refuse compile flag). INV-023.
// Phase 2 — Capability Declaration: on leader win, publish capability manifest
//
//	to RunnerConfig status. conductor-schema.md §10 step 3.
//
// Phase 3 — Service Initialization: start control loop goroutines. conductor-schema.md §10 steps 4-8.
// Phase 4 — Continuous Operation: leader election blocks until ctx is cancelled.
//
// The call blocks until goCtx is cancelled (OS signal or test cancellation).
// Returns nil on clean shutdown. conductor-design.md §4.3, conductor-schema.md §10.
func RunAgent(goCtx context.Context, execCtx config.ExecutionContext, client kubernetes.Interface, dynamicClient dynamic.Interface) error {
	// Phase 1 — Validate mode. compile flag is InvariantViolation. INV-023.
	GuardNotCompileMode(execCtx.Mode)

	if execCtx.Mode != config.ModeAgent {
		ExitInvariantViolation(fmt.Sprintf(
			"RunAgent called with mode %q; expected agent", execCtx.Mode))
	}

	ns := execCtx.Namespace
	if ns == "" {
		ns = config.DefaultNamespace
	}

	// Build the capability manifest from all registered execute-mode capabilities.
	// conductor-schema.md §10 step 3, conductor-design.md §2.10.
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	names := reg.RegisteredNames()
	manifest := agent.BuildManifest(names, agentVersion)

	publisher := agent.NewCapabilityPublisher(dynamicClient, ns)
	reconciler := agent.NewReceiptReconciler(dynamicClient, ns)

	// Phase 4 — Run leader election. Blocks until goCtx is cancelled.
	// On leader win: run capability publisher once, then start receipt reconciler loop.
	// On leader loss: goroutines see their context cancelled and stop.
	// conductor-design.md §8, conductor-schema.md §10.
	return agent.RunLeaderElection(
		goCtx,
		client,
		ns,
		execCtx.ClusterRef,
		"", // identity: resolved from hostname inside RunLeaderElection
		agent.LeaderCallbacks{
			OnStartedLeading: func(leaderCtx context.Context) {
				onLeaderStart(leaderCtx, execCtx.ClusterRef, manifest, publisher, reconciler)
			},
			OnStoppedLeading: func() {
				fmt.Printf("conductor agent: cluster=%q lost leadership — entering standby\n",
					execCtx.ClusterRef)
			},
			OnNewLeader: func(identity string) {
				fmt.Printf("conductor agent: cluster=%q new leader observed: %s\n",
					execCtx.ClusterRef, identity)
			},
		},
	)
}

// onLeaderStart is invoked by RunLeaderElection when this replica wins the lease.
// It publishes the capability manifest and then drives the receipt reconciliation
// loop until leaderCtx is cancelled. conductor-design.md §2.10.
func onLeaderStart(
	leaderCtx context.Context,
	clusterRef string,
	manifest runnerlib.CapabilityManifest,
	publisher *agent.CapabilityPublisher,
	reconciler *agent.ReceiptReconciler,
) {
	// Publish capability manifest to RunnerConfig status (one-shot on leader start).
	// Failure is logged but does not abort — the agent retries on the next leader
	// acquisition cycle. conductor-schema.md §10 step 3.
	if err := publisher.Publish(leaderCtx, clusterRef, agentVersion, "", manifest); err != nil {
		fmt.Printf("conductor agent: cluster=%q capability publish failed: %v\n", clusterRef, err)
	} else {
		fmt.Printf("conductor agent: cluster=%q published capability manifest (%d capabilities)\n",
			clusterRef, len(manifest.Entries))
	}

	// Run receipt reconciliation loop until leadership is lost.
	// conductor-schema.md §10 step 4, conductor-design.md §2.10.
	const reconcileInterval = 30 * time.Second
	ticker := time.NewTicker(reconcileInterval)
	defer ticker.Stop()

	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-ticker.C:
			if err := reconciler.Reconcile(leaderCtx); err != nil {
				fmt.Printf("conductor agent: cluster=%q receipt reconcile error: %v\n",
					clusterRef, err)
			}
		}
	}
}
