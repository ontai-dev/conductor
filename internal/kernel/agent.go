package kernel

import (
	"context"
	"fmt"
	"os"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"github.com/ontai-dev/conductor/internal/agent"
	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/permissionservice"
	"github.com/ontai-dev/conductor/internal/webhook"
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

	// Construct the receipt reconciler. When SIGNING_PUBLIC_KEY_PATH is set,
	// use the mounted Ed25519 public key for INV-026 signature enforcement.
	// When absent, operate in bootstrap window mode (INV-020).
	var reconciler *agent.ReceiptReconciler
	if keyPath := os.Getenv("SIGNING_PUBLIC_KEY_PATH"); keyPath != "" {
		var err error
		reconciler, err = agent.NewReceiptReconcilerWithKey(dynamicClient, ns, keyPath)
		if err != nil {
			return fmt.Errorf("conductor agent: build receipt reconciler with signing key: %w", err)
		}
	} else {
		reconciler = agent.NewReceiptReconciler(dynamicClient, ns)
	}

	// Construct the signing loop (management cluster only). When
	// SIGNING_PRIVATE_KEY_PATH is set, the agent is on the management cluster
	// and signs PackInstance and PermissionSnapshot CRs with the platform key.
	// conductor-schema.md §10 steps 9–10. INV-026.
	var signingLoop *agent.SigningLoop
	if privKeyPath := os.Getenv("SIGNING_PRIVATE_KEY_PATH"); privKeyPath != "" {
		var err error
		signingLoop, err = agent.NewSigningLoop(dynamicClient, privKeyPath)
		if err != nil {
			return fmt.Errorf("conductor agent: build signing loop: %w", err)
		}
		fmt.Printf("conductor agent: cluster=%q signing loop enabled (management cluster)\n",
			execCtx.ClusterRef)
	}

	// Phase 3 — Start the local PermissionService gRPC server.
	// Serves authorization decisions from the local acknowledged PermissionSnapshot
	// without requiring management cluster connectivity. All clusters (management
	// and target) start this server. conductor-schema.md §10 step 6.
	permSvcAddr := os.Getenv("PERMISSION_SERVICE_ADDR")
	if permSvcAddr == "" {
		permSvcAddr = ":50051"
	}
	snapshotStore := permissionservice.NewSnapshotStore()
	localSvc := permissionservice.NewLocalService(snapshotStore)
	go func() {
		if err := permissionservice.ListenAndServe(goCtx, permSvcAddr, localSvc); err != nil {
			fmt.Printf("conductor agent: cluster=%q permission service error: %v\n",
				execCtx.ClusterRef, err)
		}
	}()
	fmt.Printf("conductor agent: cluster=%q starting local PermissionService on %s\n",
		execCtx.ClusterRef, permSvcAddr)

	// Construct the PermissionSnapshot pull loop (target clusters only).
	// When MGMT_KUBECONFIG_PATH is set, this Conductor is on a target cluster and
	// must pull PermissionSnapshots from the management cluster, verify their
	// Ed25519 signatures (INV-026), and populate the local SnapshotStore.
	// conductor-schema.md §10 step 8, conductor-design.md §2.10.
	var snapshotPullLoop *agent.SnapshotPullLoop
	if mgmtKubeconfigPath := os.Getenv("MGMT_KUBECONFIG_PATH"); mgmtKubeconfigPath != "" {
		mgmtConfig, err := clientcmd.BuildConfigFromFlags("", mgmtKubeconfigPath)
		if err != nil {
			return fmt.Errorf("conductor agent: build management cluster REST config: %w", err)
		}
		mgmtDynamicClient, err := dynamic.NewForConfig(mgmtConfig)
		if err != nil {
			return fmt.Errorf("conductor agent: build management cluster dynamic client: %w", err)
		}
		if pubKeyPath := os.Getenv("SIGNING_PUBLIC_KEY_PATH"); pubKeyPath != "" {
			snapshotPullLoop, err = agent.NewSnapshotPullLoopWithKey(
				mgmtDynamicClient, dynamicClient, snapshotStore,
				execCtx.ClusterRef, ns, pubKeyPath,
			)
			if err != nil {
				return fmt.Errorf("conductor agent: build snapshot pull loop with signing key: %w", err)
			}
		} else {
			snapshotPullLoop = agent.NewSnapshotPullLoop(
				mgmtDynamicClient, dynamicClient, snapshotStore,
				execCtx.ClusterRef, ns,
			)
		}
		fmt.Printf("conductor agent: cluster=%q snapshot pull loop enabled (target cluster)\n",
			execCtx.ClusterRef)
	}

	// Phase 3 — Start the SealedCausalChain admission webhook server.
	// The webhook enforces spec.lineage immutability on all Seam-managed CRDs.
	// seam-core-schema.md §5, CLAUDE.md §14 Decision 1.
	// Runs on all replicas (not gated by leader election) — the webhook must serve
	// requests regardless of whether this replica holds the leader lease.
	// WEBHOOK_TLS_CERT_PATH, WEBHOOK_TLS_KEY_PATH, and WEBHOOK_ADDR are read from
	// the environment. When cert/key are not configured the webhook is skipped
	// (development/test mode).
	certPath := os.Getenv("WEBHOOK_TLS_CERT_PATH")
	keyPath := os.Getenv("WEBHOOK_TLS_KEY_PATH")
	if certPath != "" && keyPath != "" {
		webhookAddr := os.Getenv("WEBHOOK_ADDR")
		if webhookAddr == "" {
			webhookAddr = ":8443"
		}
		wh := webhook.NewWebhookServer(webhookAddr, certPath, keyPath)
		go func() {
			if err := wh.Start(goCtx, certPath, keyPath); err != nil {
				fmt.Printf("conductor agent: cluster=%q webhook server error: %v\n",
					execCtx.ClusterRef, err)
			}
		}()
		fmt.Printf("conductor agent: cluster=%q starting admission webhook on %s\n",
			execCtx.ClusterRef, webhookAddr)
	} else {
		fmt.Printf("conductor agent: cluster=%q WEBHOOK_TLS_CERT_PATH/WEBHOOK_TLS_KEY_PATH not set — webhook disabled\n",
			execCtx.ClusterRef)
	}

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
				onLeaderStart(leaderCtx, execCtx.ClusterRef, manifest, publisher, reconciler, signingLoop, snapshotPullLoop)
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
// It publishes the capability manifest, then starts all write-path goroutines.
// Blocks until leaderCtx is cancelled. conductor-design.md §2.10.
func onLeaderStart(
	leaderCtx context.Context,
	clusterRef string,
	manifest runnerlib.CapabilityManifest,
	publisher *agent.CapabilityPublisher,
	reconciler *agent.ReceiptReconciler,
	signingLoop *agent.SigningLoop,
	snapshotPullLoop *agent.SnapshotPullLoop,
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

	const reconcileInterval = 30 * time.Second
	const signingInterval = 30 * time.Second

	// Start receipt reconciliation loop as a goroutine.
	// conductor-schema.md §10 step 4, conductor-design.md §2.10.
	go func() {
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
	}()

	// Start signing loop (management cluster only).
	// conductor-schema.md §10 steps 9–10. INV-026.
	if signingLoop != nil {
		go signingLoop.Run(leaderCtx, signingInterval)
	}

	// Start PermissionSnapshot pull loop (target clusters only).
	// Pulls from management cluster, verifies signature (INV-026), populates
	// local SnapshotStore for PermissionService. conductor-schema.md §10 step 8.
	if snapshotPullLoop != nil {
		go snapshotPullLoop.Run(leaderCtx, signingInterval)
	}

	// Block until leadership is lost.
	<-leaderCtx.Done()
}
