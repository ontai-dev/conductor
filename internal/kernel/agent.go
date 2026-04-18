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
	"github.com/ontai-dev/conductor/internal/federation"
	"github.com/ontai-dev/conductor/internal/permissionservice"
	"github.com/ontai-dev/conductor/internal/webhook"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// agentVersion is the version string stamped into the capability manifest.
// Production builds override this via -ldflags -X.
const agentVersion = "dev"

// PermissionServiceEnabled reports whether a Conductor with the given role should
// start the local PermissionService gRPC server.
//
// Management cluster: returns false. PermissionService on the management cluster
// is owned by Guardian — Conductor must not start a competing instance.
//
// Tenant cluster: returns true. Each tenant Conductor serves authorization
// decisions locally from the acknowledged PermissionSnapshot without requiring
// management cluster connectivity. conductor-schema.md §15, §10 step 6.
func PermissionServiceEnabled(role Role) bool {
	return role == RoleTenant
}

// WebhookEnabled reports whether a Conductor with the given role should start the
// admission webhook server.
//
// Management cluster: returns false. The admission webhook on the management
// cluster is owned by Guardian — Conductor must not start a competing instance.
//
// Tenant cluster: returns true. Each tenant Conductor runs a local admission
// webhook that intercepts RBAC resources and enforces ontai.dev/rbac-owner=guardian.
// conductor-schema.md §15.
func WebhookEnabled(role Role) bool {
	return role == RoleTenant
}

// RunAgent implements the agent-mode pipeline.
//
// Phase 0 — Role resolution: read CONDUCTOR_ROLE and exit with InvariantViolation
//
//	if absent or unrecognized. conductor-schema.md §15.
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
	// Phase 0 — Resolve role. Exit immediately if absent or unrecognised.
	// An unresolved role is a programming error — no reconciliation loop may run
	// without a valid role declaration. conductor-schema.md §15. INV-026.
	role, roleErr := ParseRole(os.Getenv)
	if roleErr != nil {
		ExitInvariantViolation("CONDUCTOR_ROLE must be management or tenant: " + roleErr.Error())
	}

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

	// Phase 3 — PermissionService gRPC server (tenant clusters only).
	// Management cluster: PermissionService is owned by Guardian. Conductor must
	// not start a competing instance. conductor-schema.md §15.
	// Tenant clusters: serves authorization decisions from the local acknowledged
	// PermissionSnapshot without requiring management cluster connectivity.
	// conductor-schema.md §10 step 6.
	//
	// snapshotStore is constructed unconditionally — both role paths need it for
	// the federation pull loop and FederationClient.
	snapshotStore := permissionservice.NewSnapshotStore()
	if PermissionServiceEnabled(role) {
		permSvcAddr := os.Getenv("PERMISSION_SERVICE_ADDR")
		if permSvcAddr == "" {
			permSvcAddr = ":50051"
		}
		localSvc := permissionservice.NewLocalService(snapshotStore)
		go func() {
			if err := permissionservice.ListenAndServe(goCtx, permSvcAddr, localSvc); err != nil {
				fmt.Printf("conductor agent: cluster=%q permission service error: %v\n",
					execCtx.ClusterRef, err)
			}
		}()
		fmt.Printf("conductor agent: cluster=%q starting local PermissionService on %s\n",
			execCtx.ClusterRef, permSvcAddr)
	} else {
		fmt.Printf("conductor agent: cluster=%q role=management: PermissionService owned by Guardian, skipping\n",
			execCtx.ClusterRef)
	}

	// Construct the target-cluster pull loops (PermissionSnapshot + PackInstance).
	// When MGMT_KUBECONFIG_PATH is set, this Conductor is on a target cluster and
	// must pull artifacts from the management cluster, verify Ed25519 signatures
	// (INV-026), and update local state. conductor-schema.md §10, Gap 28.
	var snapshotPullLoop *agent.SnapshotPullLoop
	var packInstancePullLoop *agent.PackInstancePullLoop
	if mgmtKubeconfigPath := os.Getenv("MGMT_KUBECONFIG_PATH"); mgmtKubeconfigPath != "" {
		mgmtConfig, err := clientcmd.BuildConfigFromFlags("", mgmtKubeconfigPath)
		if err != nil {
			return fmt.Errorf("conductor agent: build management cluster REST config: %w", err)
		}
		mgmtDynamicClient, err := dynamic.NewForConfig(mgmtConfig)
		if err != nil {
			return fmt.Errorf("conductor agent: build management cluster dynamic client: %w", err)
		}

		pubKeyPath := os.Getenv("SIGNING_PUBLIC_KEY_PATH")

		// PermissionSnapshot pull loop — populates SnapshotStore for local gRPC.
		// conductor-schema.md §10 step 8, conductor-design.md §2.10.
		if pubKeyPath != "" {
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

		// PackInstance pull loop — verifies signed artifacts and writes PackReceipts.
		// Gap 28, INV-026.
		if pubKeyPath != "" {
			packInstancePullLoop, err = agent.NewPackInstancePullLoopWithKey(
				mgmtDynamicClient, dynamicClient,
				execCtx.ClusterRef, ns, pubKeyPath,
			)
			if err != nil {
				return fmt.Errorf("conductor agent: build packinstance pull loop with signing key: %w", err)
			}
		} else {
			packInstancePullLoop = agent.NewPackInstancePullLoop(
				mgmtDynamicClient, dynamicClient,
				execCtx.ClusterRef, ns,
			)
		}
		fmt.Printf("conductor agent: cluster=%q packinstance pull loop enabled (target cluster)\n",
			execCtx.ClusterRef)
	}

	// Phase 3b — Start the federation channel listener/client.
	// Management Conductor: start FederationServer when FEDERATION_CA_CERT_PATH,
	// FEDERATION_SERVER_CERT_PATH, and FEDERATION_SERVER_KEY_PATH are all set.
	// Tenant Conductor: start FederationClient when MGMT_FEDERATION_ADDR is set.
	// conductor-schema.md §18.
	fedCACertPath := os.Getenv("FEDERATION_CA_CERT_PATH")
	fedServerCertPath := os.Getenv("FEDERATION_SERVER_CERT_PATH")
	fedServerKeyPath := os.Getenv("FEDERATION_SERVER_KEY_PATH")
	mgmtFedAddr := os.Getenv("MGMT_FEDERATION_ADDR")
	fedClientCertPath := os.Getenv("FEDERATION_CLIENT_CERT_PATH")
	fedClientKeyPath := os.Getenv("FEDERATION_CLIENT_KEY_PATH")

	if fedCACertPath != "" && fedServerCertPath != "" && fedServerKeyPath != "" {
		// Management Conductor: start the federation server.
		fedServer, fedErr := federation.NewFederationServer(fedCACertPath, fedServerCertPath, fedServerKeyPath, nil)
		if fedErr != nil {
			return fmt.Errorf("conductor agent: build federation server: %w", fedErr)
		}
		fedPort := os.Getenv("FEDERATION_PORT")
		if fedPort == "" {
			fedPort = federation.DefaultFederationPort
		}
		go func() {
			if err := fedServer.Start(goCtx, fedPort); err != nil {
				fmt.Printf("conductor agent: cluster=%q federation server error: %v\n",
					execCtx.ClusterRef, err)
			}
		}()
		fmt.Printf("conductor agent: cluster=%q starting federation server on %s (management role)\n",
			execCtx.ClusterRef, fedPort)
	}

	if mgmtFedAddr != "" && fedCACertPath != "" && fedClientCertPath != "" && fedClientKeyPath != "" {
		// Tenant Conductor: open the WAL and start the federation client.
		walPath := os.Getenv("WAL_PATH")
		if walPath == "" {
			walPath = federation.DefaultWALPath
		}
		walMaxStr := os.Getenv("WAL_MAX_BYTES")
		walMax := int64(federation.DefaultWALMaxBytes)
		if walMaxStr != "" {
			if n, err := fmt.Sscanf(walMaxStr, "%d", &walMax); n != 1 || err != nil {
				fmt.Printf("conductor agent: invalid WAL_MAX_BYTES %q — using default\n", walMaxStr)
				walMax = federation.DefaultWALMaxBytes
			}
		}

		wal, walErr := federation.OpenWAL(walPath, walMax)
		if walErr != nil {
			// WAL failure is non-fatal — log and continue without WAL buffering.
			fmt.Printf("conductor agent: cluster=%q WAL open failed: %v — proceeding without WAL\n",
				execCtx.ClusterRef, walErr)
		}

		fedClient := federation.NewFederationClient(
			mgmtFedAddr, fedClientCertPath, fedClientKeyPath, fedCACertPath,
			execCtx.ClusterRef, snapshotStore,
		)
		if wal != nil {
			fedClient.SetWAL(wal)
		}
		go fedClient.Run(goCtx)
		fmt.Printf("conductor agent: cluster=%q starting federation client → %s (tenant role)\n",
			execCtx.ClusterRef, mgmtFedAddr)
	}

	// Phase 3 — Admission webhook server (tenant clusters only).
	// Management cluster: the admission webhook is owned by Guardian. Conductor
	// must not start a competing instance. conductor-schema.md §15.
	// Tenant clusters: enforces ontai.dev/rbac-owner=guardian annotation on all
	// RBAC resources via a local webhook. Runs on all replicas (not gated by
	// leader election) so the webhook serves requests without leadership dependency.
	// WEBHOOK_TLS_CERT_PATH, WEBHOOK_TLS_KEY_PATH, and WEBHOOK_ADDR are read from
	// the environment. When cert/key are absent the webhook is skipped even on
	// tenant clusters (development/test mode). seam-core-schema.md §5.
	if WebhookEnabled(role) {
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
	} else {
		fmt.Printf("conductor agent: cluster=%q role=management: admission webhook owned by Guardian, skipping\n",
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
				onLeaderStart(leaderCtx, execCtx.ClusterRef, manifest, publisher, reconciler, signingLoop, snapshotPullLoop, packInstancePullLoop)
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
	manifest []runnerlib.CapabilityEntry,
	publisher *agent.CapabilityPublisher,
	reconciler *agent.ReceiptReconciler,
	signingLoop *agent.SigningLoop,
	snapshotPullLoop *agent.SnapshotPullLoop,
	packInstancePullLoop *agent.PackInstancePullLoop,
) {
	// Publish capability manifest to RunnerConfig status with background retry.
	// If the RunnerConfig does not yet exist (Platform creates it after Conductor
	// starts), the initial attempt fails and a goroutine retries every 30s until
	// it succeeds. conductor-schema.md §10 step 3.
	publisher.PublishWithRetry(leaderCtx, clusterRef, agentVersion, "", manifest)
	fmt.Printf("conductor agent: cluster=%q capability publish initiated (%d capabilities)\n",
		clusterRef, len(manifest))

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

	// Start PackInstance pull loop (target clusters only).
	// Pulls signed artifact Secrets from management cluster, verifies Ed25519
	// signatures (INV-026), and writes PackReceipt CRs on the local cluster.
	// Gap 28, conductor-schema.md §10.
	if packInstancePullLoop != nil {
		go packInstancePullLoop.Run(leaderCtx, signingInterval)
	}

	// Block until leadership is lost.
	<-leaderCtx.Done()
}
