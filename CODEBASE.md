# conductor: Codebase Reference

## 1. Purpose

Conductor is three binaries from one repo (Decision 12). The **compiler** (`cmd/compiler`) is the offline compilation tool: bootstrap, enable, packbuild, launch, maintenance, component, domain subcommands. The **conductor-execute** binary (`cmd/conductor`, execute mode) runs named capabilities as short-lived Kueue Jobs on the management cluster. The **conductor** binary (`cmd/conductor`, agent mode) is the long-running governance agent deployed to `ont-system` on every cluster. No shell. No scripts. No execution logic in operators.

---

## 2. Key Files and Locations

### Compiler (`cmd/compiler/`)

| File | Key Symbols | What it does |
|------|-------------|--------------|
| `compile.go` | `PackBuildInput` (L498), `ClusterInput` (L382), `BootstrapSection` (L191), `compileBootstrap()`, `compilePackBuild()` | Entry points for bootstrap/packbuild; ClusterInput parsed from YAML; dispatch to helm or raw path |
| `compile_enable.go` | `compileEnableBundle(output, version, registry, kubeconfig, withCAPI, clusterName, dsnsIP, clusterRole, mgmtSigningPublicKey, signingPrivateKey, outputPublicKey)` (L346), `writePhase4Conductor()` (L1620), `writeConductorSigningKeySecret(dir, signingPrivateKeyPath, outputPublicKeyPath)` (L2950) | Orchestrates 5-phase enable bundle; signs key material; enforces INV-026 for tenant clusters |
| `compile_packbuild_helm.go` | `HelmSource` struct (URL, Chart, Version, ValuesFile, Namespace, RegistryCredentialsSecret, HelmVersion), `helmCompilePackBuild()`, `helmVersionOrDefault()` | Fetches chart, renders, splits RBAC/cluster-scoped/workload layers, pushes OCI, emits ClusterPack CR; HelmVersion override via `HelmSource.HelmVersion` (T-11) |
| `compile_packbuild_raw.go` | `RawSource` struct (Path), `rawCompilePackBuild()` | Reads all .yaml/.yml from RawSource.Path, splits into 3 OCI layers, pushes, emits ClusterPack CR |
| `compile_packbuild_kustomize.go` | `KustomizeSource` struct (Path), `kustomizeCompilePackBuild()` | Runs krusty.Kustomizer on overlay dir; splits rendered YAML into 3 OCI layers; pushes; emits ClusterPack CR. T-12 |

**`PackBuildInput` struct fields**: Name, Namespace, Version, RegistryURL, Digest, Checksum, SourceBuildRef, TargetClusters, RBACDigest, ClusterScopedDigest, WorkloadDigest, BasePackName, ValuesFile, Category `string`, HelmSource `*HelmSource`, RawSource `*RawSource`, KustomizeSource `*KustomizeSource`. Category-driven dispatch (T-05/T-13); backward-compatible nil-check dispatch when Category absent.

**`compileEnableBundle` phases**: Phase 00 (seam-core CRDs), Phase 00a (namespaces), Phase 01 (guardian bootstrap), Phase 02 (guardian deploy), Phase 03 (platform-wrapper), Phase 04 (conductor), Phase 05 (post-bootstrap). When `--cluster-role=tenant`: phases 01-03 skipped. `--signing-private-key` flag rejected for tenant (INV-026).

### Agent Mode (`internal/agent/`)

| File | Key Struct / Function | Role | What it does |
|------|-----------------------|------|--------------|
| `pack_receipt_drift_loop.go` | `PackReceiptDriftLoop`, `NewPackReceiptDriftLoop()` (L64), `checkDrift()` (L237), `teardownOrphanedReceipt()` (L155), `emitDriftSignal()` (L290), `namespaceTerminating()` | tenant | Reads `spec.deployedResources` from PackReceipt; verifies each resource exists on local cluster; emits DriftSignal to `seam-tenant-{clusterName}` on management cluster; tears down orphaned receipts when ClusterPack deleted. `namespaceTerminating()` checks the namespace DeletionTimestamp via local dynamic client -- when true, `checkDrift()` skips emitting a drift signal for resources in that namespace (terminating-state guard). |
| `drift_signal_handler.go` | `DriftSignalHandler`, `NewDriftSignalHandler()` (L38), `handleOnce()` (L61), `retriggerPackExecution()` (L116) | management | Periodic loop; lists DriftSignals in `seam-tenant-*`; on state=pending deletes PackExecution (wrapper recreates it); sets state=queued; at escalationThreshold sets TerminalDrift |
| `packinstance_pull_loop.go` | `PackInstancePullLoop`, `NewPackInstancePullLoop()`, `NewPackInstancePullLoopWithKey()`, `buildReceiptSpecPayload()`, `upsertPackReceipt()`, `verifyArtifact()`, `deleteOrphanedResources()`, `deployedResourceKey()` | tenant | Pulls signed PackInstance artifact Secrets from management cluster; verifies Ed25519 (INV-026); on version upgrade calls `deleteOrphanedResources()` to delete resources absent from new deployedResources list (CLUSTERPACK-BL-VERSION-CLEANUP); writes full spec including deployedResources to PackReceipt |
| `snapshot_pull_loop.go` | `SnapshotPullLoop`, `NewSnapshotPullLoop()`, `NewSnapshotPullLoopWithKey()` | tenant | Pulls PermissionSnapshots from management cluster; verifies signatures; populates in-memory SnapshotStore for local gRPC PermissionService |
| `rbacprofile_pull_loop.go` | `RBACProfilePullLoop`, `NewRBACProfilePullLoop()` | tenant | GETs conductor-tenant RBACProfile from seam-tenant-{cluster} on management cluster and SSA-patches into ont-system. CONDUCTOR-BL-TENANT-ROLE-RBACPROFILE-DISTRIBUTION closed |
| `rbacpolicy_pull_loop.go` | `RBACPolicyPullLoop`, `NewRBACPolicyPullLoop()` | tenant | GETs cluster-policy RBACPolicy from seam-tenant-{cluster} on management cluster and SSA-patches into ont-system. Decision C, T-17 closed |
| `signing_loop.go` | `SigningLoop` | management | Signs PackInstance and PermissionSnapshot CRs with Ed25519 private key; only management cluster conductor holds private key (INV-026) |
| `receipt_reconciler.go` | `ReceiptReconciler` | management | Reconciles PackReceipt CRs; management-cluster only |
| `capability_publisher.go` | `CapabilityPublisher` | management | On leader win, publishes 17 named capabilities to RunnerConfig status. After `runnerConfigMissingDriftThreshold=5` consecutive NotFound errors in the publish loop, calls `emitRunnerConfigMissingSignal()` to write a DriftSignal to `seam-tenant-{clusterRef}` (T-23). |
| `talos_version_drift_loop.go` | `TalosVersionDriftLoop`, `NewTalosVersionDriftLoop()`, `ParseTalosVersionFromOSImage(osImage string) string` | tenant | Reads `node.status.nodeInfo.osImage` from every node via Kubernetes API; parses Talos version from `Talos (vX.Y.Z)` format; compares against `InfrastructureTalosCluster.spec.talosVersion` in `ont-system`. When all nodes agree on a consistent version that differs from spec, emits a DriftSignal with `affectedCRRef.Kind=InfrastructureTalosCluster` to `seam-tenant-{cluster}` on the management cluster. Skips when nodes have mixed versions (mid-upgrade state). After `escalationThreshold=3` consecutive emissions without resolution, stops emitting. `DriftSignalHandler` skips InfrastructureTalosCluster kind -- platform's `DriftSignalReconciler` handles it. |

**`PackReceiptDriftLoop.checkDrift()`** reads `spec["deployedResources"]` (unstructured map), verifies each resource exists on local cluster via dynamic client. When a resource is not found, it calls `namespaceTerminating()` before emitting a drift signal -- if the resource's namespace has a `DeletionTimestamp` set, the cycle skips that resource entirely rather than incrementing the escalation counter. This prevents DriftSignal escalation for resources stuck behind finalizers during namespace teardown. `teardownOrphanedReceipt()` reads same field to determine which namespaces and cluster-scoped resources to delete when ClusterPack is deleted from management. Version-upgrade orphan diff is implemented in `PackInstancePullLoop.deleteOrphanedResources()` (CLUSTERPACK-BL-VERSION-CLEANUP closed).

### Kernel (`internal/kernel/agent.go`)

**Pull loops started per role:**

`role=management`: CapabilityPublisher, ReceiptReconciler, SigningLoop, DriftSignalHandler, FederationServer.

`role=tenant`: PermissionService gRPC server (port 50051), SnapshotPullLoop, PackInstancePullLoop, PackReceiptDriftLoop, RBACProfilePullLoop (conductor-tenant), RBACPolicyPullLoop (cluster-policy), TalosVersionDriftLoop, FederationClient, AdmissionWebhook. Five pull loops + one drift loop fully wired. Decision C satisfied. T-17 closed. T-23 Talos version drift closed.

**`onLeaderStart()` signature**: clusterRef, namespace, manifest, publisher, reconciler, signingLoop, snapshotPullLoop, packInstancePullLoop, packReceiptDriftLoop, rbacProfilePullLoop, rbacPolicyPullLoop, driftSignalHandler, dynamicClient.

### Capability (`internal/capability/platform_etcd.go`)

| Handler | Capability | Key behavior |
|---------|-----------|-------------|
| `talosUpgradeHandler` | `talos-upgrade` | (`platform_upgrade.go`) Rolling per-node sequential upgrade. Calls `params.TalosClient.Nodes()` to get node IPs from talosconfig endpoints. For each node: constructs `upgradeImage = "ghcr.io/siderolabs/installer:" + targetVersion` (targetVersion from UpgradePolicy CR), calls `Upgrade(NodeContext(ctx, nodeIP), image, false)` with `stage=false` (immediate reboot), then calls `waitForNodeReboot()` before proceeding to next node. Produces one `StepResult` per node. `waitForNodeReboot()` is two-phase: Phase 1 (2-min window) polls until node goes offline; Phase 2 (8-min window) polls until node comes back online. `NodeRebootPollInterval = 10 * time.Second` exported for test override. |
| `etcdBackupHandler` | `etcd-backup` | Lists EtcdMaintenance CRs in `seam-tenant-{cluster}`, reads `spec.s3Destination.bucket/key`, calls `TalosClient.EtcdSnapshot` into a `bytes.Buffer`, wraps with `bytes.NewReader(buf.Bytes())` (seekable) before calling `StorageClient.Upload`. Seekable wrapper is required for MinIO/Scality over HTTP -- AWS SDK v2 cannot compute checksums on unseekable streams without TLS. |
| `etcdDefragHandler` | `etcd-defrag` | Calls `TalosClient.EtcdDefragment`. |
| `etcdRestoreHandler` | `etcd-restore` | Lists EtcdMaintenance CRs, reads `spec.s3SnapshotPath`, downloads snapshot via `StorageClient.Download`, calls `TalosClient.EtcdRecover`. |
| `pkiRotateHandler` | `pki-rotate` | Lists PKIRotation CRs to verify trigger exists. Calls `TalosClient.GetMachineConfig` then `TalosClient.ApplyConfiguration` in staged mode to initiate rotation cycle. Then calls `TalosClient.Kubeconfig` (new) to generate fresh kubeconfig and writes it to `seam-mc-{cluster}-kubeconfig` and `target-cluster-kubeconfig` in `seam-tenant-{cluster}` via dynamic client. Kubeconfig refresh is best-effort: failure is logged but does not fail the overall operation. platform-schema.md §13. |
| `hardeningApplyHandler` | `hardening-apply` | (`platform_security.go`) Lists NodeMaintenance CRs to locate the trigger, reads `spec.hardeningProfileRef` to find the target HardeningProfile, lists HardeningProfile CRs, reads `spec.machineConfigPatches` via `unstructuredList` (not `unstructuredString` -- the field is `[]string`, not a scalar). Applies each patch individually via `TalosClient.ApplyConfiguration` in `no-reboot` mode. Produces one `StepResult` per patch. Empty-patches and missing-ref cases return `ValidationFailure`; `ApplyConfiguration` error returns `ExecutionFailure`. |

`tenantNamespace(clusterRef string) string` -- returns `seam-tenant-{clusterRef}`.

### Capability (`internal/capability/adapters.go`)

`S3StorageClientAdapter`: wraps `*s3.Client` (AWS SDK v2). Constructor `NewS3StorageClientAdapter()` reads `S3_REGION` (required) and `S3_ENDPOINT` (optional) from env. When `S3_ENDPOINT` is set, `UsePathStyle = true` is enabled for MinIO/Scality path-style addressing. `Upload(ctx, bucket, key, r io.Reader)` calls `PutObject`. `Download(ctx, bucket, key)` calls `GetObject`.

**`TalosClientAdapter`** (`adapters.go`): wraps the real Talos gRPC client. Extended with `nodes []string` field set at construction by `EndpointsFromTalosconfig(talosconfigPath)`. `Nodes() []string` returns the field. `Health(ctx context.Context) error` calls `a.inner.Version(ctx)` -- nil when the node Talos API is responsive, non-nil otherwise. Used by rolling upgrade reboot detection.

**`TalosNodeClient` interface** (`clients.go`) extended with two methods: `Nodes() []string` (returns node IPs from talosconfig endpoints, used by rolling upgrade to iterate nodes) and `Health(ctx context.Context) error` (liveness check via Version RPC, used by `waitForNodeReboot`).

**MinIO over HTTP**: Callers MUST pass an `io.ReadSeeker` (not a plain `io.Reader` like `*bytes.Buffer`) to `Upload` when `S3_ENDPOINT` is an HTTP URL. Without TLS, AWS SDK v2 cannot use trailing checksums and requires the stream to be seekable for upfront checksum computation. `etcdBackupHandler` uses `bytes.NewReader(buf.Bytes())` for this reason.

### Capability (`internal/capability/wrapper.go`)

**`writePackReceipt()` signature** (L934): `func writePackReceipt(ctx, tenantClient dynamic.Interface, clusterPackRef, targetCluster, rbacDigest, workloadDigest string, resources []runnerlib.DeployedResource) error`

Fields written: `clusterPackRef`, `targetClusterRef`, `rbacDigest`, `workloadDigest`, `deployedResources`. **Chart fields (chartVersion, chartURL, chartName, helmVersion) are NOT written here** -- those come from the PackInstance pull loop path. Called at three success points: single-pass staged (L307), single-pass direct (L413), split path `executeSplitPath` (L895).

**`packDeployHandler.Execute()` PE lookup**: Uses `params.OperationResultCM` as the exact PackExecution name to `Get` from `seam-tenant-{clusterRef}`. Wrapper's PE reconciler sets the `OPERATION_RESULT_CM` Job env var to the PE name. An empty `OperationResultCM` returns `ValidationFailure` immediately. Previously this used `List + break` which returned the alphabetically-first PE -- causing the wrong pack to be deployed when multiple PEs coexisted for the same cluster.

### Persistence (`internal/persistence/operationresult_writer.go`)

Single-active-revision pattern (Decision E): lists all PORs for `packExecutionRef` label, selects highest Revision as N, creates N+1 (L114-115), labels predecessor `ontai.dev/superseded=true` (L166), prunes oldest when count exceeds `maxRetainedSupersededPORs=10` (L185-216). Label keys: `ontai.dev/pack-execution`, `ontai.dev/cluster-pack`, `ontai.dev/superseded`.

### Shared Library (`pkg/runnerlib/`)

| File | Content |
|------|---------|
| `capability.go` (87L) | `CapabilityManifest`, `CapabilityEntry` types |
| `constants.go` (100L) | Named capability string constants (CapabilityXxx) |
| `generators.go` (139L) | `GenerateFromTalosCluster()`, `GenerateFromPackBuild()` |
| `jobspec.go` (271L) | `JobSpecBuilder` interface and implementation |
| `operationresult.go` (190L) | `OperationResultSpec`: Phase, Status, Capability, Artifacts, Steps, DeployedResources, ClusterPackRef, ClusterPackVersion, RBACDigest, WorkloadDigest |
| `packreceipt.go` (1L) | Package declaration only -- CRD types are in seam-core (Decision G) |
| `runnerconfig.go` (1L) | Package declaration only -- CRD types are in seam-core (Decision G) |

---

## 3. Three-Image Build

| Dockerfile | Image | Base | Mode | Constraint |
|------------|-------|------|------|------------|
| `Dockerfile.compiler` | `compiler:dev` | debian:12-slim | compile | Never deployed (INV-022) |
| `Dockerfile.execute` | `conductor-execute:dev` | debian:12-slim | execute | Kueue Jobs on management cluster only |
| `Dockerfile.agent` | `conductor:dev` | distroless/base:nonroot | agent | Deployed to `ont-system` on every cluster |

**Critical**: Pushing to `conductor:dev` does NOT update pack-deploy Job pods -- those run `conductor-execute:dev`. Always build both images after changes to shared code.

---

## 4. Primary Data Flows

**Compiler packbuild (helm)**: `helmCompilePackBuild()` fetches chart from HelmSource.URL, renders via `chartutil.ReleaseOptions`, splits via `SplitManifests()` into RBAC/cluster-scoped/workload OCI layers, pushes each, emits ClusterPack CR YAML with chart fields populated at L252-255.

**Pack-deploy execution**: PackExecution CR passes wrapper 5-gate check, Kueue Job (conductor-execute) runs `executeSplitPath()` in `capability/wrapper.go`, applies RBAC manifests directly to tenant cluster via TenantDynamicClient, submits to guardian `/rbac-intake/pack` on management cluster for governance CRs, applies cluster-scoped + workload layers, calls `writePackReceipt()`, writes PackInstance on management cluster.

**Tenant drift cycle**: `PackReceiptDriftLoop.checkDrift()` reads `spec.deployedResources`, verifies each on local cluster, on missing resource calls `emitDriftSignal()` which writes DriftSignal to `seam-tenant-{cluster}` on management. `DriftSignalHandler.handleOnce()` finds pending DriftSignal, deletes PackExecution, wrapper creates new PackExecution, pack-deploy Job reruns.

**PackInstance pull loop**: polls signed artifact Secrets (`seam-pack-signed-{cluster}-{packInstance}`) from management, verifies Ed25519, calls `buildReceiptSpecPayload()` (L271), SSA-patches PackReceipt in `ont-system` on tenant cluster including chart metadata.

---

## 5. Invariants

| ID | Rule | Location |
|----|------|----------|
| INV-022 | Agent distroless; execute debian-slim | `Dockerfile.agent`, `Dockerfile.execute` |
| INV-023 | Always `:dev` tag in lab | Enable bundle Deployment YAML |
| INV-026 | Private key on management conductor only | `writeConductorSigningKeySecret()` compile_enable.go:2950; `--signing-private-key` rejected for tenant |
| CR-INV-001 | Three-mode boundary: compile/execute/agent | `internal/kernel/mode.go`, `internal/kernel/role.go` |
| CR-INV-003 | CRD types from seam-core, not runnerlib | `pkg/runnerlib/packreceipt.go` (stub), `pkg/runnerlib/runnerconfig.go` (stub) |

---

## 6. Test Contract

| Package | Coverage |
|---------|----------|
| `test/unit/agent` | PackInstancePullLoop, SnapshotPullLoop, DriftSignalHandler, PackReceiptDriftLoop (15 tests including `TestPackReceiptDriftLoop_NamespaceTerminating_SkipsDrift`), TalosVersionDriftLoop (4 tests: emits on version mismatch, skips when all nodes match spec, skips on mixed versions, ParseTalosVersionFromOSImage) |
| `internal/agent` | `capability_publisher_test.go`: `TestCapabilityPublisher_EmitsDriftSignalAfterMissingThreshold` (T-23 drift signal create), `TestCapabilityPublisher_EmitDriftSignal_IdempotentOnAlreadyExists`, `TestCapabilityPublisher_IsPublishNotFound` (T-23). |
| `test/unit/capability` | Split path, guardian intake, RBAC apply, registry; hardeningApplyHandler (6 tests); talosUpgrade rolling (2 tests: `TestTalosUpgrade_RollingUpgrade_AllNodes` 3-node sequential with reboot wait, `TestTalosUpgrade_NoNodesReturnsValidationFailure`); pack-deploy handler (`wrapper_test.go`: nil-clients, missing OperationResultCM, fetch+apply manifests, OCI failure, `TestPackDeploy_MultiplePEsSameCluster_DeploysCorrectPack` -- guards against alphabetical-first-match bug) |
| `test/unit/compiler` | PackBuild split, helm/raw paths, enable bundle |
| `test/unit/kernel` | Role/mode init, sequencer, execute mode |
| `test/unit/persistence` | Single-active-revision, superseded label, pruning at max 10 |
| `test/unit/runnerlib` | CapabilityManifest, jobspec, generators |
| `test/e2e` | 11 spec files (68 specs total); all skip when `MGMT_KUBECONFIG` absent; skip reasons reference backlog item IDs. New in session/16: `rbacprofile_rbacpolicy_pull_loop_test.go` (T-17: RBACProfilePullLoop + RBACPolicyPullLoop), `clusterpack_version_cleanup_test.go` (CLUSTERPACK-BL-VERSION-CLEANUP invariants), `drift_injection_test.go` (Decision H full drift injection cycle), `cnpg_audit_sweep_test.go` (guardian CNPG audit_events sweep) |
| `test/integration` | RunnerConfig generation |

---

## 7. Sharp Edges

**`/var/run/secrets/kubeconfig/value` path**: The kubeconfig Secret has key `value`. Volume mounted as a directory (no subPath) at `/var/run/secrets/kubeconfig`. Kubernetes creates the file at `/var/run/secrets/kubeconfig/value`. Hardcoded at `cmd/conductor/main.go:181`. Overridable via `KUBECONFIG` env var. This is correct by design -- not a bug.

**`conductor-execute` vs `conductor`**: Pack-deploy Jobs use `conductor-execute:dev`. Agent Deployments use `conductor:dev`. Building only the agent image does not update running Job pods.

**`writePackReceipt` does not write chart fields**: Chart metadata reaches PackReceipt via the PackInstance pull loop (`buildReceiptSpecPayload()` L271), not via execute-mode `writePackReceipt()`. A freshly deployed pack will have chart fields on PackReceipt only after the tenant conductor pull loop runs.

**`pkg/runnerlib/packreceipt.go` is a 1-line stub**: The file contains only `package runnerlib`. CRD type definitions are in `seam-core/api/v1alpha1` per Decision G. `packreceipt_test.go` in the same package tests seam-core types only.

**RBACProfile pull loop (role=tenant)**: `RBACProfilePullLoop` in `rbacprofile_pull_loop.go` pulls the `conductor-tenant` RBACProfile from `seam-tenant-{cluster}` on the management cluster and SSA-patches it into `ont-system` on the local cluster. Wired into `kernel/agent.go` `onLeaderStart`. CONDUCTOR-BL-TENANT-ROLE-RBACPROFILE-DISTRIBUTION closed.

**S3 upload requires seekable stream over HTTP**: AWS SDK v2 cannot compute request checksums for `PutObject` on an unseekable `io.Reader` without TLS (trailing checksums are TLS-only). For MinIO/Scality HTTP endpoints, `etcdBackupHandler` uses `bytes.NewReader(buf.Bytes())` which wraps the in-memory snapshot as `io.ReadSeeker`. Passing `*bytes.Buffer` directly to `Upload` causes "unseekable stream is not supported without TLS and trailing checksum".

**`unstructuredString` cannot read slice fields**: `unstructuredString` calls `v.(string)` which silently fails for any field that is `[]interface{}` in the unstructured map. Capability handlers reading list-type spec fields (e.g. `machineConfigPatches`) must use `unstructuredList` followed by a per-element string type assertion. Using `unstructuredString` on a slice field always returns an empty string with no error.

**`targetTalosVersion` is a version string, not an image reference**: UpgradePolicy `spec.targetTalosVersion` holds a semantic version such as `v1.9.3`. The installer image is `ghcr.io/siderolabs/installer:{version}`. Passing the raw version string as an image reference causes `error validating installer image "v1.9.3"`.

**TalosVersionDriftLoop escalation resets must be done manually**: When `escalationCounter` reaches 3, the loop stops emitting. The DriftSignal stays at its last state forever. To re-enable detection: `kubectl patch driftsignal drift-version-{cluster} -n seam-tenant-{cluster} --type=merge -p '{"spec":{"state":"pending","escalationCounter":0}}'` on the management cluster.

**Phase 1 of `waitForNodeReboot` may complete immediately**: If the node reboots faster than `NodeRebootPollInterval` (10s default), `Health()` returns nil on all Phase 1 polls. `waitForNodeReboot` treats this as a fast reboot -- it returns nil and skips Phase 2. The TalosVersionDriftLoop will detect persistence if the upgrade did not actually apply.

**`namespaceTerminating` guard skips drift, not escalation**: When a namespace is terminating (DeletionTimestamp set), resources inside it appear absent via the dynamic client. Without the guard, each `checkDrift()` cycle would emit a DriftSignal and increment `escalationCounter`, reaching TerminalDrift threshold before the namespace finishes deleting. The guard skips the entire resource for that cycle. Counter accumulation resumes only if the namespace recovers (DeletionTimestamp cleared) while the resource remains missing.

**`packDeployHandler` requires `OperationResultCM`**: The Job env var `OPERATION_RESULT_CM` (set by wrapper's PE reconciler) must match the PackExecution name exactly. An empty or incorrect value returns `ValidationFailure`. The previous `List + break` implementation was vulnerable to alphabetical-first selection -- cert-manager PE deployed instead of nginx PE when both existed in the same cluster namespace. The fix uses `params.OperationResultCM` directly in a `Get` call.
