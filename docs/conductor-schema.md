# conductor-schema
> API Group: runner.ontai.dev
> Repository: conductor (produces both Compiler and Conductor binaries)
> All agents absorb this document. This schema governs both binaries.

---

## 1. Central Principle

The conductor repository is the platform intelligence. Operators are thin reconcilers.
All execution logic — compile-time and runtime — lives in the two binaries built from
this repository.

**Compiler** is the compile-time binary. It is a short-lived tool invoked by humans
or by the bootstrap pipeline. It never runs as a Deployment on any cluster.

**Conductor** is the runtime binary. It runs as a long-lived Deployment in ont-system
on the management cluster and every target cluster. It also runs as short-lived Kueue
Jobs on the management cluster for all named operational capabilities. It is the only
Seam binary deployed to any cluster. It is distroless.

Human-facing compile operations are invocations of the Compiler binary in compile
mode. Cluster management operations are performed by submitting Seam CRs which
operators translate into Conductor Jobs.

---

## 2. Binary Architecture

Both binaries are built from the same Go module at github.com/ontai-dev/conductor.
They share pkg/runnerlib and all internal modules except the compile-mode client
wrappers, which are excluded from Conductor at build time via Go build tags.

**Shared Go clients (both binaries):**
- kube goclient: all Kubernetes API interactions. All modes.

**Compiler exclusive clients (compile mode only, excluded from Conductor build):**
- helm goclient: Helm chart rendering. INV-014.
- kustomize goclient: overlay resolution. INV-014.
- SOPS handler: age key encryption of cluster secrets.

**Conductor exclusive clients (execute and agent modes only):**
- talos goclient: all Talos API interactions. INV-013.
  Pure Go gRPC library. No talosctl binary. No shell invocation.

**No system binary invocations in any mode.** All clients are pure Go library
integrations. No kubectl, no talosctl, no helm binary, no kustomize binary, no
shell invocations at any point in execute or agent mode. Compile mode uses Go
library wrappers only — no shell invocations.

**Three modes — no other modes exist:**

| Mode     | Binary    | Invocation              | Duration    | Image       | Cluster Scope             |
|----------|-----------|-------------------------|-------------|-------------|---------------------------|
| compile  | Compiler  | Direct CLI invocation   | Short-lived | Debian      | Never deployed to cluster |
| execute  | Conductor | Kueue Job pod           | Short-lived | Distroless  | Management cluster only   |
| agent    | Conductor | Deployment in ont-system| Long-lived  | Distroless  | Management + all targets  |

Execute mode Jobs run exclusively on the management cluster. Target clusters never
run execute-mode Jobs. All cluster operations reach target clusters remotely via
mounted kubeconfig and talosconfig Secrets.

Mode is determined by startup flag and RunnerConfig mounted at startup for execute
and agent modes. Compile mode reads input paths from CLI flags.

Invoking helm goclient or kustomize goclient in execute or agent mode is a
programming error; the Conductor binary excludes these clients at build time.
Any attempt causes InvariantViolation and immediate structured exit.

Invoking talos goclient in compile mode is a programming error. INV-013.

---

## 3. Image Tag Convention and Release Pairing

**Stable releases:**
- `registry.ontai.dev/ontai-dev/compiler:v{talosVersion}-r{revision}`
- `registry.ontai.dev/ontai-dev/conductor:v{talosVersion}-r{revision}`

The talosVersion component declares Talos API compatibility — not cosmetic. A cluster
at Talos v1.9.3 must use a Conductor tagged v1.9.3-rN. INV-012.

Compiler and Conductor always carry the same version tag built from the same source
commit. They are released together. Deploying mismatched versions against the same
cluster is unsupported. INV-024.

**Development:** dev (floating), dev-rc{N} (release candidates). Applied to both images.

**Lab builds:** pushed only to 10.20.0.1:5000/ontai-dev/compiler and
10.20.0.1:5000/ontai-dev/conductor. Lab tags never appear in the public registry.
INV-011.

Updating RunnerConfig agentImage to the new Conductor tag is a prerequisite to any
Talos version upgrade. The upgrade reconciler checks this gate before submitting any
upgrade Job.

---

## 4. Shared Runner Library

The shared runner library is owned by the conductor repository and imported by all
operators and by both binaries. It is the single source of RunnerConfig schema,
generation logic, and capability manifest structure.

**Library exports:**
- RunnerConfig schema types
- GenerateFromTalosCluster(spec) → RunnerConfig
- GenerateFromPackBuild(spec) → RunnerConfig
- CapabilityManifest types
- OperationResult types
- Job spec builder functions

When a new named capability is added to Conductor, the shared library is updated.
Operators get the new capability by updating their library dependency version. No
operator logic changes are required for new capabilities.

---

## 5. CRDs

### RunnerConfig

Scope: Namespaced — ont-system (management cluster), tenant-{cluster-name} (targets).
Short name: rc

The live operational contract between operators and Conductor for a specific cluster
or pack. Operator-generated at runtime using the shared library. Never human-authored.
Never a compile-time artifact.

Key spec fields:
- clusterRef: cluster identity this RunnerConfig governs.
- agentImage: fully qualified Conductor image including tag. Single source of truth
  for which Conductor version handles this cluster's Deployments and Jobs.
- phases: applicable phases for this cluster (launch, enable for management;
  launch, bootstrap for tenant clusters).
- operationalHistory: append-only record of every configuration change applied.
  Never deleted, only superseded by newer entries for the same concern.
- maintenanceTargetNodes: list of node names that are the subject of the operation.
  Populated by the initiating operator at RunnerConfig creation time. Used by
  Conductor execute mode for node affinity exclusion when selfOperation is true.
- operatorLeaderNode: the node currently hosting the leader pod of the initiating
  operator. Resolved at creation time via the Kubernetes downward API. Used by
  Conductor execute mode for node affinity exclusion when selfOperation is true.
- selfOperation: boolean — true when the Job's execution cluster and the target
  cluster are the same (management cluster self-operations). false for all
  tenant-targeted operations. Conductor execute mode reads this field to determine
  whether to apply NotIn node affinity constraints. When false, exclusion logic
  is skipped entirely.

Per-phase parameter sections:
- launch: vmConfig, talosInstallerImage, networkConfig, bootstrapTimeout.
- enable: prerequisiteTimeout, operatorTimeout. Management cluster only.
- bootstrap: agentPackRef, agentNamespace, retryLimit. Tenant clusters only.

Status fields (Conductor in agent mode populates):
- capabilities: the self-declared capability manifest. List of named capabilities
  the current agentImage supports, each with version and parameter schema.
  Operators read this before submitting Jobs. If capability absent: raise
  CapabilityUnavailable on the operational CR and wait.
- agentVersion: the Conductor image version currently running as agent.
- agentLeader: the pod name currently holding the leader election lease.

Status conditions: LaunchComplete, EnableComplete, BootstrapComplete, PhaseFailed,
CapabilityUnavailable.

---

### OperatorManifest

Scope: Namespaced — ont-system. Management cluster only.
Short name: om

Declares one operator for Compiler's enable phase to install. Not used for
target cluster operator installation — that is ClusterPack delivery.

The bootstrap RBACPolicy that is available before Guardian installs must
authorize the enable phase operations declared here.

Key spec fields:
- operatorName: canonical name matching repository name.
- version: Helm chart version to install.
- chartRef: chart repository URL and chart name.
- installNamespace: namespace on management cluster.
- ownedCRDs: list of CRD names for post-installation verification.
- requiredCRDs: CRD names that must be registered before installation.
- requiredSecrets: Secrets that must exist before installation.
- upgradeStrategy: RollingUpgrade or RecreateOnChange.
- installOrder: integer defining enable phase sequence. Lower installs first.
  Prerequisites use order 0. Guardian must always be order 1. This is enforced
  by Compiler — any OperatorManifest with installOrder 1 that is not Guardian
  is a programming error.

Status conditions: Installed, Healthy, Failed.

Compiler processes OperatorManifests in ascending installOrder sequence. It does
not proceed to the next until the current reaches Healthy. This enforces Guardian
deployment priority and all subsequent dependency ordering.

---

## 6. Named Capabilities

Named capabilities are Conductor's execution vocabulary in execute mode. Every
operator Job maps to exactly one named capability. New capabilities are added to
Conductor and declared in the shared library. No operator code changes are required.

**Capability invocation contract:**
The operator stamps the capability name into the Job spec as an environment variable.
Conductor reads the capability name on startup in execute mode and executes the
corresponding implementation. Conductor exits with a structured OperationResult.

**Current named capabilities:**

| Capability          | Owner operator | Triggering CRD                                              | Description                               |
|---------------------|----------------|-------------------------------------------------------------|-------------------------------------------|
| bootstrap           | Platform       | TalosCluster                                                | Full cluster bootstrap from seed nodes    |
| talos-upgrade       | Platform       | UpgradePolicy (capi.enabled=false only)                     | CAPI-delegated for target clusters; direct runner Job for management cluster |
| kube-upgrade        | Platform       | UpgradePolicy (capi.enabled=false only)                     | CAPI-delegated for target clusters; direct runner Job for management cluster |
| stack-upgrade       | Platform       | UpgradePolicy (capi.enabled=false only)                     | CAPI-delegated for target clusters; direct runner Job for management cluster |
| node-patch          | Platform       | NodeMaintenance                                             | Machine config patch to nodes             |
| node-scale-up       | Platform       | NodeOperation (capi.enabled=false only)                     | CAPI-delegated for target clusters; direct runner Job for management cluster |
| node-decommission   | Platform       | NodeOperation (capi.enabled=false only)                     | CAPI-delegated for target clusters; direct runner Job for management cluster |
| node-reboot         | Platform       | NodeOperation (capi.enabled=false only)                     | CAPI-delegated for target clusters; direct runner Job for management cluster |
| etcd-backup         | Platform       | EtcdMaintenance                                             | etcd snapshot + config export to S3       |
| etcd-defrag         | Platform       | EtcdMaintenance                                             | etcd defrag and optional snapshot         |
| etcd-restore        | Platform       | EtcdMaintenance                                             | Disaster recovery from S3 snapshot        |
| pki-rotate          | Platform       | PKIRotation                                                 | PKI certificate rotation                  |
| credential-rotate   | Platform       | NodeMaintenance                                             | Service account key rotation              |
| hardening-apply     | Platform       | NodeMaintenance                                             | Apply HardeningProfile                    |
| cluster-reset       | Platform       | ClusterReset                                                | Destructive factory reset with human gate |
| pack-compile        | Wrapper        | PackBuild spec file (compile mode — not a cluster CRD, not a Kueue Job) | Compiler compile mode: renders PackBuild inputs into ClusterPack OCI artifact |
| pack-deploy         | Wrapper        | PackExecution                                               | Apply ClusterPack to target cluster       |
| rbac-provision      | Guardian       | (agent-initiated)                                           | Provision RBAC artifacts from snapshot    |

Note: talos-upgrade, kube-upgrade, stack-upgrade, node-scale-up, node-decommission,
and node-reboot are confirmed retained. They are not orphaned. The Triggering CRD
for each is UpgradePolicy or NodeOperation, active when TalosCluster
spec.capi.enabled=false only (management cluster direct path). For capi.enabled=true
clusters CAPI handles these operations natively.

All capabilities except pack-compile run in execute mode on the management cluster as
Kueue Jobs using the distroless Conductor image. No capability runs on a target cluster
as a Job. All capabilities reach target clusters via mounted kubeconfig and talosconfig
Secrets.

Note on pack-compile: pack-compile is the sole compile-mode entry in this table. It does
not follow the execute-mode Job pattern that all other capabilities follow. It appears
here for completeness — the capability name constant exists in the shared library so that
Wrapper and Compiler share a common vocabulary. However, pack-compile is never submitted
as a Kueue Job, never run by Conductor, and never runs on any cluster.

**Future-proofing:** When a new named capability is added to Conductor, it is
declared in the shared library's capability manifest. Operators discover it via
RunnerConfig status. No operator requires changes.

---

## 7. Inter-Job State — Temporary PVC Protocol

For multi-step sequence capabilities (bootstrap, stack-upgrade, cluster-reset),
Conductor uses a temporary PVC for inter-step state transfer.

Protocol:
1. First Job creates a PVC named ont-{capability}-{cr-name}. Executes step.
   Writes intermediate artifacts to PVC. Updates operational CR status.
2. Subsequent Jobs mount the same PVC. Read previous step outputs. Execute step.
   Write outputs. Update status.
3. Final Job consumes all intermediate artifacts. Creates Kubernetes assets on
   management cluster. Deletes PVC. Writes terminal OperationResult.

The operator never sees the PVC. It only sees CR status advancing. Conductor
manages PVC lifecycle entirely within the Job sequence. Storage class requirement:
the management cluster must have a storage class available.

---

## 8. OperationResult Protocol

Every execute-mode Job writes an OperationResult JSON document to a ConfigMap before
exit. ConfigMap name derived from Job name. The operator reads it to advance CR status.
No other inter-process communication channel exists between operator and Conductor.

Structure: phase, status (succeeded or failed), startedAt, completedAt, artifacts
(structured references — never raw content), failureReason (category and detail
when failed), steps (individual step results for multi-step capabilities).

The operator must read OperationResult within the Job's configured TTL. After TTL
expiry the ConfigMap is garbage collected.

---

## 9. Compile Mode (Compiler binary)

### Canonical Compiler Command Surface

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

The complete and authoritative compiler command surface is:

| Subcommand    | Purpose                                                          | Cluster connectivity | Output                                          |
|---------------|------------------------------------------------------------------|----------------------|-------------------------------------------------|
| `bootstrap`   | Management cluster formation — Talos machineconfigs + bootstrap CRs | No              | CR YAML + encrypted machineconfig files         |
| `launch`      | Management cluster CRD installation manifest                     | No                   | CRD manifest YAML bundle                        |
| `enable`      | Seam operator deployment manifest bundle                         | No                   | Operator YAML bundle (Deployments, RBAC, etc.)  |
| `packbuild`   | ClusterPack OCI artifact + CR YAML                               | No                   | ClusterPack CR YAML + OCI push                  |
| `maintenance` | MaintenanceBundle CR with pre-resolved scheduling context         | Yes (management)     | MaintenanceBundle CR YAML                       |
| `component`   | RBACProfile CRs for third-party components                       | Optional (--discover)| RBACProfile CR YAML(s)                          |
| `domain`      | Reserved — domain CR compilation not yet implemented             | —                    | —                                               |

No other compiler subcommands exist. New subcommands require a Platform Governor directive before implementation.

---

### Management Cluster Bootstrap Sequence Authority

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

The management cluster bootstrap sequence is owned exclusively by the Compiler in three steps. Platform operator has no involvement in management cluster bootstrap. This is a locked invariant.

**Step 1 — `compiler bootstrap`**
Forms the management cluster itself. Reads TalosCluster spec and human-provided machineconfig inputs. Validates spec against platform-schema.md rules. SOPS-encrypts talos-secret, machineconfigs, and talosconfig using the admin's age key. Writes encrypted files to output path. Produces bootstrap CRs (TalosCluster in mode: bootstrap) as YAML. No cluster connection required. Compiler never applies resources — the GitOps pipeline or operator's kubectl applies the output.

**Step 2 — `compiler launch`**
Installs all Seam CRDs onto the management cluster. Reads the CRD manifest set for all Seam API groups (platform.ontai.dev, security.ontai.dev, infra.ontai.dev, runner.ontai.dev, infrastructure.ontai.dev). Produces a CRD manifest YAML bundle ready for GitOps application. No cluster connection required. Compiler never applies resources.

**Step 3 — `compiler enable`**
Produces the complete Seam operator deployment manifest bundle as YAML output. The bundle
is structured as six sequenced phases applied in ascending order. Each phase carries a
`phase-meta.yaml` declaring name, order, readinessGate, and applyOrder.

**Six-phase enable bundle structure:**

| Phase | Directory                        | Contents                                                                          | Readiness gate before next phase |
|-------|----------------------------------|-----------------------------------------------------------------------------------|----------------------------------|
| 0     | `00-infrastructure-dependencies` | CNPG operator manifests + CNPG Cluster CR for management Guardian                | CNPG Cluster ready               |
| 1     | `01-guardian-bootstrap`          | Guardian CRDs, bootstrap RBACPolicy, namespace-labels.yaml (seam.ontai.dev/webhook-mode=exempt on seam-system) | Guardian CRDs registered |
| 2     | `02-guardian-deploy`             | Guardian Deployment, Guardian RBACProfile, admission webhook configuration        | Guardian webhook operational     |
| 3     | `03-platform-wrapper`            | Platform Deployment, Wrapper Deployment, seam-core Deployment, their RBACProfiles | All three operators healthy      |
| 4     | `04-conductor`                   | Conductor Deployment in ont-system **stamped with `role=management`** (see §15 Role Declaration Contract), Conductor RBACProfile | Conductor agent ready |
| 5     | `05-post-bootstrap`              | Remaining cluster-wide resources, Kueue ClusterQueue scaffold, metallb config     | —                                |

Phase 0 is the prerequisite phase that resolves the CNPG dependency before Guardian
is deployed. Guardian's startup migration runner connects to CNPG before registering
any controller — phase 0 must reach readiness before phase 1 may begin. See
guardian-schema.md §16 CNPG Deployment Contract.

Phase 1 namespace-labels.yaml stamps `seam.ontai.dev/webhook-mode=exempt` on
`kube-system` and `seam-system` as SSA metadata-only patches. This satisfies Guardian's
CheckBootstrapLabels startup gate before the webhook registers in phase 2. See
guardian-schema.md §4 Bootstrap RBAC Window.

The full bundle also contains:
- All RBAC resources for all Seam operator service accounts (distributed across phases by operator).
- All leader election Lease templates.
- First-class platform-owned RBACProfile CRs for all Seam operator service accounts — one per operator, Guardian-governed, human-reviewed before GitOps commit.

Compiler never applies resources directly. The GitOps pipeline applies this bundle after human review. No cluster connection required.

**F-P8:** Phase 0 (`00-infrastructure-dependencies`) implementation — adding CNPG operator
manifests and CNPG Cluster CR to the enable bundle output — requires a Conductor Engineer
session. See CONTEXT.md F-P8.

**Invariant:** Platform operator is not involved at any step of management cluster bootstrap. Management cluster formation, CRD installation, and operator deployment are all compiler-driven. Platform's role begins when the management cluster is operational and CRDs are registered.

---

### compiler maintenance

**Subcommand:** `compiler maintenance --operation <type> [--talosconfig <path>] [--kubeconfig <path>] [--output <path>]`

Produces a MaintenanceBundle CR that pre-encodes all scheduling context required for
a maintenance operation. Platform and Conductor execute without rediscovering any of
this context from the live cluster at execution time. See F-P5.

**Cluster access resolution:**
talosconfig and kubeconfig are resolved from three sources in order, consistent with
how `launch` and `enable` resolve cluster access when operating against a pre-existing
cluster:
1. Explicit flag: `--talosconfig <path>`, `--kubeconfig <path>`
2. Environment variable: `TALOSCONFIG`, `KUBECONFIG`
3. Conventional path: `./talos/config`, `~/.kube/config`

There is no special connectivity requirement. The Compiler always has management
cluster access through one of these three sources.

**Pre-encoded context written into the MaintenanceBundle CR:**

- `maintenanceTargetNodes`: the target node set for the operation. Directly populates
  `RunnerConfig.MaintenanceTargetNodes` when Platform creates the RunnerConfig from
  this bundle. The node set is validated against the live cluster at compile time —
  nodes that do not exist cause a compile-time failure.

- `operatorLeaderNode`: the node currently hosting the leader pod of the initiating
  Platform operator, resolved live from the management cluster's Platform leader
  election lease (`platform-leader` Lease in `seam-system`). Directly populates
  `RunnerConfig.OperatorLeaderNode`. Compiler fails fast if the lease does not exist
  or has no holder — this is a safety gate, not a recoverable condition.

- `s3ConfigSecretRef`: the S3 configuration Secret reference, resolved and validated
  to exist before the MaintenanceBundle CR is committed. If the Secret is absent,
  `compiler maintenance` fails fast at compile time with a structured error. A
  MaintenanceBundle is never committed without a valid, resolvable S3 reference.
  Resolution follows platform-schema.md §10 (Etcd Backup Destination Contract).

- `operation`: one of `drain`, `upgrade`, `etcd-backup`, or `machineconfig-rotation`.
  Determines which Conductor capability the resulting RunnerConfig targets.

**MaintenanceBundle CRD:**
MaintenanceBundle is a new CRD in the `platform.ontai.dev` API group. Its complete
type definition, spec fields, status conditions, and reconciler implementation are
deferred to a Platform Schema Engineer session (F-P5). The contract above establishes
the semantic requirements — the CR carries pre-resolved scheduling context so neither
Platform nor Conductor need to perform cluster queries at execution time.

---

## 10. Agent Mode (Conductor binary)

Conductor in agent mode is a long-lived Deployment in ont-system on every cluster.
Same binary, agent startup flag. Implements leader election — only one replica
writes to RunnerConfig status and receipt CRs at a time.

**On startup (all clusters):**
1. Read RunnerConfig for this cluster.
2. Validate mode: refuse compile mode flag. INV-023.
3. Write capability manifest to RunnerConfig status (self-declaration).
4. Start receipt reconciliation loops (PackReceipt, PermissionSnapshotReceipt).
5. Start admission webhook server (intercepts RBAC resources).
6. Start local PermissionService gRPC server (serves authorization decisions).
7. Start drift detection loop for PackReceipt.
8. Start PermissionSnapshot pull loop (pulls from management cluster).

**Additional on management cluster Conductor startup:**
9. Start PackInstance signing loop: watches for new ClusterPack registrations,
   signs PackInstance CRs with the platform signing key.
10. Start PermissionSnapshot signing loop: watches for new PermissionSnapshot
    generation by Guardian, signs them with the platform signing key.

**Signing and verification model:**
The management cluster Conductor holds the signing key (mounted Secret). It signs
PackInstance and PermissionSnapshot CRs by writing a cryptographic signature
to a dedicated annotation field. Target cluster Conductors verify this signature
before acknowledging receipt. A signature failure blocks the receipt
acknowledgement and raises DegradedSecurityState. This chain of custody ensures
that target clusters only execute packs and honor permissions that have been
explicitly authorized by the management cluster's security plane.

**Leader election lease:** Lives in ont-system as a Lease resource named
conductor-{cluster-name}. All Conductor Deployment replicas compete. Only the
leader performs writes.

---

## 11. Licensing

Seam is fully open source with no licensing tier. All clusters are equal. No enforcement.

---

## 12. Operational Readiness Gates

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

### Gate 1 — Port 50000 Talos API Reachability

Platform operator is the sole owner of Talos apid port 50000 reachability validation
across both native and CAPI cluster paths. No other operator and no Conductor
capability handler performs port 50000 validation. This is a locked invariant.

**Native clusters (spec.capi.enabled=false):**
The gate is triggered by node IP declaration in TalosCluster spec. When a node IP is
recorded in TalosCluster spec, Platform operator validates reachability to port 50000
before proceeding with any node-level operation.

**CAPI clusters (spec.capi.enabled=true):**
The gate is triggered by CAPI Machine reaching provisioned state. Platform operator
validates port 50000 reachability as part of the SeamInfrastructureMachineReconciler
provisioning sequence. No other reconciler or operator repeats this check.

**Permanent exclusions:**
- Screen (future operator) never performs this check. Screen's responsibility ends at
  infrastructure existence. Port 50000 ownership belongs exclusively to Platform.
- Guardian, Wrapper, Conductor execute mode, and Conductor agent mode never perform
  port 50000 validation under any circumstance.
- Adding port 50000 validation to any component other than Platform operator requires
  a Platform Governor constitutional amendment.

---

## 13. RunnerConfig Self-Operation Contract

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

The RunnerConfig spec carries three fields as a first-class scheduling contract. These
fields govern Conductor execute mode node affinity exclusion for management cluster
self-operations. They are populated exclusively by the initiating operator at
RunnerConfig creation time.

**The three fields (defined in Section 5):**
- `maintenanceTargetNodes`: list of node names that are the subject of the operation.
- `operatorLeaderNode`: the node currently hosting the leader pod of the initiating
  operator, resolved via the Kubernetes downward API.
- `selfOperation`: boolean — true when the Job's execution cluster and target cluster
  are the same (management cluster self-operations); false for all tenant-targeted
  operations.

**Operator responsibility at RunnerConfig creation:**
The initiating operator populates all three fields. `operatorLeaderNode` is resolved
at creation time using the Kubernetes downward API (fieldRef: spec.nodeName on the
operator's own pod). The operator must not cache this value — it must be resolved
fresh at each RunnerConfig creation to reflect the current leader pod's node.

**Conductor execute mode contract:**
When selfOperation is true, Conductor translates maintenanceTargetNodes and
operatorLeaderNode into Kueue Job node affinity NotIn constraints before submitting
the Job. This ensures the Job pod does not land on a node that is itself a target
of the maintenance operation, and does not land on the node hosting the operator's
leader pod (which would cause a scheduling deadlock if the node were cordoned).

When selfOperation is false, Conductor skips exclusion resolution entirely. Tenant-
targeted operations are exempt — the Job executes on the management cluster regardless
of which nodes the remote target cluster is operating on.

**Conductor agent mode recovery path:**
Conductor agent mode acts as a recovery path only. It detects Jobs that landed on
maintenance-targeted nodes due to scheduling races (i.e., the NotIn constraint was
applied but a race between admission and cordoning resulted in incorrect placement)
and signals rescheduling by annotating the Job pod. It does not proactively schedule
Jobs. The agent mode recovery path is not a substitute for correct operator-side
field population.

**Permanent exclusions:**
- No other component populates these three fields. They are operator-authored at
  creation time and Conductor-consumed at Job materialisation time.
- Conductor agent mode does not populate these fields. It reads them.
- These fields are never modified after RunnerConfig creation. They are immutable
  for the lifetime of the RunnerConfig instance.

---

## 14. Dockerfile Standards

**Compiler Dockerfile (compile mode, debian):**

Build pattern: golang:1.25 builder stage with build tag `-tags compiler` compiles the
Compiler binary with compile-mode clients included. debian:12-slim final stage
includes: bash, curl, jq, python3, openssl, psql, helm binary (for CA bundle
verification during chart pull), kubectl, and the compiled Compiler binary.
USER 65532:65532. No package manager retained in the final image.

The debian base is required for: /etc/ssl/certs (Helm chart HTTPS pulls), SOPS age
key operations (python3), and psql (CNPG health verification in enable phase).
No other reason for debian exists. This image is never deployed to any cluster.

**Conductor Dockerfile (execute and agent modes, distroless):**

Build pattern: golang:1.25 builder stage with build tag `-tags conductor` compiles the
Conductor binary. Compile-mode clients (helm goclient, kustomize goclient, SOPS
handler) are excluded at build time. gcr.io/distroless/base:nonroot final stage.
USER 65532:65532. No shell. No package manager. No system tools.

gcr.io/distroless/base (not static) is required because the talos goclient and
gRPC stack require libc for certain TLS and crypto operations. Verify at build time
that the produced binary runs cleanly on the distroless/base image before release.

---

## 15. Role Declaration Contract

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

Conductor reads a role declaration field at startup to determine which loops to
activate and which responsibilities to assume. The role field is stamped externally
at Deployment creation time. Conductor does not determine its own role and never
infers it from cluster state.

**Two valid role values:**

| Role         | Who stamps it          | When                                              |
|--------------|------------------------|---------------------------------------------------|
| `management` | `compiler enable`      | At management cluster manifest production time    |
| `tenant`     | Platform operator      | At tenant cluster Conductor Deployment creation   |

**Role field location:**
The role declaration is a first-class field on the Conductor Deployment, not an
environment variable or ConfigMap mount. It is stamped once at Deployment creation
time and never modified by any controller.

**Management role startup sequence:**
Conductor with role=management activates all loops from §10 — including PackInstance
signing, PermissionSnapshot signing, and the full agent mode startup sequence.

**Tenant role startup sequence:**
Conductor with role=tenant activates the target-cluster-specific loops from §10 —
PackReceipt, PermissionSnapshotReceipt, admission webhook, PermissionService gRPC,
drift detection, and PermissionSnapshot pull loop. It does not activate signing loops.
Signing is management-only. INV-026.

**Invariants:**
- Conductor with an absent or unrecognized role field exits immediately with
  InvariantViolation structured exit. This is a hard gate — a Conductor without a
  valid role declaration is a programming error, not a recoverable condition.
- No component other than `compiler enable` (management) and Platform operator (tenant)
  may stamp the role field. Guardian, Wrapper, seam-core, and humans never set this field.
- Platform operator is exclusively responsible for stamping role=tenant on every Conductor
  Deployment it creates. See platform-schema.md §12 Conductor Deployment Contract.

---

## 16. compiler component Subcommand

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

`compiler component` emits RBACProfile CRs as YAML output for third-party components
operating in a Guardian-governed cluster. Guardian's admission webhook enforces what
RBACProfiles declare — it never generates them and never guesses.
`compiler component` is a prerequisite for any third-party component operating in a
Guardian-governed cluster.

**Subcommand signature:**
```
compiler component [--component <name>]... [--descriptor <path>] [--discover] [--output <path>]
```

**Two operating modes:**

### Catalog Mode (no cluster connectivity required)

Compiler ships with a versioned embedded catalog of canonical RBACProfile definitions
for known ecosystem components. The catalog lives at `internal/catalog/` in the
conductor repository as a versioned embedded filesystem. The human selects which
components are in scope via one or more `--component` flags.

Compiler emits the corresponding RBACProfile YAMLs from the embedded catalog,
stamped and ready for Guardian-governed application. No cluster connectivity is
required.

**Current catalog entries:**

| Component             | RBACProfile name                     | Description                              |
|-----------------------|--------------------------------------|------------------------------------------|
| cilium                | rbac-cilium                          | Cilium CNI agent and operator            |
| cnpg                  | rbac-cnpg                            | CloudNativePG operator                   |
| kueue                 | rbac-kueue                           | Kueue batch scheduler                    |
| cert-manager          | rbac-cert-manager                    | cert-manager controller and webhook      |
| local-path-provisioner| rbac-local-path-provisioner          | Rancher local-path-provisioner           |

New catalog entries are added via Pull Request to the conductor repository. New
entries require a canonical RBACProfile definition reviewed by the Platform Governor
before merge.

### Custom Mode (optional cluster connectivity)

For components not in the catalog, the human provides a component descriptor file
via `--descriptor <path>`. Compiler renders an RBACProfile scaffold from the descriptor
for human review before GitOps commit.

The optional `--discover` flag enables cluster connectivity to auto-detect deployed
third-party resources. Cluster access follows the same resolution order as
`compiler maintenance` (flag → env var → conventional path).

**Invariants:**
- `compiler component` is the only path to creating third-party RBACProfiles. No
  operator generates third-party RBACProfiles at runtime.
- Guardian's admission webhook enforces RBACProfile declarations but never creates
  them. The contract direction is: component declares needs → Guardian enforces.
- The embedded catalog is the versioned source of truth for canonical component
  RBACProfiles. Catalog version is tied to the compiler release tag.
- F-P6: compiler component real implementation — catalog scaffold and RBACProfile
  template rendering — requires a Conductor Engineer session.

---

## 17. RunnerConfig Execution Model

**LOCKED GOVERNOR DECISION — Platform Governor directive 2026-04-05.**

### Multi-Step Intent

A RunnerConfig represents a multi-step operation intent, not a single Job. The spec
carries a `steps` list where each step declares:

- `name`: unique identifier within this RunnerConfig's step list.
- `capability`: the named capability identifier Conductor execute mode dispatches.
- `parameters`: input parameter map passed to the capability at Job materialisation.
- `dependsOn`: optional reference to a prior step name. The step is not eligible
  for execution until the referenced step has reached Succeeded state.
- `haltOnFailure`: boolean. When true, any failure on this step terminates the
  entire RunnerConfig with terminal condition Failed and no further steps execute.

A RunnerConfig with a single entry in the steps list is the degenerate case of this
model — not a separate model. All RunnerConfigs use the steps list, even single-step
operations.

### Step Sequencer Responsibility

Conductor execute mode is the sole authority over step-to-step progression. It
materialises one Job at a time in declared order. The sequencer:

1. Scans the steps list for the first step that is eligible: its `dependsOn` step
   (if any) has reached Succeeded, and the step itself has not yet reached a
   terminal state.
2. Materialises the Kueue Job for that step using the step's capability and parameter
   fields.
3. Monitors the Job for terminal state (Succeeded or Failed).
4. On Job completion, harvests the structured output from the well-known ConfigMap
   named after the Job in ont-system.
5. Writes a `StepResult` entry into RunnerConfig status for that step.
6. Evaluates terminal conditions and either advances to the next eligible step or
   writes the terminal RunnerConfig condition.

The owning operator never drives step-to-step progression. The owning operator
never submits individual Jobs. This boundary is permanent and locked.

### StepResult

Each completed step produces one StepResult entry in RunnerConfig status:

- `stepName`: matches the step name declared in spec.
- `phase`: lifecycle state — `Pending`, `Running`, `Succeeded`, or `Failed`.
- `outputRef`: reference to the ConfigMap in ont-system from which the result was
  harvested. ConfigMap is named after the Job. It is garbage-collected after TTL.
- `result`: raw JSON payload — the structured OperationResult document from the
  ConfigMap. Conductor writes this verbatim without semantic interpretation.

### Terminal Conditions

RunnerConfig reaches one of two terminal conditions:

- `Completed`: all steps in the steps list reached Succeeded state.
- `Failed`: any step reached Failed state with `haltOnFailure: true`, or the final
  step failed.

Once a terminal condition is written, the RunnerConfig is inert. No further Jobs
are submitted. The owning operator reads the terminal condition and step results
from status to perform semantic interpretation and downstream action in its own
reconciliation loop.

### Boundary Contract

**Conductor harvests and records only.** Conductor never interprets what a step
result means for the domain. It does not know whether a step failure is retryable.
It does not know whether a partial completion is acceptable. It writes StepResult
entries and terminal conditions — nothing more.

**The owning operator interprets only.** The owning operator watches RunnerConfig
status for the terminal condition. It reads StepResult entries. It decides what the
results mean and what downstream actions to take. It never submits Jobs directly.
It never calls into Conductor. The operator–Conductor contract is mediated entirely
by RunnerConfig spec and status.

This boundary is permanent and locked. No future implementation work may blur it
without a Governor constitutional amendment.

---

## 18. Federation Channel Contract

**LOCKED INVARIANT — Platform Governor directive 2026-04-05.**

The Conductor agent process on the management cluster exposes two listener ports.

**Internal port:**
Serves cluster-local callers using cluster-internal certificate authority trust.
All local components — operators, Guardian, Compiler tools during bootstrap — call
this port. Environment variable: `INTERNAL_PORT`.

**Federation port:**
Environment variable: `FEDERATION_PORT`, defaulting to `9443`. Serves tenant
Conductor connections exclusively. The TLS configuration on the federation port uses
management CA-issued client certificates with the connecting tenant's cluster ID
embedded as Subject Alternative Name. The federation port rejects any connection
that cannot present a valid management CA certificate. No other caller type may
connect to the federation port.

**Stream model:**
One persistent bidirectional gRPC stream per connected tenant Conductor. The stream
is established by the tenant Conductor; the management Conductor accepts it. The
management side is stateless with respect to connection lifecycle — it does not
initiate connections and does not maintain expectations about which tenants are
connected at any given time.

**Typed message envelope:**
All messages on the stream carry a typed envelope. The following message types form
the stable contract:

| Message                        | Direction               | Purpose                                                              |
|--------------------------------|-------------------------|----------------------------------------------------------------------|
| RunnerConfigValidationRequest  | tenant → management     | Validate RunnerConfig parameters before Job materialisation          |
| RunnerConfigValidationResponse | management → tenant     | Validation result: approved or rejected with structured reason       |
| AuditEventBatch                | tenant → management     | Batch of sequenced audit events for management AuditSink             |
| AuditEventAck                  | management → tenant     | Acknowledgement of received audit batch by sequence number           |
| RevocationPush                 | management → tenant     | Management-initiated revocation of a PackInstance or PermissionSnapshot |
| HeartBeat                      | bidirectional           | Liveness signal, sent by both sides at 30-second intervals           |

RevocationPush messages are sent management → tenant without waiting for the tenant
to initiate. This is the architectural justification for a persistent bidirectional
stream over individual RPCs: the management side must be able to push revocations
to tenants without the tenant polling. A stateless request-response model cannot
satisfy this requirement.

**Heartbeat discipline:**
Both sides send HeartBeat messages at 30-second intervals. Three consecutive missed
HeartBeat acknowledgments from a tenant marks that channel degraded in management
status tracking. Management records the degraded state in RunnerConfig status for
the affected tenant — the state is observable but not actionable by the management
side. Reconnect responsibility belongs entirely to the tenant Conductor.

**Tenant Conductor behavior on channel degradation:**
- Buffers audit events in a local write-ahead buffer backed by a PVC in ont-system.
- Fails RunnerConfig validation calls closed — no validation is attempted without
  an active management channel stream.
- Emits `FederationChannelDegraded` condition on its local RunnerConfig status.

**Tenant Conductor behavior on reconnect:**
Replays unacknowledged audit events starting from the last acknowledged sequence
number. The management AuditSink deduplicates received events on sequence number —
replays are safe and expected. RunnerConfig validation calls resume immediately on
stream re-establishment.

**Ownership boundary:**
The federation channel is a Conductor concern exclusively. Guardian on both the
management cluster and tenant clusters is a consumer and producer of channel
contents — Guardian produces audit events and receives revocations — but Guardian
does not own the channel, does not configure it, and does not monitor its health.
Guardian health and federation channel health are independent.

Tenant Guardian running role=management (sovereign mode) has no federation channel
relationship with the management Guardian. A sovereign tenant Guardian is fully
independent — no audit forwarding, no cross-cluster identity federation unless an
explicit `federated-downstream` IdentityProvider CR is authored by a human. The
tenant Conductor for a sovereign tenant cluster still connects to the management
Conductor federation port for RunnerConfig validation — the channel is always
Conductor-to-Conductor, never Guardian-to-Guardian.

This is a locked invariant. The typed message envelope contract is stable. Adding
new message types requires a Platform Governor directive before implementation.

---

*runner.ontai.dev schema — conductor repository*
*Amendments appended below with date and rationale.*

2026-03-30 — Two-binary model adopted. Compiler confined to compile mode (debian).
  Conductor owns execute and agent modes (distroless). runnerImage field renamed to
  agentImage on RunnerConfig. Execute mode Jobs confirmed as pure Go — no system
  binaries required. Signing and verification model added to agent
  responsibilities. CR-INV-009 through CR-INV-010 merged into root CLAUDE.md as
  INV-022 through INV-026.

2026-03-30 — Capability table updated with Triggering CRD column (Path B ruling).
  talos-upgrade, kube-upgrade, stack-upgrade, node-scale-up, node-decommission, and
  node-reboot confirmed retained. Triggering CRDs are active when TalosCluster
  spec.capi.enabled=false only. For capi.enabled=true target clusters CAPI handles
  these operations natively. Orphaned-constant finding closed — these six capability
  constants are not orphaned.

2026-04-03 — Binary rename throughout: conductor → Compiler, conductor → Conductor.
  Repository renamed conductor (was conductor). Module path updated to
  github.com/ontai-dev/conductor. Section 11 Enterprise License Enforcement removed
  entirely — Seam is fully open source with no licensing tier; replaced with single
  sentence. All licensing references removed from RunnerConfig spec and status fields:
  licenseSecretRef removed, licenseStatus removed, LicenseConstraint condition removed.
  Agent startup sequence step 3 (license check) removed; steps renumbered. Section 9
  pack compilation corrected: removed erroneous description of pack-compile as Kueue
  Job triggered by Wrapper; pack-compile is a Compiler invocation mode only.
  Operator name references updated: Platform (formerly platform), Guardian
  (formerly guardian), Wrapper (formerly wrapper). Capability table updated
  to reference consolidated day-two CRDs: UpgradePolicy, NodeOperation,
  EtcdMaintenance, NodeMaintenance, PKIRotation, ClusterReset, HardeningProfile.

2026-04-05 — Two locked Governor directives added. Section 12 "Operational Readiness
  Gates": Platform operator is the sole owner of port 50000 Talos API reachability
  validation across native and CAPI paths; Screen and all other components are
  permanently excluded. Section 13 "RunnerConfig Self-Operation Contract": three new
  first-class scheduling fields added to RunnerConfig spec (maintenanceTargetNodes,
  operatorLeaderNode, selfOperation); Conductor execute mode applies NotIn node
  affinity constraints when selfOperation=true; skips exclusion when selfOperation=false;
  agent mode acts as recovery path only. Dockerfile Standards renumbered to Section 14.

2026-04-05 — compiler maintenance subcommand added to Section 9. Produces
  MaintenanceBundle CR with pre-encoded scheduling context: maintenanceTargetNodes
  (validated node set), operatorLeaderNode (resolved from platform-leader Lease),
  s3ConfigSecretRef (validated at compile time per platform-schema.md §10),
  operation (drain/upgrade/etcd-backup/machineconfig-rotation). Cluster access
  resolved from flag → env var → conventional path, consistent with launch and enable.
  Fails fast if leader lease absent, nodes invalid, or S3 Secret missing.
  MaintenanceBundle CRD definition deferred to Platform Schema Engineer session (F-P5).

2026-04-05 — Three locked Governor directives added. (1) Management Cluster Bootstrap
  Sequence Authority added to Section 9: bootstrap/launch/enable owned exclusively by
  Compiler in three steps; Platform has no involvement; Compiler never applies
  resources; compiler enable stamps role=management on Conductor Deployment. Canonical
  Compiler Command Surface table added (bootstrap, launch, enable, packbuild,
  maintenance, component, domain). (2) Section 15 "Role Declaration Contract" added:
  Conductor reads role field (management or tenant) stamped externally at Deployment
  creation; compiler enable stamps management; Platform operator stamps tenant;
  Conductor never infers its own role; absent/unrecognized role causes InvariantViolation
  exit. (3) Section 16 "compiler component Subcommand" added: emits RBACProfile CRs
  for third-party components; catalog mode (embedded versioned catalog: Cilium, CNPG,
  Kueue, cert-manager, local-path-provisioner) and custom mode (--descriptor flag,
  optional --discover for cluster auto-detect); catalog at internal/catalog/;
  F-P6 open finding for implementation.

2026-04-05 — Section 17 "RunnerConfig Execution Model" added as locked Governor
  decision. RunnerConfig is a multi-step operation intent: spec carries a steps list
  (name, capability, parameters, dependsOn, haltOnFailure). Conductor execute mode is
  the sole authority over step-to-step progression — materialises one Job at a time,
  harvests OperationResult ConfigMap from ont-system on completion, writes StepResult
  (stepName, phase, outputRef, result payload) into RunnerConfig status. Terminal
  conditions: Completed (all steps succeeded) and Failed (halt-on-failure semantics).
  Owning operator watches terminal condition and interprets step results — never drives
  progression. Conductor harvests and records only — never interprets. Boundary is
  permanent and locked. F-P7 added to CONTEXT.md: all existing platform day-2
  reconcilers must migrate from single-capability RunnerConfig to step list model.

2026-04-05 — Two locked Governor directives added. (1) §9 compiler enable bundle
  restructured to six phases: phase 0 (00-infrastructure-dependencies: CNPG operator
  manifests + CNPG Cluster CR for management Guardian, readiness gate: CNPG Cluster
  ready), phase 1 (01-guardian-bootstrap), phases 2-5 are the existing phases
  (02-guardian-deploy, 03-platform-wrapper, 04-conductor, 05-post-bootstrap). Phase 0
  resolves the CNPG dependency before Guardian is deployed; Guardian startup migration
  runner connects to CNPG before registering any controller. F-P8 recorded: phase 0
  implementation requires a Conductor Engineer session. (2) §18 "Federation Channel
  Contract" added (locked invariant): management Conductor exposes two ports — internal
  (cluster-local CA) and federation (FEDERATION_PORT=9443, management CA client certs,
  cluster ID as SAN, rejects invalid certs). One persistent bidirectional gRPC stream
  per connected tenant Conductor; management side stateless on connection lifecycle;
  tenant Conductor owns reconnect. Typed message envelope: RunnerConfigValidationRequest/
  Response, AuditEventBatch, AuditEventAck, RevocationPush, HeartBeat. RevocationPush
  is management-initiated — architectural justification for persistent stream over RPCs.
  Heartbeat: 30s interval; 3 missed ACKs = channel degraded in management RunnerConfig
  status. Tenant on degradation: WAL PVC buffer in ont-system, validation calls fail
  closed, FederationChannelDegraded condition. On reconnect: replay from last acked
  sequence; AuditSink deduplicates. Federation channel is Conductor-Conductor exclusively;
  Guardian is a consumer/producer, not an owner. Sovereign tenants (role=management
  Guardian) have no Guardian-to-Guardian federation relationship; Conductor channel
  remains. Adding message envelope types requires a Governor directive.
