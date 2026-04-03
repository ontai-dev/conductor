# Development Standard
> This document governs all internal design decisions for the conductor repository.
> The repository produces two binaries: conductor (compile mode, debian) and
> conductor (execute and agent modes, distroless).
> Every agent reads this before beginning any implementation work.

---

## 1. Architectural Principles

### 1.1 Single Authority
All execution logic resides inside conductor. No delegation to external binaries
or shell. No capability logic outside conductor. Operators are declarative
frontends. Kubernetes is the scheduling and state persistence substrate.

All compile-time logic resides inside conductor. No compile-mode client invocations
outside conductor. The two binaries share a codebase but have strictly separated
responsibilities.

### 1.2 Deterministic Execution
Same inputs produce identical outputs. No hidden state. No implicit dependencies.
All side effects are explicit and recorded in OperationResult.

### 1.3 Mode Isolation
Compile, execute, and agent modes share the codebase but not behavior. Mode
boundaries are enforced at two levels: runtime (InvariantViolation check on client
invocation) and build time (compile-mode clients excluded from conductor via Go
build tags). Each mode has a dedicated execution pipeline.

### 1.4 Explicit State Contracts
RunnerConfig is the input contract. OperationResult is the output contract.
PVC is the optional intermediate state contract for multi-step capabilities.
No other state channels exist.

### 1.5 Capability as Instruction
Each capability is an instruction. conductor is an execution kernel interpreting
instructions. No dynamic behavior exists outside declared capabilities. Unknown
capabilities cause immediate structured failure — never silent.

### 1.6 Zero Attack Surface on Clusters
Every image deployed to any cluster in the ONT stack is distroless. The only
non-distroless image is conductor, which is never deployed to any cluster.
No shell. No package manager. No system tools on any cluster. INV-022.

---

## 2. Internal Module Topology

Both binaries are built from the same internal module structure. Modules marked
(conductor only) are excluded from the conductor build via the `-tags agent`
build tag. All other modules are shared.

### 2.1 Core Kernel Layer
Responsibility: process lifecycle, mode dispatch, fatal invariant enforcement.
Subcomponents: Bootstrap Loader, Mode Router, Panic Boundary Handler,
Structured Exit Controller.

The Mode Router enforces binary identity: if the binary is conductor and the
requested mode is compile, it calls the Structured Exit Controller with
InvariantViolation before any other initialization proceeds.

### 2.2 Configuration Layer
Responsibility: load and validate RunnerConfig, provide immutable configuration
snapshot for the entire execution lifecycle.
Subcomponents: RunnerConfig Loader, Schema Validator, Immutable Context Builder.
Output: ExecutionContext — read-only for the entire lifecycle. No mutation after
construction.

### 2.3 Capability Engine
Responsibility: resolve and execute named capabilities.
Subcomponents: Capability Registry, Capability Resolver, Capability Dispatcher.
Registration is static at build time. No runtime plugin loading. Missing capability
causes immediate structured failure before any execution begins.
This module exists only in conductor.

### 2.4 Execution Pipeline Engine
Responsibility: orchestrate step-by-step execution inside a capability.
Subcomponents: Step Executor, Step Dependency Graph (multi-step only), Retry
Controller, Timeout Controller.
Guarantees: ordered execution, deterministic step transitions, explicit failure
propagation. No step begins until its declared dependencies are satisfied.
This module exists only in conductor.

### 2.5 State Management Layer
Responsibility: manage all execution state transitions.
Subcomponents: PVC State Manager (execute mode only), Artifact Registry
(in-memory references only — never raw payloads), State Serializer.
Rule: no implicit state. All intermediate outputs explicitly written and read.

### 2.6 Client Abstraction Layer
Responsibility: provide controlled access to external systems.
Subcomponents:
- Kubernetes Client Wrapper: all modes, both binaries.
- Talos Client Wrapper: execute and agent modes, conductor only. Pure Go gRPC.
  No talosctl binary. INV-013.
- Helm Renderer: compile mode, conductor only. Excluded from conductor build. INV-014.
- Kustomize Resolver: compile mode, conductor only. Excluded from conductor build. INV-014.
- SOPS Handler: compile mode, conductor only. Excluded from conductor build.

Mode-based access enforcement:
- Compile mode (conductor): Helm Renderer, Kustomize Resolver, SOPS Handler allowed.
  Talos Client forbidden.
- Execute mode (conductor): Kubernetes Client and Talos Client allowed.
  Helm Renderer, Kustomize Resolver, and SOPS Handler not present in binary.
- Agent mode (conductor): Kubernetes Client allowed. Talos Client for node health
  observation only. Helm Renderer, Kustomize Resolver, SOPS Handler not present.

Violation of compile-mode client usage in execute or agent mode is impossible at
runtime because those clients do not exist in the conductor binary.

### 2.7 Result Construction Layer
Responsibility: build OperationResult deterministically.
Subcomponents: Result Builder, Step Result Aggregator, Failure Classifier.
Output: structured OperationResult object. Never partial. Never implicit.
This module exists only in conductor (execute mode).

### 2.8 Persistence Layer
Responsibility: persist execution outputs.
Subcomponents:
- ConfigMap Writer: OperationResult in execute mode. conductor only.
- Receipt Writer: PackReceipt and PermissionSnapshotReceipt. Agent mode. conductor only.
- RunnerConfig Status Writer: capability manifest. Agent mode. conductor only.
- Signing Writer: writes cryptographic signatures to PackInstance and PermissionSnapshot.
  Agent mode, management cluster only. conductor only.

### 2.9 Security Layer
Responsibility: cryptographic signature verification for PackInstance and PermissionSnapshot.
Seam is fully open source with no licensing tier.

### 2.10 Agent Control Loop Layer (agent mode only, conductor only)
Responsibility: long-running reconciliation loops.

**Subcomponents shared across management and target cluster agents:**
- Leader Election Controller
- Capability Publisher (writes capability manifest to RunnerConfig status)
- Drift Detection Engine (PackReceipt drift comparison)
- Permission Snapshot Pull Loop (pulls PermissionSnapshot from management cluster)
- Receipt Reconciliation Engine (PackReceipt, PermissionSnapshotReceipt)
- Admission Webhook Server (intercepts RBAC resources, enforces ownership annotation)
- Local PermissionService gRPC Server (serves authorization decisions from local receipt)

**Additional subcomponents on management cluster agent only:**
- PackInstance Signing Loop: watches for new ClusterPack registrations by wrapper,
  signs PackInstance CRs with the platform signing key, writes signature annotation.
- PermissionSnapshot Signing Loop: watches for new PermissionSnapshot generation by
  guardian, signs them with the platform signing key, writes signature annotation.

Single writer via leader election. Read operations may be concurrent.

### 2.11 Compile Pipeline (compile mode only, conductor only)
Responsibility: cluster secret generation, pack compilation, bootstrap orchestration.
Subcomponents: TalosCluster Validator, Secret Generator, SOPS Encryptor,
Helm Pack Renderer, Kustomize Pack Resolver, Manifest Normalizer, OCI Pack Publisher,
Bootstrap Launch Orchestrator, Enable Phase Orchestrator.

---

## 3. Binary Entry Points

**conductor compile** — compile mode. conductor binary. No cluster connection
required unless verifying against a running cluster. Validates input specs, generates
and SOPS-encrypts secrets, produces compiled output for git or OCI registry. Also
orchestrates the bootstrap launch phase and the enable phase for a new management
cluster. Exit code 0 on success, non-zero with structured error on failure.

**conductor execute** — execute mode. conductor binary. Reads CAPABILITY,
CLUSTER_REF, and OPERATION_RESULT_CM from environment. Reads RunnerConfig from
mounted ConfigMap. Executes the named capability using pure Go clients only.
Writes OperationResult to named ConfigMap. Exits.

**conductor agent** — agent mode. conductor binary. Reads RunnerConfig from mounted
ConfigMap. Validates mode (refuses compile flag). Starts leader election. On leader
win: starts all control loops and servers. Runs indefinitely.

---

## 4. Execution Flow by Mode

### 4.1 Compile Mode Pipeline (conductor)

Phase 1 — Input Ingestion: load spec (TalosCluster or PackBuild), validate schema
and invariants.

Phase 2 — Resolution: Helm rendering via helm goclient, Kustomize overlay resolution
via kustomize goclient, manifest normalization to flat Kubernetes resources.

Phase 3 — Validation: schema validation against target Kubernetes version,
dependency graph acyclic check (circular dependencies are compile errors).

Phase 4 — Artifact Construction: RBAC synthesis for minimum necessary permissions,
execution ordering, image digest pinning (no floating tags), checksum computation.

Phase 5 — Output: write to filesystem (cluster compilation, SOPS-encrypted) or
OCI registry (pack compilation), produce validation report to stdout.

### 4.2 Execute Mode Pipeline (conductor)

Phase 1 — Bootstrap: load RunnerConfig, validate capability availability, read
CAPABILITY env var. Confirm mode is execute. Refuse compile mode.

Phase 2 — Capability Resolution: resolve from registry, initialize execution
pipeline with step sequence.

Phase 3 — State Initialization: attach or create PVC if multi-step, load previous
step state from PVC if resuming.

Phase 4 — Execution: execute steps sequentially using pure Go clients only.
Persist intermediate artifacts to PVC after each step.

Phase 5 — Finalization: aggregate step results, write OperationResult to ConfigMap,
clean up PVC if terminal step.

Phase 6 — Exit: structured exit code 0 (success) or non-zero (failure).

### 4.3 Agent Mode Pipeline (conductor)

Phase 1 — Bootstrap: load RunnerConfig, confirm mode is agent (refuse compile mode,
INV-023). Validate license (management cluster only).

Phase 2 — Capability Declaration: on leader win, publish capability manifest to
RunnerConfig status.

Phase 3 — Service Initialization: start admission webhook server, start local
PermissionService gRPC server, initialize control loop goroutines.

Phase 4 — Continuous Operation: drift detection loop, permission snapshot pull
loop, receipt reconciliation loop, admission webhook serving.

Phase 5 — Signing Loops (management cluster agent only): PackInstance signing
loop, PermissionSnapshot signing loop.

Phase 6 — Periodic Tasks: license revalidation every 24 hours (management cluster
only), RunnerConfig status refresh on capability manifest change.

---

## 5. Capability Authoring Specification

### 5.1 Capability Identity Rules

- name: globally unique, immutable. Capability names are permanent. Renaming is
  forbidden. Fundamental behavior changes require a new name.
- version: semver of the capability implementation within the agent binary.
- mode: always ExecutorMode for named capabilities. Agent-mode behaviors are not
  named capabilities — they are control loops.

### 5.2 Capability Parameter Contract

Each capability declares its parameters in the CapabilityEntry.ParameterSchema.
The operator builds the Job spec with these parameters as environment variables.
conductor reads them on execute mode startup. Unknown parameters are ignored.
Missing required parameters cause ValidationFailure before execution begins.

### 5.3 Pure Go Constraint

All capability implementations must use only the Go clients declared in Section 2.6
for their mode. No exec.Command. No os.Exec. No subprocess invocations of any kind.
The capability has access to: kube goclient, talos goclient (execute mode). That is
the complete list. If a capability requires an operation not available through these
clients, the operation must be added to the goclient wrapper, not worked around
via subprocess.

### 5.4 Capability Result Contract

Every capability writes a complete OperationResult before exit. Partial writes are
InvariantViolation. The OperationResult must be readable by the operator within the
Job's TTL (default 600 seconds). If the capability cannot produce a complete result
(fatal error mid-execution), it writes a failed OperationResult with the last known
step state and the failure category and reason.

### 5.5 Multi-Step Capabilities

Capabilities that require more than one Job step use the PVC protocol. The PVC
is named ont-{capability}-{cr-name}. The capability declares its step count and
the expected PVC layout in its CapabilityEntry. Each step reads the previous step's
PVC output before executing. The final step deletes the PVC.

Capabilities currently using the PVC protocol: bootstrap, stack-upgrade, cluster-reset.

---

## 6. Failure Domain Model

### 6.1 Failure Categories

Every failure is classified into exactly one:
- ValidationFailure: input does not meet schema or invariant requirements.
- CapabilityUnavailable: requested capability not in registry.
- ExecutionFailure: step-level failure during execution.
- ExternalDependencyFailure: Kubernetes API, Talos API, or OCI registry unreachable.
- InvariantViolation: programming error — mode boundary crossed, forbidden client
  invoked, contract violated.
- LicenseViolation: license constraints not satisfied.
- StorageUnavailable: PVC creation failed for multi-step capability.

### 6.2 Failure Behavior

Failure halts execution immediately. No implicit retries beyond the declared retry
policy on the specific step. Every failure produces a structured OperationResult
with category, precise reason, and failed step name.

The operator advances the CR to Failed status. The operator never auto-retries.
Every retry is a deliberate human action. INV-018.

### 6.3 Isolation

Each Job is a hard isolation boundary. No shared memory across Jobs. Only PVC
provides controlled continuity within a single multi-step capability sequence.

---

## 7. Licensing

Seam is fully open source with no licensing tier. All clusters are equal. No enforcement.

---

## 8. Concurrency Model

Job-level concurrency: controlled by Kueue. Each capability execution runs in an
isolated Job. conductor has no awareness of other concurrently running Jobs.

Intra-job concurrency: allowed only within steps where explicitly declared.
Must not violate determinism.

Agent concurrency: single writer via leader election lease. Read operations may
be concurrent across replicas. Leader election lease name: conductor-{cluster-name}.
Lease namespace: ont-system. Lease duration: 15s. Renew deadline: 10s. Retry: 2s.

On leader win: start all write-path goroutines. On leadership loss: stop all
write-path goroutines. Do not exit — remain in standby for next election cycle.

---

## 9. Observability Model

Logs: structured per step. Correlated by Job name and capability name. Include
step name, duration, outcome. Input and output references only — never raw secret
content or credential values.

Metrics: step duration, failure rates per capability, retry counts, capability
execution frequency. Exposed as Prometheus metrics from agent mode.

External visibility: OperationResult is the authoritative execution record.
RunnerConfig status is capability and agent health. Receipts are ground truth on
target clusters. Logs are diagnostic only — correctness depends on OperationResult.

---

## 10. Security Constraints in Capabilities

Capabilities must not create or modify RBAC resources directly. All RBAC flows
through guardian. A capability needing RBAC changes submits the requirement
to guardian's intake — it never writes Role, ClusterRole, RoleBinding,
ClusterRoleBinding, or ServiceAccount resources directly.

Secrets are never logged. Only references are stored in artifacts. Secrets are
resolved before the Job starts via mounted volumes — conductor does not read
secrets from the Kubernetes API during capability execution.

All inputs are validated before execution. Invalid input triggers ValidationFailure
immediately before any step executes.

---

## 11. Anti-Patterns — Strictly Forbidden

- Invoking shell commands or external binaries from any capability or agent loop.
- Any system binary or shell invocation in conductor at any point.
- Dynamic capability behavior not declared in the capability manifest.
- Implicit retries without a declared retry policy on the step.
- Reading cluster state without version or context validation.
- Writing unmanaged Kubernetes resources (without ONT ownership labels).
- Bypassing the OperationResult protocol — partial writes, missing fields.
- Cross-capability state sharing.
- Hardcoding agent image references in capability logic.
- Writing RBAC resources directly from any capability.
- Running Jobs on target clusters.
- Implementing compile-mode logic in conductor.

---

## 12. Shared Library Export Contract

Package path: github.com/ontai-dev/conductor/pkg/runnerlib
Versioned using Go module semver.

Exported surface (operators and both binaries depend on these):
- RunnerConfigSpec and all nested types. Note: runnerImage field is renamed to
  agentImage in this version.
- CapabilityManifest and CapabilityEntry types.
- OperationResultSpec type.
- GenerateFromTalosCluster(spec) → RunnerConfigSpec.
- GenerateFromPackBuild(spec) → RunnerConfigSpec.
- JobSpecBuilder interface.

Breaking change protocol: major version bump, all operators and both binaries update
dependency, all released together.

---

## 13. Dockerfile Standards

### 13.1 conductor Dockerfile (compile mode, debian)

Stage 1 — Builder:
- Base: golang:1.25
- Build tag: `-tags runner`
- Compile-mode clients (helm goclient, kustomize goclient, SOPS handler) are included.
- Output: conductor binary.

Stage 2 — Final:
- Base: debian:12-slim
- Packages: bash, curl, jq, python3, openssl, psql, helm binary (required for
  /etc/ssl/certs CA bundle during chart pulls).
- Copy: conductor binary from builder stage.
- USER 65532:65532.
- No package manager retained.

This image is never deployed to any cluster. It runs on operator workstations and
in compile-phase pipelines only. INV-022.

### 13.2 conductor Dockerfile (execute and agent modes, distroless)

Stage 1 — Builder:
- Base: golang:1.25
- Build tag: `-tags agent`
- Compile-mode clients (helm goclient, kustomize goclient, SOPS handler) are
  excluded at build time. The conductor binary cannot invoke them regardless of
  runtime flags.
- Output: conductor binary.

Stage 2 — Final:
- Base: gcr.io/distroless/base:nonroot
  (distroless/base not static: talos goclient and gRPC require libc for TLS and
  crypto operations. Verify the produced binary runs correctly on base before release.)
- Copy: conductor binary from builder stage only.
- USER 65532:65532.
- No shell. No package manager. No system tools. No binaries other than conductor.

This image is deployed to every cluster in the ONT stack as the agent Deployment
and as executor Jobs. INV-022.

---

*conductor development standard*
*Amendments appended below with date and rationale.*

2026-03-30 — Two-binary model adopted throughout. conductor confined to compile
  mode (debian, never deployed). conductor owns execute and agent modes (distroless,
  deployed everywhere). All shell invocations and binary dependencies removed from
  execute/agent modes — confirmed pure Go. License tier corrected: 5 target clusters
  community, management cluster excluded. Signing loop subcomponents added to
  Agent Control Loop Layer. Anti-patterns updated to include target cluster Job
  prohibition. Two Dockerfile standards defined.