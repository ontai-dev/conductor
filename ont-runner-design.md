# Development Standard

> This document governs all internal design decisions for the ont-runner binary.
> Every agent reads this before beginning any runner implementation work.

---

## 1. Architectural Principles

### 1.1 Single Authority
All execution logic resides inside the runner. No delegation to external binaries
or shell. No capability logic outside the runner. Operators are declarative
frontends. Kubernetes is the scheduling and state persistence substrate.

### 1.2 Deterministic Execution
Same inputs produce identical outputs. No hidden state. No implicit dependencies.
All side effects are explicit and recorded in OperationResult.

### 1.3 Mode Isolation
Compile, executor, and agent modes share the codebase but not behavior. Cross-mode
client invocation is forbidden by invariant enforcement at the kernel layer. Each
mode has a dedicated execution pipeline. Violation causes immediate structured exit.

### 1.4 Explicit State Contracts
RunnerConfig is the input contract. OperationResult is the output contract.
PVC is the optional intermediate state contract for multi-step capabilities.
No other state channels exist.

### 1.5 Capability as Instruction
Each capability is an instruction. The runner is an execution kernel interpreting
instructions. No dynamic behavior exists outside declared capabilities. Unknown
capabilities cause immediate structured failure — never silent.

---

## 2. Internal Module Topology

The runner is segmented into strict internal modules. These are not optional —
they define maintainability boundaries. No module accesses another module's
internals directly. All inter-module communication is through defined interfaces.

### 2.1 Core Kernel Layer
Responsibility: process lifecycle, mode dispatch, fatal invariant enforcement.
Subcomponents: Bootstrap Loader, Mode Router, Panic Boundary Handler,
Structured Exit Controller.

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

### 2.4 Execution Pipeline Engine
Responsibility: orchestrate step-by-step execution inside a capability.
Subcomponents: Step Executor, Step Dependency Graph (multi-step only), Retry
Controller, Timeout Controller.
Guarantees: ordered execution, deterministic step transitions, explicit failure
propagation. No step begins until its declared dependencies are satisfied.

### 2.5 State Management Layer
Responsibility: manage all execution state transitions.
Subcomponents: PVC State Manager (executor mode only), Artifact Registry
(in-memory references only — never raw payloads), State Serializer.
Rule: no implicit state. All intermediate outputs explicitly written and read.

### 2.6 Client Abstraction Layer
Responsibility: provide controlled access to external systems.
Subcomponents: Kubernetes Client Wrapper, Talos Client Wrapper,
Helm Renderer (compile mode only), Kustomize Resolver (compile mode only).

Mode-based access enforcement:
- Compile mode: Helm Renderer and Kustomize Resolver allowed. Talos Client
  forbidden.
- Executor mode: Kubernetes Client and Talos Client allowed. Helm Renderer and
  Kustomize Resolver forbidden.
- Agent mode: Kubernetes Client allowed. Talos Client for node health observation
  only. Helm Renderer and Kustomize Resolver forbidden.

Violation is classified as InvariantViolation and causes immediate structured exit.
Enforcement happens in the client wrappers — they check current mode before any
call.

### 2.7 Result Construction Layer
Responsibility: build OperationResult deterministically.
Subcomponents: Result Builder, Step Result Aggregator, Failure Classifier.
Output: structured OperationResult object. Never partial. Never implicit.

### 2.8 Persistence Layer
Responsibility: persist execution outputs.
Subcomponents: ConfigMap Writer (OperationResult in executor mode),
Receipt Writer (agent mode only), RunnerConfig Status Writer (agent mode only).

### 2.9 Security and License Layer
Responsibility: enforce license constraints and validate JWT.
Subcomponents: License Validator, Cluster Count Evaluator, Enforcement Gate.
Behavior: hard stop on violation. Agent refuses to start for a violating cluster.
Exit code 2 (license error — distinct from crash exit codes).

### 2.10 Agent Control Loop Layer (agent mode only)
Responsibility: long-running reconciliation loops.
Subcomponents: Leader Election Controller, Capability Publisher, Drift Detection
Engine, Permission Snapshot Pull Loop, Receipt Reconciliation Engine, Webhook Server.
Single writer via leader election. Read operations may be concurrent.

---

## 3. Binary Entry Points

**ont-runner compile** — compile mode. No cluster connection required. Validates
input specs, generates and SOPS-encrypts secrets, produces compiled output for git
or OCI registry. Exit code 0 on success, non-zero with structured error on failure.

**ont-runner execute** — executor mode. Reads CAPABILITY, CLUSTER_REF, and
OPERATION_RESULT_CM from environment. Reads RunnerConfig from mounted ConfigMap.
Executes the named capability. Writes OperationResult to named ConfigMap. Exits.

**ont-runner agent** — agent mode. Reads RunnerConfig from mounted ConfigMap.
Validates license before any other initialization. Starts leader election. On
leader win: starts webhook server and all control loops. Runs indefinitely.

---

## 4. Execution Flow by Mode

### 4.1 Compile Mode Pipeline

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

### 4.2 Executor Mode Pipeline

Phase 1 — Bootstrap: load RunnerConfig, validate capability availability, read
CAPABILITY env var.

Phase 2 — Capability Resolution: resolve from registry, initialize execution
pipeline with step sequence.

Phase 3 — State Initialization: attach or create PVC if multi-step, load previous
step state from PVC if resuming.

Phase 4 — Execution: execute steps sequentially, persist intermediate artifacts to
PVC after each step.

Phase 5 — Finalization: aggregate step results, write OperationResult to ConfigMap,
clean up PVC if terminal step.

Phase 6 — Exit: structured exit code 0 (success) or non-zero (failure). No
lingering processes.

### 4.3 Agent Mode Pipeline

Phase 1 — Bootstrap: load RunnerConfig, validate license, start leader election.

Phase 2 — Capability Declaration: on leader win, publish capability manifest to
RunnerConfig status.

Phase 3 — Service Initialization: start webhook server, initialize control loop
goroutines.

Phase 4 — Continuous Operation: drift detection loop, permission snapshot pull
loop, receipt reconciliation loop.

Phase 5 — Periodic Tasks: license revalidation every 24 hours, RunnerConfig
status refresh on capability manifest change.

---

## 5. Capability Authoring Specification

Every Runner Engineer reads this section before implementing any new capability.

### 5.1 Capability Identity Rules

- name: globally unique, immutable. Capability names are permanent. Renaming is
  forbidden. Fundamental behavior changes require a new name.
- owner: originating operator domain (platform, infra, security).
- mode: executor-only unless explicitly agent-internal.
- category: lifecycle, infrastructure, security, compile.

### 5.2 Capability Versioning

Each capability carries an internal semantic version tied to the runner version.
No independent capability releases.
- major: breaking change in behavior or input/output schema.
- minor: backward-compatible enhancements.
- patch: bug fixes with identical semantics.

### 5.3 Input and Output Contracts

Input contract: strict schema derived from RunnerConfig (primary context), the
operational CR (intent), and environment variables. Inputs are fully validated
before execution begins, immutable throughout, and version-aware. No dynamic
input discovery during execution.

Output contract: every capability produces a mandatory OperationResult and optional
artifact references (never raw content). Outputs are structured, deterministic,
and reproducible from identical inputs.

### 5.4 Capability Classification

Single-Step: atomic, no PVC. Examples: node-reboot, credential-rotate.

Multi-Step: composed of ordered steps, requires PVC state protocol.
Examples: bootstrap, stack-upgrade, cluster-reset.

Compile: compile mode only, artifact-producing. Examples: pack-compile.

Agent-Internal: not externally invokable, used by agent loops.
Examples: drift-evaluation (internal), snapshot-application (internal).

### 5.5 Step Design Rules

Each step defines: name (unique within capability), explicit input dependencies,
explicit output artifact references, deterministic execution function, timeout,
and retry policy.

Steps execute strictly sequentially unless explicitly parallelized within the step.
No hidden dependencies. All data exchange via explicit artifacts.

Idempotency: every step must be safe to re-run, resistant to partial execution,
free from duplicate side effects. This is tested — not assumed.

Isolation: no shared mutable state across steps except PVC artifacts. No global
variables. No reliance on external timing.

### 5.6 PVC Protocol for Multi-Step Capabilities

PVC name convention: ont-{capability}-{operation-cr-name}
Namespace: same as the Job (ont-system or tenant namespace).
Size: declared in shared library capability manifest. Not dynamically sized.

First step: create PVC if absent. Execute. Write artifacts to PVC.
Intermediate steps: mount PVC. Read prior artifacts. Execute. Write outputs.
Final step: consume all artifacts. Create management cluster Kubernetes assets.
Delete PVC. Write terminal OperationResult.

PVC creation failure: exit immediately with StorageUnavailable. The operator
gate failure protocol handles retry. The runner never auto-retries PVC failures.

### 5.7 Capability Registration Requirements

Before a capability may be added to the registry:
- Input schema finalized and documented.
- Step sequence validated with all failure modes declared.
- Invariants documented.
- All test requirements met (unit, integration, failure injection, determinism).

Registration process: Phase 1 Design → Phase 2 Implementation → Phase 3 Validation
→ Phase 4 Registration (add to registry, add to shared library manifest) → Phase 5
Release (included in runner version, exposed via agent capability manifest) →
Phase 6 Evolution (versioned extensions, backward compatibility maintained).

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

The operator advances the CR to Failed status and surfaces the structured reason.
The operator never auto-retries. Every retry is a deliberate human action.
INV-018.

### 6.3 Isolation

Each Job is a hard isolation boundary. No shared memory across Jobs. Only PVC
provides controlled continuity within a single multi-step capability sequence.

---

## 7. License JWT Specification

### 7.1 Token Structure

All ONT JWTs — community and enterprise — share the same structure. The runner's
license validation code path is identical for both. No special-casing.

Standard claims: iss (must be ontave.dev), sub (community or customerID), exp
(expiry unix timestamp), iat (issued at).

ONT custom claims:
- ont_tier: community, standard, or enterprise.
- ont_max_clusters: integer. Community always 3.
- ont_max_tenants: integer. Community always 3.
- ont_customer_id: community or organization ID.
- ont_features: array of feature flag strings.

### 7.2 Community Token

A well-known public JWT embedded as a constant in the runner binary. Not secret.
Claims are public. Used when no licenseSecretRef is configured. The runner always
validates a JWT — community or enterprise — through identical code.

### 7.3 Signature

RS256 (RSA + SHA-256). The ontave.dev private key signs all tokens at issuance.
The corresponding public key is embedded in the runner binary at build time.
Validation requires no network call. Works fully air-gapped.

### 7.4 Enterprise Token Injection

Stored as Kubernetes Secret in ont-system. Secret key: license.jwt.
RunnerConfig spec.licenseSecretRef references this Secret by name. Agent mounts
it as a projected volume. License revalidation reads the mounted file every 24h.
Token renewal updates the Secret — projected volume updates automatically. No
agent restart required for license renewal.

### 7.5 License Enforcement Sequence (Agent Startup)

Executes before any other initialization:

1. Count managed clusters by listing RunnerConfig resources across namespaces.
2. Count <= 5: proceed with community tier. Validate embedded community JWT
   structurally only.
3. Count > 5 and no licenseSecretRef: write LicenseConstraint to RunnerConfig
   status. Exit code 2.
4. licenseSecretRef present: read Secret, parse JWT, validate iss=ontave.dev,
   exp in future, ont_max_clusters >= current count. Pass: write Licensed, proceed.
   Fail: write LicenseExpired or LicenseConstraint. Exit code 2.

Revalidation every 24h in background goroutine. On failure: update status. Do not
terminate running Jobs or disrupt workloads. Block only new Job submissions via
CapabilityUnavailable.

---

## 8. Concurrency Model

Job-level concurrency: controlled by Kueue. Each capability execution runs in an
isolated Job. The runner has no awareness of other concurrently running Jobs.

Intra-job concurrency: allowed only within steps where explicitly declared.
Must not violate determinism.

Agent concurrency: single writer via leader election lease. Read operations may
be concurrent across replicas. Leader election lease name: ont-runner-agent.
Lease namespace: ont-system (management) or tenant-{name} (target cluster).
Lease duration: 15s. Renew deadline: 10s. Retry period: 2s.

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
through ont-security. A capability needing RBAC changes submits the requirement
to ont-security's intake — it never writes Role, ClusterRole, RoleBinding,
ClusterRoleBinding, or ServiceAccount resources directly.

Secrets are never logged. Only references are stored in artifacts. Secrets are
resolved before the Job starts via mounted volumes — the runner does not read
secrets from the Kubernetes API during capability execution.

All inputs are validated before execution. Invalid input triggers ValidationFailure
immediately before any step executes.

---

## 11. Anti-Patterns — Strictly Forbidden

- Invoking shell commands or external binaries from capability logic.
- Embedding Helm or Kustomize invocations in executor or agent mode.
- Dynamic capability behavior not declared in the capability manifest.
- Implicit retries without a declared retry policy on the step.
- Reading cluster state without version or context validation.
- Writing unmanaged Kubernetes resources (without ONT ownership labels).
- Bypassing the OperationResult protocol — partial writes, missing fields.
- Cross-capability state sharing.
- Hardcoding runner image references in capability logic.
- Writing RBAC resources directly from any capability.

---

## 12. Shared Library Export Contract

Package path: github.com/ontai-dev/ont-runner/pkg/runnerlib
Versioned using Go module semver.

Exported surface (operators depend on these — changes require major version for
breaking, minor for non-breaking):
- RunnerConfigSpec and all nested types.
- CapabilityManifest and CapabilityEntry types.
- OperationResultSpec type.
- GenerateFromTalosCluster(spec) → RunnerConfigSpec.
- GenerateFromPackBuild(spec) → RunnerConfigSpec.
- JobSpecBuilder interface.

Breaking change protocol: major version bump, all operators update dependency,
runner and operators released together.
Non-breaking change protocol: minor version bump, runner release may precede
operator dependency updates.

---

## 13. Dockerfile Standard

Base: debian:12-slim. Not distroless.

Rationale: The runner requires system-level runtime dependencies that distroless
cannot satisfy: system CA bundles for full PKI operations beyond Go's crypto/tls,
psql for CNPG health verification in security plane operations, Python3 for SOPS
age key handling in compile mode, kubectl and talosctl binaries for operations
not yet covered by the goclient wrappers. These are genuine runtime requirements.

Build pattern: golang:1.25 builder stage compiles the runner binary. debian:12-slim
final stage includes: bash, curl, jq, python3, openssl, psql, helm binary, kubectl,
talosctl, and the compiled runner Go binary. USER 65532:65532. No package manager
retained in the final image.

---

*ont-runner development standard*
*Amendments appended below with date and rationale.*