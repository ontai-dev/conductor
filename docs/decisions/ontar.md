# ONTAR: Operator Native Task Application Runtime
## Governor Decision Record: April 2026

**Status:** Architectural thesis. Pending specification. Not yet in implementation.
**Owner:** Krishna
**Depends on:** Guardian PermissionSnapshot, Conductor compilation pipeline,
seam-core lineage chain

---

## The Thesis

The Talos philosophy applied to the container runtime boundary.

Talos proved that the shell is not a necessary primitive for operating a
Linux node. Every operation that operators historically needed a shell for
was a lifecycle event. Shell was the cheapest implementation path, not a
requirement. Talos replaced cheapest with correct.

ONTAR makes the same argument one layer down. Shell inside a container is
not a requirement. It is a habit. Every operation an application needs a
shell for is a lifecycle event. The Go binary as PID 1 is what apid is to
a Talos node: the sole legitimate API surface for all lifecycle operations,
with certificates as the only trust mechanism and no shell anywhere in
the chain.

---

## The Governance Chain

Guardian resolves permissions at cluster policy level.
Conductor orchestrates workload execution from above the pod boundary.
ONTAR enforces the execution contract from inside the pod boundary.

The PermissionSnapshot travels:
  Guardian signs cluster-level policy
    Conductor compiles pod-level execution contract from workload declaration
      ONTAR enforces execution contract at runtime

The chain from operator-declared intent to what a JVM thread is
permitted to call is unbroken and cryptographically signed at every
handoff.

---

## The Boundary Definition (Precise)

ONTAR owns: identity, network, filesystem, and secret access at the
intent boundary.

The kernel owns: execution beneath the intent boundary.
Syscalls from the child runtime to the kernel are not mediated by ONTAR.
gVisor owns the hypervisor boundary if that layer is required.

These are complementary layers. ONTAR does not claim to govern
all execution. It governs all intentional behavior above the kernel
boundary.

---

## The Five Intent Categories

1. Execution Intent: side-effectful actions with declared phase,
   preconditions, idempotency token, and maximum duration.

2. Connectivity Intent: outbound service access with declared purpose.
   ONTAR resolves mTLS, returns local socket. Child never holds
   a certificate or knows the target address.

3. Configuration Intent: typed configuration object at declared version.
   ONTAR fetches, validates signature, returns typed object.
   No environment variable mutation. No mounted secret paths.

4. Lifecycle Signal: child declares state transition as intent.
   ONTAR validates, commits phase change, emits authoritative event.
   Child proposes. ONTAR commits.

5. Audit Assertion: child emits signed statement about what it did
   and why. ONTAR countersigns and ships to audit sink.

---

## The Six Invariants

1. Nothing undeclared can execute.
2. Nothing declared can execute outside its phase.
3. Nothing can execute beyond its time window.
4. Nothing can escape its failure paths.
5. Nothing can reorder beyond its concurrency model.
6. The compiler produces hash-stable graphs.
   Same workload spec produces identical snapshot hash.
   Any behavioral change produces different hash.

---

## Phase Authority

Phase is ONTAR internal state. Not derived from child signals.

Phase transitions are two-step commits:
  Child submits PhaseTransitionRequest intent.
  ONTAR validates: transition allowed in graph, exit conditions satisfied.
  ONTAR commits transition internally.
  ONTAR emits PhaseTransitionEvent (authoritative).

Phase entry and exit conditions are declared in the PermissionSnapshot
phase model section. Entry conditions and exit conditions are both
required for each named phase.

Time inside ONTAR is monotonic and phase-relative.
No wall-clock dependency for enforcement.
Intent nonce per phase prevents replay within same phase window.

---

## Failure Classification

Each intent node declares:
  Success outputs (typed).
  Failure outputs (typed, enumerated).

Failure type is proposed by child, validated by ONTAR
against observable facts.

If mismatch between claimed failure type and observable reality:
  Transition to IntegrityViolation failure edge.

Every failure edge declares:
  Failure type.
  Validation rule (what ONTAR can verify).
  Allowed successor nodes.

---

## Concurrency Model

Each intent declares its concurrency model:
  Exclusive: only one active at a time.
  Parallel: N max declared.
  Ordered: must follow declared sequence.

Execution is deterministic under declared parallelism.
Replay produces same outcome.

---

## Bootstrap Trust Architecture

Binary embeds bootstrap identity only.
Short-lived SPIFFE-compatible certificate issued at pod scheduling time
via projected service account token exchange.

Go layer uses bootstrap identity to call Conductor-adjacent identity
endpoint, proves pod identity cryptographically, receives runtime CA
material for this pod instance.

Runtime CA is ephemeral and per-pod-instance.
Blast radius of pod compromise is exactly one pod runtime CA.
Not the cluster CA.

---

## The PermissionSnapshot Pod Contract

The cluster-level PermissionSnapshot and the pod-level ONTAR snapshot
are related objects in the lineage chain, not the same CRD.

Pod-level snapshot sections required:
  Identity binding: workload identity hash, pod spec hash,
    execution graph hash, validity window, trust root fingerprint.
  Phase model: named phases, transition edges (success and failure),
    entry conditions, exit conditions, initial phase.
  Intent registry: each allowed intent by name, category, valid phases,
    max invocation count per phase, expiry window, typed input/output schema.
  Connectivity graph: allowed outbound service identities, intent
    categories that may initiate connectivity, mTLS profile required.
  Audit contract: audit sink identity, signing key reference, which
    intent categories require mandatory audit emission.
  Schema version and lineage: operator-level CRD version that produced
    this snapshot. Full lineage from cluster declaration to pod
    enforcement is a single typed chain.

---

## Sequencing Decisions

ONTAR does not enter the current seam-core implementation.

After alpha release: formalize ONTAR PermissionSnapshot schema
as seam-core/docs/ontar-spec.md. Specification document only.
No implementation.

After first production fintech operator: begin Go runtime reference
implementation with one language adapter, one framework, one real
organization.

Parallel to Vortex development: ONTAR audit assertions feed the
same audit sink Vortex surfaces.

---

## What Does Not Change

This decision record does not change any current implementation.
The five existing Seam operators are not modified.
The current Guardian PermissionSnapshot schema is not modified.
The current Conductor pipeline is not modified.

ONTAR is the next trust boundary after the current operators are stable.

---

## The One Sentence

The governance chain does not stop at the pod boundary.