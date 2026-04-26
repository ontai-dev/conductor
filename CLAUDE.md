## conductor: Operational Constraints
> Read ~/ontai/CLAUDE.md first. The constraints below extend the root constitutional document.

### Schema authority
Primary: docs/conductor-schema.md (conductor behavioral specification: modes, capabilities, job protocol, signing)
CRD schema authority: ~/ontai/seam-core/docs/seam-core-schema.md (Decision G: seam-core owns InfrastructureRunnerConfig, InfrastructurePackReceipt, and all cross-operator CRD type definitions)
Supporting: all operator schema docs -- conductor implements capabilities for every domain.
Read ALL schema documents before any capability implementation work begins.

### Invariants
CR-INV-001 -- Three-mode boundary is absolute: compile, executor, agent. No mode bleed. Compile-mode clients raise a fatal error if invoked in executor or agent mode. (root INV-014)
CR-INV-002 -- talos goclient is executor and agent mode only. Never in compile mode. (root INV-013)
CR-INV-003 -- The shared library (pkg/runnerlib) provides generation logic and job-spec builders. CRD type definitions are imported from seam-core/api/v1alpha1, not defined in runnerlib. Breaking changes to generation logic or job-spec builders require a major version bump and operator dependency updates before the runner release is cut. Breaking changes to CRD types require a seam-core PR first (Decision G, Decision 11).
CR-INV-004 -- Named capabilities are additive. New capabilities never change existing capability behavior. Existing capability parameter schemas are never modified in a breaking way.
CR-INV-005 -- The capability manifest in RunnerConfig status is self-declared by the agent on startup. Operators never hardcode capability availability assumptions.
CR-INV-006 -- Leader election in agent mode is not optional. One leader writes to RunnerConfig status and receipt CRs. All other replicas are standby.
INV-014 -- Helm goclient and kustomize goclient are compile mode only. They exist exclusively in the Compiler binary. Excluded from Conductor at build time via Go build tags.
INV-023 -- Conductor binary supports only execute and agent modes. Compile mode attempted on Conductor causes an immediate InvariantViolation structured exit before any other initialization proceeds.
INV-024 -- Compiler and Conductor are always released together from the same source commit and carry the same version tag. Deploying mismatched versions against the same cluster is unsupported and undefined behavior.
INV-026 -- PackInstance signing and PermissionSnapshot signing are performed exclusively by the management cluster Conductor in agent mode. Target cluster Conductor verifies but never signs. Verification failure blocks receipt acknowledgement.

### Image and binary constraints
Three images from this repo (Decision 12 in root CLAUDE.md):
- Compiler: debian-slim. compile mode only. Never deployed to cluster.
- Conductor execute: debian-slim. Kueue Job pods on management cluster only.
- Conductor agent: distroless. Deployed to ont-system on every cluster.
Execute image must never be distroless. Agent image must never be debian-slim.

### Session protocol additions
Step 4a -- Read conductor-design.md in this repository.
Step 4b -- Before implementing a new named capability, verify it is not a duplicate. Check the capability table in docs/conductor-schema.md.
Step 4c -- Before modifying the shared library, assess operator impact. Document breaking vs non-breaking change in PROGRESS.md before proceeding.
