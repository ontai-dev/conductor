# conductor

**Seam Platform Intelligence**
**API Group:** `runner.ontai.dev`
**Images:** `registry.ontai.dev/ontai-dev/compiler:<semver>` (Compiler) and `registry.ontai.dev/ontai-dev/conductor:<semver>` (Conductor)

---

## What this repository is

`conductor` builds two binaries from one Go module: Compiler and Conductor.

**Compiler** is compile-time intelligence. It runs as a short-lived CLI tool or in
a compile-phase pipeline on the management cluster before Kueue is operational. It
owns the bootstrap launch sequence, enable phase operator installation, pack
compilation, Helm rendering, Kustomize resolution, and SOPS encryption. Compiler
is never deployed as a Deployment and never runs on any cluster.

**Conductor** is runtime intelligence. It runs as a long-running Deployment in
`ont-system` on the management cluster and on every target cluster. It is also
stamped into Kueue Job specs for all named operational capabilities.

---

## Binary modes

| Mode | Binary | Invocation | Duration | Image |
|---|---|---|---|---|
| compile | Compiler | Direct CLI invocation | Short-lived | Debian |
| execute | Conductor | Kueue Job pod | Short-lived | Distroless |
| agent | Conductor | Deployment in ont-system | Long-lived | Distroless |

Compile mode attempted on the Conductor binary causes an immediate `InvariantViolation`
structured exit before any other initialization proceeds.

---

## Management cluster Conductor responsibilities

- Declares capability manifest to `RunnerConfig` status on startup.
- Implements leader election.
- Maintains PackInstance signing: signs `PackInstance` CRs after wrapper confirms
  `ClusterPack` registration.
- Maintains PermissionSnapshot signing: signs `PermissionSnapshot` CRs after guardian
  generates them.
- Publishes signed artifacts for target cluster Conductor verification.

## Target cluster Conductor responsibilities

- Declares capability manifest to `RunnerConfig` status on startup.
- Implements leader election.
- Maintains `PackReceipt`: verifies signed `PackInstance` from management cluster,
  records local drift status.
- Maintains `PermissionSnapshotReceipt`: verifies signed `PermissionSnapshot` from
  management cluster, acknowledges delivery.
- Runs local admission webhook: intercepts all RBAC resources, enforces
  `ontai.dev/rbac-owner=guardian` annotation.
- Serves local PermissionService gRPC endpoint for authorization decisions.
- Runs drift detection loop: compares expected pack state to live cluster state.

---

## Shared library

`pkg/runnerlib` is owned by this repository. All operators and both binaries import
it. It defines the `RunnerConfig` schema, generation logic, and capability manifest
structure. It is the single source of truth for the RunnerConfig contract.

---

## Building

```sh
# Compiler binary
go build ./cmd/compiler

# Conductor binary
go build ./cmd/conductor
```

Container images:

```sh
docker build -f Dockerfile.compiler -t registry.ontai.dev/ontai-dev/compiler:<semver> .
docker build -f Dockerfile.agent    -t registry.ontai.dev/ontai-dev/conductor:<semver> .
```

---

## Testing

```sh
go test ./...
```

---

## Schema and design reference

- `docs/conductor-schema.md` - RunnerConfig contract, capability manifest, operational capabilities
- `docs/decisions/ontar.md` - Architecture Decision Records for key design choices
- `conductor-design.md` - Implementation architecture and design

---

## Status

Alpha. Deployed and tested on management cluster (ccs-mgmt).
Tenant cluster onboarding is not yet verified end to end.
See [docs/conductor-schema.md](./docs/conductor-schema.md)
for current capability and known gaps.

CRDs are deployed and reconciling on the live management cluster.
The schema specification is published at:
https://schema.ontai.dev/v1alpha1/

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) before opening a pull
request. Every new reconciliation behavior requires a written
specification and senior engineer sign-off before any code is
written.

File issues at https://github.com/ontai-dev/conductor/issues.
For security issues contact security@ontai.dev directly.

---

*conductor - Seam Platform Intelligence (Compiler + Conductor)*
*Apache License, Version 2.0*
