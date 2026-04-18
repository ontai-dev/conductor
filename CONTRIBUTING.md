# Contributing to conductor

Thank you for your interest in contributing to the Seam platform.

---

## Before you begin

Read the Seam Platform Constitution (`CLAUDE.md` in the ontai root repository)
and the conductor component constitution (`CLAUDE.md` in this repository) before
opening a Pull Request. All contributions must respect the platform invariants
defined in those documents.

Key invariants for this repository:

- Three modes only: compile (Compiler binary), execute (Conductor binary), agent
  (Conductor binary). No other modes exist.
- Compile mode attempted on the Conductor binary causes an immediate
  `InvariantViolation` structured exit. This check is enforced before any other
  initialization.
- Helm goclient, Kustomize goclient, and SOPS handler are Compiler-exclusive.
  They are excluded from Conductor at build time via Go build tags.
- Compiler and Conductor are always released together from the same source commit.
  Deploying mismatched versions is undefined behavior.
- The Conductor image is distroless. Any new runtime dependency must work in a
  distroless environment.
- `pkg/runnerlib` is the single source of RunnerConfig schema. All operators
  import it from this repository.

---

## Development setup

```sh
git clone https://github.com/ontai-dev/conductor
cd conductor

# Build Compiler
go build ./cmd/compiler

# Build Conductor
go build ./cmd/conductor

# Run all tests
go test ./...
```

---

## Two-binary discipline

Before adding any package or dependency, determine whether it belongs to:

- **Compiler-only**: compile-time system tool wrappers (Helm renderer, Kustomize
  resolver, SOPS handler). These are excluded from Conductor via build tags.
- **Conductor-only**: agent or execute mode logic that must not run in compile mode.
- **Shared**: packages in `pkg/` that both binaries import.

Do not add Helm, Kustomize, or SOPS imports to any shared package or Conductor
package. The build tag discipline is a hard invariant.

---

## Adding a new capability

A new execute-mode capability requires:

1. A named capability entry in the capability manifest structure in `pkg/runnerlib`.
2. A corresponding `RunnerConfig` generation path in the relevant operator.
3. A capability verification check in the relevant operator reconciler before Job
   submission.
4. An entry in `docs/conductor-schema.md` under the capabilities table.

All four must land in the same release. Do not ship a capability without the
schema documentation.

---

## Schema changes

Changes to `RunnerConfig` schema or the capability manifest structure in
`pkg/runnerlib` are breaking changes for all operators. They require:

- A Platform Governor review before merging.
- A coordinated release with all operators that import `pkg/runnerlib`.
- An update to `docs/conductor-schema.md`.

---

## Pull Request checklist

- [ ] `go build ./cmd/compiler` passes
- [ ] `go build ./cmd/conductor` passes
- [ ] `go test ./...` passes
- [ ] No em dashes in any new documentation
- [ ] No shell scripts added (Go only, per INV-001)
- [ ] Compile-mode packages excluded from Conductor via build tags if applicable
- [ ] `docs/conductor-schema.md` updated if RunnerConfig schema or capabilities changed

---

## Reporting issues

Open an issue at: https://github.com/ontai-dev/conductor/issues

For security vulnerabilities, contact the maintainers directly rather than
opening a public issue.

---

## License

By contributing, you agree that your contributions will be licensed under the
Apache License, Version 2.0. See `LICENSE` for the full text.

---

*conductor - Seam Platform Intelligence (Compiler + Conductor)*
