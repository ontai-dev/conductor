// Package runnerlib is the shared library between the conductor binary and all
// ONT platform operators.
//
// All operators import this package. Breaking changes require a major version bump
// and simultaneous operator dependency updates before the runner release is cut.
// Non-breaking changes (new optional fields, new capability entries) require only
// a minor version bump and may precede operator dependency updates.
//
// The library defines:
//   - RunnerConfig types: RunnerConfigSpec, RunnerConfigStatus, and all nested types.
//   - CapabilityManifest types: CapabilityManifest, CapabilityEntry, CapabilityMode,
//     ParameterDef.
//   - OperationResult types: OperationResultSpec, ResultStatus, ArtifactRef,
//     FailureReason, FailureCategory, StepResult.
//   - Generator functions: GenerateFromTalosCluster, GenerateFromPackBuild.
//   - JobSpecBuilder interface and concrete implementation.
//   - Named capability string constants.
//
// Do not add logic to this package beyond what is declared in
// conductor-design.md Section 12. This package is an API contract, not an
// implementation. All execution logic lives in the runner binary's internal
// packages.
//
// INV-009: RunnerConfig is operator-generated at runtime using this package.
// INV-010: This package is the single source of RunnerConfig schema.
// CR-INV-003: Breaking changes to this package require a major version bump.
package runnerlib
