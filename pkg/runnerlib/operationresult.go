package runnerlib

import "time"

// OperationResultSpec is the complete result document written to a Kubernetes
// ConfigMap by every runner executor Job before exit. The operator reads this
// ConfigMap to advance the CR status. No other communication channel exists
// between operator and runner. conductor-design.md Section 8, conductor-schema.md
// Section 8.
type OperationResultSpec struct {
	// Phase identifies the RunnerConfig phase this result belongs to.
	Phase string

	// Status is the overall result of the capability execution.
	Status ResultStatus

	// Capability is the name of the named capability that produced this result.
	// Matches one of the Capability* constants in constants.go.
	Capability string

	// StartedAt is the time the capability execution began.
	StartedAt time.Time

	// CompletedAt is the time the capability execution finished (success or failure).
	CompletedAt time.Time

	// Artifacts is the list of artifacts produced by this execution.
	// References only — never raw content, never secret values. Section 10 of
	// conductor-design.md.
	Artifacts []ArtifactRef

	// FailureReason is populated when Status is ResultFailed. Nil on success.
	FailureReason *FailureReason

	// Steps contains individual step results for multi-step capabilities.
	// Empty for single-step capabilities.
	Steps []StepResult

	// DeployedResources is the list of Kubernetes resources applied during
	// this execution. Populated by pack-deploy on success. Used by the wrapper
	// PackInstanceReconciler to write PackInstance.Status.DeployedResources for
	// deletion cleanup. wrapper-schema.md §3, Decision 11.
	// +optional
	DeployedResources []DeployedResource
}

// ResultStatus is a typed string representing the terminal status of a capability
// execution or individual step.
type ResultStatus string

const (
	// ResultSucceeded indicates the capability or step completed without error.
	ResultSucceeded ResultStatus = "Succeeded"

	// ResultFailed indicates the capability or step encountered a failure.
	// FailureReason is populated on the containing OperationResultSpec.
	ResultFailed ResultStatus = "Failed"
)

// ArtifactRef is a structured reference to an artifact produced by a capability.
// Never contains raw artifact content. Secrets are referenced, never embedded.
// conductor-design.md Section 10.
type ArtifactRef struct {
	// Name is a logical identifier for this artifact within the OperationResult.
	Name string

	// Kind declares the artifact type.
	// One of: ConfigMap, Secret, OCIImage, S3Object.
	Kind string

	// Reference is the fully qualified reference for the artifact kind.
	// For ConfigMap/Secret: namespace/name.
	// For OCIImage: registry/repository:tag@digest.
	// For S3Object: s3://bucket/key.
	Reference string

	// Checksum is the content-addressed checksum of the artifact.
	// Format: sha256:<hex>. Empty if not applicable for the kind.
	Checksum string
}

// FailureReason is a structured failure description populated in OperationResultSpec
// when Status is ResultFailed. Every failure is classified into exactly one
// FailureCategory. conductor-design.md Section 6.1.
type FailureReason struct {
	// Category classifies the failure. Never empty.
	Category FailureCategory

	// Reason is a human-readable description of the specific failure.
	Reason string

	// FailedStep is the name of the step that failed, for multi-step capabilities.
	// Empty for single-step capabilities.
	FailedStep string
}

// FailureCategory is a typed string classifying the failure domain.
// Every failure is classified into exactly one category.
// conductor-design.md Section 6.1.
type FailureCategory string

const (
	// ValidationFailure indicates input does not meet schema or invariant requirements.
	// The failure occurred before any execution step began.
	ValidationFailure FailureCategory = "ValidationFailure"

	// CapabilityUnavailable indicates the requested capability is not in the runner
	// registry. The operator should raise CapabilityUnavailable on its operational CR
	// and wait for a runner version that supports the capability.
	CapabilityUnavailable FailureCategory = "CapabilityUnavailable"

	// ExecutionFailure indicates a step-level failure during execution.
	// The capability began executing but a step did not complete successfully.
	ExecutionFailure FailureCategory = "ExecutionFailure"

	// ExternalDependencyFailure indicates the Kubernetes API, Talos API, or OCI
	// registry was unreachable during execution.
	ExternalDependencyFailure FailureCategory = "ExternalDependencyFailure"

	// InvariantViolation indicates a programming error: a mode boundary was crossed,
	// a forbidden client was invoked, or a contract was violated. These failures
	// indicate runner implementation bugs and should never occur in production.
	InvariantViolation FailureCategory = "InvariantViolation"

	// LicenseViolation indicates license constraints are not satisfied for this
	// cluster. The runner agent enforces this at startup. CR-INV-007, CR-INV-008.
	LicenseViolation FailureCategory = "LicenseViolation"

	// StorageUnavailable indicates PVC creation failed for a multi-step capability.
	// The management cluster must have a storage class available.
	// conductor-design.md Section 5.6, conductor-schema.md Section 7.
	StorageUnavailable FailureCategory = "StorageUnavailable"
)

// DeployedResource records a single Kubernetes resource applied by a pack-deploy
// capability. Stored in OperationResultSpec.DeployedResources so the wrapper
// PackInstanceReconciler can write the list to PackInstance.Status.DeployedResources
// for use by the deletion handler. wrapper-schema.md §3, Decision 11.
type DeployedResource struct {
	// APIVersion is the Kubernetes apiVersion (e.g., apps/v1, v1).
	APIVersion string `json:"apiVersion"`

	// Kind is the Kubernetes resource Kind (e.g., Deployment, Namespace).
	Kind string `json:"kind"`

	// Namespace is the resource namespace. Empty for cluster-scoped resources.
	Namespace string `json:"namespace,omitempty"`

	// Name is the resource name.
	Name string `json:"name"`
}

// StepResult is the execution result for one step within a multi-step capability.
// Aggregated into OperationResultSpec.Steps for multi-step capabilities.
// Empty for single-step capabilities.
type StepResult struct {
	// Name is the step identifier within the capability. Unique within the capability.
	Name string

	// Status is the terminal status of this step.
	Status ResultStatus

	// StartedAt is the time this step began execution.
	StartedAt time.Time

	// CompletedAt is the time this step finished execution.
	CompletedAt time.Time

	// Message provides additional context about the step outcome.
	// Populated on both success and failure.
	Message string
}
