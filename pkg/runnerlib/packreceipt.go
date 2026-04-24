package runnerlib

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PackReceiptDriftStatus is the drift state reported by the target cluster
// Conductor after comparing expected pack state to live cluster state.
// conductor-schema.md §10, §15; wrapper-schema.md §8.
type PackReceiptDriftStatus string

const (
	// PackReceiptDriftStatusInSync indicates the live cluster state matches
	// the expected state from the current ClusterPack version.
	PackReceiptDriftStatusInSync PackReceiptDriftStatus = "InSync"

	// PackReceiptDriftStatusDrifted indicates the live cluster state has
	// diverged from the expected state. Conductor raises the Drifted condition
	// and the management cluster PackInstance reflects this via its Drifted
	// condition. Remediation requires a new PackExecution.
	PackReceiptDriftStatusDrifted PackReceiptDriftStatus = "Drifted"

	// PackReceiptDriftStatusUnknown indicates drift detection has not yet
	// completed or the last check did not produce a conclusive result.
	PackReceiptDriftStatusUnknown PackReceiptDriftStatus = "Unknown"
)

// ClusterPackRef is a reference to a specific ClusterPack version by name
// and semver version string.
type ClusterPackRef struct {
	// Name is the ClusterPack CR name on the management cluster.
	Name string

	// Version is the semver version of the ClusterPack being delivered.
	Version string
}

// PackReceiptSpec is the immutable declaration written by the target cluster
// Conductor when it acknowledges delivery of a ClusterPack. The spec is sealed
// after creation -- no controller mutates it after the first write. INV-026.
type PackReceiptSpec struct {
	// ClusterPackRef identifies the specific ClusterPack version this receipt
	// records delivery for.
	ClusterPackRef ClusterPackRef

	// TargetClusterRef identifies the target cluster this receipt was created on.
	// Matches the cluster-ref flag the Conductor Deployment was started with.
	TargetClusterRef string

	// PackSignature is the base64-encoded Ed25519 signature written by the
	// management cluster Conductor on the PackInstance CR. The target cluster
	// Conductor verifies this signature before setting SignatureVerified=true
	// on the receipt. INV-026.
	PackSignature string

	// SignatureVerified is true when the target cluster Conductor has
	// successfully verified PackSignature against the management cluster's
	// public signing key. A PackReceipt where SignatureVerified=false is a
	// security incident -- the PackInstance on the management cluster raises a
	// SecurityViolation condition and blocks further pack operations on this
	// cluster. wrapper-schema.md §5, conductor-schema.md §10.
	SignatureVerified bool

	// RBACDigest is the OCI digest of the RBAC layer of the ClusterPack artifact.
	// Durable recovery anchor: allows re-fetching and re-applying the RBAC layer
	// without access to the full artifact. Carried through from ClusterPack.spec.
	// Absent on pre-split ClusterPack receipts. T-04 schema, T-08.
	RBACDigest string

	// WorkloadDigest is the OCI digest of the workload layer of the ClusterPack artifact.
	// Durable recovery anchor: allows re-fetching and re-applying the workload layer
	// without access to the full artifact. Carried through from ClusterPack.spec.
	// Absent on pre-split ClusterPack receipts. T-04 schema, T-08.
	WorkloadDigest string

	// ChartVersion is the version of the Helm chart that was rendered to produce
	// this ClusterPack. Carried through from ClusterPack.spec.chartVersion.
	// Absent for kustomize and raw category packs. Decision B, T-04 schema.
	ChartVersion string

	// ChartURL is the URL of the Helm chart repository. Carried through from
	// ClusterPack.spec.chartURL. Absent for kustomize and raw category packs.
	// Decision B, T-04 schema.
	ChartURL string

	// ChartName is the name of the Helm chart that was rendered. Carried through
	// from ClusterPack.spec.chartName. Absent for kustomize and raw category packs.
	// Decision B, T-04 schema.
	ChartName string

	// HelmVersion is the version of the Helm SDK used to render the ClusterPack.
	// Carried through from ClusterPack.spec.helmVersion. Ensures rendering
	// reproducibility across SDK versions. Absent for kustomize and raw category
	// packs. Decision B, T-04 schema.
	HelmVersion string
}

// PackReceiptStatus is the mutable state maintained by the target cluster
// Conductor. Updated during receipt reconciliation and drift detection loops.
// conductor-schema.md §10, §15.
type PackReceiptStatus struct {
	// DeliveredAt is the timestamp when the target cluster Conductor first
	// acknowledged delivery of this ClusterPack version.
	DeliveredAt metav1.Time

	// DriftStatus is the current drift state of the live cluster relative to
	// the expected state from this ClusterPack version. Updated by the drift
	// detection loop. wrapper-schema.md §8.
	DriftStatus PackReceiptDriftStatus

	// DriftSummary is a human-readable description of the detected drift, if any.
	// Empty when DriftStatus is InSync or Unknown.
	DriftSummary string

	// LastDriftCheckAt is the timestamp of the most recent drift detection
	// check performed by the Conductor drift detection loop.
	LastDriftCheckAt metav1.Time

	// Conditions is the standard Kubernetes condition list for PackReceipt.
	// Condition types: Delivered, SignatureVerified, Drifted, SecurityViolation.
	// conductor-schema.md §15.
	Conditions []metav1.Condition
}
