// Package persistence TalosClusterResultWriter appends operation records to the
// per-cluster InfrastructureTalosClusterOperationResult CR.
// One TCOR per cluster, named by cluster name, lives in seam-tenant-{clusterRef}.
// conductor-schema.md §8, seam-core-schema.md §TCOR.
package persistence

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// tenantNamespaceFor returns the seam-tenant-{clusterRef} namespace name.
func tenantNamespaceFor(clusterRef string) string {
	return "seam-tenant-" + clusterRef
}

// TalosClusterResultWriter appends a completed operation record to the
// per-cluster InfrastructureTalosClusterOperationResult CR.
type TalosClusterResultWriter interface {
	// AppendOperationRecord appends the result as a TalosClusterOperationRecord
	// to the TCOR named clusterRef in seam-tenant-{clusterRef}.
	// jobRef is the Kubernetes Job name that produced the result (used by the
	// platform reconciler to correlate the record with the Job it submitted).
	// Returns ExecutionFailure if the TCOR does not exist — the platform operator
	// is responsible for creating it before submitting any day-2 Jobs.
	AppendOperationRecord(ctx context.Context, clusterRef, jobRef string, result runnerlib.OperationResultSpec) error
}

// kubeTalosClusterResultWriter is the production implementation.
type kubeTalosClusterResultWriter struct {
	client ctrlclient.Client
}

// NewKubeTalosClusterResultWriter constructs a TalosClusterResultWriter backed
// by the provided controller-runtime client.
func NewKubeTalosClusterResultWriter(client ctrlclient.Client) TalosClusterResultWriter {
	return &kubeTalosClusterResultWriter{client: client}
}

// AppendOperationRecord gets the cluster TCOR and appends the record.
func (w *kubeTalosClusterResultWriter) AppendOperationRecord(
	ctx context.Context,
	clusterRef, jobRef string,
	result runnerlib.OperationResultSpec,
) error {
	tenantNS := tenantNamespaceFor(clusterRef)
	tcor := &seamv1alpha1.InfrastructureTalosClusterOperationResult{}
	if err := w.client.Get(ctx, types.NamespacedName{Name: clusterRef, Namespace: tenantNS}, tcor); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("tcor writer: TCOR %s/%s not found — platform must create it before submitting day-2 Jobs", tenantNS, clusterRef)
		}
		return fmt.Errorf("tcor writer: get TCOR %s/%s: %w", tenantNS, clusterRef, err)
	}

	record := buildOperationRecord(jobRef, result)
	patch := ctrlclient.MergeFrom(tcor.DeepCopy())
	if tcor.Spec.Operations == nil {
		tcor.Spec.Operations = make(map[string]seamv1alpha1.TalosClusterOperationRecord)
	}
	tcor.Spec.Operations[jobRef] = record
	tcor.Spec.OperationCount = int64(len(tcor.Spec.Operations))
	if err := w.client.Patch(ctx, tcor, patch); err != nil {
		return fmt.Errorf("tcor writer: patch TCOR %s/%s: %w", tenantNS, clusterRef, err)
	}

	slog.InfoContext(ctx, "tcor writer: appended record",
		"cluster", clusterRef, "namespace", tenantNS,
		"jobRef", jobRef, "status", record.Status,
		"revision", tcor.Spec.Revision)
	return nil
}

// buildOperationRecord converts an OperationResultSpec into a TalosClusterOperationRecord.
func buildOperationRecord(jobRef string, result runnerlib.OperationResultSpec) seamv1alpha1.TalosClusterOperationRecord {
	status := seamv1alpha1.TalosClusterResultSucceeded
	if result.Status == runnerlib.ResultFailed {
		status = seamv1alpha1.TalosClusterResultFailed
	}

	message := string(status)
	if result.FailureReason != nil && result.FailureReason.Reason != "" {
		message = result.FailureReason.Reason
	}

	rec := seamv1alpha1.TalosClusterOperationRecord{
		Capability: result.Capability,
		JobRef:     jobRef,
		Status:     status,
		Message:    message,
	}

	if !result.StartedAt.IsZero() {
		t := metav1.NewTime(result.StartedAt)
		rec.StartedAt = &t
	} else {
		now := metav1.NewTime(time.Now())
		rec.StartedAt = &now
	}
	if !result.CompletedAt.IsZero() {
		t := metav1.NewTime(result.CompletedAt)
		rec.CompletedAt = &t
	} else {
		now := metav1.NewTime(time.Now())
		rec.CompletedAt = &now
	}

	if result.FailureReason != nil {
		rec.FailureReason = &seamv1alpha1.TalosClusterOperationFailureReason{
			Category: string(result.FailureReason.Category),
			Reason:   result.FailureReason.Reason,
		}
	}

	return rec
}

// NoopTalosClusterResultWriter discards all writes. Used in unit tests.
type NoopTalosClusterResultWriter struct{}

// AppendOperationRecord discards the result without writing anything.
func (NoopTalosClusterResultWriter) AppendOperationRecord(_ context.Context, _, _ string, _ runnerlib.OperationResultSpec) error {
	return nil
}
