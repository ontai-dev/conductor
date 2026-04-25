// Package persistence TalosClusterOperationResultWriter creates
// InfrastructureTalosClusterOperationResult CRs for day-2 TalosCluster
// operations. One CR per Job, named by the OPERATION_RESULT_CR env var.
// conductor-schema.md §8, seam-core-schema.md.
package persistence

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TalosClusterResultWriter creates an InfrastructureTalosClusterOperationResult
// CR in the Job namespace. Used by day-2 TalosCluster execute-mode Jobs.
type TalosClusterResultWriter interface {
	// WriteTalosClusterResult creates the TCOR CR named crName in namespace.
	// Idempotent: if the CR already exists (re-run), it is updated in-place.
	WriteTalosClusterResult(ctx context.Context, namespace, crName, jobRef string, result runnerlib.OperationResultSpec) error
}

// kubeTalosClusterResultWriter is the production implementation.
type kubeTalosClusterResultWriter struct {
	client     ctrlclient.Client
	clusterRef string
}

// NewKubeTalosClusterResultWriter constructs a TalosClusterResultWriter backed
// by the provided controller-runtime client. clusterRef is written to spec.clusterRef.
func NewKubeTalosClusterResultWriter(client ctrlclient.Client, clusterRef string) TalosClusterResultWriter {
	return &kubeTalosClusterResultWriter{client: client, clusterRef: clusterRef}
}

// WriteTalosClusterResult creates (or idempotently updates) the TCOR CR.
func (w *kubeTalosClusterResultWriter) WriteTalosClusterResult(
	ctx context.Context,
	namespace, crName, jobRef string,
	result runnerlib.OperationResultSpec,
) error {
	status := seamv1alpha1.TalosClusterResultSucceeded
	if result.Status == runnerlib.ResultFailed {
		status = seamv1alpha1.TalosClusterResultFailed
	}

	message := string(status)
	if result.FailureReason != nil && result.FailureReason.Reason != "" {
		message = result.FailureReason.Reason
	}

	spec := seamv1alpha1.InfrastructureTalosClusterOperationResultSpec{
		Capability: result.Capability,
		ClusterRef: w.clusterRef,
		JobRef:     jobRef,
		Status:     status,
		Message:    message,
	}

	if !result.StartedAt.IsZero() {
		t := metav1.NewTime(result.StartedAt)
		spec.StartedAt = &t
	} else {
		now := metav1.NewTime(time.Now())
		spec.StartedAt = &now
	}
	if !result.CompletedAt.IsZero() {
		t := metav1.NewTime(result.CompletedAt)
		spec.CompletedAt = &t
	} else {
		now := metav1.NewTime(time.Now())
		spec.CompletedAt = &now
	}

	if result.FailureReason != nil {
		spec.FailureReason = &seamv1alpha1.TalosClusterOperationFailureReason{
			Category: string(result.FailureReason.Category),
			Reason:   result.FailureReason.Reason,
		}
	}

	tcor := &seamv1alpha1.InfrastructureTalosClusterOperationResult{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      crName,
		},
		Spec: spec,
	}

	existing := &seamv1alpha1.InfrastructureTalosClusterOperationResult{}
	err := w.client.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: crName}, existing)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("tcor writer: get existing %q in %q: %w", crName, namespace, err)
	}

	if apierrors.IsNotFound(err) {
		if createErr := w.client.Create(ctx, tcor); createErr != nil {
			return fmt.Errorf("tcor writer: create %q in %q: %w", crName, namespace, createErr)
		}
		slog.InfoContext(ctx, "tcor writer: created", "name", crName, "namespace", namespace, "status", status)
		return nil
	}

	// Already exists — update spec in place (idempotent re-run).
	existing.Spec = spec
	if updateErr := w.client.Update(ctx, existing); updateErr != nil {
		return fmt.Errorf("tcor writer: update %q in %q: %w", crName, namespace, updateErr)
	}
	slog.InfoContext(ctx, "tcor writer: updated", "name", crName, "namespace", namespace, "status", status)
	return nil
}

// NoopTalosClusterResultWriter discards all writes. Used in unit tests.
type NoopTalosClusterResultWriter struct{}

// WriteTalosClusterResult discards the result without writing anything.
func (NoopTalosClusterResultWriter) WriteTalosClusterResult(_ context.Context, _, _, _ string, _ runnerlib.OperationResultSpec) error {
	return nil
}
