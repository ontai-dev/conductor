// Package persistence OperationResultWriter writes OperationResultSpec to a
// PackOperationResult CR in seam-core (infrastructure.ontai.dev/v1alpha1).
// Replaces the ConfigMap output channel. seam-core-schema.md §8, Decision 11.
package persistence

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// labelPackExecution is the label key used to group PackOperationResult CRs by
// the PackExecution they belong to. The single-active-revision pattern relies on
// this label for list queries.
const labelPackExecution = "ontai.dev/pack-execution"

// labelClusterPack is the label key used to group PackOperationResult CRs by
// the ClusterPack they belong to. Used by the wrapper rollback handler to find
// the current POR for a given ClusterPack without knowing the PackExecution ref.
// seam-core-schema.md §7.8.
const labelClusterPack = "ontai.dev/cluster-pack"

// OperationResultWriter writes an OperationResultSpec to a named
// PackOperationResult CR. This is the output channel between Conductor
// execute-mode Jobs and the wrapper operator.
type OperationResultWriter interface {
	// WriteResult implements the single-active-revision pattern: lists existing
	// PackOperationResults for packExecutionRef, creates a new CR at revision N+1,
	// logs the predecessor spec, then deletes the predecessor. After a successful
	// call exactly one PackOperationResult labelled by packExecutionRef exists in
	// the namespace.
	WriteResult(ctx context.Context, namespace, packExecutionRef string, result runnerlib.OperationResultSpec) error
}

// kubeOperationResultWriter is the production OperationResultWriter backed by
// a controller-runtime client targeting the management cluster.
type kubeOperationResultWriter struct {
	client     ctrlclient.Client
	clusterRef string
}

// NewKubeOperationResultWriter constructs an OperationResultWriter backed by
// the provided controller-runtime client. clusterRef is written to
// spec.targetClusterRef. In execute mode, the client is always in-cluster.
func NewKubeOperationResultWriter(client ctrlclient.Client, clusterRef string) OperationResultWriter {
	return &kubeOperationResultWriter{client: client, clusterRef: clusterRef}
}

// WriteResult implements the single-active-revision pattern for PackOperationResult.
//
// Steps:
//  1. List all PackOperationResults in namespace labelled by packExecutionRef.
//  2. Select the one with the highest Revision as the predecessor (N). If none
//     exist, N=0 and there is no predecessor.
//  3. Build the new spec at revision N+1 and create a CR named
//     pack-deploy-result-{packExecutionRef}-r{N+1}.
//  4. Log the predecessor spec at INFO level (GraphQuery DB stub).
//  5. Delete the predecessor CR.
//
// After a successful call exactly one PackOperationResult labelled by
// packExecutionRef exists in the namespace.
func (w *kubeOperationResultWriter) WriteResult(
	ctx context.Context,
	namespace, packExecutionRef string,
	result runnerlib.OperationResultSpec,
) error {
	list := &seamv1alpha1.PackOperationResultList{}
	if err := w.client.List(ctx, list,
		ctrlclient.InNamespace(namespace),
		ctrlclient.MatchingLabels{labelPackExecution: packExecutionRef},
	); err != nil {
		return fmt.Errorf("operationresult writer: list %q in %q: %w", packExecutionRef, namespace, err)
	}

	var prev *seamv1alpha1.PackOperationResult
	var highestRevision int64
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.Revision > highestRevision {
			highestRevision = item.Spec.Revision
			prev = item
		}
	}

	newRevision := highestRevision + 1
	newName := fmt.Sprintf("pack-deploy-result-%s-r%d", packExecutionRef, newRevision)

	prevRef := ""
	if prev != nil {
		prevRef = prev.Name
	}

	spec := buildPackOperationResultSpec(result, packExecutionRef, w.clusterRef)
	spec.Revision = newRevision
	spec.PreviousRevisionRef = prevRef

	// Copy rollback anchor fields from predecessor before it is deleted.
	// This embeds the N-1 state in the N POR so one-step rollback works without
	// retaining superseded POR objects. seam-core-schema.md §7.8.
	if prev != nil {
		spec.PreviousClusterPackVersion = prev.Spec.ClusterPackVersion
		spec.PreviousRBACDigest = prev.Spec.RBACDigest
		spec.PreviousWorkloadDigest = prev.Spec.WorkloadDigest
	}

	// Set ownerReference to the PackExecution so Kubernetes GC deletes this POR
	// when the PE is deleted, preventing stale PORs from surviving a redeploy.
	// If the PE is already gone (late-arriving result), skip the ownerRef.
	var ownerRefs []metav1.OwnerReference
	pe := &seamv1alpha1.InfrastructurePackExecution{}
	if getErr := w.client.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: packExecutionRef}, pe); getErr == nil {
		blockOwner := true
		ownerRefs = []metav1.OwnerReference{{
			APIVersion:         "infrastructure.ontai.dev/v1alpha1",
			Kind:               "InfrastructurePackExecution",
			Name:               pe.Name,
			UID:                pe.UID,
			BlockOwnerDeletion: &blockOwner,
		}}
	}

	labels := map[string]string{labelPackExecution: packExecutionRef}
	if result.ClusterPackRef != "" {
		labels[labelClusterPack] = result.ClusterPackRef
	}

	por := &seamv1alpha1.PackOperationResult{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       namespace,
			Name:            newName,
			Labels:          labels,
			OwnerReferences: ownerRefs,
		},
		Spec: spec,
	}

	if err := w.client.Create(ctx, por); err != nil {
		return fmt.Errorf("operationresult writer: create %q in %q: %w", newName, namespace, err)
	}

	if prev != nil {
		specJSON, _ := json.Marshal(prev.Spec)
		slog.InfoContext(ctx, "operationresult writer: superseding previous revision",
			"predecessor", prev.Name,
			"namespace", namespace,
			"supersededRevision", prev.Spec.Revision,
			"newRevision", newRevision,
			"packExecutionRef", packExecutionRef,
			"spec", string(specJSON),
		)
		if err := w.client.Delete(ctx, prev); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("operationresult writer: delete predecessor %q in %q: %w", prev.Name, namespace, err)
		}
	}

	return nil
}

// buildPackOperationResultSpec maps OperationResultSpec fields to
// PackOperationResultSpec. Revision and PreviousRevisionRef are set by
// WriteResult after this function returns.
func buildPackOperationResultSpec(
	result runnerlib.OperationResultSpec,
	packExecutionRef, clusterRef string,
) seamv1alpha1.PackOperationResultSpec {
	spec := seamv1alpha1.PackOperationResultSpec{
		PackExecutionRef:   packExecutionRef,
		ClusterPackRef:     result.ClusterPackRef,
		TargetClusterRef:   clusterRef,
		Capability:         result.Capability,
		Phase:              result.Phase,
		Status:             seamv1alpha1.PackResultStatus(result.Status),
		ClusterPackVersion: result.ClusterPackVersion,
		RBACDigest:         result.RBACDigest,
		WorkloadDigest:     result.WorkloadDigest,
	}

	if !result.StartedAt.IsZero() {
		t := metav1.NewTime(result.StartedAt)
		spec.StartedAt = &t
	}
	if !result.CompletedAt.IsZero() {
		t := metav1.NewTime(result.CompletedAt)
		spec.CompletedAt = &t
	}

	if result.FailureReason != nil {
		spec.FailureReason = &seamv1alpha1.PackOperationFailureReason{
			Category:   string(result.FailureReason.Category),
			Reason:     result.FailureReason.Reason,
			FailedStep: result.FailureReason.FailedStep,
		}
	}

	for _, dr := range result.DeployedResources {
		spec.DeployedResources = append(spec.DeployedResources, seamv1alpha1.PackOperationDeployedResource{
			APIVersion: dr.APIVersion,
			Kind:       dr.Kind,
			Namespace:  dr.Namespace,
			Name:       dr.Name,
		})
	}

	for _, a := range result.Artifacts {
		spec.Artifacts = append(spec.Artifacts, seamv1alpha1.PackOperationArtifact{
			Name:      a.Name,
			Kind:      a.Kind,
			Reference: a.Reference,
			Checksum:  a.Checksum,
		})
	}

	for _, s := range result.Steps {
		sr := seamv1alpha1.PackOperationStepResult{
			Name:    s.Name,
			Status:  seamv1alpha1.PackResultStatus(s.Status),
			Message: s.Message,
		}
		if !s.StartedAt.IsZero() {
			t := metav1.NewTime(s.StartedAt)
			sr.StartedAt = &t
		}
		if !s.CompletedAt.IsZero() {
			t := metav1.NewTime(s.CompletedAt)
			sr.CompletedAt = &t
		}
		spec.Steps = append(spec.Steps, sr)
	}

	return spec
}

// NoopOperationResultWriter is an OperationResultWriter that discards all writes.
// Used in unit tests where the CR write side-effect is not under test.
type NoopOperationResultWriter struct{}

// WriteResult discards the result without writing anything.
func (NoopOperationResultWriter) WriteResult(_ context.Context, _, _ string, _ runnerlib.OperationResultSpec) error {
	return nil
}
