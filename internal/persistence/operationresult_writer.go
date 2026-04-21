// Package persistence OperationResultWriter writes OperationResultSpec to a
// PackOperationResult CR in seam-core (infrastructure.ontai.dev/v1alpha1).
// Replaces the ConfigMap output channel. seam-core-schema.md §8, Decision 11.
package persistence

import (
	"context"
	"fmt"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// OperationResultWriter writes an OperationResultSpec to a named
// PackOperationResult CR. This is the output channel between Conductor
// execute-mode Jobs and the wrapper operator.
type OperationResultWriter interface {
	WriteResult(ctx context.Context, namespace, name string, result runnerlib.OperationResultSpec) error
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

// WriteResult serializes the OperationResultSpec fields into a PackOperationResult
// CR and creates or updates it in the given namespace under the given name.
func (w *kubeOperationResultWriter) WriteResult(
	ctx context.Context,
	namespace, name string,
	result runnerlib.OperationResultSpec,
) error {
	spec := buildPackOperationResultSpec(result, name, w.clusterRef)

	por := &seamv1alpha1.PackOperationResult{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Spec: spec,
	}

	createErr := w.client.Create(ctx, por)
	if createErr == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(createErr) {
		return fmt.Errorf("operationresult writer: create %q in %q: %w", name, namespace, createErr)
	}

	existing := &seamv1alpha1.PackOperationResult{}
	if err := w.client.Get(ctx, ctrlclient.ObjectKey{Namespace: namespace, Name: name}, existing); err != nil {
		return fmt.Errorf("operationresult writer: get existing %q in %q: %w", name, namespace, err)
	}
	existing.Spec = spec
	if err := w.client.Update(ctx, existing); err != nil {
		return fmt.Errorf("operationresult writer: update existing %q in %q: %w", name, namespace, err)
	}
	return nil
}

// buildPackOperationResultSpec maps OperationResultSpec fields to
// PackOperationResultSpec, deriving packExecutionRef from the result name
// by stripping the "pack-deploy-result-" prefix.
func buildPackOperationResultSpec(
	result runnerlib.OperationResultSpec,
	name, clusterRef string,
) seamv1alpha1.PackOperationResultSpec {
	spec := seamv1alpha1.PackOperationResultSpec{
		PackExecutionRef: strings.TrimPrefix(name, "pack-deploy-result-"),
		TargetClusterRef: clusterRef,
		Capability:       result.Capability,
		Phase:            result.Phase,
		Status:           seamv1alpha1.PackResultStatus(result.Status),
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

