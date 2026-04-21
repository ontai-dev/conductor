package persistence_test

import (
	"context"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/internal/persistence"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

func buildTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := seamv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme seamv1alpha1: %v", err)
	}
	return s
}

// TestOperationResultWriter_CreatesPOR verifies that WriteResult creates a
// PackOperationResult CR with correct spec fields. Decision 11.
func TestOperationResultWriter_CreatesPOR(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeOperationResultWriter(fakeClient, "cluster-a")

	now := time.Now().UTC().Truncate(time.Second)
	result := runnerlib.OperationResultSpec{
		Capability:  "pack-deploy",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: now.Add(5 * time.Second),
		DeployedResources: []runnerlib.DeployedResource{
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "ingress-nginx", Name: "nginx-controller"},
		},
	}

	if err := writer.WriteResult(context.Background(), "seam-tenant-cluster-a", "pack-deploy-result-pe1", result); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	por := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-cluster-a", Name: "pack-deploy-result-pe1"},
		por); err != nil {
		t.Fatalf("PackOperationResult not found after WriteResult: %v", err)
	}

	if por.Spec.Capability != "pack-deploy" {
		t.Errorf("Capability=%q, want pack-deploy", por.Spec.Capability)
	}
	if por.Spec.Status != seamv1alpha1.PackResultSucceeded {
		t.Errorf("Status=%q, want Succeeded", por.Spec.Status)
	}
	if por.Spec.TargetClusterRef != "cluster-a" {
		t.Errorf("TargetClusterRef=%q, want cluster-a", por.Spec.TargetClusterRef)
	}
	if por.Spec.PackExecutionRef != "pe1" {
		t.Errorf("PackExecutionRef=%q, want pe1", por.Spec.PackExecutionRef)
	}
	if len(por.Spec.DeployedResources) != 1 {
		t.Fatalf("DeployedResources len=%d, want 1", len(por.Spec.DeployedResources))
	}
	dr := por.Spec.DeployedResources[0]
	if dr.Kind != "Deployment" || dr.Name != "nginx-controller" {
		t.Errorf("DeployedResources[0]=%+v, want Deployment/nginx-controller", dr)
	}
}

// TestOperationResultWriter_UpdatesExistingPOR verifies that WriteResult updates
// an already-existing PackOperationResult rather than failing with AlreadyExists.
func TestOperationResultWriter_UpdatesExistingPOR(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeOperationResultWriter(fakeClient, "cluster-b")

	first := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultFailed,
		FailureReason: &runnerlib.FailureReason{
			Category: runnerlib.ExecutionFailure,
			Reason:   "step failed",
		},
	}
	if err := writer.WriteResult(context.Background(), "ont-system", "por-name", first); err != nil {
		t.Fatalf("first WriteResult: %v", err)
	}

	second := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultSucceeded,
	}
	if err := writer.WriteResult(context.Background(), "ont-system", "por-name", second); err != nil {
		t.Fatalf("second WriteResult: %v", err)
	}

	por := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "ont-system", Name: "por-name"},
		por); err != nil {
		t.Fatalf("PackOperationResult not found: %v", err)
	}
	if por.Spec.Status != seamv1alpha1.PackResultSucceeded {
		t.Errorf("expected updated Status Succeeded; got %q", por.Spec.Status)
	}
	if por.Spec.FailureReason != nil {
		t.Error("expected FailureReason nil after update to Succeeded")
	}
}

// TestNoopOperationResultWriter_WriteResultReturnsNil verifies the noop writer.
func TestNoopOperationResultWriter_WriteResultReturnsNil(t *testing.T) {
	var w persistence.NoopOperationResultWriter
	result := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultSucceeded,
	}
	if err := w.WriteResult(context.Background(), "ont-system", "result-por", result); err != nil {
		t.Errorf("NoopOperationResultWriter.WriteResult expected nil; got %v", err)
	}
}
