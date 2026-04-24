package persistence_test

import (
	"bytes"
	"context"
	"log/slog"
	"testing"

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

// TestOperationResultWriter_FirstWrite verifies that the first WriteResult call
// creates a POR at revision=1 with no predecessor and the expected label.
func TestOperationResultWriter_FirstWrite(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeOperationResultWriter(fakeClient, "cluster-a")

	result := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultSucceeded,
		DeployedResources: []runnerlib.DeployedResource{
			{APIVersion: "apps/v1", Kind: "Deployment", Namespace: "ingress-nginx", Name: "nginx-controller"},
		},
	}

	if err := writer.WriteResult(context.Background(), "seam-tenant-cluster-a", "pe1", result); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	por := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-cluster-a", Name: "pack-deploy-result-pe1-r1"},
		por); err != nil {
		t.Fatalf("POR not found after first write: %v", err)
	}

	if por.Spec.Revision != 1 {
		t.Errorf("Revision=%d, want 1", por.Spec.Revision)
	}
	if por.Spec.PreviousRevisionRef != "" {
		t.Errorf("PreviousRevisionRef=%q, want empty for first revision", por.Spec.PreviousRevisionRef)
	}
	if por.Spec.PackExecutionRef != "pe1" {
		t.Errorf("PackExecutionRef=%q, want pe1", por.Spec.PackExecutionRef)
	}
	if por.Spec.Capability != "pack-deploy" {
		t.Errorf("Capability=%q, want pack-deploy", por.Spec.Capability)
	}
	if por.Spec.TargetClusterRef != "cluster-a" {
		t.Errorf("TargetClusterRef=%q, want cluster-a", por.Spec.TargetClusterRef)
	}
	if got := por.Labels["ontai.dev/pack-execution"]; got != "pe1" {
		t.Errorf("label ontai.dev/pack-execution=%q, want pe1", got)
	}
	if len(por.Spec.DeployedResources) != 1 {
		t.Fatalf("DeployedResources len=%d, want 1", len(por.Spec.DeployedResources))
	}
}

// TestOperationResultWriter_SecondWriteDeletesPredecessor verifies that the
// second WriteResult call creates a POR at revision=2, sets PreviousRevisionRef,
// and deletes the revision=1 CR -- leaving exactly one POR in the namespace.
func TestOperationResultWriter_SecondWriteDeletesPredecessor(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeOperationResultWriter(fakeClient, "cluster-b")

	first := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultFailed,
		FailureReason: &runnerlib.FailureReason{
			Category: runnerlib.ExecutionFailure,
			Reason:   "transient error",
		},
	}
	if err := writer.WriteResult(context.Background(), "seam-tenant-cluster-b", "pe2", first); err != nil {
		t.Fatalf("first WriteResult: %v", err)
	}

	second := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultSucceeded,
	}
	if err := writer.WriteResult(context.Background(), "seam-tenant-cluster-b", "pe2", second); err != nil {
		t.Fatalf("second WriteResult: %v", err)
	}

	// Revision 2 must exist with PreviousRevisionRef set.
	r2 := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-cluster-b", Name: "pack-deploy-result-pe2-r2"},
		r2); err != nil {
		t.Fatalf("revision 2 POR not found: %v", err)
	}
	if r2.Spec.Revision != 2 {
		t.Errorf("r2.Revision=%d, want 2", r2.Spec.Revision)
	}
	if r2.Spec.PreviousRevisionRef != "pack-deploy-result-pe2-r1" {
		t.Errorf("r2.PreviousRevisionRef=%q, want pack-deploy-result-pe2-r1", r2.Spec.PreviousRevisionRef)
	}
	if r2.Spec.Status != seamv1alpha1.PackResultSucceeded {
		t.Errorf("r2.Status=%q, want Succeeded", r2.Spec.Status)
	}

	// Exactly one POR must remain in the namespace for this packExecutionRef.
	list := &seamv1alpha1.PackOperationResultList{}
	if err := fakeClient.List(context.Background(), list,
		ctrlclient.InNamespace("seam-tenant-cluster-b"),
		ctrlclient.MatchingLabels{"ontai.dev/pack-execution": "pe2"},
	); err != nil {
		t.Fatalf("list PORs: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("POR count=%d after two writes, want 1 (single-active-revision)", len(list.Items))
	}
}

// TestOperationResultWriter_PredecessorDumpLogged verifies that WriteResult emits
// a structured INFO log containing the superseded revision spec before deletion.
func TestOperationResultWriter_PredecessorDumpLogged(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()

	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	writer := persistence.NewKubeOperationResultWriter(fakeClient, "cluster-c")

	first := runnerlib.OperationResultSpec{Capability: "pack-deploy", Status: runnerlib.ResultFailed}
	if err := writer.WriteResult(context.Background(), "seam-tenant-cluster-c", "pe3", first); err != nil {
		t.Fatalf("first WriteResult: %v", err)
	}

	second := runnerlib.OperationResultSpec{Capability: "pack-deploy", Status: runnerlib.ResultSucceeded}
	if err := writer.WriteResult(context.Background(), "seam-tenant-cluster-c", "pe3", second); err != nil {
		t.Fatalf("second WriteResult: %v", err)
	}

	logged := buf.String()
	if logged == "" {
		t.Error("expected structured log output after second write; got nothing")
	}
	for _, want := range []string{"superseding previous revision", "pack-deploy-result-pe3-r1", "pe3"} {
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("log output missing %q; got:\n%s", want, logged)
		}
	}
}

// TestNoopOperationResultWriter_WriteResultReturnsNil verifies the noop writer.
func TestNoopOperationResultWriter_WriteResultReturnsNil(t *testing.T) {
	var w persistence.NoopOperationResultWriter
	result := runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Status:     runnerlib.ResultSucceeded,
	}
	if err := w.WriteResult(context.Background(), "ont-system", "pe-noop", result); err != nil {
		t.Errorf("NoopOperationResultWriter.WriteResult expected nil; got %v", err)
	}
}
