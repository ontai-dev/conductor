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

// TestOperationResultWriter_SecondWriteRetainsPredecessorAsSuperseded verifies that the
// second WriteResult call creates a POR at revision=2, sets PreviousRevisionRef,
// and labels the revision=1 CR ontai.dev/superseded=true (retained for N-step rollback).
// Both PORs must exist after two writes. seam-core-schema.md §7.8.
func TestOperationResultWriter_SecondWriteRetainsPredecessorAsSuperseded(t *testing.T) {
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

	// Revision 2 must exist with PreviousRevisionRef set and no superseded label.
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
	if got := r2.Labels["ontai.dev/superseded"]; got != "" {
		t.Errorf("r2 superseded label=%q, want empty (active revision)", got)
	}

	// Revision 1 must still exist and be labeled superseded.
	r1 := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-cluster-b", Name: "pack-deploy-result-pe2-r1"},
		r1); err != nil {
		t.Fatalf("revision 1 POR not found after second write (must be retained): %v", err)
	}
	if got := r1.Labels["ontai.dev/superseded"]; got != "true" {
		t.Errorf("r1 superseded label=%q, want true", got)
	}

	// Both PORs must be present in the namespace for this packExecutionRef.
	list := &seamv1alpha1.PackOperationResultList{}
	if err := fakeClient.List(context.Background(), list,
		ctrlclient.InNamespace("seam-tenant-cluster-b"),
		ctrlclient.MatchingLabels{"ontai.dev/pack-execution": "pe2"},
	); err != nil {
		t.Fatalf("list PORs: %v", err)
	}
	if len(list.Items) != 2 {
		t.Errorf("POR count=%d after two writes, want 2 (active + superseded retained)", len(list.Items))
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

// TestOperationResultWriter_ClusterPackLabelSet verifies that the ontai.dev/cluster-pack
// label is set on the POR when ClusterPackRef is populated in the result.
func TestOperationResultWriter_ClusterPackLabelSet(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeOperationResultWriter(fakeClient, "ccs-dev")

	result := runnerlib.OperationResultSpec{
		Capability:     "pack-deploy",
		Status:         runnerlib.ResultSucceeded,
		ClusterPackRef: "nginx-ccs-dev",
	}
	if err := writer.WriteResult(context.Background(), "seam-tenant-ccs-dev", "nginx-ccs-dev-ccs-dev", result); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	por := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-ccs-dev", Name: "pack-deploy-result-nginx-ccs-dev-ccs-dev-r1"},
		por); err != nil {
		t.Fatalf("POR not found: %v", err)
	}
	if got := por.Labels["ontai.dev/cluster-pack"]; got != "nginx-ccs-dev" {
		t.Errorf("label ontai.dev/cluster-pack=%q, want nginx-ccs-dev", got)
	}
	if por.Spec.ClusterPackRef != "nginx-ccs-dev" {
		t.Errorf("ClusterPackRef=%q, want nginx-ccs-dev", por.Spec.ClusterPackRef)
	}
}

// TestOperationResultWriter_SupersededPORRetainsRollbackAnchor verifies that after two
// deploys, the superseded revision 1 POR is retained with its own version/digest fields
// intact -- making N-step rollback possible by reading those fields directly.
// seam-core-schema.md §7.8, wrapper-schema.md §6.2.
func TestOperationResultWriter_SupersededPORRetainsRollbackAnchor(t *testing.T) {
	scheme := buildTestScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeOperationResultWriter(fakeClient, "ccs-dev")

	// First deploy: version v1 with known digests.
	first := runnerlib.OperationResultSpec{
		Capability:         "pack-deploy",
		Status:             runnerlib.ResultSucceeded,
		ClusterPackRef:     "nginx-ccs-dev",
		ClusterPackVersion: "v4.9.0-r1",
		RBACDigest:         "sha256:aaaa",
		WorkloadDigest:     "sha256:bbbb",
	}
	if err := writer.WriteResult(context.Background(), "seam-tenant-ccs-dev", "pe-rollback", first); err != nil {
		t.Fatalf("first WriteResult: %v", err)
	}

	// Second deploy: version v2 supersedes v1.
	second := runnerlib.OperationResultSpec{
		Capability:         "pack-deploy",
		Status:             runnerlib.ResultSucceeded,
		ClusterPackRef:     "nginx-ccs-dev",
		ClusterPackVersion: "v4.10.0-r1",
		RBACDigest:         "sha256:cccc",
		WorkloadDigest:     "sha256:dddd",
	}
	if err := writer.WriteResult(context.Background(), "seam-tenant-ccs-dev", "pe-rollback", second); err != nil {
		t.Fatalf("second WriteResult: %v", err)
	}

	// Active revision 2 must reflect the second deploy.
	r2 := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-ccs-dev", Name: "pack-deploy-result-pe-rollback-r2"},
		r2); err != nil {
		t.Fatalf("revision 2 POR not found: %v", err)
	}
	if r2.Spec.ClusterPackVersion != "v4.10.0-r1" {
		t.Errorf("r2.ClusterPackVersion=%q, want v4.10.0-r1", r2.Spec.ClusterPackVersion)
	}
	if r2.Spec.RBACDigest != "sha256:cccc" {
		t.Errorf("r2.RBACDigest=%q, want sha256:cccc", r2.Spec.RBACDigest)
	}
	if r2.Spec.WorkloadDigest != "sha256:dddd" {
		t.Errorf("r2.WorkloadDigest=%q, want sha256:dddd", r2.Spec.WorkloadDigest)
	}
	if got := r2.Labels["ontai.dev/superseded"]; got != "" {
		t.Errorf("r2 should not be labeled superseded; got %q", got)
	}

	// Superseded revision 1 must still exist with its original anchor fields intact.
	r1 := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-ccs-dev", Name: "pack-deploy-result-pe-rollback-r1"},
		r1); err != nil {
		t.Fatalf("revision 1 POR must be retained for rollback: %v", err)
	}
	if got := r1.Labels["ontai.dev/superseded"]; got != "true" {
		t.Errorf("r1 superseded label=%q, want true", got)
	}
	// The wrapper rollback handler reads ClusterPackVersion/RBACDigest/WorkloadDigest
	// directly from this retained POR to restore the ClusterPack spec.
	if r1.Spec.ClusterPackVersion != "v4.9.0-r1" {
		t.Errorf("r1.ClusterPackVersion=%q, want v4.9.0-r1", r1.Spec.ClusterPackVersion)
	}
	if r1.Spec.RBACDigest != "sha256:aaaa" {
		t.Errorf("r1.RBACDigest=%q, want sha256:aaaa", r1.Spec.RBACDigest)
	}
	if r1.Spec.WorkloadDigest != "sha256:bbbb" {
		t.Errorf("r1.WorkloadDigest=%q, want sha256:bbbb", r1.Spec.WorkloadDigest)
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
