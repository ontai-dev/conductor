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

// TestOperationResultWriter_PreviousStateEmbedded verifies that the second POR write
// copies the predecessor's ClusterPackVersion/RBACDigest/WorkloadDigest into the new
// POR's Previous* fields before deleting the predecessor. This is the rollback anchor.
// seam-core-schema.md §7.8, wrapper-schema.md §6.2.
func TestOperationResultWriter_PreviousStateEmbedded(t *testing.T) {
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

	r2 := &seamv1alpha1.PackOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-ccs-dev", Name: "pack-deploy-result-pe-rollback-r2"},
		r2); err != nil {
		t.Fatalf("revision 2 POR not found: %v", err)
	}

	// Current revision fields must reflect the second deploy.
	if r2.Spec.ClusterPackVersion != "v4.10.0-r1" {
		t.Errorf("ClusterPackVersion=%q, want v4.10.0-r1", r2.Spec.ClusterPackVersion)
	}
	if r2.Spec.RBACDigest != "sha256:cccc" {
		t.Errorf("RBACDigest=%q, want sha256:cccc", r2.Spec.RBACDigest)
	}
	if r2.Spec.WorkloadDigest != "sha256:dddd" {
		t.Errorf("WorkloadDigest=%q, want sha256:dddd", r2.Spec.WorkloadDigest)
	}

	// Previous fields must reflect the first deploy (rollback anchors).
	if r2.Spec.PreviousClusterPackVersion != "v4.9.0-r1" {
		t.Errorf("PreviousClusterPackVersion=%q, want v4.9.0-r1", r2.Spec.PreviousClusterPackVersion)
	}
	if r2.Spec.PreviousRBACDigest != "sha256:aaaa" {
		t.Errorf("PreviousRBACDigest=%q, want sha256:aaaa", r2.Spec.PreviousRBACDigest)
	}
	if r2.Spec.PreviousWorkloadDigest != "sha256:bbbb" {
		t.Errorf("PreviousWorkloadDigest=%q, want sha256:bbbb", r2.Spec.PreviousWorkloadDigest)
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
