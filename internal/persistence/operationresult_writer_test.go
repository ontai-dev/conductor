package persistence

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

func newTestScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := seamv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("add seam scheme: %v", err)
	}
	return s
}

func minimalResult() runnerlib.OperationResultSpec {
	return runnerlib.OperationResultSpec{
		Capability: "pack-deploy",
		Phase:      "Succeeded",
		Status:     runnerlib.ResultSucceeded,
	}
}

// TestWriteResult_FirstWrite creates revision r1 with no predecessor.
func TestWriteResult_FirstWrite(t *testing.T) {
	scheme := newTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	w := NewKubeOperationResultWriter(cl, "ccs-mgmt")

	if err := w.WriteResult(context.Background(), "seam-tenant-ccs-mgmt", "pe-abc", minimalResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	list := &seamv1alpha1.PackOperationResultList{}
	if err := cl.List(context.Background(), list,
		ctrlclient.InNamespace("seam-tenant-ccs-mgmt"),
		ctrlclient.MatchingLabels{labelPackExecution: "pe-abc"},
	); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 POR, got %d", len(list.Items))
	}
	if list.Items[0].Spec.Revision != 1 {
		t.Errorf("revision = %d, want 1", list.Items[0].Spec.Revision)
	}
	if list.Items[0].Name != "pack-deploy-result-pe-abc-r1" {
		t.Errorf("name = %q, want %q", list.Items[0].Name, "pack-deploy-result-pe-abc-r1")
	}
}

// TestWriteResult_UpgradesRevision verifies N→N+1: active POR advances to revision 2,
// predecessor is retained as superseded (ontai.dev/superseded=true) for N-step rollback.
func TestWriteResult_UpgradesRevision(t *testing.T) {
	scheme := newTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	w := NewKubeOperationResultWriter(cl, "ccs-mgmt")
	ns := "seam-tenant-ccs-mgmt"
	peRef := "pe-upgrade"

	if err := w.WriteResult(context.Background(), ns, peRef, minimalResult()); err != nil {
		t.Fatalf("first write: %v", err)
	}
	if err := w.WriteResult(context.Background(), ns, peRef, minimalResult()); err != nil {
		t.Fatalf("second write: %v", err)
	}

	list := &seamv1alpha1.PackOperationResultList{}
	if err := cl.List(context.Background(), list,
		ctrlclient.InNamespace(ns),
		ctrlclient.MatchingLabels{labelPackExecution: peRef},
	); err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("expected 2 PORs after upgrade (active + superseded), got %d", len(list.Items))
	}

	// Find active (revision 2) and superseded (revision 1).
	var active, superseded *seamv1alpha1.PackOperationResult
	for i := range list.Items {
		item := &list.Items[i]
		if item.Spec.Revision == 2 {
			active = item
		} else if item.Spec.Revision == 1 {
			superseded = item
		}
	}
	if active == nil {
		t.Fatal("revision 2 POR not found")
	}
	if superseded == nil {
		t.Fatal("revision 1 POR not found (must be retained as superseded)")
	}
	if active.Spec.PreviousRevisionRef != "pack-deploy-result-pe-upgrade-r1" {
		t.Errorf("previousRevisionRef = %q, want %q",
			active.Spec.PreviousRevisionRef, "pack-deploy-result-pe-upgrade-r1")
	}
	if got := superseded.Labels[labelSuperseded]; got != "true" {
		t.Errorf("revision 1 superseded label=%q, want true", got)
	}
	if got := active.Labels[labelSuperseded]; got != "" {
		t.Errorf("revision 2 should not be labeled superseded; got %q", got)
	}
}

// TestWriteResult_SetsOwnerReferenceWhenPEExists verifies that POR gets an ownerReference
// to the PackExecution so Kubernetes GC cascades deletion.
func TestWriteResult_SetsOwnerReferenceWhenPEExists(t *testing.T) {
	scheme := newTestScheme(t)
	peUID := types.UID("test-pe-uid-1234")
	pe := &seamv1alpha1.InfrastructurePackExecution{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pe-with-owner",
			Namespace: "seam-tenant-ccs-mgmt",
			UID:       peUID,
		},
	}
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(pe).Build()
	w := NewKubeOperationResultWriter(cl, "ccs-mgmt")

	if err := w.WriteResult(context.Background(), "seam-tenant-ccs-mgmt", "pe-with-owner", minimalResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	por := &seamv1alpha1.PackOperationResult{}
	if err := cl.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-ccs-mgmt", Name: "pack-deploy-result-pe-with-owner-r1"},
		por,
	); err != nil {
		t.Fatalf("get POR: %v", err)
	}

	if len(por.OwnerReferences) != 1 {
		t.Fatalf("expected 1 ownerReference, got %d", len(por.OwnerReferences))
	}
	ref := por.OwnerReferences[0]
	if ref.Kind != "InfrastructurePackExecution" {
		t.Errorf("ownerRef.Kind = %q, want InfrastructurePackExecution", ref.Kind)
	}
	if ref.UID != peUID {
		t.Errorf("ownerRef.UID = %q, want %q", ref.UID, peUID)
	}
	if ref.BlockOwnerDeletion == nil || !*ref.BlockOwnerDeletion {
		t.Error("ownerRef.BlockOwnerDeletion must be true")
	}
}

// TestWriteResult_NoOwnerReferenceWhenPEAbsent verifies that WriteResult succeeds
// without ownerReference when the PackExecution has already been deleted.
func TestWriteResult_NoOwnerReferenceWhenPEAbsent(t *testing.T) {
	scheme := newTestScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()
	w := NewKubeOperationResultWriter(cl, "ccs-mgmt")

	if err := w.WriteResult(context.Background(), "seam-tenant-ccs-mgmt", "pe-deleted", minimalResult()); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	por := &seamv1alpha1.PackOperationResult{}
	if err := cl.Get(context.Background(),
		ctrlclient.ObjectKey{Namespace: "seam-tenant-ccs-mgmt", Name: "pack-deploy-result-pe-deleted-r1"},
		por,
	); err != nil {
		t.Fatalf("get POR: %v", err)
	}

	if len(por.OwnerReferences) != 0 {
		t.Errorf("expected 0 ownerReferences when PE absent, got %d", len(por.OwnerReferences))
	}
}
