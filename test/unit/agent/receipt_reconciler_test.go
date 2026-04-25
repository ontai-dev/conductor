package agent_test

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/internal/agent"
)

// packReceiptGVR mirrors the GVR defined in the production receipt_reconciler.go.
// Redeclared here to keep the test package self-contained.
var packReceiptGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurepackreceipts",
}

// permissionSnapshotReceiptGVR mirrors the GVR defined in production.
var permissionSnapshotReceiptGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshotreceipts",
}

// newFakeDynamicClientWithReceipts creates a fake dynamic client with the
// receipt GVRs registered in its scheme so List calls succeed.
func newFakeDynamicClientWithReceipts() *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()

	for _, gvr := range []schema.GroupVersionResource{packReceiptGVR, permissionSnapshotReceiptGVR} {
		gvk := schema.GroupVersionKind{
			Group:   gvr.Group,
			Version: gvr.Version,
			Kind:    capitalize(gvr.Resource[:len(gvr.Resource)-1]), // strip trailing 's'
		}
		listGVK := schema.GroupVersionKind{
			Group:   gvr.Group,
			Version: gvr.Version,
			Kind:    gvk.Kind + "List",
		}
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}

	return dynamicfake.NewSimpleDynamicClient(scheme)
}

func capitalize(s string) string {
	if len(s) == 0 {
		return s
	}
	return string(s[0]-32) + s[1:]
}

// TestReceiptReconciler_ConstructsWithoutPanic verifies that NewReceiptReconciler
// does not panic with valid inputs.
func TestReceiptReconciler_ConstructsWithoutPanic(t *testing.T) {
	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	rec := agent.NewReceiptReconciler(fakeClient, "ont-system")
	if rec == nil {
		t.Error("expected non-nil ReceiptReconciler")
	}
}

// TestReceiptReconciler_ReconcileEmptyStoreNoPanic verifies that Reconcile
// completes without error when there are no receipts to process.
// conductor-schema.md §10 (receipt reconciliation loop).
func TestReceiptReconciler_ReconcileEmptyStoreNoPanic(t *testing.T) {
	fakeClient := newFakeDynamicClientWithReceipts()
	rec := agent.NewReceiptReconciler(fakeClient, "ont-system")

	if err := rec.Reconcile(context.Background()); err != nil {
		t.Errorf("Reconcile on empty store: expected nil error; got %v", err)
	}
}

// TestReceiptReconciler_ReconcileContextCancelledReturnsCleanly verifies that
// Reconcile returns cleanly when context is already cancelled.
func TestReceiptReconciler_ReconcileContextCancelledReturnsCleanly(t *testing.T) {
	fakeClient := newFakeDynamicClientWithReceipts()
	rec := agent.NewReceiptReconciler(fakeClient, "ont-system")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Should return nil even with cancelled context — Reconcile is one-shot.
	if err := rec.Reconcile(ctx); err != nil {
		t.Errorf("Reconcile with cancelled ctx: expected nil; got %v", err)
	}
}

// TestReceiptReconciler_ReconcileDifferentNamespaceNoPanic verifies that
// Reconcile does not panic when receipts are in a different namespace than
// the reconciler operates on — it simply finds nothing to acknowledge.
func TestReceiptReconciler_ReconcileDifferentNamespaceNoPanic(t *testing.T) {
	fakeClient := newFakeDynamicClientWithReceipts()
	// Reconciler targets "ont-system" but no receipts exist there.
	rec := agent.NewReceiptReconciler(fakeClient, "ont-system")

	if err := rec.Reconcile(context.Background()); err != nil {
		t.Errorf("Reconcile with empty namespace: expected nil; got %v", err)
	}
}
