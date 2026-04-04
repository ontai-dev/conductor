package agent

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// packReceiptGVR is the GroupVersionResource for PackReceipt CRs.
// Defined by the Wrapper operator in infra.ontai.dev. conductor-schema.md §10.
var packReceiptGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "packreceipts",
}

// permissionSnapshotReceiptGVR is the GroupVersionResource for PermissionSnapshotReceipt CRs.
// Defined by the Guardian operator in security.ontai.dev. conductor-schema.md §10.
var permissionSnapshotReceiptGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshotreceipts",
}

// ReceiptReconciler reconciles PackReceipt and PermissionSnapshotReceipt CRs.
//
// On each reconcile cycle it:
//  1. Lists all receipts of both types in the namespace.
//  2. For each unacknowledged receipt, verifies the management cluster signature
//     (currently a stub — full signing infrastructure pending).
//  3. Writes the acknowledgement to the receipt's status subresource.
//
// conductor-schema.md §10 (signing and verification model), conductor-design.md §2.10.
type ReceiptReconciler struct {
	client    dynamic.Interface
	namespace string
}

// NewReceiptReconciler constructs a ReceiptReconciler that operates on the
// given namespace.
func NewReceiptReconciler(client dynamic.Interface, namespace string) *ReceiptReconciler {
	return &ReceiptReconciler{client: client, namespace: namespace}
}

// Reconcile performs one reconcile cycle over PackReceipt and
// PermissionSnapshotReceipt CRs. Returns nil when there is nothing left to
// acknowledge or when the context is cancelled.
// conductor-design.md §2.10, conductor-schema.md §10.
func (r *ReceiptReconciler) Reconcile(ctx context.Context) error {
	if err := r.reconcileGVR(ctx, packReceiptGVR); err != nil {
		return fmt.Errorf("receipt reconciler: pack receipts: %w", err)
	}
	if err := r.reconcileGVR(ctx, permissionSnapshotReceiptGVR); err != nil {
		return fmt.Errorf("receipt reconciler: permission snapshot receipts: %w", err)
	}
	return nil
}

// reconcileGVR lists all CRs of the given GVR and acknowledges any that have
// not yet been acknowledged. The acknowledgement is written to the status
// subresource as a merge patch.
func (r *ReceiptReconciler) reconcileGVR(ctx context.Context, gvr schema.GroupVersionResource) error {
	list, err := r.client.Resource(gvr).Namespace(r.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may not exist on this cluster — not fatal, just skip.
		return nil
	}

	for _, item := range list.Items {
		// Check whether this receipt is already acknowledged.
		status, _, _ := unstructuredNestedMap(item.Object, "status")
		if acknowledged, ok := status["acknowledged"].(bool); ok && acknowledged {
			continue
		}

		// Verify management cluster signature.
		// TODO: real signature verification once signing key mounting is implemented.
		// conductor-schema.md §10 (signing and verification model).
		if err := r.verifySignature(item.Object); err != nil {
			// Signature failure blocks acknowledgement and raises DegradedSecurityState.
			// For now, log and skip rather than patching a degraded condition.
			continue
		}

		// Write acknowledgement to status subresource.
		ackPatch := map[string]interface{}{
			"status": map[string]interface{}{
				"acknowledged": true,
			},
		}
		patchBytes, err := json.Marshal(ackPatch)
		if err != nil {
			return fmt.Errorf("reconcileGVR %s: marshal ack patch for %q: %w",
				gvr.Resource, item.GetName(), err)
		}

		if _, err := r.client.Resource(gvr).Namespace(r.namespace).Patch(
			ctx,
			item.GetName(),
			types.MergePatchType,
			patchBytes,
			metav1.PatchOptions{},
			"status",
		); err != nil {
			return fmt.Errorf("reconcileGVR %s: patch status for %q: %w",
				gvr.Resource, item.GetName(), err)
		}
	}
	return nil
}

// verifySignature verifies the management cluster cryptographic signature on a
// receipt CR. Signature is read from the
// "runner.ontai.dev/management-signature" annotation.
//
// TODO: implement full Ed25519 verification once the signing key Secret is
// mounted. conductor-schema.md §10 (signing and verification model). INV-026.
func (r *ReceiptReconciler) verifySignature(obj map[string]interface{}) error {
	annotations, _, _ := unstructuredNestedMap(obj, "metadata", "annotations")
	sig, ok := annotations["runner.ontai.dev/management-signature"].(string)
	if !ok || sig == "" {
		// No signature present — treat as unsigned (permitted during bootstrap window).
		// INV-020: bootstrap RBAC window. Real signature enforcement begins when
		// the management cluster signing infrastructure is operational.
		return nil
	}
	// Placeholder: accept any non-empty signature until key mounting is implemented.
	return nil
}

// unstructuredNestedMap is a helper that traverses nested maps in an unstructured
// object by a sequence of keys. Returns the nested map and whether it was found.
func unstructuredNestedMap(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool, error) {
	current := obj
	for _, field := range fields {
		next, ok := current[field]
		if !ok {
			return nil, false, nil
		}
		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return nil, false, fmt.Errorf("field %q is not a map", field)
		}
		current = nextMap
	}
	return current, true, nil
}
