package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// packReceiptGVR is the GroupVersionResource for PackReceipt CRs.
// Defined in infrastructure.ontai.dev (seam-core). conductor-schema.md §10, §15.
var packReceiptGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurepackreceipts",
}

// permissionSnapshotReceiptGVR is the GroupVersionResource for PermissionSnapshotReceipt CRs.
// Defined by the Guardian operator in security.ontai.dev. conductor-schema.md §10.
var permissionSnapshotReceiptGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshotreceipts",
}

// managementSignatureAnnotation is the annotation key under which the
// management cluster Conductor writes the base64-encoded Ed25519 signature
// of the receipt CR's spec field. INV-026.
const managementSignatureAnnotation = "infrastructure.ontai.dev/management-signature"

// ReceiptReconciler reconciles PackReceipt and PermissionSnapshotReceipt CRs.
//
// On each reconcile cycle it:
//  1. Lists all receipts of both types in the namespace.
//  2. For each unacknowledged receipt, verifies the management cluster signature
//     using the configured Ed25519 public key.
//  3. On verification success: writes the acknowledgement to the receipt status.
//  4. On verification failure: patches DegradedSecurityState on the receipt
//     status and blocks acknowledgement. INV-026.
//
// When pubKey is nil (no key mounted), the reconciler operates in bootstrap
// window mode: unsigned receipts are accepted and non-empty signatures are
// accepted without verification. INV-020. Real enforcement begins when the
// key is mounted.
//
// conductor-schema.md §10 (signing and verification model), conductor-design.md §2.10.
type ReceiptReconciler struct {
	client    dynamic.Interface
	namespace string
	pubKey    ed25519.PublicKey // nil during bootstrap window (key not yet mounted)
}

// NewReceiptReconciler constructs a ReceiptReconciler that operates on the
// given namespace. pubKey may be nil to operate in bootstrap window mode.
func NewReceiptReconciler(client dynamic.Interface, namespace string) *ReceiptReconciler {
	return &ReceiptReconciler{client: client, namespace: namespace}
}

// NewReceiptReconcilerWithKey constructs a ReceiptReconciler that enforces
// INV-026 Ed25519 signature verification using the given public key. The
// public key is expected as a PKIX PEM-encoded Ed25519 public key.
// publicKeyPath is the path to the file containing the PEM-encoded public key,
// typically mounted from a Kubernetes Secret volume.
func NewReceiptReconcilerWithKey(client dynamic.Interface, namespace, publicKeyPath string) (*ReceiptReconciler, error) {
	keyBytes, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("NewReceiptReconcilerWithKey: read public key %s: %w", publicKeyPath, err)
	}
	pubKey, err := parseEd25519PublicKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("NewReceiptReconcilerWithKey: parse Ed25519 public key: %w", err)
	}
	return &ReceiptReconciler{client: client, namespace: namespace, pubKey: pubKey}, nil
}

// parseEd25519PublicKey decodes a PEM block containing a PKIX Ed25519 public
// key and returns the crypto/ed25519.PublicKey.
func parseEd25519PublicKey(pemBytes []byte) (ed25519.PublicKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in public key data")
	}
	raw, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKIX public key: %w", err)
	}
	ed, ok := raw.(ed25519.PublicKey)
	if !ok {
		return nil, fmt.Errorf("public key is not Ed25519 (got %T)", raw)
	}
	return ed, nil
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

		// Verify management cluster signature. INV-026.
		if err := r.verifySignature(item.Object); err != nil {
			// Signature failure blocks acknowledgement and raises DegradedSecurityState.
			// conductor-schema.md §10.
			_ = r.patchDegradedSecurityState(ctx, gvr, item.GetName(), err.Error())
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

// verifySignature verifies the management cluster Ed25519 signature on a
// receipt CR. The signature is base64-encoded in the
// "runner.ontai.dev/management-signature" annotation. The signed message is the
// JSON encoding of the CR's spec field. INV-026.
//
// When pubKey is nil (bootstrap window mode, INV-020):
//   - No signature present → accepted (bootstrap window permits unsigned receipts).
//   - Signature present → accepted without verification (key not yet mounted).
//
// When pubKey is set (normal operation):
//   - No signature → rejected (unsigned receipt is a security violation).
//   - Signature present → verified with Ed25519; rejected if invalid.
func (r *ReceiptReconciler) verifySignature(obj map[string]interface{}) error {
	annotations, _, _ := unstructuredNestedMap(obj, "metadata", "annotations")
	sig64, hasSig := annotations[managementSignatureAnnotation].(string)

	// Bootstrap window mode — key not yet mounted.
	if r.pubKey == nil {
		// Accept all receipts (signed or unsigned) during bootstrap window.
		// INV-020: bootstrap RBAC window. Enforcement begins when key is mounted.
		return nil
	}

	// Normal operation — key is mounted; enforce INV-026.
	if !hasSig || sig64 == "" {
		return fmt.Errorf("receipt missing required signature annotation %q (INV-026)",
			managementSignatureAnnotation)
	}

	sigBytes, err := base64.StdEncoding.DecodeString(sig64)
	if err != nil {
		return fmt.Errorf("decode signature base64: %w", err)
	}

	// Compute the message: JSON encoding of the spec field.
	spec, _, _ := unstructuredNestedMap(obj, "spec")
	message, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshal spec for signature verification: %w", err)
	}

	if !ed25519.Verify(r.pubKey, message, sigBytes) {
		return fmt.Errorf("Ed25519 signature verification failed for receipt %q (INV-026)",
			annotations["name"])
	}
	return nil
}

// patchDegradedSecurityState sets DegradedSecurityState=True on the receipt CR
// status. This signals the management cluster that this receipt cannot be
// acknowledged due to a signature verification failure. conductor-schema.md §10.
func (r *ReceiptReconciler) patchDegradedSecurityState(ctx context.Context, gvr schema.GroupVersionResource, name, reason string) error {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"degradedSecurityState": true,
			"degradedReason":        reason,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("patchDegradedSecurityState: marshal patch: %w", err)
	}
	if _, err := r.client.Resource(gvr).Namespace(r.namespace).Patch(
		ctx,
		name,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"status",
	); err != nil {
		return fmt.Errorf("patchDegradedSecurityState %s/%s: %w", gvr.Resource, name, err)
	}
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
