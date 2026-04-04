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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// packInstanceGVR is the GroupVersionResource for PackInstance CRs.
// Defined by the Wrapper operator in infra.ontai.dev. conductor-schema.md §10.
var packInstanceGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "packinstances",
}

// permissionSnapshotGVR is the GroupVersionResource for PermissionSnapshot CRs.
// Defined by the Guardian operator in security.ontai.dev. conductor-schema.md §10.
var permissionSnapshotGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshots",
}

// SigningLoop implements the management-cluster signing loops for PackInstance
// and PermissionSnapshot CRs. It runs on the management cluster only.
//
// On each cycle it:
//  1. Lists all PackInstance CRs across all namespaces.
//  2. Lists all PermissionSnapshot CRs across all namespaces.
//  3. For each CR without a management-signature annotation: computes an
//     Ed25519 signature of json.Marshal(spec) and writes it as an annotation patch.
//
// The signature is consumed by the target cluster ReceiptReconciler (Session 15 WS2)
// before acknowledging receipt. INV-026, conductor-schema.md §10.
//
// Only the leader runs the signing loop (called from onLeaderStart).
// Signing is idempotent — already-signed CRs are skipped.
type SigningLoop struct {
	client  dynamic.Interface
	privKey ed25519.PrivateKey
}

// NewSigningLoop constructs a SigningLoop that reads the Ed25519 private key from
// privateKeyPath. The private key must be a PKCS#8 PEM-encoded Ed25519 private key,
// typically mounted from a Kubernetes Secret volume on the management cluster.
func NewSigningLoop(client dynamic.Interface, privateKeyPath string) (*SigningLoop, error) {
	keyBytes, err := os.ReadFile(privateKeyPath)
	if err != nil {
		return nil, fmt.Errorf("NewSigningLoop: read private key %s: %w", privateKeyPath, err)
	}
	privKey, err := parseEd25519PrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("NewSigningLoop: parse Ed25519 private key: %w", err)
	}
	return &SigningLoop{client: client, privKey: privKey}, nil
}

// parseEd25519PrivateKey decodes a PKCS#8 PEM block containing an Ed25519 private key.
func parseEd25519PrivateKey(pemBytes []byte) (ed25519.PrivateKey, error) {
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, fmt.Errorf("no PEM block found in private key data")
	}
	raw, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS8 private key: %w", err)
	}
	priv, ok := raw.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key is not Ed25519 (got %T)", raw)
	}
	return priv, nil
}

// Run runs the signing loop until ctx is cancelled. It fires once immediately
// then repeats on interval. conductor-schema.md §10 steps 9–10.
// interval must be positive (> 0).
func (l *SigningLoop) Run(ctx context.Context, interval time.Duration) {
	l.signAll(ctx)

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.signAll(ctx)
		}
	}
}

// signAll iterates over PackInstance and PermissionSnapshot CRs and signs any
// that are missing the management-signature annotation.
func (l *SigningLoop) signAll(ctx context.Context) {
	l.signGVR(ctx, packInstanceGVR)
	l.signGVR(ctx, permissionSnapshotGVR)
}

// signGVR lists all CRs of the given GVR and patches the management-signature
// annotation on any that do not yet have it.
func (l *SigningLoop) signGVR(ctx context.Context, gvr schema.GroupVersionResource) {
	// List across all namespaces.
	list, err := l.client.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may not exist on this cluster yet — not fatal.
		return
	}

	for _, item := range list.Items {
		annotations := item.GetAnnotations()
		if annotations != nil {
			if _, signed := annotations[managementSignatureAnnotation]; signed {
				// Already signed — skip.
				continue
			}
		}

		// Compute signature: Ed25519(privKey, json.Marshal(spec)).
		spec, _, _ := unstructuredNestedMap(item.Object, "spec")
		message, err := json.Marshal(spec)
		if err != nil {
			continue
		}
		sig := ed25519.Sign(l.privKey, message)
		sigB64 := base64.StdEncoding.EncodeToString(sig)

		// Patch the annotation onto the CR. Use a metadata-only merge patch so
		// the status and spec are untouched.
		patch := map[string]interface{}{
			"metadata": map[string]interface{}{
				"annotations": map[string]interface{}{
					managementSignatureAnnotation: sigB64,
				},
			},
		}
		patchBytes, err := json.Marshal(patch)
		if err != nil {
			continue
		}

		ns := item.GetNamespace()
		if _, err := l.client.Resource(gvr).Namespace(ns).Patch(
			ctx,
			item.GetName(),
			types.MergePatchType,
			patchBytes,
			metav1.PatchOptions{},
		); err != nil {
			// Log-only failure — signing is best-effort within each cycle.
			// The next cycle will retry unsigned CRs.
			_ = err
		}
	}
}
