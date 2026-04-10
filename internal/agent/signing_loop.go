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

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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

// clusterPackGVR is the GroupVersionResource for ClusterPack CRs.
// Defined by the Wrapper operator in infra.ontai.dev. conductor-schema.md §10.
// The management cluster Conductor signs ClusterPacks so the wrapper
// ClusterPackReconciler can transition Status.Signed=true and Available.
var clusterPackGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "clusterpacks",
}

// clusterPackSignatureAnnotation is the annotation key read by the wrapper
// ClusterPackReconciler to detect that conductor has signed the ClusterPack.
// Defined in wrapper/internal/controller/clusterpack_reconciler.go as
// packSignatureAnnotation. Must remain in sync with that constant.
const clusterPackSignatureAnnotation = "ontai.dev/pack-signature"

// permissionSnapshotGVR is the GroupVersionResource for PermissionSnapshot CRs.
// Defined by the Guardian operator in security.ontai.dev. conductor-schema.md §10.
var permissionSnapshotGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshots",
}

// secretGVR is the GroupVersionResource for Kubernetes core Secrets.
// Used by the signing loop to store signed PackInstance artifacts on the
// management cluster for consumption by the target cluster PackInstancePullLoop.
// Gap 28, conductor-schema.md §10.
var secretGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "secrets",
}

// SigningLoop implements the management-cluster signing loops for PackInstance
// and PermissionSnapshot CRs. It runs on the management cluster only.
//
// On each cycle it:
//  1. For each PackInstance CR without a management-signature annotation:
//     computes an Ed25519 signature of json.Marshal(spec) and writes it as an
//     annotation patch. Then stores the signed artifact as a Secret in the
//     seam-tenant-{clusterName} namespace (Gap 28).
//  2. For each PermissionSnapshot CR without a management-signature annotation:
//     computes and writes the annotation.
//
// The PackInstance signature is consumed by the target cluster PackInstancePullLoop.
// The PermissionSnapshot signature is consumed by the SnapshotPullLoop. INV-026.
//
// Only the leader runs the signing loop (called from onLeaderStart).
// Signing and Secret storage are both idempotent.
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

// signAll iterates over PackInstance, PermissionSnapshot, and ClusterPack CRs
// and signs any that are missing their respective signature annotations.
// PackInstances also have their signed artifact stored as a Secret on the
// management cluster (Gap 28).
func (l *SigningLoop) signAll(ctx context.Context) {
	l.signPackInstances(ctx)
	l.signGVR(ctx, permissionSnapshotGVR, managementSignatureAnnotation)
	l.signGVR(ctx, clusterPackGVR, clusterPackSignatureAnnotation)
}

// signPackInstances handles the full signing cycle for PackInstance CRs:
//  1. Signs any unsigned PackInstance (writes managementSignatureAnnotation).
//  2. Stores the signed artifact as a Secret in seam-tenant-{clusterName}
//     so the target cluster PackInstancePullLoop can consume it. Gap 28.
//
// Both signing and Secret storage are idempotent.
func (l *SigningLoop) signPackInstances(ctx context.Context) {
	list, err := l.client.Resource(packInstanceGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may not exist on this cluster yet — not fatal.
		return
	}

	for _, item := range list.Items {
		annotations := item.GetAnnotations()

		// Read existing signature annotation (empty string if absent).
		var sigB64 string
		if annotations != nil {
			sigB64 = annotations[managementSignatureAnnotation]
		}

		if sigB64 == "" {
			// Not yet signed — compute Ed25519 signature and patch the annotation.
			spec, _, _ := unstructuredNestedMap(item.Object, "spec")
			message, err := json.Marshal(spec)
			if err != nil {
				continue
			}
			sig := ed25519.Sign(l.privKey, message)
			sigB64 = base64.StdEncoding.EncodeToString(sig)

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
			if _, err := l.client.Resource(packInstanceGVR).Namespace(ns).Patch(
				ctx,
				item.GetName(),
				types.MergePatchType,
				patchBytes,
				metav1.PatchOptions{},
			); err != nil {
				// Patch failed — skip Secret storage this cycle; next cycle retries.
				continue
			}
		}

		// Store the signed artifact as a Secret on the management cluster.
		// Target cluster PackInstancePullLoop polls these Secrets. Gap 28.
		spec, _, _ := unstructuredNestedMap(item.Object, "spec")
		l.storeSignedArtifactSecret(ctx, item.GetName(), spec, sigB64)
	}
}

// storeSignedArtifactSecret creates or updates a Secret in
// seam-tenant-{clusterName} encoding the signed PackInstance artifact.
//
// Secret name pattern: seam-pack-signed-{clusterName}-{packInstanceName}.
// Data fields:
//   - artifact: base64(json.Marshal(spec)) — the full PackInstance spec as JSON.
//   - signature: sigB64 — the base64-encoded Ed25519 signature.
//
// Idempotency rules:
//   - Secret absent → create.
//   - Secret exists, data.signature matches sigB64 → skip (no-op).
//   - Secret exists, data.signature differs → overwrite.
//
// Gap 28, INV-026.
func (l *SigningLoop) storeSignedArtifactSecret(ctx context.Context, packInstanceName string, spec map[string]interface{}, sigB64 string) {
	clusterName, _ := spec["clusterRef"].(string)
	if clusterName == "" {
		// Cannot determine target namespace without clusterRef — skip.
		return
	}

	secretName := fmt.Sprintf("seam-pack-signed-%s-%s", clusterName, packInstanceName)
	secretNS := fmt.Sprintf("seam-tenant-%s", clusterName)

	// Serialize artifact spec to JSON and base64-encode for Secret data storage.
	artifactJSON, err := json.Marshal(spec)
	if err != nil {
		return
	}
	artifactB64 := base64.StdEncoding.EncodeToString(artifactJSON)

	// Check for existing Secret.
	existing, err := l.client.Resource(secretGVR).Namespace(secretNS).Get(ctx, secretName, metav1.GetOptions{})
	if err == nil {
		// Secret exists — check idempotency on the signature field.
		data, _, _ := unstructuredNestedMap(existing.Object, "data")
		existingSig, _ := data["signature"].(string)
		if existingSig == sigB64 {
			// Signature unchanged — Secret is current. Skip. Gap 28.
			return
		}
		// Signature changed — overwrite with updated data.
		updated := &unstructured.Unstructured{
			Object: map[string]interface{}{
				"apiVersion": "v1",
				"kind":       "Secret",
				"metadata": map[string]interface{}{
					"name":            secretName,
					"namespace":       secretNS,
					"resourceVersion": existing.GetResourceVersion(),
				},
				"type": "Opaque",
				"data": map[string]interface{}{
					"artifact":  artifactB64,
					"signature": sigB64,
				},
			},
		}
		if _, err := l.client.Resource(secretGVR).Namespace(secretNS).Update(
			ctx, updated, metav1.UpdateOptions{},
		); err != nil {
			// Log-only — next cycle retries.
			_ = err
		}
		return
	}

	if !k8serrors.IsNotFound(err) {
		// Real error (not NotFound) — skip this cycle; next cycle retries.
		return
	}

	// Secret does not exist — create it.
	secret := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]interface{}{
				"name":      secretName,
				"namespace": secretNS,
			},
			"type": "Opaque",
			"data": map[string]interface{}{
				"artifact":  artifactB64,
				"signature": sigB64,
			},
		},
	}
	if _, err := l.client.Resource(secretGVR).Namespace(secretNS).Create(
		ctx, secret, metav1.CreateOptions{},
	); err != nil {
		// Log-only — next cycle retries.
		_ = err
	}
}

// signGVR lists all CRs of the given GVR and patches the given annotationKey
// on any that do not yet have it. Used for PermissionSnapshot and ClusterPack CRs.
// Each caller passes the annotation key appropriate for the CR type — the wrapper
// ClusterPackReconciler reads "ontai.dev/pack-signature" while PermissionSnapshot
// consumers read "runner.ontai.dev/management-signature".
func (l *SigningLoop) signGVR(ctx context.Context, gvr schema.GroupVersionResource, annotationKey string) {
	// List across all namespaces.
	list, err := l.client.Resource(gvr).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may not exist on this cluster yet — not fatal.
		return
	}

	for _, item := range list.Items {
		annotations := item.GetAnnotations()
		if annotations != nil {
			if _, signed := annotations[annotationKey]; signed {
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
					annotationKey: sigB64,
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
