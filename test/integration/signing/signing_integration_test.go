// Package signing_test contains integration tests for the conductor agent signing
// and pull loops. Tests use in-process Ed25519 key generation and a fake dynamic
// client to simulate management and tenant cluster Kubernetes API servers.
//
// conductor-schema.md §10, INV-026, Gap 28.
package signing_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ontai-dev/conductor/internal/agent"
	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// ── GVR definitions mirroring internal/agent ─────────────────────────────────

var (
	packInstanceGVR = schema.GroupVersionResource{
		Group: "infra.ontai.dev", Version: "v1alpha1", Resource: "packinstances",
	}
	clusterPackGVR = schema.GroupVersionResource{
		Group: "infra.ontai.dev", Version: "v1alpha1", Resource: "clusterpacks",
	}
	packReceiptGVR = schema.GroupVersionResource{
		Group: "runner.ontai.dev", Version: "v1alpha1", Resource: "packreceipts",
	}
	secretGVR = schema.GroupVersionResource{
		Group: "", Version: "v1", Resource: "secrets",
	}
	permissionSnapshotGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "permissionsnapshots",
	}
	permissionSnapshotReceiptGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "permissionsnapshotreceipts",
	}
)

// ── Key helpers ───────────────────────────────────────────────────────────────

// testKeypair generates an Ed25519 key pair and writes the private key as a
// PKCS#8 PEM file and the public key as a PKIX PEM file to a temp directory.
// Returns the key pair and both file paths.
func testKeypair(t *testing.T) (pub ed25519.PublicKey, priv ed25519.PrivateKey, privKeyPath, pubKeyPath string) {
	t.Helper()
	var err error
	pub, priv, err = ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 key: %v", err)
	}
	dir := t.TempDir()

	// Write PKCS#8 PEM private key.
	privDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal private key: %v", err)
	}
	privKeyPath = filepath.Join(dir, "signing.key")
	if err := os.WriteFile(privKeyPath, pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER}), 0o600); err != nil {
		t.Fatalf("write private key: %v", err)
	}

	// Write PKIX PEM public key.
	pubDER, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	pubKeyPath = filepath.Join(dir, "signing.pub")
	if err := os.WriteFile(pubKeyPath, pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: pubDER}), 0o600); err != nil {
		t.Fatalf("write public key: %v", err)
	}
	return pub, priv, privKeyPath, pubKeyPath
}

// signSpec computes an Ed25519 signature over json.Marshal(spec) using priv.
func signSpec(t *testing.T, spec map[string]interface{}, priv ed25519.PrivateKey) string {
	t.Helper()
	msg, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, msg))
}

// gvrToListKind maps GVRs to their List kind names.
// NewSimpleDynamicClientWithCustomListKinds requires this explicit mapping.
var gvrToListKind = map[schema.GroupVersionResource]string{
	packInstanceGVR:              "PackInstanceList",
	clusterPackGVR:               "ClusterPackList",
	packReceiptGVR:               "PackReceiptList",
	secretGVR:                    "SecretList",
	permissionSnapshotGVR:        "PermissionSnapshotList",
	permissionSnapshotReceiptGVR: "PermissionSnapshotReceiptList",
}

// newFakeDynamic builds a fake dynamic client registered with the provided GVRs.
func newFakeDynamic(t *testing.T, gvrs ...schema.GroupVersionResource) *dynamicfake.FakeDynamicClient {
	t.Helper()
	scheme := runtime.NewScheme()
	listKinds := make(map[schema.GroupVersionResource]string)
	for _, gvr := range gvrs {
		listKind := gvrToListKind[gvr]
		if listKind == "" {
			t.Fatalf("no list kind registered for GVR %v", gvr)
		}
		listKinds[gvr] = listKind
	}
	// Always add core Secret.
	listKinds[secretGVR] = "SecretList"
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, listKinds)
}

// createUnstructured creates an unstructured object on the given client.
func createUnstructured(t *testing.T, client *dynamicfake.FakeDynamicClient, gvr schema.GroupVersionResource, ns string, obj *unstructured.Unstructured) *unstructured.Unstructured {
	t.Helper()
	created, err := client.Resource(gvr).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s/%s: %v", gvr.Resource, obj.GetName(), err)
	}
	return created
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestSigningLoop_SignsPackInstance_StoresSecret verifies that SigningLoop.Run
// signs a PackInstance CR (adding the management-signature annotation) and
// creates a signed artifact Secret in seam-tenant-{clusterName}.
func TestSigningLoop_SignsPackInstance_StoresSecret(t *testing.T) {
	_, _, privKeyPath, _ := testKeypair(t)

	// Build a fake dynamic client with all GVRs that signAll touches.
	dynClient := newFakeDynamic(t, packInstanceGVR, clusterPackGVR, permissionSnapshotGVR, secretGVR)

	// Pre-create a PackInstance in seam-tenant-ccs-test.
	packInstance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infra.ontai.dev/v1alpha1",
			"kind":       "PackInstance",
			"metadata":   map[string]interface{}{"name": "nginx", "namespace": "seam-tenant-ccs-test"},
			"spec": map[string]interface{}{
				"clusterRef": "ccs-test",
				"packRef":    "nginx",
				"version":    "1.0.0",
			},
		},
	}
	createUnstructured(t, dynClient, packInstanceGVR, "seam-tenant-ccs-test", packInstance)

	// Ensure the seam-tenant-ccs-test namespace exists in the fake client.
	// (fake client creates the namespace implicitly via tracker for namespace-scoped resources)

	loop, err := agent.NewSigningLoop(dynClient, privKeyPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	// Run one cycle synchronously via Run with a very long interval — context
	// cancels before the first tick.
	go loop.Run(ctx, 10*time.Minute)
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Verify signature annotation was patched onto the PackInstance.
	updated, err := dynClient.Resource(packInstanceGVR).Namespace("seam-tenant-ccs-test").Get(
		context.Background(), "nginx", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PackInstance after sign: %v", err)
	}
	ann := updated.GetAnnotations()
	if ann == nil || ann["runner.ontai.dev/management-signature"] == "" {
		t.Error("PackInstance missing management-signature annotation after signing loop")
	}

	// Verify the signed artifact Secret was created.
	secretName := "seam-pack-signed-ccs-test-nginx"
	secret, err := dynClient.Resource(secretGVR).Namespace("seam-tenant-ccs-test").Get(
		context.Background(), secretName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get signed artifact Secret %q: %v", secretName, err)
	}
	data, _, _ := nestedMap(secret.Object, "data")
	if data["signature"] == "" {
		t.Error("Secret data.signature is empty; want base64-encoded Ed25519 signature")
	}
	if data["artifact"] == "" {
		t.Error("Secret data.artifact is empty; want base64-encoded spec JSON")
	}
}

// TestSigningLoop_IdempotentOnStaleSignature verifies that when the signature
// annotation already matches the current spec, the Secret is not overwritten.
// Idempotency is defined as: same sigB64 → skip (no update action).
func TestSigningLoop_IdempotentOnStaleSignature(t *testing.T) {
	_, priv, privKeyPath, _ := testKeypair(t)

	dynClient := newFakeDynamic(t, packInstanceGVR, clusterPackGVR, permissionSnapshotGVR, secretGVR)

	spec := map[string]interface{}{"clusterRef": "ccs-test", "packRef": "redis", "version": "1.0.0"}
	existingSig := signSpec(t, spec, priv)

	// PackInstance already has a signature annotation.
	packInstance := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infra.ontai.dev/v1alpha1",
			"kind":       "PackInstance",
			"metadata": map[string]interface{}{
				"name":      "redis",
				"namespace": "seam-tenant-ccs-test",
				"annotations": map[string]interface{}{
					"runner.ontai.dev/management-signature": existingSig,
				},
			},
			"spec": spec,
		},
	}
	createUnstructured(t, dynClient, packInstanceGVR, "seam-tenant-ccs-test", packInstance)

	// Pre-create the Secret with the same signature.
	artifactJSON, _ := json.Marshal(spec)
	artifactB64 := base64.StdEncoding.EncodeToString(artifactJSON)
	existingSecret := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata":   map[string]interface{}{"name": "seam-pack-signed-ccs-test-redis", "namespace": "seam-tenant-ccs-test"},
			"type":       "Opaque",
			"data":       map[string]interface{}{"artifact": artifactB64, "signature": existingSig},
		},
	}
	createUnstructured(t, dynClient, secretGVR, "seam-tenant-ccs-test", existingSecret)

	loop, err := agent.NewSigningLoop(dynClient, privKeyPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go loop.Run(ctx, 10*time.Minute)
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Count updates to confirm no Secret Update action was performed.
	updateCount := 0
	for _, action := range dynClient.Actions() {
		if action.GetVerb() == "update" && action.GetResource() == secretGVR {
			updateCount++
		}
	}
	if updateCount > 0 {
		t.Errorf("Secret was updated %d times; want 0 (idempotent: same signature)", updateCount)
	}
}

// TestPackInstancePullLoop_ValidSignature_CreatesReceipt verifies that
// PackInstancePullLoop creates a PackReceipt with Verified=true when the
// signed artifact Secret contains a valid Ed25519 signature.
func TestPackInstancePullLoop_ValidSignature_CreatesReceipt(t *testing.T) {
	pub, priv, _, pubKeyPath := testKeypair(t)
	_ = pub

	mgmtClient := newFakeDynamic(t, secretGVR, packReceiptGVR)
	localClient := newFakeDynamic(t, packReceiptGVR)

	// Create the signed artifact Secret on the management cluster.
	spec := map[string]interface{}{"clusterRef": "ccs-test", "packRef": "nginx", "version": "1.0.0"}
	artifactJSON, _ := json.Marshal(spec)
	artifactB64 := base64.StdEncoding.EncodeToString(artifactJSON)
	sigB64 := signSpec(t, spec, priv)

	secret := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata":   map[string]interface{}{"name": "seam-pack-signed-ccs-test-nginx", "namespace": "seam-tenant-ccs-test"},
			"type":       "Opaque",
			"data":       map[string]interface{}{"artifact": artifactB64, "signature": sigB64},
		},
	}
	createUnstructured(t, mgmtClient, secretGVR, "seam-tenant-ccs-test", secret)

	loop, err := agent.NewPackInstancePullLoopWithKey(
		mgmtClient, localClient, "ccs-test", "ont-system", pubKeyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go loop.Run(ctx, 10*time.Minute)
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Verify PackReceipt was created on the local client.
	receipt, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "nginx", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PackReceipt: %v", err)
	}
	status, _, _ := nestedMap(receipt.Object, "status")
	if status["verified"] != true {
		t.Errorf("PackReceipt status.verified = %v; want true", status["verified"])
	}
}

// TestPackInstancePullLoop_TamperedSignature_ReceiptVerifiedFalse verifies that
// when the artifact has been tampered with (signature does not match), the
// PackReceipt is created with Verified=false and a failure reason.
func TestPackInstancePullLoop_TamperedSignature_ReceiptVerifiedFalse(t *testing.T) {
	pub, priv, _, pubKeyPath := testKeypair(t)
	_ = pub

	mgmtClient := newFakeDynamic(t, secretGVR, packReceiptGVR)
	localClient := newFakeDynamic(t, packReceiptGVR)

	// Sign the original spec...
	originalSpec := map[string]interface{}{"clusterRef": "ccs-test", "packRef": "nginx", "version": "1.0.0"}
	sigB64 := signSpec(t, originalSpec, priv)

	// ...but store a tampered artifact (different version).
	tamperedSpec := map[string]interface{}{"clusterRef": "ccs-test", "packRef": "nginx", "version": "9.9.9"}
	tamperedJSON, _ := json.Marshal(tamperedSpec)
	tamperedB64 := base64.StdEncoding.EncodeToString(tamperedJSON)

	secret := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata":   map[string]interface{}{"name": "seam-pack-signed-ccs-test-nginx", "namespace": "seam-tenant-ccs-test"},
			"type":       "Opaque",
			"data":       map[string]interface{}{"artifact": tamperedB64, "signature": sigB64},
		},
	}
	createUnstructured(t, mgmtClient, secretGVR, "seam-tenant-ccs-test", secret)

	loop, err := agent.NewPackInstancePullLoopWithKey(
		mgmtClient, localClient, "ccs-test", "ont-system", pubKeyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go loop.Run(ctx, 10*time.Minute)
	time.Sleep(200 * time.Millisecond)
	cancel()

	// PackReceipt should exist with Verified=false.
	receipt, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "nginx", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get PackReceipt: %v", err)
	}
	status, _, _ := nestedMap(receipt.Object, "status")
	if status["verified"] != false {
		t.Errorf("PackReceipt status.verified = %v; want false for tampered signature", status["verified"])
	}
	if status["verificationFailedReason"] == "" {
		t.Error("PackReceipt status.verificationFailedReason is empty; want non-empty failure reason")
	}
}

// TestSnapshotPullLoop_InvalidSignature_PatchesDegradedSecurityState verifies that
// SnapshotPullLoop patches DegradedSecurityState=true on the local
// PermissionSnapshotReceipt when a snapshot has an invalid signature.
func TestSnapshotPullLoop_InvalidSignature_PatchesDegradedSecurityState(t *testing.T) {
	pub, priv, _, pubKeyPath := testKeypair(t)
	_ = pub

	mgmtClient := newFakeDynamic(t, permissionSnapshotGVR)
	localClient := newFakeDynamic(t, permissionSnapshotReceiptGVR)

	// Sign one spec, store a different spec in the snapshot (tamper).
	originalSpec := map[string]interface{}{"targetCluster": "ccs-test", "version": "2026-01-01T00:00:00Z"}
	sigB64 := signSpec(t, originalSpec, priv)

	tamperedSpec := map[string]interface{}{"targetCluster": "ccs-test", "version": "TAMPERED"}

	snapshot := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "PermissionSnapshot",
			"metadata": map[string]interface{}{
				"name":      "snapshot-ccs-test",
				"namespace": "security-system",
				"annotations": map[string]interface{}{
					"runner.ontai.dev/management-signature": sigB64,
				},
			},
			"spec": tamperedSpec,
		},
	}
	createUnstructured(t, mgmtClient, permissionSnapshotGVR, "security-system", snapshot)

	// Pre-create the local PermissionSnapshotReceipt so the patch has a target.
	receipt := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "PermissionSnapshotReceipt",
			"metadata":   map[string]interface{}{"name": "snapshot-ccs-test", "namespace": "ont-system"},
			"status":     map[string]interface{}{},
		},
	}
	createUnstructured(t, localClient, permissionSnapshotReceiptGVR, "ont-system", receipt)

	store := permissionservice.NewSnapshotStore()
	loop, err := agent.NewSnapshotPullLoopWithKey(
		mgmtClient, localClient, store, "ccs-test", "ont-system", pubKeyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go loop.Run(ctx, 10*time.Minute)
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Verify DegradedSecurityState was patched.
	patchFound := false
	for _, action := range localClient.Actions() {
		if action.GetVerb() == "patch" && action.GetResource() == permissionSnapshotReceiptGVR {
			patchAction := action.(k8stesting.PatchAction)
			var patchBody map[string]interface{}
			if err := json.Unmarshal(patchAction.GetPatch(), &patchBody); err != nil {
				continue
			}
			if statusMap, ok := patchBody["status"].(map[string]interface{}); ok {
				if statusMap["degradedSecurityState"] == true {
					patchFound = true
					break
				}
			}
		}
	}
	if !patchFound {
		t.Error("DegradedSecurityState=true patch not found on PermissionSnapshotReceipt after invalid signature")
	}
	// Verify the SnapshotStore was NOT updated with invalid snapshot data.
	_, _, _, ready := store.Get()
	if ready {
		t.Error("SnapshotStore was updated with an invalid (tampered) snapshot; want not ready")
	}
}

// nestedMap is a test helper that traverses nested maps in an unstructured object.
func nestedMap(obj map[string]interface{}, fields ...string) (map[string]interface{}, bool, error) {
	cur := obj
	for _, f := range fields {
		v, ok := cur[f]
		if !ok {
			return nil, false, nil
		}
		m, ok := v.(map[string]interface{})
		if !ok {
			return nil, false, nil
		}
		cur = m
	}
	return cur, true, nil
}
