package agent_test

// Signing integration tests for the ReceiptReconciler Ed25519 key enforcement.
// Tests validate: bootstrap window mode, key-enforced mode, valid/invalid signatures.
// INV-026, conductor-schema.md §10.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/internal/agent"
)

// genEd25519KeyPair generates a fresh Ed25519 key pair for testing.
func genEd25519KeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 key pair: %v", err)
	}
	return pub, priv
}

// writePubKeyFile encodes pub as a PKIX PEM file and writes it to a temp file.
// Returns the file path; cleanup is registered with t.Cleanup.
func writePubKeyFile(t *testing.T, pub ed25519.PublicKey) string {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatalf("marshal PKIX public key: %v", err)
	}
	block := &pem.Block{Type: "PUBLIC KEY", Bytes: der}
	f, err := os.CreateTemp(t.TempDir(), "pubkey-*.pem")
	if err != nil {
		t.Fatalf("create temp public key file: %v", err)
	}
	if err := pem.Encode(f, block); err != nil {
		t.Fatalf("PEM encode public key: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close public key file: %v", err)
	}
	return f.Name()
}

// signSpec signs the JSON encoding of specObj with priv and returns the
// base64-encoded signature to place in the management-signature annotation.
func signSpec(t *testing.T, priv ed25519.PrivateKey, specObj map[string]interface{}) string {
	t.Helper()
	msg, err := json.Marshal(specObj)
	if err != nil {
		t.Fatalf("marshal spec for signing: %v", err)
	}
	sig := ed25519.Sign(priv, msg)
	return base64.StdEncoding.EncodeToString(sig)
}

// makeReceipt returns an unstructured PackReceipt-like CR with the given spec
// and an optional signature annotation. sigAnnotation may be "" for unsigned.
func makeReceipt(name string, specObj map[string]interface{}, sigAnnotation string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(packReceiptGVR.GroupVersion().WithKind("Packreceipt"))
	obj.SetName(name)
	obj.SetNamespace("ont-system")
	if sigAnnotation != "" {
		obj.SetAnnotations(map[string]string{
			"runner.ontai.dev/management-signature": sigAnnotation,
		})
	}
	if err := unstructured.SetNestedMap(obj.Object, specObj, "spec"); err != nil {
		panic("makeReceipt: set spec: " + err.Error())
	}
	return obj
}

// newFakeDynamicClientWithReceiptsAndObjects creates a fake dynamic client
// pre-populated with the given receipt objects.
func newFakeDynamicClientWithReceiptsAndObjects(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	for _, gvr := range []schema.GroupVersionResource{packReceiptGVR, permissionSnapshotReceiptGVR} {
		gvk := schema.GroupVersionKind{
			Group:   gvr.Group,
			Version: gvr.Version,
			Kind:    capitalize(gvr.Resource[:len(gvr.Resource)-1]),
		}
		listGVK := schema.GroupVersionKind{
			Group:   gvr.Group,
			Version: gvr.Version,
			Kind:    gvk.Kind + "List",
		}
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	return dynamicfake.NewSimpleDynamicClient(scheme, objs...)
}

// ── Bootstrap window mode ─────────────────────────────────────────────────────

// TestReceiptReconciler_BootstrapWindow_AcceptsUnsigned verifies that in
// bootstrap window mode (no key), unsigned receipts are accepted.
func TestReceiptReconciler_BootstrapWindow_AcceptsUnsigned(t *testing.T) {
	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	receipt := makeReceipt("pack-receipt-1", spec, "") // no signature
	fakeClient := newFakeDynamicClientWithReceiptsAndObjects(receipt)

	rec := agent.NewReceiptReconciler(fakeClient, "ont-system")
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("bootstrap window unsigned: unexpected error: %v", err)
	}
}

// TestReceiptReconciler_BootstrapWindow_AcceptsSignedWithoutVerification verifies
// that in bootstrap window mode (no key), any non-empty signature is accepted.
func TestReceiptReconciler_BootstrapWindow_AcceptsSignedWithoutVerification(t *testing.T) {
	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	receipt := makeReceipt("pack-receipt-2", spec, "dGhpcyBpcyBub3QgYSB2YWxpZCBzaWduYXR1cmU=")
	fakeClient := newFakeDynamicClientWithReceiptsAndObjects(receipt)

	rec := agent.NewReceiptReconciler(fakeClient, "ont-system")
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("bootstrap window with signature: unexpected error: %v", err)
	}
}

// ── Key-enforced mode ─────────────────────────────────────────────────────────

// TestNewReceiptReconcilerWithKey_ValidKey verifies that a valid PEM Ed25519
// public key file produces a non-nil reconciler without error.
func TestNewReceiptReconcilerWithKey_ValidKey(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	rec, err := agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", keyPath)
	if err != nil {
		t.Fatalf("NewReceiptReconcilerWithKey: unexpected error: %v", err)
	}
	if rec == nil {
		t.Error("expected non-nil ReceiptReconciler")
	}
}

// TestNewReceiptReconcilerWithKey_MissingFile verifies that a missing key file
// returns an error.
func TestNewReceiptReconcilerWithKey_MissingFile(t *testing.T) {
	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	_, err := agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", "/nonexistent/key.pem")
	if err == nil {
		t.Fatal("expected error for missing key file; got nil")
	}
}

// TestNewReceiptReconcilerWithKey_InvalidPEM verifies that a file without a
// valid PEM block returns an error.
func TestNewReceiptReconcilerWithKey_InvalidPEM(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "badkey-*.pem")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.WriteString("this is not a PEM block\n") //nolint:errcheck
	f.Close()                                  //nolint:errcheck

	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	_, err = agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", f.Name())
	if err == nil {
		t.Fatal("expected error for invalid PEM; got nil")
	}
}

// TestReceiptReconciler_KeyEnforced_ValidSignature verifies that a receipt
// with a correct Ed25519 signature is accepted and acknowledged.
func TestReceiptReconciler_KeyEnforced_ValidSignature(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	spec := map[string]interface{}{"clusterRef": "ccs-dev", "packRef": "base-pack"}
	sig := signSpec(t, priv, spec)
	receipt := makeReceipt("pack-receipt-valid", spec, sig)

	fakeClient := newFakeDynamicClientWithReceiptsAndObjects(receipt)
	rec, err := agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", keyPath)
	if err != nil {
		t.Fatalf("NewReceiptReconcilerWithKey: %v", err)
	}

	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile with valid signature: unexpected error: %v", err)
	}

	// Verify the receipt was acknowledged in the fake store.
	got, err := fakeClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "pack-receipt-valid", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get receipt after reconcile: %v", err)
	}
	acked, _, _ := unstructured.NestedBool(got.Object, "status", "acknowledged")
	if !acked {
		t.Error("expected receipt to be acknowledged after valid signature; got false")
	}
}

// TestReceiptReconciler_KeyEnforced_InvalidSignature verifies that a receipt
// with a corrupted signature is NOT acknowledged (DegradedSecurityState set).
func TestReceiptReconciler_KeyEnforced_InvalidSignature(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	// Provide a syntactically valid base64 string that is not a valid signature.
	badSig := base64.StdEncoding.EncodeToString(make([]byte, 64))
	receipt := makeReceipt("pack-receipt-bad-sig", spec, badSig)

	fakeClient := newFakeDynamicClientWithReceiptsAndObjects(receipt)
	rec, err := agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", keyPath)
	if err != nil {
		t.Fatalf("NewReceiptReconcilerWithKey: %v", err)
	}

	// Reconcile must return nil (signature failures do not bubble up as errors).
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile with invalid signature: expected nil error; got %v", err)
	}

	// Receipt must NOT be acknowledged.
	got, err := fakeClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "pack-receipt-bad-sig", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get receipt after reconcile: %v", err)
	}
	acked, _, _ := unstructured.NestedBool(got.Object, "status", "acknowledged")
	if acked {
		t.Error("expected receipt NOT to be acknowledged after invalid signature; got acknowledged=true")
	}
}

// TestReceiptReconciler_KeyEnforced_MissingSignature verifies that a receipt
// with no signature annotation is NOT acknowledged when a key is mounted.
func TestReceiptReconciler_KeyEnforced_MissingSignature(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	receipt := makeReceipt("pack-receipt-no-sig", spec, "") // no signature

	fakeClient := newFakeDynamicClientWithReceiptsAndObjects(receipt)
	rec, err := agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", keyPath)
	if err != nil {
		t.Fatalf("NewReceiptReconcilerWithKey: %v", err)
	}

	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile with missing signature: expected nil error; got %v", err)
	}

	// Receipt must NOT be acknowledged.
	got, err := fakeClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "pack-receipt-no-sig", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get receipt after reconcile: %v", err)
	}
	acked, _, _ := unstructured.NestedBool(got.Object, "status", "acknowledged")
	if acked {
		t.Error("expected receipt NOT to be acknowledged when signature absent and key is mounted; got acknowledged=true")
	}
}

// TestReceiptReconciler_KeyEnforced_AlreadyAcknowledgedSkipped verifies that
// already-acknowledged receipts are not reprocessed.
func TestReceiptReconciler_KeyEnforced_AlreadyAcknowledgedSkipped(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	receipt := makeReceipt("pack-receipt-already-acked", spec, "")

	// Pre-set acknowledged=true in status.
	if err := unstructured.SetNestedField(receipt.Object, true, "status", "acknowledged"); err != nil {
		t.Fatalf("set acknowledged=true: %v", err)
	}

	fakeClient := newFakeDynamicClientWithReceiptsAndObjects(receipt)
	rec, err := agent.NewReceiptReconcilerWithKey(fakeClient, "ont-system", keyPath)
	if err != nil {
		t.Fatalf("NewReceiptReconcilerWithKey: %v", err)
	}

	// Should not attempt to verify or patch — no error.
	if err := rec.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile with already-acknowledged receipt: unexpected error: %v", err)
	}
}
