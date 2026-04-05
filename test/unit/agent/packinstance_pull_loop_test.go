package agent_test

// Unit tests for PackInstancePullLoop — tenant cluster Conductor pulls signed
// PackInstance artifact Secrets from the management cluster, verifies Ed25519
// signatures (INV-026), and creates or updates local PackReceipt CRs.
// Gap 28, conductor-schema.md §10.

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ontai-dev/conductor/internal/agent"
)

// mgmtSecretGVR mirrors secretGVR from signing_loop.go (core v1 Secrets).
var mgmtSecretGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "secrets",
}

// makePackArtifactSecret builds a management cluster Secret encoding a signed
// PackInstance artifact. artifactB64 is base64(raw JSON); sigB64 is the
// base64-encoded Ed25519 signature of those same JSON bytes.
func makePackArtifactSecret(secretName, ns, artifactB64, sigB64 string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]interface{}{
				"name":      secretName,
				"namespace": ns,
			},
			"type": "Opaque",
			"data": map[string]interface{}{
				"artifact":  artifactB64,
				"signature": sigB64,
			},
		},
	}
}

// makeExistingPackReceipt builds a pre-existing PackReceipt CR for use in
// idempotency tests.
func makeExistingPackReceipt(name, ns string, verified bool, sigB64 string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infra.ontai.dev/v1alpha1",
			"kind":       "PackReceipt",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ns,
			},
			"spec": map[string]interface{}{
				"packInstanceRef": name,
			},
			"status": map[string]interface{}{
				"verified":  verified,
				"signature": sigB64,
			},
		},
	}
}

// newMgmtSecretClient returns a fake dynamic client with the Secret GVR registered.
func newMgmtSecretClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{mgmtSecretGVR}, objs...)
}

// newLocalReceiptClient returns a fake dynamic client with the PackReceipt GVR registered.
func newLocalReceiptClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{packReceiptGVR}, objs...)
}

// signBytes signs raw bytes with an Ed25519 private key and returns base64(sig).
func signBytes(t *testing.T, priv ed25519.PrivateKey, message []byte) string {
	t.Helper()
	sig := ed25519.Sign(priv, message)
	return base64.StdEncoding.EncodeToString(sig)
}

// getPackReceiptStatus returns the status map from a PackReceipt unstructured object.
func getPackReceiptStatus(t *testing.T, obj *unstructured.Unstructured) map[string]interface{} {
	t.Helper()
	status, ok := obj.Object["status"].(map[string]interface{})
	if !ok {
		t.Fatal("PackReceipt has no status map")
	}
	return status
}

// ── constructor tests ─────────────────────────────────────────────────────────

// TestPackInstancePullLoop_ConstructsWithoutKey verifies that NewPackInstancePullLoop
// returns a non-nil loop in bootstrap window mode (no key). INV-020.
func TestPackInstancePullLoop_ConstructsWithoutKey(t *testing.T) {
	loop := agent.NewPackInstancePullLoop(
		newMgmtSecretClient(), newLocalReceiptClient(),
		"ccs-dev", "ont-system",
	)
	if loop == nil {
		t.Fatal("expected non-nil PackInstancePullLoop")
	}
}

// TestPackInstancePullLoop_ConstructsWithValidKey verifies that
// NewPackInstancePullLoopWithKey accepts a valid PKIX PEM Ed25519 public key.
func TestPackInstancePullLoop_ConstructsWithValidKey(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	loop, err := agent.NewPackInstancePullLoopWithKey(
		newMgmtSecretClient(), newLocalReceiptClient(),
		"ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: unexpected error: %v", err)
	}
	if loop == nil {
		t.Fatal("expected non-nil PackInstancePullLoop")
	}
}

// TestPackInstancePullLoop_ConstructsWithMissingKeyFile verifies that a missing
// key file returns an error.
func TestPackInstancePullLoop_ConstructsWithMissingKeyFile(t *testing.T) {
	_, err := agent.NewPackInstancePullLoopWithKey(
		newMgmtSecretClient(), newLocalReceiptClient(),
		"ccs-dev", "ont-system", "/nonexistent/key.pem",
	)
	if err == nil {
		t.Fatal("expected error for missing key file; got nil")
	}
}

// TestPackInstancePullLoop_ConstructsWithInvalidPEM verifies that a non-PEM
// key file returns an error.
func TestPackInstancePullLoop_ConstructsWithInvalidPEM(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "badkey-*.pem")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.WriteString("not a PEM block\n") //nolint:errcheck
	f.Close()                          //nolint:errcheck

	_, err = agent.NewPackInstancePullLoopWithKey(
		newMgmtSecretClient(), newLocalReceiptClient(),
		"ccs-dev", "ont-system", f.Name(),
	)
	if err == nil {
		t.Fatal("expected error for invalid PEM; got nil")
	}
}

// ── verification success ──────────────────────────────────────────────────────

// TestPackInstancePullLoop_ValidSignature_CreatesPackReceipt verifies that a
// Secret with a valid Ed25519 signature produces a PackReceipt with
// status.verified=true. INV-026, Gap 28.
func TestPackInstancePullLoop_ValidSignature_CreatesPackReceipt(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	// Build artifact: raw JSON bytes of a PackInstance spec.
	specJSON := []byte(`{"clusterRef":"ccs-dev","packRef":"base-pack-v1"}`)
	artifactB64 := base64.StdEncoding.EncodeToString(specJSON)
	sigB64 := signBytes(t, priv, specJSON)

	secretObj := makePackArtifactSecret(
		"seam-pack-signed-ccs-dev-my-pack", "seam-tenant-ccs-dev",
		artifactB64, sigB64,
	)

	mgmtClient := newMgmtSecretClient(secretObj)
	localClient := newLocalReceiptClient()

	loop, err := agent.NewPackInstancePullLoopWithKey(
		mgmtClient, localClient, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// PackReceipt must be created with verified=true.
	got, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "my-pack", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("PackReceipt not created after valid signature: %v", err)
	}
	status := getPackReceiptStatus(t, got)
	if verified, _ := status["verified"].(bool); !verified {
		t.Error("expected status.verified=true after valid signature")
	}
	if sig, _ := status["signature"].(string); sig != sigB64 {
		t.Errorf("expected status.signature=%q; got %q", sigB64, sig)
	}
}

// ── verification failure ──────────────────────────────────────────────────────

// TestPackInstancePullLoop_InvalidSignature_PackReceiptVerifiedFalse verifies
// that a Secret with a corrupted signature produces a PackReceipt with
// status.verified=false and a non-empty verificationFailedReason. INV-026.
func TestPackInstancePullLoop_InvalidSignature_PackReceiptVerifiedFalse(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	specJSON := []byte(`{"clusterRef":"ccs-dev","packRef":"base-pack-v1"}`)
	artifactB64 := base64.StdEncoding.EncodeToString(specJSON)
	// Wrong signature: syntactically valid base64 but not a valid Ed25519 sig.
	badSigB64 := base64.StdEncoding.EncodeToString(make([]byte, 64))

	secretObj := makePackArtifactSecret(
		"seam-pack-signed-ccs-dev-bad-pack", "seam-tenant-ccs-dev",
		artifactB64, badSigB64,
	)

	mgmtClient := newMgmtSecretClient(secretObj)
	localClient := newLocalReceiptClient()

	loop, err := agent.NewPackInstancePullLoopWithKey(
		mgmtClient, localClient, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// PackReceipt must be created with verified=false and a failure reason.
	got, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "bad-pack", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("PackReceipt not created after invalid signature: %v", err)
	}
	status := getPackReceiptStatus(t, got)
	if verified, _ := status["verified"].(bool); verified {
		t.Error("expected status.verified=false after invalid signature")
	}
	if reason, _ := status["verificationFailedReason"].(string); reason == "" {
		t.Error("expected non-empty verificationFailedReason after signature failure")
	}
}

// ── bootstrap window ──────────────────────────────────────────────────────────

// TestPackInstancePullLoop_BootstrapWindow_CreatesVerifiedReceipt verifies that
// in bootstrap window mode (pubKey=nil), an artifact with any signature value
// is accepted and the PackReceipt is created with verified=true. INV-020.
func TestPackInstancePullLoop_BootstrapWindow_CreatesVerifiedReceipt(t *testing.T) {
	specJSON := []byte(`{"clusterRef":"ccs-dev","packRef":"base-pack-v1"}`)
	artifactB64 := base64.StdEncoding.EncodeToString(specJSON)
	// Deliberately wrong signature — bootstrap window must accept it.
	wrongSigB64 := base64.StdEncoding.EncodeToString(make([]byte, 64))

	secretObj := makePackArtifactSecret(
		"seam-pack-signed-ccs-dev-bootstrap-pack", "seam-tenant-ccs-dev",
		artifactB64, wrongSigB64,
	)

	mgmtClient := newMgmtSecretClient(secretObj)
	localClient := newLocalReceiptClient()

	// Bootstrap window mode: no key provided. INV-020.
	loop := agent.NewPackInstancePullLoop(
		mgmtClient, localClient, "ccs-dev", "ont-system",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	got, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "bootstrap-pack", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("PackReceipt not created in bootstrap window mode: %v", err)
	}
	status := getPackReceiptStatus(t, got)
	if verified, _ := status["verified"].(bool); !verified {
		t.Error("expected verified=true in bootstrap window mode regardless of signature")
	}
}

// ── idempotent update ─────────────────────────────────────────────────────────

// TestPackInstancePullLoop_IdempotentUpdate_NoUpdateWhenUnchanged verifies that
// a pull cycle that finds an existing PackReceipt with the same verified status
// and signature emits no update call. Gap 28.
func TestPackInstancePullLoop_IdempotentUpdate_NoUpdateWhenUnchanged(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	specJSON := []byte(`{"clusterRef":"ccs-dev","packRef":"base-pack-v1"}`)
	artifactB64 := base64.StdEncoding.EncodeToString(specJSON)
	sigB64 := signBytes(t, priv, specJSON)

	secretObj := makePackArtifactSecret(
		"seam-pack-signed-ccs-dev-idempotent-pack", "seam-tenant-ccs-dev",
		artifactB64, sigB64,
	)

	// Pre-create PackReceipt with the same verified=true and same sigB64.
	existingReceipt := makeExistingPackReceipt("idempotent-pack", "ont-system", true, sigB64)

	mgmtClient := newMgmtSecretClient(secretObj)
	localClient := newFakeDynamicClientWithGVRs(
		[]schema.GroupVersionResource{packReceiptGVR}, existingReceipt,
	)

	loop, err := agent.NewPackInstancePullLoopWithKey(
		mgmtClient, localClient, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// No update actions should be emitted when content is unchanged.
	updateCount := 0
	for _, action := range localClient.Actions() {
		if action.GetVerb() == "update" && action.GetResource() == packReceiptGVR {
			updateCount++
		}
	}
	if updateCount > 0 {
		t.Errorf("idempotent cycle must not issue PackReceipt updates; got %d update(s)", updateCount)
	}
}

// ── connectivity error ────────────────────────────────────────────────────────

// TestPackInstancePullLoop_ConnectivityError_NoPanic verifies that a List error
// from the management cluster is handled gracefully — the loop logs and returns
// without panicking. No PackReceipts are created. Gap 28.
func TestPackInstancePullLoop_ConnectivityError_NoPanic(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	// Inject a list error to simulate management cluster unreachability.
	mgmtClient := newMgmtSecretClient()
	mgmtClient.PrependReactor("list", "secrets", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated connectivity error")
	})

	localClient := newLocalReceiptClient()

	loop, err := agent.NewPackInstancePullLoopWithKey(
		mgmtClient, localClient, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewPackInstancePullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0) // must not panic

	// No PackReceipts should be created when management cluster is unreachable.
	createCount := 0
	for _, action := range localClient.Actions() {
		if action.GetVerb() == "create" {
			createCount++
		}
	}
	if createCount > 0 {
		t.Errorf("no PackReceipts must be created on connectivity error; got %d", createCount)
	}
}
