package agent_test

// Unit tests for SigningLoop — management cluster signing of PackInstance and
// PermissionSnapshot CRs. INV-026, conductor-schema.md §10 steps 9–10.

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

// packInstanceGVR mirrors the GVR defined in signing_loop.go.
var packInstanceGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "packinstances",
}

// psGVR mirrors the permissionSnapshotGVR in signing_loop.go.
var psGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshots",
}

// clusterPackGVR mirrors the GVR defined in signing_loop.go.
var clusterPackGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "clusterpacks",
}

// secretGVR mirrors the secretGVR defined in signing_loop.go (core v1 Secrets).
var secretGVR = schema.GroupVersionResource{
	Group:    "",
	Version:  "v1",
	Resource: "secrets",
}

// writePrivKeyFile writes an Ed25519 private key in PKCS#8 PEM format to a temp file.
func writePrivKeyFile(t *testing.T, priv ed25519.PrivateKey) string {
	t.Helper()
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal PKCS8 private key: %v", err)
	}
	block := &pem.Block{Type: "PRIVATE KEY", Bytes: der}
	f, err := os.CreateTemp(t.TempDir(), "privkey-*.pem")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	if err := pem.Encode(f, block); err != nil {
		t.Fatalf("PEM encode private key: %v", err)
	}
	f.Close() //nolint:errcheck
	return f.Name()
}

// genKeyPair generates a fresh Ed25519 key pair.
func genKeyPair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key pair: %v", err)
	}
	return pub, priv
}

// newFakeDynamicClientWithGVRs creates a fake dynamic client with the given GVRs registered.
func newFakeDynamicClientWithGVRs(gvrs []schema.GroupVersionResource, objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	scheme := runtime.NewScheme()
	for _, gvr := range gvrs {
		gvk := schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: capitalize(singular(gvr.Resource))}
		listGVK := schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: gvk.Kind + "List"}
		scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		scheme.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
	return dynamicfake.NewSimpleDynamicClient(scheme, objs...)
}

// singular removes the trailing 's' from a plural resource name.
func singular(resource string) string {
	if len(resource) > 0 && resource[len(resource)-1] == 's' {
		return resource[:len(resource)-1]
	}
	return resource
}

// makeCR builds a test unstructured CR with the given spec.
func makeCR(gvr schema.GroupVersionResource, name, namespace string, specObj map[string]interface{}) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvr.GroupVersion().WithKind(capitalize(singular(gvr.Resource))))
	obj.SetName(name)
	obj.SetNamespace(namespace)
	if err := unstructured.SetNestedMap(obj.Object, specObj, "spec"); err != nil {
		panic("makeCR: set spec: " + err.Error())
	}
	return obj
}

// ── constructor tests ─────────────────────────────────────────────────────────

// TestNewSigningLoop_ValidKey verifies that a valid private key file constructs
// a SigningLoop without error.
func TestNewSigningLoop_ValidKey(t *testing.T) {
	_, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: unexpected error: %v", err)
	}
	if loop == nil {
		t.Fatal("expected non-nil SigningLoop")
	}
}

// TestNewSigningLoop_MissingFile verifies that a missing private key file returns an error.
func TestNewSigningLoop_MissingFile(t *testing.T) {
	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	_, err := agent.NewSigningLoop(fakeClient, "/nonexistent/privkey.pem")
	if err == nil {
		t.Fatal("expected error for missing private key file; got nil")
	}
}

// TestNewSigningLoop_InvalidPEM verifies that a non-PEM file returns an error.
func TestNewSigningLoop_InvalidPEM(t *testing.T) {
	f, _ := os.CreateTemp(t.TempDir(), "bad-*.pem")
	f.WriteString("not a pem block") //nolint:errcheck
	f.Close()                        //nolint:errcheck

	fakeClient := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())
	_, err := agent.NewSigningLoop(fakeClient, f.Name())
	if err == nil {
		t.Fatal("expected error for invalid PEM; got nil")
	}
}

// ── signing behaviour tests ───────────────────────────────────────────────────

// TestSigningLoop_SignsUnsignedPackInstance verifies that after one signAll cycle,
// a PackInstance without a signature annotation is signed.
func TestSigningLoop_SignsUnsignedPackInstance(t *testing.T) {
	pub, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{"clusterRef": "ccs-dev", "packRef": "base-pack-v1"}
	cr := makeCR(packInstanceGVR, "pack-instance-1", "ont-system", spec)

	gvrs := []schema.GroupVersionResource{packInstanceGVR, psGVR, clusterPackGVR}
	fakeClient := newFakeDynamicClientWithGVRs(gvrs, cr)

	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	// Run one sign cycle via a cancelled context (triggers immediate signAll then stops).
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancelled immediately so Run exits after the first signAll
	loop.Run(ctx, 0)

	// Verify the signature annotation was written.
	got, err := fakeClient.Resource(packInstanceGVR).Namespace("ont-system").Get(
		context.Background(), "pack-instance-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after sign: %v", err)
	}

	annotations := got.GetAnnotations()
	sigB64, ok := annotations["runner.ontai.dev/management-signature"]
	if !ok || sigB64 == "" {
		t.Fatal("expected management-signature annotation to be set after signing")
	}

	// Verify the signature is cryptographically valid.
	sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		t.Fatalf("decode signature base64: %v", err)
	}
	specJSON, _ := json.Marshal(spec)
	if !ed25519.Verify(pub, specJSON, sigBytes) {
		t.Error("signature verification failed — signed message does not match spec JSON")
	}
}

// TestSigningLoop_SignsUnsignedPermissionSnapshot verifies that after one sign
// cycle, a PermissionSnapshot without a signature annotation is signed.
func TestSigningLoop_SignsUnsignedPermissionSnapshot(t *testing.T) {
	pub, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{
		"targetCluster": "ccs-dev",
		"version":       "2026-04-04T12:00:00Z",
	}
	cr := makeCR(psGVR, "permission-snapshot-1", "tenant-ccs-dev", spec)

	gvrs := []schema.GroupVersionResource{packInstanceGVR, psGVR, clusterPackGVR}
	fakeClient := newFakeDynamicClientWithGVRs(gvrs, cr)

	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	got, err := fakeClient.Resource(psGVR).Namespace("tenant-ccs-dev").Get(
		context.Background(), "permission-snapshot-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get after sign: %v", err)
	}

	annotations := got.GetAnnotations()
	sigB64, ok := annotations["runner.ontai.dev/management-signature"]
	if !ok || sigB64 == "" {
		t.Fatal("expected management-signature annotation to be set")
	}

	sigBytes, _ := base64.StdEncoding.DecodeString(sigB64)
	specJSON, _ := json.Marshal(spec)
	if !ed25519.Verify(pub, specJSON, sigBytes) {
		t.Error("PermissionSnapshot signature verification failed")
	}
}

// TestSigningLoop_SkipsAlreadySignedCRs verifies that a CR with an existing
// management-signature annotation is not re-signed.
func TestSigningLoop_SkipsAlreadySignedCRs(t *testing.T) {
	_, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	cr := makeCR(packInstanceGVR, "pack-signed", "ont-system", spec)
	// Pre-set a fixed (fake) signature annotation.
	cr.SetAnnotations(map[string]string{
		"runner.ontai.dev/management-signature": "ZmFrZXNpZ25hdHVyZQ==",
	})

	gvrs := []schema.GroupVersionResource{packInstanceGVR, psGVR, clusterPackGVR}
	fakeClient := newFakeDynamicClientWithGVRs(gvrs, cr)

	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// Annotation must still be the original fake value (not overwritten).
	got, _ := fakeClient.Resource(packInstanceGVR).Namespace("ont-system").Get(
		context.Background(), "pack-signed", metav1.GetOptions{})
	if sig := got.GetAnnotations()["runner.ontai.dev/management-signature"]; sig != "ZmFrZXNpZ25hdHVyZQ==" {
		t.Errorf("already-signed CR must not be re-signed; got %q", sig)
	}
}

// TestSigningLoop_EmptyStoreNoPanic verifies that signAll completes without
// panic or error when no CRs exist in the fake store.
func TestSigningLoop_EmptyStoreNoPanic(t *testing.T) {
	_, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	gvrs := []schema.GroupVersionResource{packInstanceGVR, psGVR, clusterPackGVR}
	fakeClient := newFakeDynamicClientWithGVRs(gvrs)

	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0) // must not panic
}

// ── Secret storage tests (Gap 28) ────────────────────────────────────────────

// makeSignedSecretObj builds a test Secret object with the given artifact and
// signature data fields.
func makeSignedSecretObj(name, ns, artifactB64, sigB64 string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]interface{}{
				"name":      name,
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

// allSigningLoopGVRs returns a GVR list that includes PackInstance,
// PermissionSnapshot, ClusterPack, and core Secret so all signing loop
// operations succeed in tests without panicking on unregistered List calls.
func allSigningLoopGVRs() []schema.GroupVersionResource {
	return []schema.GroupVersionResource{packInstanceGVR, psGVR, clusterPackGVR, secretGVR}
}

// TestSigningLoop_StoresNewSecretForPackInstance verifies that after the sign
// cycle completes for an unsigned PackInstance, a Secret encoding the signed
// artifact is created in seam-tenant-{clusterName}. Gap 28.
func TestSigningLoop_StoresNewSecretForPackInstance(t *testing.T) {
	pub, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{"clusterRef": "ccs-dev", "packRef": "base-pack-v1"}
	cr := makeCR(packInstanceGVR, "my-pack", "seam-tenant-ccs-dev", spec)

	fakeClient := newFakeDynamicClientWithGVRs(allSigningLoopGVRs(), cr)
	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// The Secret must be created in seam-tenant-ccs-dev.
	secretName := "seam-pack-signed-ccs-dev-my-pack"
	secret, err := fakeClient.Resource(secretGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), secretName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("Secret not created after signing cycle: %v", err)
	}

	// Verify the signature field matches the PackInstance annotation signature.
	pi, _ := fakeClient.Resource(packInstanceGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "my-pack", metav1.GetOptions{},
	)
	piSig := pi.GetAnnotations()["runner.ontai.dev/management-signature"]

	data := secret.Object["data"].(map[string]interface{})
	secretSig, _ := data["signature"].(string)
	if secretSig != piSig {
		t.Errorf("Secret signature %q does not match PackInstance annotation %q", secretSig, piSig)
	}

	// Verify the artifact field is the base64-encoded JSON of the spec.
	artifactB64, _ := data["artifact"].(string)
	if artifactB64 == "" {
		t.Error("Secret artifact field must not be empty")
	}
	// Confirm the embedded signature is cryptographically valid.
	sigBytes, _ := base64.StdEncoding.DecodeString(secretSig)
	specJSON, _ := json.Marshal(spec)
	if !ed25519.Verify(pub, specJSON, sigBytes) {
		t.Error("signature stored in Secret does not verify against spec JSON")
	}
}

// TestSigningLoop_IdempotentSkipWhenSignatureMatches verifies that when the
// Secret already exists and its signature field matches the PackInstance
// annotation, the signing cycle does not overwrite the Secret. Gap 28.
func TestSigningLoop_IdempotentSkipWhenSignatureMatches(t *testing.T) {
	_, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	cr := makeCR(packInstanceGVR, "existing-pack", "seam-tenant-ccs-dev", spec)
	// Pre-set a stable fake signature annotation on the PackInstance.
	cr.SetAnnotations(map[string]string{
		"runner.ontai.dev/management-signature": "stableSig==",
	})

	fakeClient := newFakeDynamicClientWithGVRs(allSigningLoopGVRs(), cr)

	// Pre-create the Secret with the same signature and a sentinel artifact value.
	sentinelArtifact := "c2VudGluZWwtYXJ0aWZhY3Q=" // base64 of "sentinel-artifact"
	secretObj := makeSignedSecretObj(
		"seam-pack-signed-ccs-dev-existing-pack", "seam-tenant-ccs-dev",
		sentinelArtifact, "stableSig==",
	)
	if _, err := fakeClient.Resource(secretGVR).Namespace("seam-tenant-ccs-dev").Create(
		context.Background(), secretObj, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-create Secret: %v", err)
	}

	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// Verify the Secret was NOT overwritten — sentinel artifact value is preserved.
	got, err := fakeClient.Resource(secretGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "seam-pack-signed-ccs-dev-existing-pack", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get Secret after idempotent cycle: %v", err)
	}
	data := got.Object["data"].(map[string]interface{})
	if data["artifact"] != sentinelArtifact {
		t.Errorf("idempotent cycle must not overwrite Secret artifact; got %q, want %q",
			data["artifact"], sentinelArtifact)
	}
}

// TestSigningLoop_OverwritesSecretOnSignatureMismatch verifies that when the
// Secret already exists but its signature field differs from the PackInstance
// annotation, the signing cycle overwrites the Secret. Gap 28.
func TestSigningLoop_OverwritesSecretOnSignatureMismatch(t *testing.T) {
	_, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{"clusterRef": "ccs-dev"}
	cr := makeCR(packInstanceGVR, "updated-pack", "seam-tenant-ccs-dev", spec)
	// PackInstance annotation carries the new (current) signature.
	cr.SetAnnotations(map[string]string{
		"runner.ontai.dev/management-signature": "newSig==",
	})

	fakeClient := newFakeDynamicClientWithGVRs(allSigningLoopGVRs(), cr)

	// Pre-create the Secret with an outdated signature.
	oldSecret := makeSignedSecretObj(
		"seam-pack-signed-ccs-dev-updated-pack", "seam-tenant-ccs-dev",
		"b2xkYXJ0aWZhY3Q=", "oldSig==",
	)
	if _, err := fakeClient.Resource(secretGVR).Namespace("seam-tenant-ccs-dev").Create(
		context.Background(), oldSecret, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-create Secret: %v", err)
	}

	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	// Verify the Secret was overwritten with the new signature.
	got, err := fakeClient.Resource(secretGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "seam-pack-signed-ccs-dev-updated-pack", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get Secret after mismatch cycle: %v", err)
	}
	data := got.Object["data"].(map[string]interface{})
	if data["signature"] != "newSig==" {
		t.Errorf("Secret signature after overwrite: got %q, want %q", data["signature"], "newSig==")
	}
}

// ── ClusterPack signing tests ─────────────────────────────────────────────────

// TestSigningLoop_SignsUnsignedClusterPack verifies that after one signAll cycle,
// an unsigned ClusterPack receives the "ontai.dev/pack-signature" annotation
// (not "runner.ontai.dev/management-signature"). The wrapper
// ClusterPackReconciler reads this specific annotation to transition
// Status.Signed=true and Available. conductor-schema.md §10 steps 9–10.
func TestSigningLoop_SignsUnsignedClusterPack(t *testing.T) {
	pub, priv := genKeyPair(t)
	privPath := writePrivKeyFile(t, priv)

	spec := map[string]interface{}{
		"packRef": "base-pack",
		"version": "v1.0.0",
	}
	cr := makeCR(clusterPackGVR, "base-pack-v1", "ont-system", spec)

	fakeClient := newFakeDynamicClientWithGVRs(allSigningLoopGVRs(), cr)
	loop, err := agent.NewSigningLoop(fakeClient, privPath)
	if err != nil {
		t.Fatalf("NewSigningLoop: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	got, err := fakeClient.Resource(clusterPackGVR).Namespace("ont-system").Get(
		context.Background(), "base-pack-v1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ClusterPack after sign cycle: %v", err)
	}

	annotations := got.GetAnnotations()
	// Wrapper reads "ontai.dev/pack-signature" — must use this key, not
	// "runner.ontai.dev/management-signature". wrapper-schema.md §3.
	sigB64, ok := annotations["ontai.dev/pack-signature"]
	if !ok || sigB64 == "" {
		t.Fatal("expected ontai.dev/pack-signature annotation to be set on ClusterPack after signing")
	}
	if _, wrongKey := annotations["runner.ontai.dev/management-signature"]; wrongKey {
		t.Error("ClusterPack must not carry runner.ontai.dev/management-signature; wrapper reads ontai.dev/pack-signature")
	}

	// Verify the signature is cryptographically valid.
	sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		t.Fatalf("decode ClusterPack signature base64: %v", err)
	}
	specJSON, _ := json.Marshal(spec)
	if !ed25519.Verify(pub, specJSON, sigBytes) {
		t.Error("ClusterPack signature verification failed — signed message does not match spec JSON")
	}
}
