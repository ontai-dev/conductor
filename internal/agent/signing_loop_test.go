package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

func setupSigningLoopScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "security.ontai.dev", Version: "v1alpha1", Kind: "PermissionSnapshot",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "security.ontai.dev", Version: "v1alpha1", Kind: "PermissionSnapshotList",
	}, &unstructured.UnstructuredList{})
	return s
}

// fakePermissionSnapshot builds a fake PermissionSnapshot with the given
// annotations. Pass nil for no annotations (unsigned snapshot).
func fakePermissionSnapshot(name, ns string, annotations map[string]string) *unstructured.Unstructured {
	meta := map[string]interface{}{
		"name":            name,
		"namespace":       ns,
		"resourceVersion": "1",
	}
	if len(annotations) > 0 {
		annMap := make(map[string]interface{}, len(annotations))
		for k, v := range annotations {
			annMap[k] = v
		}
		meta["annotations"] = annMap
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "PermissionSnapshot",
			"metadata":   meta,
			"spec": map[string]interface{}{
				"targetCluster": "ccs-dev",
				"version":       "v1",
			},
		},
	}
}

// snapshotSpec returns the spec that fakePermissionSnapshot uses, for computing
// the expected signature and hash in tests.
func snapshotSpec() map[string]interface{} {
	return map[string]interface{}{
		"targetCluster": "ccs-dev",
		"version":       "v1",
	}
}

// TestSpecContentHash_Deterministic verifies that specContentHash produces the
// same output on repeated calls for the same input.
func TestSpecContentHash_Deterministic(t *testing.T) {
	spec := map[string]interface{}{"a": "1", "b": "2"}
	h1, err1 := specContentHash(spec)
	h2, err2 := specContentHash(spec)
	if err1 != nil || err2 != nil {
		t.Fatalf("unexpected error: %v / %v", err1, err2)
	}
	if h1 == "" {
		t.Fatal("hash is empty")
	}
	if h1 != h2 {
		t.Errorf("hash not deterministic: %q != %q", h1, h2)
	}
}

// TestSignPermissionSnapshots_InitialSign verifies that an unsigned snapshot
// receives both the signature and spec-hash annotations on the first signing cycle.
func TestSignPermissionSnapshots_InitialSign(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)

	snapshot := fakePermissionSnapshot("snapshot-ccs-dev", "seam-tenant-ccs-dev", nil)
	scheme := setupSigningLoopScheme()
	client := fake.NewSimpleDynamicClient(scheme, snapshot)
	loop := &SigningLoop{client: client, privKey: priv}

	loop.signPermissionSnapshots(context.Background())

	got, err := client.Resource(permissionSnapshotGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "snapshot-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	anns := got.GetAnnotations()
	sigB64 := anns[managementSignatureAnnotation]
	hashVal := anns[managementSpecHashAnnotation]

	if sigB64 == "" {
		t.Fatal("managementSignatureAnnotation not set after initial sign")
	}
	if hashVal == "" {
		t.Fatal("managementSpecHashAnnotation not set after initial sign")
	}

	// Verify the signature is valid for the spec.
	message, _ := json.Marshal(snapshotSpec())
	sigBytes, _ := base64.StdEncoding.DecodeString(sigB64)
	if !ed25519.Verify(pub, message, sigBytes) {
		t.Error("signature is not valid for snapshot spec after initial sign")
	}

	// Verify the stored hash matches the spec content.
	sum := sha256.Sum256(message)
	wantHash := hex.EncodeToString(sum[:])
	if hashVal != wantHash {
		t.Errorf("spec hash after initial sign: got %q, want %q", hashVal, wantHash)
	}
}

// TestSignPermissionSnapshots_SpecChanged_Resigns verifies that a snapshot whose
// stored spec hash no longer matches the current spec (Guardian updated spec after
// the initial sign) is re-signed on the next signing cycle.
func TestSignPermissionSnapshots_SpecChanged_Resigns(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)

	// Snapshot carries a stale sig and hash from a previous spec version.
	snapshot := fakePermissionSnapshot("snapshot-ccs-dev", "seam-tenant-ccs-dev", map[string]string{
		managementSignatureAnnotation: "stale-sig",
		managementSpecHashAnnotation:  "stale-hash",
	})

	scheme := setupSigningLoopScheme()
	client := fake.NewSimpleDynamicClient(scheme, snapshot)
	loop := &SigningLoop{client: client, privKey: priv}

	loop.signPermissionSnapshots(context.Background())

	got, err := client.Resource(permissionSnapshotGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "snapshot-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	anns := got.GetAnnotations()
	sigB64 := anns[managementSignatureAnnotation]
	hashVal := anns[managementSpecHashAnnotation]

	if sigB64 == "stale-sig" {
		t.Error("signature was not updated despite stale spec hash")
	}
	if hashVal == "stale-hash" {
		t.Error("spec hash was not updated despite stale hash mismatch")
	}

	// The new signature must be valid for the current spec.
	message, _ := json.Marshal(snapshotSpec())
	sigBytes, _ := base64.StdEncoding.DecodeString(sigB64)
	if !ed25519.Verify(pub, message, sigBytes) {
		t.Error("new signature is not valid for snapshot spec after re-sign")
	}

	sum := sha256.Sum256(message)
	wantHash := hex.EncodeToString(sum[:])
	if hashVal != wantHash {
		t.Errorf("spec hash after re-sign: got %q, want %q", hashVal, wantHash)
	}
}

// TestSignPermissionSnapshots_HashMatch_NoResign verifies that a snapshot whose
// stored hash matches the current spec is not re-signed -- the annotation value
// remains equal to the pre-computed valid signature.
func TestSignPermissionSnapshots_HashMatch_NoResign(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(rand.Reader)

	// Pre-compute the correct sig and hash for the spec fakePermissionSnapshot uses.
	message, _ := json.Marshal(snapshotSpec())
	sig := ed25519.Sign(priv, message)
	sigB64 := base64.StdEncoding.EncodeToString(sig)
	sum := sha256.Sum256(message)
	hashVal := hex.EncodeToString(sum[:])

	snapshot := fakePermissionSnapshot("snapshot-ccs-dev", "seam-tenant-ccs-dev", map[string]string{
		managementSignatureAnnotation: sigB64,
		managementSpecHashAnnotation:  hashVal,
	})

	scheme := setupSigningLoopScheme()
	client := fake.NewSimpleDynamicClient(scheme, snapshot)
	loop := &SigningLoop{client: client, privKey: priv}

	loop.signPermissionSnapshots(context.Background())

	got, err := client.Resource(permissionSnapshotGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "snapshot-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}

	gotSig := got.GetAnnotations()[managementSignatureAnnotation]
	if gotSig != sigB64 {
		t.Errorf("signature changed despite hash match: got %q, want %q", gotSig, sigB64)
	}
	gotHash := got.GetAnnotations()[managementSpecHashAnnotation]
	if gotHash != hashVal {
		t.Errorf("hash changed despite hash match: got %q, want %q", gotHash, hashVal)
	}
}
