package agent_test

// Unit tests for SnapshotPullLoop — target cluster Conductor pulls
// PermissionSnapshots from management cluster, verifies Ed25519 signatures
// (INV-026), and populates the local SnapshotStore.
// conductor-schema.md §10 step 8, conductor-design.md §2.10.

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ontai-dev/conductor/internal/agent"
	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// allPullLoopGVRs lists the GVRs needed by the pull loop tests.
var allPullLoopGVRs = []schema.GroupVersionResource{
	psGVR,                      // PermissionSnapshot — on management cluster
	permissionSnapshotReceiptGVR, // PermissionSnapshotReceipt — on local cluster
}

// newMgmtClient returns a fake dynamic client with PermissionSnapshot GVR registered.
func newMgmtClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{psGVR}, objs...)
}

// newLocalClient returns a fake dynamic client with PermissionSnapshotReceipt GVR registered.
func newLocalClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs(
		[]schema.GroupVersionResource{permissionSnapshotReceiptGVR},
		objs...,
	)
}

// makeSnapshot builds a PermissionSnapshot CR.
// specObj must include "targetCluster". sigAnnotation may be "" for unsigned.
func makeSnapshot(name, ns, sigAnnotation string, specObj map[string]interface{}) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(psGVR.GroupVersion().WithKind(capitalize(singular(psGVR.Resource))))
	obj.SetName(name)
	obj.SetNamespace(ns)
	if sigAnnotation != "" {
		obj.SetAnnotations(map[string]string{"runner.ontai.dev/management-signature": sigAnnotation})
	}
	if err := unstructured.SetNestedMap(obj.Object, specObj, "spec"); err != nil {
		panic("makeSnapshot: set spec: " + err.Error())
	}
	return obj
}

// makeLocalSnapshotReceipt builds a minimal PermissionSnapshotReceipt CR for the
// local cluster, used as the target for DegradedSecurityState patches.
func makeLocalSnapshotReceipt(name string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(permissionSnapshotReceiptGVR.GroupVersion().WithKind(
		capitalize(singular(permissionSnapshotReceiptGVR.Resource)),
	))
	obj.SetName(name)
	obj.SetNamespace("ont-system")
	return obj
}

// ── constructor tests ─────────────────────────────────────────────────────────

// TestSnapshotPullLoop_ConstructsWithoutKey verifies that NewSnapshotPullLoop
// returns a non-nil loop in bootstrap window mode.
func TestSnapshotPullLoop_ConstructsWithoutKey(t *testing.T) {
	loop := agent.NewSnapshotPullLoop(
		newMgmtClient(), newLocalClient(),
		permissionservice.NewSnapshotStore(),
		"ccs-dev", "ont-system",
	)
	if loop == nil {
		t.Fatal("expected non-nil SnapshotPullLoop")
	}
}

// TestSnapshotPullLoop_ConstructsWithValidKey verifies that NewSnapshotPullLoopWithKey
// accepts a valid PKIX PEM Ed25519 public key file.
func TestSnapshotPullLoop_ConstructsWithValidKey(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	loop, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(), newLocalClient(),
		permissionservice.NewSnapshotStore(),
		"ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: unexpected error: %v", err)
	}
	if loop == nil {
		t.Fatal("expected non-nil SnapshotPullLoop")
	}
}

// TestSnapshotPullLoop_ConstructsWithMissingKeyFile verifies that a missing key
// file returns an error.
func TestSnapshotPullLoop_ConstructsWithMissingKeyFile(t *testing.T) {
	_, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(), newLocalClient(),
		permissionservice.NewSnapshotStore(),
		"ccs-dev", "ont-system", "/nonexistent/key.pem",
	)
	if err == nil {
		t.Fatal("expected error for missing key file; got nil")
	}
}

// TestSnapshotPullLoop_ConstructsWithInvalidPEM verifies that a non-PEM key file
// returns an error.
func TestSnapshotPullLoop_ConstructsWithInvalidPEM(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "badkey-*.pem")
	if err != nil {
		t.Fatalf("create temp file: %v", err)
	}
	f.WriteString("not a PEM block\n") //nolint:errcheck
	f.Close()                          //nolint:errcheck

	_, err = agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(), newLocalClient(),
		permissionservice.NewSnapshotStore(),
		"ccs-dev", "ont-system", f.Name(),
	)
	if err == nil {
		t.Fatal("expected error for invalid PEM; got nil")
	}
}

// ── bootstrap window mode ─────────────────────────────────────────────────────

// TestSnapshotPullLoop_BootstrapWindow_AcceptsUnsigned verifies that in bootstrap
// window mode (no key), an unsigned snapshot is accepted and populates the store.
func TestSnapshotPullLoop_BootstrapWindow_AcceptsUnsigned(t *testing.T) {
	spec := map[string]interface{}{"targetCluster": "ccs-dev", "version": "v1"}
	snapshot := makeSnapshot("snap-unsigned", "ont-system", "", spec)
	store := permissionservice.NewSnapshotStore()

	loop := agent.NewSnapshotPullLoop(
		newMgmtClient(snapshot), newLocalClient(), store, "ccs-dev", "ont-system",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	_, version, cluster, ready := store.Get()
	if !ready {
		t.Fatal("expected store ready after bootstrap window pull; not ready")
	}
	if cluster != "ccs-dev" {
		t.Errorf("cluster: expected ccs-dev, got %q", cluster)
	}
	if version != "v1" {
		t.Errorf("version: expected v1, got %q", version)
	}
}

// ── key-enforced mode ─────────────────────────────────────────────────────────

// TestSnapshotPullLoop_ValidSignature_UpdatesStore verifies that a PermissionSnapshot
// with a valid Ed25519 signature populates the SnapshotStore with parsed entries.
func TestSnapshotPullLoop_ValidSignature_UpdatesStore(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	// Build spec with principalPermissions.
	principals := []interface{}{
		map[string]interface{}{
			"principalRef": "alice",
			"allowedOperations": []interface{}{
				map[string]interface{}{
					"resource": "pods",
					"verbs":    []interface{}{"get", "list"},
				},
			},
		},
	}
	spec := map[string]interface{}{
		"targetCluster":        "ccs-dev",
		"version":              "v2",
		"principalPermissions": principals,
	}
	sigB64 := signSpec(t, priv, spec)
	snapshot := makeSnapshot("snap-valid", "ont-system", sigB64, spec)

	store := permissionservice.NewSnapshotStore()
	loop, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(snapshot), newLocalClient(), store, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	entries, version, cluster, ready := store.Get()
	if !ready {
		t.Fatal("expected store ready after valid snapshot pull; not ready")
	}
	if cluster != "ccs-dev" {
		t.Errorf("cluster: expected ccs-dev, got %q", cluster)
	}
	if version != "v2" {
		t.Errorf("version: expected v2, got %q", version)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 principal entry; got %d", len(entries))
	}
	if entries[0].PrincipalRef != "alice" {
		t.Errorf("PrincipalRef: expected alice, got %q", entries[0].PrincipalRef)
	}
}

// TestSnapshotPullLoop_InvalidSignature_StoreNotUpdated verifies that a snapshot
// with a corrupted signature does NOT update the SnapshotStore.
func TestSnapshotPullLoop_InvalidSignature_StoreNotUpdated(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	spec := map[string]interface{}{"targetCluster": "ccs-dev", "version": "v1"}
	badSig := base64.StdEncoding.EncodeToString(make([]byte, 64)) // valid base64, wrong signature
	snapshot := makeSnapshot("snap-bad-sig", "ont-system", badSig, spec)
	receipt := makeLocalSnapshotReceipt("snap-bad-sig")

	store := permissionservice.NewSnapshotStore()
	loop, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(snapshot), newLocalClient(receipt), store, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	_, _, _, ready := store.Get()
	if ready {
		t.Error("expected store NOT ready after invalid signature; store was updated")
	}
}

// TestSnapshotPullLoop_MissingSignature_StoreNotUpdated verifies that a snapshot
// with no signature annotation is NOT accepted when a key is mounted.
func TestSnapshotPullLoop_MissingSignature_StoreNotUpdated(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	spec := map[string]interface{}{"targetCluster": "ccs-dev", "version": "v1"}
	snapshot := makeSnapshot("snap-no-sig", "ont-system", "", spec) // no signature
	receipt := makeLocalSnapshotReceipt("snap-no-sig")

	store := permissionservice.NewSnapshotStore()
	loop, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(snapshot), newLocalClient(receipt), store, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	_, _, _, ready := store.Get()
	if ready {
		t.Error("expected store NOT ready when signature absent with key mounted; store was updated")
	}
}

// TestSnapshotPullLoop_FiltersOtherClusters verifies that snapshots for other
// clusters are silently skipped and do not populate the store.
func TestSnapshotPullLoop_FiltersOtherClusters(t *testing.T) {
	pub, priv := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	// Snapshot targets "ccs-test", not "ccs-dev".
	spec := map[string]interface{}{"targetCluster": "ccs-test", "version": "v1"}
	sigB64 := signSpec(t, priv, spec)
	snapshot := makeSnapshot("snap-other", "ont-system", sigB64, spec)

	store := permissionservice.NewSnapshotStore()
	loop, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(snapshot), newLocalClient(), store, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	_, _, _, ready := store.Get()
	if ready {
		t.Error("expected store NOT ready — snapshot targets ccs-test, not ccs-dev")
	}
}

// TestSnapshotPullLoop_EmptyManagementCluster verifies that a pull cycle against
// an empty management cluster completes without error or panic.
func TestSnapshotPullLoop_EmptyManagementCluster(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	store := permissionservice.NewSnapshotStore()
	loop, err := agent.NewSnapshotPullLoopWithKey(
		newMgmtClient(), newLocalClient(), store, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0) // must not panic

	_, _, _, ready := store.Get()
	if ready {
		t.Error("expected store NOT ready after empty management cluster pull")
	}
}

// TestSnapshotPullLoop_ConnectivityError_NoPanic verifies that a list error from
// the management cluster is handled gracefully — the loop logs and returns without
// panicking, and the store remains unpopulated.
func TestSnapshotPullLoop_ConnectivityError_NoPanic(t *testing.T) {
	pub, _ := genEd25519KeyPair(t)
	keyPath := writePubKeyFile(t, pub)

	// Inject a list error via reactor to simulate management cluster unreachability.
	mgmtClient := newMgmtClient()
	mgmtClient.PrependReactor("list", "permissionsnapshots", func(_ k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, fmt.Errorf("simulated connectivity error")
	})
	store := permissionservice.NewSnapshotStore()

	loop, err := agent.NewSnapshotPullLoopWithKey(
		mgmtClient, newLocalClient(), store, "ccs-dev", "ont-system", keyPath,
	)
	if err != nil {
		t.Fatalf("NewSnapshotPullLoopWithKey: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0) // must not panic on connectivity error

	_, _, _, ready := store.Get()
	if ready {
		t.Error("expected store NOT ready after connectivity error")
	}
}
