package capability_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"

	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// permissionSnapshotGVR mirrors the GVR in guardian.go.
var permissionSnapshotGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "permissionsnapshots",
}

// newFakeDynamicWithSnapshots builds a fake dynamic client with a
// PermissionSnapshot for the given clusterRef.
func newFakeDynamicWithSnapshots(clusterRef string) *dynamicfake.FakeDynamicClient {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "Permissionsnapshot"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "PermissionsnapshotList"},
		&unstructured.UnstructuredList{},
	)

	snap := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "Permissionsnapshot",
			"metadata": map[string]interface{}{
				"name":      "snap-" + clusterRef,
				"namespace": "security-system",
			},
			"spec": map[string]interface{}{
				"targetCluster": clusterRef,
				"version":       "2026-01-01T00:00:00Z",
				"generatedAt":   "2026-01-01T00:00:00Z",
				"principalPermissions": []interface{}{
					map[string]interface{}{
						"principalRef": "platform-admin",
						"allowedOperations": []interface{}{
							map[string]interface{}{
								"apiGroup": "apps",
								"resource": "deployments",
								"verbs":    []interface{}{"get", "list", "watch", "create", "update", "patch", "delete"},
							},
						},
					},
				},
			},
		},
	}

	client := dynamicfake.NewSimpleDynamicClient(s)
	_, _ = client.Resource(permissionSnapshotGVR).Namespace("security-system").
		Create(context.Background(), snap, metav1.CreateOptions{})
	return client
}

// TestRBACProvision_NilClientsReturnsValidationFailure verifies the nil-client
// contract: if DynamicClient or KubeClient is nil, rbac-provision returns
// ValidationFailure and does not panic. conductor-schema.md §6.
func TestRBACProvision_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: "ccs-dev",
		// ExecuteClients zero: all nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ValidationFailure {
		t.Errorf("expected ValidationFailure; got %+v", result.FailureReason)
	}
}

// TestRBACProvision_NoSnapshotReturnsExecutionFailure verifies that when no
// PermissionSnapshot exists for the cluster, rbac-provision returns
// ExecutionFailure (not panic). conductor-schema.md §6.
func TestRBACProvision_NoSnapshotReturnsExecutionFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	// Empty dynamic client — no PermissionSnapshot.
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "Permissionsnapshot"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "PermissionsnapshotList"},
		&unstructured.UnstructuredList{},
	)
	dynClient := dynamicfake.NewSimpleDynamicClient(s)
	kubeClient := fake.NewSimpleClientset()

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: "ccs-dev",
		ExecuteClients: capability.ExecuteClients{
			KubeClient:    kubeClient,
			DynamicClient: dynClient,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ExecutionFailure {
		t.Errorf("expected ExecutionFailure; got %+v", result.FailureReason)
	}
}

// TestRBACProvision_AppliesClusterRolesFromSnapshot verifies that rbac-provision
// creates ClusterRoles for each principal in the PermissionSnapshot.
// guardian-schema.md §7, conductor-schema.md §6.
func TestRBACProvision_AppliesClusterRolesFromSnapshot(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	clusterRef := "ccs-dev"
	dynClient := newFakeDynamicWithSnapshots(clusterRef)
	kubeClient := fake.NewSimpleClientset()

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:    kubeClient,
			DynamicClient: dynClient,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		raw, _ := json.Marshal(result.FailureReason)
		t.Fatalf("expected ResultSucceeded; got %q (reason: %s)", result.Status, raw)
	}

	// Verify the ClusterRole was created.
	cr, getErr := kubeClient.RbacV1().ClusterRoles().Get(
		context.Background(), "seam:principal:platform-admin", metav1.GetOptions{})
	if getErr != nil {
		t.Fatalf("ClusterRole seam:principal:platform-admin not found: %v", getErr)
	}

	// Verify the ClusterRole rules match the snapshot.
	if len(cr.Rules) == 0 {
		t.Error("expected at least one PolicyRule in the ClusterRole")
	}
	found := false
	for _, rule := range cr.Rules {
		for _, r := range rule.Resources {
			if r == "deployments" {
				found = true
			}
		}
	}
	if !found {
		t.Error("expected ClusterRole to contain rule for 'deployments' resource")
	}
}

// TestRBACProvision_UpdatesExistingClusterRole verifies that rbac-provision
// updates an already-existing ClusterRole without error. The RBAC plane must
// converge even if a prior provisioning run partially applied resources.
func TestRBACProvision_UpdatesExistingClusterRole(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	clusterRef := "ccs-dev"
	dynClient := newFakeDynamicWithSnapshots(clusterRef)

	// Pre-create the ClusterRole so the handler exercises the update path.
	existingCR := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{Name: "seam:principal:platform-admin"},
		Rules:      []rbacv1.PolicyRule{},
	}
	kubeClient := fake.NewSimpleClientset(existingCR)

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:    kubeClient,
			DynamicClient: dynClient,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		raw, _ := json.Marshal(result.FailureReason)
		t.Fatalf("expected ResultSucceeded; got %q (reason: %s)", result.Status, raw)
	}
}

// snapshotSpec returns the spec map used by newFakeDynamicWithSnapshots and the
// signed-snapshot helper, so the signed message can be reproduced identically.
func snapshotSpec(clusterRef string) map[string]interface{} {
	return map[string]interface{}{
		"targetCluster": clusterRef,
		"version":       "2026-01-01T00:00:00Z",
		"generatedAt":   "2026-01-01T00:00:00Z",
		"principalPermissions": []interface{}{
			map[string]interface{}{
				"principalRef": "platform-admin",
				"allowedOperations": []interface{}{
					map[string]interface{}{
						"apiGroup": "apps",
						"resource": "deployments",
						"verbs":    []interface{}{"get", "list", "watch", "create", "update", "patch", "delete"},
					},
				},
			},
		},
	}
}

// newFakeDynamicWithSignedSnapshot builds a fake dynamic client with a
// PermissionSnapshot whose spec is signed with privKey. Pass privKey == nil
// to create a snapshot with no signature annotation (unsigned snapshot).
func newFakeDynamicWithSignedSnapshot(clusterRef string, privKey ed25519.PrivateKey) *dynamicfake.FakeDynamicClient {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "Permissionsnapshot"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: "PermissionsnapshotList"},
		&unstructured.UnstructuredList{},
	)

	spec := snapshotSpec(clusterRef)

	meta := map[string]interface{}{
		"name":      "snap-" + clusterRef,
		"namespace": "security-system",
	}
	if privKey != nil {
		specBytes, _ := json.Marshal(spec)
		sigBytes := ed25519.Sign(privKey, specBytes)
		meta["annotations"] = map[string]interface{}{
			"infrastructure.ontai.dev/management-signature": base64.StdEncoding.EncodeToString(sigBytes),
		}
	}

	snap := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "Permissionsnapshot",
			"metadata":   meta,
			"spec":       spec,
		},
	}

	client := dynamicfake.NewSimpleDynamicClient(s)
	_, _ = client.Resource(permissionSnapshotGVR).Namespace("security-system").
		Create(context.Background(), snap, metav1.CreateOptions{})
	return client
}

// TestRBACProvision_BootstrapWindowNilKey_Succeeds verifies that when
// SigningPublicKey is nil (bootstrap window mode, INV-020), the handler
// accepts a snapshot with no signature annotation and applies ClusterRoles.
func TestRBACProvision_BootstrapWindowNilKey_Succeeds(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	clusterRef := "ccs-dev"
	// unsigned snapshot — no annotation
	dynClient := newFakeDynamicWithSignedSnapshot(clusterRef, nil)
	kubeClient := fake.NewSimpleClientset()

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:      kubeClient,
			DynamicClient:   dynClient,
			SigningPublicKey: nil, // bootstrap window — INV-020
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		raw, _ := json.Marshal(result.FailureReason)
		t.Fatalf("expected ResultSucceeded in bootstrap window mode; got %q (%s)", result.Status, raw)
	}
}

// TestRBACProvision_ValidSignatureSucceeds verifies that a correctly signed
// PermissionSnapshot passes Ed25519 verification and ClusterRoles are applied.
// INV-026.
func TestRBACProvision_ValidSignatureSucceeds(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	pubKey, privKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 key: %v", err)
	}

	clusterRef := "ccs-dev"
	dynClient := newFakeDynamicWithSignedSnapshot(clusterRef, privKey)
	kubeClient := fake.NewSimpleClientset()

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:      kubeClient,
			DynamicClient:   dynClient,
			SigningPublicKey: pubKey,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		raw, _ := json.Marshal(result.FailureReason)
		t.Fatalf("expected ResultSucceeded with valid signature; got %q (%s)", result.Status, raw)
	}
}

// TestRBACProvision_InvalidSignatureFails verifies that a snapshot signed with
// a different key is rejected with ExecutionFailure. INV-026.
func TestRBACProvision_InvalidSignatureFails(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	// Signing key used to sign the snapshot.
	_, signingKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate signing key: %v", err)
	}
	// Verification key is different — verification must fail.
	wrongPubKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate wrong key: %v", err)
	}

	clusterRef := "ccs-dev"
	dynClient := newFakeDynamicWithSignedSnapshot(clusterRef, signingKey)
	kubeClient := fake.NewSimpleClientset()

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:      kubeClient,
			DynamicClient:   dynClient,
			SigningPublicKey: wrongPubKey,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Fatalf("expected ResultFailed for invalid signature; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ExecutionFailure {
		t.Errorf("expected ExecutionFailure; got %+v", result.FailureReason)
	}
}

// TestRBACProvision_MissingAnnotationWithKeyFails verifies that when a signing
// key is present but the snapshot has no signature annotation, the handler
// returns ExecutionFailure. INV-026.
func TestRBACProvision_MissingAnnotationWithKeyFails(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	pubKey, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	clusterRef := "ccs-dev"
	// unsigned snapshot — no annotation
	dynClient := newFakeDynamicWithSignedSnapshot(clusterRef, nil)
	kubeClient := fake.NewSimpleClientset()

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:      kubeClient,
			DynamicClient:   dynClient,
			SigningPublicKey: pubKey, // key present — annotation must be present too
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Fatalf("expected ResultFailed for missing annotation; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ExecutionFailure {
		t.Errorf("expected ExecutionFailure; got %+v", result.FailureReason)
	}
}

// TestRBACProvision_ArtifactsNonNil verifies that Artifacts is non-nil even
// on success (operators range over this slice).
func TestRBACProvision_ArtifactsNonNil(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	h, _ := reg.Resolve(runnerlib.CapabilityRBACProvision)
	result, _ := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityRBACProvision,
	})
	if result.Artifacts == nil {
		t.Error("Artifacts must not be nil — operators range over this slice")
	}
}
