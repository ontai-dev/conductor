package agent

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"testing"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

// fakePackReceipt builds a fake InfrastructurePackReceipt unstructured object.
func fakePackReceipt(name, clusterPackRef, packSig string, sigVerified bool, resources []map[string]interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"clusterPackRef":    clusterPackRef,
		"targetClusterRef":  "ccs-dev",
		"packSignature":     packSig,
		"signatureVerified": sigVerified,
	}
	if len(resources) > 0 {
		items := make([]interface{}, len(resources))
		for i, r := range resources {
			items[i] = r
		}
		spec["deployedResources"] = items
	}
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructurePackReceipt",
			"metadata": map[string]interface{}{
				"name":            name,
				"namespace":       "ont-system",
				"resourceVersion": "1",
			},
			"spec": spec,
		},
	}
}

// fakeClusterPack builds a fake InfrastructureClusterPack for signature message reconstruction.
func fakeClusterPack(name, ns string, specPayload map[string]interface{}) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureClusterPack",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ns,
			},
			"spec": specPayload,
		},
	}
}

func setupDriftLoopScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructurePackReceipt",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructurePackReceiptList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureClusterPack",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureClusterPackList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "apps", Version: "v1", Kind: "Deployment",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "apps", Version: "v1", Kind: "DeploymentList",
	}, &unstructured.UnstructuredList{})
	return s
}

// TestPackReceiptDriftLoop_BootstrapWindow_AcceptsWithoutKey verifies that in
// bootstrap window mode (pubKey nil) all receipts are accepted without signature
// verification and signatureVerified is patched to true. INV-020.
func TestPackReceiptDriftLoop_BootstrapWindow_AcceptsWithoutKey(t *testing.T) {
	scheme := setupDriftLoopScheme()
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", "", false, nil)
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	updated, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "nginx-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated receipt: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	if verified, _ := spec["signatureVerified"].(bool); !verified {
		t.Error("expected signatureVerified=true in bootstrap window mode")
	}
}

// TestPackReceiptDriftLoop_SignatureVerified_ValidKey verifies that a correctly
// signed PackReceipt passes verification and gets signatureVerified=true. INV-026.
func TestPackReceiptDriftLoop_SignatureVerified_ValidKey(t *testing.T) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	cpSpec := map[string]interface{}{
		"componentName": "nginx-ingress",
		"version":       "v4.9.0-r1",
	}
	message, _ := json.Marshal(cpSpec)
	sig := ed25519.Sign(priv, message)
	packSig := base64.StdEncoding.EncodeToString(sig)

	scheme := setupDriftLoopScheme()
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", packSig, false, nil)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", cpSpec)

	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

	loop := &PackReceiptDriftLoop{
		localClient:   localClient,
		mgmtClient:    mgmtClient,
		pubKey:        pub,
		clusterName:   "ccs-dev",
		namespace:     "ont-system",
		mgmtTenantNS:  "seam-tenant-ccs-dev",
	}
	loop.runOnce(context.Background())

	updated, err := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "nginx-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated receipt: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	if verified, _ := spec["signatureVerified"].(bool); !verified {
		t.Error("expected signatureVerified=true after valid signature")
	}
}

// TestPackReceiptDriftLoop_SignatureVerified_InvalidKey verifies that an incorrectly
// signed PackReceipt does not get signatureVerified=true. INV-026.
func TestPackReceiptDriftLoop_SignatureVerified_InvalidKey(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key pair 1: %v", err)
	}
	_, wrongPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key pair 2: %v", err)
	}

	cpSpec := map[string]interface{}{"componentName": "nginx-ingress"}
	message, _ := json.Marshal(cpSpec)
	sig := ed25519.Sign(wrongPriv, message) // signed with wrong key
	packSig := base64.StdEncoding.EncodeToString(sig)

	scheme := setupDriftLoopScheme()
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", packSig, false, nil)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", cpSpec)

	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

	loop := &PackReceiptDriftLoop{
		localClient:   localClient,
		mgmtClient:    mgmtClient,
		pubKey:        pub,
		clusterName:   "ccs-dev",
		namespace:     "ont-system",
		mgmtTenantNS:  "seam-tenant-ccs-dev",
	}
	loop.runOnce(context.Background())

	updated, _ := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "nginx-ccs-dev", metav1.GetOptions{},
	)
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	if verified, _ := spec["signatureVerified"].(bool); verified {
		t.Error("expected signatureVerified=false after invalid signature")
	}
}

// TestPackReceiptDriftLoop_DriftDetected_EmitsDriftSignal verifies that when a
// verified PackReceipt has a deployedResource missing from the cluster, a
// DriftSignal is written to the management cluster. Decision H.
func TestPackReceiptDriftLoop_DriftDetected_EmitsDriftSignal(t *testing.T) {
	scheme := setupDriftLoopScheme()
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	// Receipt already verified, resource NOT present in local cluster.
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", "", true, resources)
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	signals, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").List(
		context.Background(), metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("list DriftSignals: %v", err)
	}
	if len(signals.Items) != 1 {
		t.Fatalf("expected 1 DriftSignal, got %d", len(signals.Items))
	}
	spec, _, _ := unstructuredNestedMap(signals.Items[0].Object, "spec")
	if state, _ := spec["state"].(string); state != "pending" {
		t.Errorf("expected state=pending, got %q", state)
	}
}

// TestPackReceiptDriftLoop_NoDrift_NoSignal verifies that when all deployedResources
// exist on the cluster, no DriftSignal is emitted.
func TestPackReceiptDriftLoop_NoDrift_NoSignal(t *testing.T) {
	scheme := setupDriftLoopScheme()

	// Deployment exists on local cluster.
	deploy := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": "ingress-nginx-controller", "namespace": "ingress-nginx",
			},
		},
	}
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", "", true, resources)
	localClient := fake.NewSimpleDynamicClient(scheme, receipt, deploy)
	mgmtClient := fake.NewSimpleDynamicClient(scheme)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	signals, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").List(
		context.Background(), metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("list DriftSignals: %v", err)
	}
	if len(signals.Items) != 0 {
		t.Errorf("expected 0 DriftSignals, got %d", len(signals.Items))
	}
}

// TestPackReceiptDriftLoop_EscalationThreshold_StopsEmitting verifies that when
// escalationCounter reaches escalationThreshold no new signal is emitted.
func TestPackReceiptDriftLoop_EscalationThreshold_StopsEmitting(t *testing.T) {
	scheme := setupDriftLoopScheme()
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", "", true, resources)

	// Pre-existing DriftSignal at threshold.
	existing := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name": "drift-nginx-ccs-dev", "namespace": "seam-tenant-ccs-dev",
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"state":             "pending",
				"escalationCounter": int64(escalationThreshold),
				"correlationID":     "drift-12345",
				"driftReason":       "resource missing",
				"observedAt":        "2026-05-01T00:00:00Z",
				"affectedCRRef":     map[string]interface{}{"group": "apps", "kind": "Deployment", "name": "ingress-nginx-controller"},
			},
		},
	}
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existing)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	// Counter should NOT be incremented beyond threshold.
	updated, _ := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "drift-nginx-ccs-dev", metav1.GetOptions{},
	)
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	counter, _ := spec["escalationCounter"].(int64)
	if int32(counter) != escalationThreshold {
		t.Errorf("expected escalationCounter=%d (unchanged), got %d", escalationThreshold, counter)
	}
}

// TestPackReceiptDriftLoop_DriftPersistsQueued_IncrementsCounter verifies that when
// a DriftSignal is in queued state and drift still persists, the counter is incremented
// and state is reset to pending so the management handler can retrigger again.
func TestPackReceiptDriftLoop_DriftPersistsQueued_IncrementsCounter(t *testing.T) {
	scheme := setupDriftLoopScheme()
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", "", true, resources)

	// Pre-existing DriftSignal in queued state, counter=0.
	existing := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name": "drift-nginx-ccs-dev", "namespace": "seam-tenant-ccs-dev",
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"state":             "queued",
				"escalationCounter": int64(0),
				"correlationID":     "drift-12345",
				"driftReason":       "resource missing",
				"observedAt":        "2026-05-01T00:00:00Z",
				"affectedCRRef":     map[string]interface{}{"group": "apps", "kind": "Deployment", "name": "ingress-nginx-controller"},
			},
		},
	}
	// Deployment is NOT present — drift persists.
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existing)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	updated, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "drift-nginx-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated DriftSignal: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	counter, _ := spec["escalationCounter"].(int64)
	if counter != 1 {
		t.Errorf("expected escalationCounter=1 after queued re-escalation, got %d", counter)
	}
	if state, _ := spec["state"].(string); state != "pending" {
		t.Errorf("expected state=pending after queued re-escalation, got %q", state)
	}
}

// TestPackReceiptDriftLoop_DriftResolved_ConfirmsSignal verifies that when all deployed
// resources exist and a DriftSignal is in queued state, the signal is set to confirmed.
func TestPackReceiptDriftLoop_DriftResolved_ConfirmsSignal(t *testing.T) {
	scheme := setupDriftLoopScheme()

	// Deployment exists on local cluster.
	deploy := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "apps/v1",
			"kind":       "Deployment",
			"metadata": map[string]interface{}{
				"name": "ingress-nginx-controller", "namespace": "ingress-nginx",
			},
		},
	}
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", "", true, resources)

	// Pre-existing DriftSignal in queued state (management retrigger issued).
	existing := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name": "drift-nginx-ccs-dev", "namespace": "seam-tenant-ccs-dev",
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"state":             "queued",
				"escalationCounter": int64(0),
				"correlationID":     "drift-12345",
				"driftReason":       "resource missing",
				"observedAt":        "2026-05-01T00:00:00Z",
				"affectedCRRef":     map[string]interface{}{"group": "apps", "kind": "Deployment", "name": "ingress-nginx-controller"},
			},
		},
	}
	localClient := fake.NewSimpleDynamicClient(scheme, receipt, deploy)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existing)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	updated, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "drift-nginx-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated DriftSignal: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	if state, _ := spec["state"].(string); state != "confirmed" {
		t.Errorf("expected state=confirmed after drift resolved, got %q", state)
	}
}

// TestPluralizeKind verifies that irregular plural forms are handled correctly.
func TestPluralizeKind(t *testing.T) {
	cases := []struct{ kind, want string }{
		{"Deployment", "deployments"},
		{"Ingress", "ingresses"},
		{"NetworkPolicy", "networkpolicies"},
		{"Service", "services"},
		{"ConfigMap", "configmaps"},
		{"IngressClass", "ingressclasses"},
	}
	for _, c := range cases {
		got := pluralizeKind(c.kind)
		if got != c.want {
			t.Errorf("pluralizeKind(%q) = %q, want %q", c.kind, got, c.want)
		}
	}
}

// Ensure the fake client returns NotFound for absent resources, matching the real
// k8s API behavior that the drift detection loop depends on.
func TestGVRFromAPIVersionKind_CoreGroup(t *testing.T) {
	gvr, err := gvrFromAPIVersionKind("v1", "ConfigMap")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gvr.Group != "" {
		t.Errorf("expected empty group for core v1, got %q", gvr.Group)
	}
	if gvr.Resource != "configmaps" {
		t.Errorf("expected configmaps, got %q", gvr.Resource)
	}
}

func TestGVRFromAPIVersionKind_NamedGroup(t *testing.T) {
	gvr, err := gvrFromAPIVersionKind("apps/v1", "Deployment")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gvr.Group != "apps" {
		t.Errorf("expected group apps, got %q", gvr.Group)
	}
	if gvr.Resource != "deployments" {
		t.Errorf("expected deployments, got %q", gvr.Resource)
	}
}

// fakeNotFound returns a k8s NotFound error matching what the drift loop expects.
func fakeNotFound(name string) error {
	return k8serrors.NewNotFound(schema.GroupResource{Resource: "deployments"}, name)
}
