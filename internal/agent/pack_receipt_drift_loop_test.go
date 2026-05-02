package agent

import (
	"context"
	"testing"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

// fakePackReceipt builds a fake InfrastructurePackReceipt unstructured object.
// verified is nil for executor-created receipts (status.verified absent),
// pointer-to-true for receipts confirmed by the packinstance pull loop,
// pointer-to-false for receipts whose signature verification failed.
func fakePackReceipt(name, clusterPackRef string, verified *bool, resources []map[string]interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{
		"clusterPackRef":   clusterPackRef,
		"targetClusterRef": "ccs-dev",
	}
	if len(resources) > 0 {
		items := make([]interface{}, len(resources))
		for i, r := range resources {
			items[i] = r
		}
		spec["deployedResources"] = items
	}
	obj := map[string]interface{}{
		"apiVersion": "infrastructure.ontai.dev/v1alpha1",
		"kind":       "InfrastructurePackReceipt",
		"metadata": map[string]interface{}{
			"name":            name,
			"namespace":       "ont-system",
			"resourceVersion": "1",
		},
		"spec": spec,
	}
	if verified != nil {
		obj["status"] = map[string]interface{}{
			"verified": *verified,
		}
	}
	return &unstructured.Unstructured{Object: obj}
}

// fakeClusterPack builds a fake InfrastructureClusterPack for the management cluster.
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
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "", Version: "v1", Kind: "Namespace",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "", Version: "v1", Kind: "NamespaceList",
	}, &unstructured.UnstructuredList{})
	return s
}

// TestPackReceiptDriftLoop_VerifiedAbsent_ProceedsToDriftCheck verifies that an
// executor-created receipt (no status.verified field) is trusted and proceeds to
// drift checking. INV-020: executor-created receipts pre-date the packinstance pull
// loop and are always accepted. Decision H.
func TestPackReceiptDriftLoop_VerifiedAbsent_ProceedsToDriftCheck(t *testing.T) {
	scheme := setupDriftLoopScheme()
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	// verified=nil → status.verified absent → executor-created receipt.
	// Resource missing from cluster → DriftSignal must be emitted.
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", nil, resources)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	signals, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").List(
		context.Background(), metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("list DriftSignals: %v", err)
	}
	if len(signals.Items) != 1 {
		t.Fatalf("expected 1 DriftSignal for executor-created receipt, got %d", len(signals.Items))
	}
}

// TestPackReceiptDriftLoop_VerifiedFalse_SkipsDriftCheck verifies that when the
// packinstance pull loop has written status.verified=false (Ed25519 verification
// failed), the drift loop skips drift checking for that receipt. INV-026.
func TestPackReceiptDriftLoop_VerifiedFalse_SkipsDriftCheck(t *testing.T) {
	scheme := setupDriftLoopScheme()
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	boolFalse := false
	// status.verified=false → packinstance pull loop set verification failed.
	// Resource missing, but drift loop must skip → no DriftSignal emitted.
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolFalse, resources)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	signals, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").List(
		context.Background(), metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("list DriftSignals: %v", err)
	}
	if len(signals.Items) != 0 {
		t.Errorf("expected 0 DriftSignals when status.verified=false, got %d", len(signals.Items))
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
	boolTrue := true
	// status.verified=true → packinstance pull loop confirmed signature. Resource missing → emit signal.
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolTrue, resources)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

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
	boolTrue := true
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolTrue, resources)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	localClient := fake.NewSimpleDynamicClient(scheme, receipt, deploy)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

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
	boolTrue := true
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolTrue, resources)

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
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existing, cp)

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
	boolTrue := true
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolTrue, resources)

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
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	// Deployment is NOT present — drift persists.
	localClient := fake.NewSimpleDynamicClient(scheme, receipt)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existing, cp)

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
	boolTrue := true
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolTrue, resources)

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
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	localClient := fake.NewSimpleDynamicClient(scheme, receipt, deploy)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existing, cp)

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
	// correlationID must be cleared when confirming -- drift event correlation chain is closed.
	// conductor-schema.md §7.9.
	if cid, _ := spec["correlationID"].(string); cid != "" {
		t.Errorf("expected correlationID cleared on confirmation, got %q", cid)
	}
}

// TestPackReceiptDriftLoop_OrphanReceipt_TearsDownResources verifies that when the
// ClusterPack referenced by a PackReceipt no longer exists on the management cluster,
// the drift loop deletes all deployedResources from the local cluster (including
// namespaces that housed namespace-scoped resources), deletes the PackReceipt,
// and deletes any associated DriftSignal. Decision H.
func TestPackReceiptDriftLoop_OrphanReceipt_TearsDownResources(t *testing.T) {
	scheme := setupDriftLoopScheme()
	// Register Namespace and ClusterRole kinds in the scheme so the fake client can store them.
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Namespace"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NamespaceList"}, &unstructured.UnstructuredList{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRole"}, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "rbac.authorization.k8s.io", Version: "v1", Kind: "ClusterRoleList"}, &unstructured.UnstructuredList{})

	// Namespace object exists on local cluster.
	ns := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1", "kind": "Namespace",
			"metadata": map[string]interface{}{"name": "ingress-nginx", "resourceVersion": "1"},
		},
	}
	// ClusterRole is cluster-scoped.
	cr := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole",
			"metadata": map[string]interface{}{"name": "ingress-nginx", "resourceVersion": "1"},
		},
	}
	resources := []map[string]interface{}{
		// Namespace-scoped resource — should trigger namespace deletion.
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
		// Cluster-scoped resource — should be deleted individually.
		{"apiVersion": "rbac.authorization.k8s.io/v1", "kind": "ClusterRole", "name": "ingress-nginx"},
	}
	// Orphan path runs before the verified gate; verified=nil is fine here.
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", nil, resources)

	// Pre-existing DriftSignal that should also be deleted.
	signal := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1", "kind": "DriftSignal",
			"metadata": map[string]interface{}{
				"name": "drift-nginx-ccs-dev", "namespace": "seam-tenant-ccs-dev",
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{"state": "pending"},
		},
	}

	// mgmtClient has NO ClusterPack — simulates deletion.
	localClient := fake.NewSimpleDynamicClient(scheme, receipt, ns, cr)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, signal)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	// PackReceipt must be deleted from local cluster.
	_, getErr := localClient.Resource(packReceiptGVR).Namespace("ont-system").Get(
		context.Background(), "nginx-ccs-dev", metav1.GetOptions{},
	)
	if !k8serrors.IsNotFound(getErr) {
		t.Errorf("expected PackReceipt to be deleted, got: %v", getErr)
	}

	// The namespace must be deleted (cascade deletes namespace-scoped resources).
	nsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	_, nsErr := localClient.Resource(nsGVR).Get(context.Background(), "ingress-nginx", metav1.GetOptions{})
	if !k8serrors.IsNotFound(nsErr) {
		t.Errorf("expected namespace ingress-nginx to be deleted, got: %v", nsErr)
	}

	// The cluster-scoped ClusterRole must be deleted individually.
	crGVR := schema.GroupVersionResource{Group: "rbac.authorization.k8s.io", Version: "v1", Resource: "clusterroles"}
	_, crErr := localClient.Resource(crGVR).Get(context.Background(), "ingress-nginx", metav1.GetOptions{})
	if !k8serrors.IsNotFound(crErr) {
		t.Errorf("expected ClusterRole ingress-nginx to be deleted, got: %v", crErr)
	}

	// DriftSignal must be deleted from management cluster.
	_, signalErr := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "drift-nginx-ccs-dev", metav1.GetOptions{},
	)
	if !k8serrors.IsNotFound(signalErr) {
		t.Errorf("expected DriftSignal to be deleted, got: %v", signalErr)
	}
}

// TestPackReceiptDriftLoop_NamespaceTerminating_SkipsDrift verifies that when a
// resource's namespace has a DeletionTimestamp set (Terminating), the drift loop
// skips the signal for that cycle and does not increment the escalation counter.
// This prevents the counter from exhausting the escalation budget while a corrective
// Job is still waiting for the namespace to fully terminate before recreating it.
func TestPackReceiptDriftLoop_NamespaceTerminating_SkipsDrift(t *testing.T) {
	scheme := setupDriftLoopScheme()

	now := metav1.Now()
	terminatingNS := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Namespace",
			"metadata": map[string]interface{}{
				"name":              "ingress-nginx",
				"resourceVersion":   "1",
				"deletionTimestamp": now.UTC().Format("2006-01-02T15:04:05Z"),
			},
		},
	}
	resources := []map[string]interface{}{
		{"apiVersion": "apps/v1", "kind": "Deployment", "namespace": "ingress-nginx", "name": "ingress-nginx-controller"},
	}
	boolTrue := true
	receipt := fakePackReceipt("nginx-ccs-dev", "nginx-ccs-dev", &boolTrue, resources)
	cp := fakeClusterPack("nginx-ccs-dev", "seam-tenant-ccs-dev", map[string]interface{}{})
	// Deployment is NOT present but namespace IS Terminating.
	localClient := fake.NewSimpleDynamicClient(scheme, receipt, terminatingNS)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, cp)

	loop := NewPackReceiptDriftLoop(localClient, mgmtClient, "ccs-dev", "ont-system")
	loop.runOnce(context.Background())

	signals, err := mgmtClient.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").List(
		context.Background(), metav1.ListOptions{},
	)
	if err != nil {
		t.Fatalf("list DriftSignals: %v", err)
	}
	if len(signals.Items) != 0 {
		t.Errorf("expected 0 DriftSignals when namespace is Terminating, got %d", len(signals.Items))
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
