package agent

import (
	"context"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

func setupK8sDriftScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureTalosCluster",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureTalosClusterList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "", Version: "v1", Kind: "Node",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "", Version: "v1", Kind: "NodeList",
	}, &unstructured.UnstructuredList{})
	return s
}

func fakeTalosCluster(name, ns, k8sVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureTalosCluster",
			"metadata": map[string]interface{}{
				"name": name, "namespace": ns,
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"talosVersion":      "v1.9.3",
				"kubernetesVersion": k8sVersion,
			},
		},
	}
}

func fakeNode(name, kubeletVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata": map[string]interface{}{
				"name":            name,
				"resourceVersion": "1",
			},
			"status": map[string]interface{}{
				"nodeInfo": map[string]interface{}{
					"kubeletVersion": kubeletVersion,
				},
			},
		},
	}
}

// TestKubernetesVersionDriftLoop_EmitSignalOnDrift verifies that a DriftSignal is created
// when all nodes report a kubeletVersion that differs from spec.kubernetesVersion.
func TestKubernetesVersionDriftLoop_EmitSignalOnDrift(t *testing.T) {
	const clusterRef = "ccs-dev"
	const ns = "ont-system"
	const mgmtTenantNS = "seam-tenant-ccs-dev"

	scheme := setupK8sDriftScheme()
	tc := fakeTalosCluster(clusterRef, ns, "1.32.3")
	node1 := fakeNode("node1", "v1.32.4")
	node2 := fakeNode("node2", "v1.32.4")

	localClient := fake.NewSimpleDynamicClient(scheme, tc, node1, node2)
	mgmtClient := fake.NewSimpleDynamicClient(scheme)

	loop := NewKubernetesVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)
	loop.checkOnce(context.Background())

	signalName := k8sVersionDriftSignalPrefix + clusterRef
	got, err := mgmtClient.Resource(driftSignalGVR).Namespace(mgmtTenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("expected DriftSignal to be created, got error: %v", err)
	}

	spec, _, _ := unstructuredNestedMap(got.Object, "spec")
	if state, _ := spec["state"].(string); state != "pending" {
		t.Errorf("expected state=pending, got %q", state)
	}
	driftReason, _ := spec["driftReason"].(string)
	if driftReason != "kubernetes version drift: spec=1.32.3 observed=1.32.4" {
		t.Errorf("unexpected driftReason: %q", driftReason)
	}
}

// TestKubernetesVersionDriftLoop_NoSignalWhenVersionsMatch verifies no DriftSignal is
// created when all nodes report the same kubeletVersion as spec.kubernetesVersion.
func TestKubernetesVersionDriftLoop_NoSignalWhenVersionsMatch(t *testing.T) {
	const clusterRef = "ccs-dev"
	const ns = "ont-system"
	const mgmtTenantNS = "seam-tenant-ccs-dev"

	scheme := setupK8sDriftScheme()
	tc := fakeTalosCluster(clusterRef, ns, "1.32.3")
	node1 := fakeNode("node1", "v1.32.3")

	localClient := fake.NewSimpleDynamicClient(scheme, tc, node1)
	mgmtClient := fake.NewSimpleDynamicClient(scheme)

	loop := NewKubernetesVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)
	loop.checkOnce(context.Background())

	signalName := k8sVersionDriftSignalPrefix + clusterRef
	_, err := mgmtClient.Resource(driftSignalGVR).Namespace(mgmtTenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err == nil {
		t.Fatal("expected no DriftSignal when versions match, but one was created")
	}
}

// TestKubernetesVersionDriftLoop_NoSignalOnMixedVersions verifies that a DriftSignal is
// not emitted when nodes report different kubeletVersions (mid-upgrade).
func TestKubernetesVersionDriftLoop_NoSignalOnMixedVersions(t *testing.T) {
	const clusterRef = "ccs-dev"
	const ns = "ont-system"
	const mgmtTenantNS = "seam-tenant-ccs-dev"

	scheme := setupK8sDriftScheme()
	tc := fakeTalosCluster(clusterRef, ns, "1.32.3")
	node1 := fakeNode("node1", "v1.32.3")
	node2 := fakeNode("node2", "v1.32.4") // mid-upgrade

	localClient := fake.NewSimpleDynamicClient(scheme, tc, node1, node2)
	mgmtClient := fake.NewSimpleDynamicClient(scheme)

	loop := NewKubernetesVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)
	loop.checkOnce(context.Background())

	signalName := k8sVersionDriftSignalPrefix + clusterRef
	_, err := mgmtClient.Resource(driftSignalGVR).Namespace(mgmtTenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err == nil {
		t.Fatal("expected no DriftSignal during mixed-version (mid-upgrade) state, but one was created")
	}
}

// TestKubernetesVersionDriftLoop_ConfirmSignalWhenResolved verifies that an existing
// DriftSignal is patched to state=confirmed when versions converge.
func TestKubernetesVersionDriftLoop_ConfirmSignalWhenResolved(t *testing.T) {
	const clusterRef = "ccs-dev"
	const ns = "ont-system"
	const mgmtTenantNS = "seam-tenant-ccs-dev"

	scheme := setupK8sDriftScheme()
	tc := fakeTalosCluster(clusterRef, ns, "1.32.3")
	node1 := fakeNode("node1", "v1.32.3") // now matches spec

	// Pre-create a pending DriftSignal (drift was detected earlier).
	signalName := k8sVersionDriftSignalPrefix + clusterRef
	existingSignal := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name": signalName, "namespace": mgmtTenantNS,
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"state":             "queued",
				"escalationCounter": int64(0),
				"driftReason":       "kubernetes version drift: spec=1.32.3 observed=1.32.4",
				"correlationID":     "k8s-version-ccs-dev-123",
				"observedAt":        "2026-05-02T00:00:00Z",
			},
		},
	}

	localClient := fake.NewSimpleDynamicClient(scheme, tc, node1)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existingSignal)

	loop := NewKubernetesVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)
	loop.checkOnce(context.Background())

	got, err := mgmtClient.Resource(driftSignalGVR).Namespace(mgmtTenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get DriftSignal after confirm: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(got.Object, "spec")
	if state, _ := spec["state"].(string); state != "confirmed" {
		t.Errorf("expected state=confirmed after version convergence, got %q", state)
	}
}

// TestKubernetesVersionDriftLoop_IncrementCounterOnQueued verifies that a queued signal
// has its escalationCounter incremented and is reset to pending on the next drift cycle.
func TestKubernetesVersionDriftLoop_IncrementCounterOnQueued(t *testing.T) {
	const clusterRef = "ccs-dev"
	const ns = "ont-system"
	const mgmtTenantNS = "seam-tenant-ccs-dev"

	scheme := setupK8sDriftScheme()
	tc := fakeTalosCluster(clusterRef, ns, "1.32.3")
	node1 := fakeNode("node1", "v1.32.4") // still drifted

	signalName := k8sVersionDriftSignalPrefix + clusterRef
	existingSignal := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name": signalName, "namespace": mgmtTenantNS,
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"state":             "queued",
				"escalationCounter": int64(1),
				"driftReason":       "kubernetes version drift: spec=1.32.3 observed=1.32.4",
				"correlationID":     "k8s-version-ccs-dev-123",
				"observedAt":        "2026-05-02T00:00:00Z",
			},
		},
	}

	localClient := fake.NewSimpleDynamicClient(scheme, tc, node1)
	mgmtClient := fake.NewSimpleDynamicClient(scheme, existingSignal)

	loop := NewKubernetesVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)
	loop.checkOnce(context.Background())

	got, err := mgmtClient.Resource(driftSignalGVR).Namespace(mgmtTenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get DriftSignal after increment: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(got.Object, "spec")
	if state, _ := spec["state"].(string); state != "pending" {
		t.Errorf("expected state=pending after queued increment, got %q", state)
	}
	if counter, _ := spec["escalationCounter"].(int64); counter != 2 {
		t.Errorf("expected escalationCounter=2, got %d", counter)
	}
}
