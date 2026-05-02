package agent_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/internal/agent"
)

var versionDriftSignalGVR = schema.GroupVersionResource{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: "driftsignals"}
var versionNodeGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}
var versionTalosClusterGVR = schema.GroupVersionResource{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: "infrastructuretalosclusters"}

func buildFakeDriftScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal"}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList"}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureTalosCluster"}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureTalosClusterList"}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "Node"}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{Group: "", Version: "v1", Kind: "NodeList"}, &unstructured.UnstructuredList{})
	return s
}

func makeNode(name, osImage string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Node",
			"metadata":   map[string]interface{}{"name": name},
			"status": map[string]interface{}{
				"nodeInfo": map[string]interface{}{
					"osImage": osImage,
				},
			},
		},
	}
}

func makeTalosClusterForVersion(clusterRef, ns, talosVersion string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureTalosCluster",
			"metadata":   map[string]interface{}{"name": clusterRef, "namespace": ns},
			"spec":       map[string]interface{}{"talosVersion": talosVersion},
		},
	}
}

// TestTalosVersionDriftLoop_EmitsDriftSignalOnVersionMismatch verifies that a DriftSignal
// is created when all nodes report a different Talos version than spec.talosVersion.
func TestTalosVersionDriftLoop_EmitsDriftSignalOnVersionMismatch(t *testing.T) {
	scheme := buildFakeDriftScheme()
	clusterRef := "ccs-dev"
	ns := "ont-system"
	mgmtTenantNS := "seam-tenant-" + clusterRef
	specVersion := "v1.7.0"
	observedVersion := "v1.7.4"

	localClient := dynamicfake.NewSimpleDynamicClient(scheme,
		makeNode("node1", "Talos ("+observedVersion+")"),
		makeNode("node2", "Talos ("+observedVersion+")"),
		makeTalosClusterForVersion(clusterRef, ns, specVersion),
	)
	mgmtScheme := runtime.NewScheme()
	mgmtScheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal"}, &unstructured.Unstructured{})
	mgmtScheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList"}, &unstructured.UnstructuredList{})
	mgmtClient := dynamicfake.NewSimpleDynamicClient(mgmtScheme)

	loop := agent.NewTalosVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	loop.Run(ctx, 10*time.Second) // fires once then ctx times out

	list, err := mgmtClient.Resource(versionDriftSignalGVR).Namespace(mgmtTenantNS).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("list DriftSignals: %v", err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("expected 1 DriftSignal, got %d", len(list.Items))
	}

	spec := list.Items[0].Object["spec"].(map[string]interface{})
	if spec["state"] != "pending" {
		t.Errorf("expected state=pending, got %q", spec["state"])
	}
	driftReason, _ := spec["driftReason"].(string)
	if !contains(driftReason, "observed="+observedVersion) {
		t.Errorf("expected driftReason to contain observed=%s, got %q", observedVersion, driftReason)
	}
	affectedRef, _ := spec["affectedCRRef"].(map[string]interface{})
	if affectedRef["kind"] != "InfrastructureTalosCluster" {
		t.Errorf("expected affectedCRRef.Kind=InfrastructureTalosCluster, got %q", affectedRef["kind"])
	}
}

// TestTalosVersionDriftLoop_NoSignalWhenVersionsMatch verifies that no DriftSignal is emitted
// when all nodes match spec.talosVersion.
func TestTalosVersionDriftLoop_NoSignalWhenVersionsMatch(t *testing.T) {
	scheme := buildFakeDriftScheme()
	clusterRef := "ccs-dev"
	ns := "ont-system"
	mgmtTenantNS := "seam-tenant-" + clusterRef
	version := "v1.7.4"

	localClient := dynamicfake.NewSimpleDynamicClient(scheme,
		makeNode("node1", "Talos ("+version+")"),
		makeTalosClusterForVersion(clusterRef, ns, version),
	)
	mgmtScheme := runtime.NewScheme()
	mgmtScheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal"}, &unstructured.Unstructured{})
	mgmtScheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList"}, &unstructured.UnstructuredList{})
	mgmtClient := dynamicfake.NewSimpleDynamicClient(mgmtScheme)

	loop := agent.NewTalosVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	loop.Run(ctx, 10*time.Second)

	list, _ := mgmtClient.Resource(versionDriftSignalGVR).Namespace(mgmtTenantNS).List(context.Background(), metav1.ListOptions{})
	if len(list.Items) != 0 {
		t.Errorf("expected no DriftSignals when versions match, got %d", len(list.Items))
	}
}

// TestTalosVersionDriftLoop_MixedVersionsNoSignal verifies that no signal is emitted when nodes
// report different versions (mid-upgrade in progress).
func TestTalosVersionDriftLoop_MixedVersionsNoSignal(t *testing.T) {
	scheme := buildFakeDriftScheme()
	clusterRef := "ccs-dev"
	ns := "ont-system"
	mgmtTenantNS := "seam-tenant-" + clusterRef

	localClient := dynamicfake.NewSimpleDynamicClient(scheme,
		makeNode("node1", "Talos (v1.7.0)"),
		makeNode("node2", "Talos (v1.7.4)"), // mixed versions
		makeTalosClusterForVersion(clusterRef, ns, "v1.7.0"),
	)
	mgmtScheme := runtime.NewScheme()
	mgmtScheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal"}, &unstructured.Unstructured{})
	mgmtScheme.AddKnownTypeWithName(schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList"}, &unstructured.UnstructuredList{})
	mgmtClient := dynamicfake.NewSimpleDynamicClient(mgmtScheme)

	loop := agent.NewTalosVersionDriftLoop(localClient, mgmtClient, clusterRef, ns)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	loop.Run(ctx, 10*time.Second)

	list, _ := mgmtClient.Resource(versionDriftSignalGVR).Namespace(mgmtTenantNS).List(context.Background(), metav1.ListOptions{})
	if len(list.Items) != 0 {
		t.Errorf("expected no DriftSignal for mixed-version nodes (mid-upgrade), got %d", len(list.Items))
	}
}

// TestParseTalosVersionFromOSImage verifies that the Talos version is correctly extracted
// from various osImage string formats.
func TestParseTalosVersionFromOSImage(t *testing.T) {
	cases := []struct {
		osImage  string
		expected string
	}{
		{"Talos (v1.7.4)", "v1.7.4"},
		{"Talos (v1.6.0)", "v1.6.0"},
		{"Talos (v2.0.0-alpha.1)", "v2.0.0-alpha.1"},
		{"Ubuntu 22.04 LTS", ""},
		{"", ""},
	}
	for _, tc := range cases {
		got := agent.ParseTalosVersionFromOSImage(tc.osImage)
		if got != tc.expected {
			t.Errorf("ParseTalosVersionFromOSImage(%q): got %q, want %q", tc.osImage, got, tc.expected)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || (len(s) > 0 && containsStr(s, substr)))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
