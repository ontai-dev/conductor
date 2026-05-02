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

func setupDriftHandlerScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructurePackExecution",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructurePackExecutionList",
	}, &unstructured.UnstructuredList{})
	return s
}

func fakeDriftSignal(name, ns, state string, counter int64) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name": name, "namespace": ns,
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"state":             state,
				"escalationCounter": counter,
				"correlationID":     "drift-12345",
				"driftReason":       "resource missing",
				"observedAt":        "2026-05-01T00:00:00Z",
				"affectedCRRef":     map[string]interface{}{"group": "apps", "kind": "Deployment", "name": "ingress-nginx-controller"},
			},
		},
	}
}

func fakePackExecution(name, ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructurePackExecution",
			"metadata": map[string]interface{}{
				"name": name, "namespace": ns,
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{
				"packRef":          "nginx-ccs-dev",
				"targetClusterRef": "ccs-dev",
			},
		},
	}
}

// TestDriftSignalHandler_Pending_RetrieggersPackExecution verifies that a pending
// DriftSignal causes the handler to delete the PackExecution (retrigger) and
// advance state to queued. Decision H.
func TestDriftSignalHandler_Pending_RetrieggersPackExecution(t *testing.T) {
	scheme := setupDriftHandlerScheme()
	signal := fakeDriftSignal("drift-nginx-ccs-dev", "seam-tenant-ccs-dev", "pending", 0)
	pe := fakePackExecution("nginx-ccs-dev-ccs-dev", "seam-tenant-ccs-dev")

	client := fake.NewSimpleDynamicClient(scheme, signal, pe)
	handler := NewDriftSignalHandler(client)
	handler.handleOnce(context.Background())

	// PackExecution should be deleted.
	_, err := client.Resource(packExecutionGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "nginx-ccs-dev-ccs-dev", metav1.GetOptions{},
	)
	if err == nil {
		t.Error("expected PackExecution to be deleted after retrigger")
	}

	// DriftSignal state should be queued.
	updated, err := client.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "drift-nginx-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get DriftSignal: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(updated.Object, "spec")
	if state, _ := spec["state"].(string); state != "queued" {
		t.Errorf("expected state=queued after retrigger, got %q", state)
	}
}

// TestDriftSignalHandler_EscalationThreshold_SetsTerminalDrift verifies that a
// DriftSignal at threshold counter sets TerminalDrift and does not retrigger.
func TestDriftSignalHandler_EscalationThreshold_SetsTerminalDrift(t *testing.T) {
	scheme := setupDriftHandlerScheme()
	signal := fakeDriftSignal("drift-nginx-ccs-dev", "seam-tenant-ccs-dev", "pending", int64(escalationThreshold))
	pe := fakePackExecution("nginx-ccs-dev-ccs-dev", "seam-tenant-ccs-dev")

	client := fake.NewSimpleDynamicClient(scheme, signal, pe)
	handler := NewDriftSignalHandler(client)
	handler.handleOnce(context.Background())

	// PackExecution must NOT be deleted.
	_, err := client.Resource(packExecutionGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "nginx-ccs-dev-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Error("PackExecution should not be deleted at escalation threshold")
	}

	// DriftSignal should have TerminalDrift condition in status.
	updated, err := client.Resource(driftSignalGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "drift-nginx-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get DriftSignal: %v", err)
	}
	status, _, _ := unstructuredNestedMap(updated.Object, "status")
	conditions, _ := status["conditions"].([]interface{})
	found := false
	for _, c := range conditions {
		cond, _ := c.(map[string]interface{})
		if t2, _ := cond["type"].(string); t2 == "TerminalDrift" {
			found = true
		}
	}
	if !found {
		t.Error("expected TerminalDrift condition in DriftSignal status")
	}
}

// TestDriftSignalHandler_NonPending_Ignored verifies that signals not in pending
// state are not processed.
func TestDriftSignalHandler_NonPending_Ignored(t *testing.T) {
	scheme := setupDriftHandlerScheme()
	signal := fakeDriftSignal("drift-nginx-ccs-dev", "seam-tenant-ccs-dev", "queued", 0)
	pe := fakePackExecution("nginx-ccs-dev-ccs-dev", "seam-tenant-ccs-dev")

	client := fake.NewSimpleDynamicClient(scheme, signal, pe)
	handler := NewDriftSignalHandler(client)
	handler.handleOnce(context.Background())

	// PackExecution should still exist — state=queued is not acted on.
	_, err := client.Resource(packExecutionGVR).Namespace("seam-tenant-ccs-dev").Get(
		context.Background(), "nginx-ccs-dev-ccs-dev", metav1.GetOptions{},
	)
	if err != nil {
		t.Error("PackExecution should not be deleted for non-pending DriftSignal")
	}
}
