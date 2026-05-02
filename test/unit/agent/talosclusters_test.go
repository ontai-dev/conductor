package agent_test

// Unit tests for SetTalosClusterReady — conductor role=tenant patches the
// InfrastructureTalosCluster status to Ready=True after winning leader election.
// conductor-schema.md §15, seam-core conditions.ConditionTypeReady.

import (
	"context"
	"encoding/json"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/internal/agent"
)

var itcGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructuretalosclusters",
}

func makeItc(name, namespace string) *unstructured.Unstructured {
	return &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureTalosCluster",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"mode": "import",
				"role": "tenant",
			},
			"status": map[string]interface{}{},
		},
	}
}

func TestSetTalosClusterReady_PatchesStatusReady(t *testing.T) {
	scheme := runtime.NewScheme()
	obj := makeItc("ccs-dev", "ont-system")
	fakeClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		itcGVR: "InfrastructureTalosClusterList",
	}, obj)

	ctx := context.Background()
	if err := agent.SetTalosClusterReady(ctx, fakeClient, "ont-system", "ccs-dev"); err != nil {
		t.Fatalf("SetTalosClusterReady: %v", err)
	}

	// The fake client intercepts Patch with SubResources via the actions list.
	// Verify a patch action was recorded targeting the status subresource.
	actions := fakeClient.Actions()
	if len(actions) == 0 {
		t.Fatal("expected at least one action, got none")
	}
	found := false
	for _, a := range actions {
		if a.GetVerb() == "patch" && a.GetResource() == itcGVR && a.GetSubresource() == "status" {
			found = true
			pa, ok := a.(interface{ GetPatch() []byte })
			if !ok {
				t.Fatal("action is not a PatchAction")
			}
			var patch map[string]interface{}
			if err := json.Unmarshal(pa.GetPatch(), &patch); err != nil {
				t.Fatalf("unmarshal patch: %v", err)
			}
			statusRaw, ok := patch["status"]
			if !ok {
				t.Fatal("patch missing status field")
			}
			statusMap, ok := statusRaw.(map[string]interface{})
			if !ok {
				t.Fatal("status is not a map")
			}
			condsRaw, ok := statusMap["conditions"]
			if !ok {
				t.Fatal("status missing conditions")
			}
			conds, ok := condsRaw.([]interface{})
			if !ok || len(conds) == 0 {
				t.Fatal("conditions is not a non-empty slice")
			}
			cond, ok := conds[0].(map[string]interface{})
			if !ok {
				t.Fatal("condition is not a map")
			}
			if cond["type"] != "Ready" {
				t.Errorf("condition type = %q, want Ready", cond["type"])
			}
			if cond["status"] != "True" {
				t.Errorf("condition status = %q, want True", cond["status"])
			}
			if cond["reason"] != "ClusterReady" {
				t.Errorf("condition reason = %q, want ClusterReady", cond["reason"])
			}
		}
	}
	if !found {
		t.Error("no patch action on status subresource found")
	}
}

func TestSetTalosClusterReady_ReturnsErrorWhenNotFound(t *testing.T) {
	scheme := runtime.NewScheme()
	fakeClient := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		itcGVR: "InfrastructureTalosClusterList",
	})

	// Pre-populate with a different cluster name so the target does not exist.
	other := makeItc("other-cluster", "ont-system")
	if _, err := fakeClient.Resource(itcGVR).Namespace("ont-system").Create(
		context.Background(), other, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("setup: create other: %v", err)
	}

	ctx := context.Background()
	err := agent.SetTalosClusterReady(ctx, fakeClient, "ont-system", "ccs-dev")
	if err == nil {
		t.Fatal("expected error for non-existent TalosCluster, got nil")
	}
}
