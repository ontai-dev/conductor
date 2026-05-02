package agent_test

// Unit tests for RBACPolicyPullLoop -- target cluster Conductor pulls
// cluster-policy RBACPolicy from seam-tenant-{cluster} on the management
// cluster and SSA-patches it into ont-system on the local cluster.
// Decision C, T-17.

import (
	"context"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ontai-dev/conductor/internal/agent"
)

// rbacPolicyTestGVR mirrors rbacPolicyGVR from rbacpolicy_pull_loop.go.
var rbacPolicyTestGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "rbacpolicies",
}

// newRBACPolicyMgmtClient returns a fake management cluster client with the
// RBACPolicy GVR registered, pre-populated with the given objects.
func newRBACPolicyMgmtClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{rbacPolicyTestGVR}, objs...)
}

// newRBACPolicyLocalClient returns a fake local cluster client with the
// RBACPolicy GVR registered (empty).
func newRBACPolicyLocalClient() *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{rbacPolicyTestGVR})
}

// buildClusterPolicyObject builds an unstructured cluster-policy RBACPolicy CR.
func buildClusterPolicyObject(namespace string, specMap map[string]interface{}) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "RBACPolicy",
			"metadata": map[string]interface{}{
				"name":      "cluster-policy",
				"namespace": namespace,
			},
			"spec": specMap,
		},
	}
	obj.SetGroupVersionKind(rbacPolicyTestGVR.GroupVersion().WithKind("RBACPolicy"))
	return obj
}

// ── constructor ───────────────────────────────────────────────────────────────

// TestRBACPolicyPullLoop_ConstructsWithoutPanic verifies that NewRBACPolicyPullLoop
// returns a non-nil loop without panicking.
func TestRBACPolicyPullLoop_ConstructsWithoutPanic(t *testing.T) {
	loop := agent.NewRBACPolicyPullLoop(
		newRBACPolicyMgmtClient(), newRBACPolicyLocalClient(),
		"ccs-dev", "seam-tenant-ccs-dev", "ont-system",
	)
	if loop == nil {
		t.Fatal("expected non-nil RBACPolicyPullLoop")
	}
}

// ── tick behaviour ────────────────────────────────────────────────────────────

// TestRBACPolicyPullLoop_AbsentOnMgmt_NoLocalPatch verifies that when
// cluster-policy is absent on the management cluster, no Patch action is
// issued on the local client.
func TestRBACPolicyPullLoop_AbsentOnMgmt_NoLocalPatch(t *testing.T) {
	mgmtClient := newRBACPolicyMgmtClient() // no cluster-policy present
	localClient := newRBACPolicyLocalClient()

	loop := agent.NewRBACPolicyPullLoop(
		mgmtClient, localClient,
		"ccs-dev", "seam-tenant-ccs-dev", "ont-system",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	for _, a := range localClient.Actions() {
		if _, ok := a.(k8stesting.PatchAction); ok {
			t.Errorf("unexpected Patch on local client when mgmt policy absent: %v", a)
		}
	}
}

// TestRBACPolicyPullLoop_PresentOnMgmt_IssuesApplyPatch verifies that when
// cluster-policy exists on the management cluster, a Patch(ApplyPatchType)
// targeting rbacpolicies/cluster-policy in ont-system is issued on the local
// client. Decision C, T-17.
func TestRBACPolicyPullLoop_PresentOnMgmt_IssuesApplyPatch(t *testing.T) {
	spec := map[string]interface{}{
		"permissionSetRef": "cluster-maximum",
	}
	policy := buildClusterPolicyObject("seam-tenant-ccs-dev", spec)

	mgmtClient := newRBACPolicyMgmtClient()
	if _, err := mgmtClient.Resource(rbacPolicyTestGVR).Namespace("seam-tenant-ccs-dev").Create(
		context.Background(), policy, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-populate management RBACPolicy: %v", err)
	}

	localClient := newRBACPolicyLocalClient()

	loop := agent.NewRBACPolicyPullLoop(
		mgmtClient, localClient,
		"ccs-dev", "seam-tenant-ccs-dev", "ont-system",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	var found bool
	for _, a := range localClient.Actions() {
		pa, ok := a.(k8stesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			continue
		}
		if pa.GetResource() != rbacPolicyTestGVR {
			continue
		}
		if pa.GetName() != "cluster-policy" {
			continue
		}
		if pa.GetNamespace() != "ont-system" {
			continue
		}
		found = true
	}
	if !found {
		t.Errorf("expected SSA Patch(ApplyPatchType) on rbacpolicies/cluster-policy in ont-system; actions: %v",
			localClient.Actions())
	}
}

// TestRBACPolicyPullLoop_PatchPayloadContainsSpec verifies that the SSA patch
// body includes the spec field copied from the management cluster policy.
func TestRBACPolicyPullLoop_PatchPayloadContainsSpec(t *testing.T) {
	spec := map[string]interface{}{
		"permissionSetRef": "cluster-maximum",
	}
	policy := buildClusterPolicyObject("seam-tenant-ccs-dev", spec)

	mgmtClient := newRBACPolicyMgmtClient()
	if _, err := mgmtClient.Resource(rbacPolicyTestGVR).Namespace("seam-tenant-ccs-dev").Create(
		context.Background(), policy, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-populate management RBACPolicy: %v", err)
	}

	localClient := newRBACPolicyLocalClient()

	loop := agent.NewRBACPolicyPullLoop(
		mgmtClient, localClient,
		"ccs-dev", "seam-tenant-ccs-dev", "ont-system",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	for _, a := range localClient.Actions() {
		pa, ok := a.(k8stesting.PatchAction)
		if !ok || pa.GetPatchType() != types.ApplyPatchType {
			continue
		}
		patch := string(pa.GetPatch())
		if !strings.Contains(patch, `"spec"`) {
			t.Errorf("SSA patch payload does not contain spec field: %s", patch)
		}
		return
	}
	t.Error("no SSA Apply patch found in local client actions")
}
