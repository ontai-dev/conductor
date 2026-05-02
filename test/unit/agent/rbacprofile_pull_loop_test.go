package agent_test

// Unit tests for RBACProfilePullLoop — target cluster Conductor pulls
// conductor-tenant RBACProfile from seam-tenant-{cluster} on the management
// cluster and SSA-patches it into ont-system on the local cluster.
// Decision C, CONDUCTOR-BL-TENANT-ROLE-RBACPROFILE-DISTRIBUTION.

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

// rbacProfileTestGVR mirrors rbacProfileGVR from rbacprofile_pull_loop.go.
var rbacProfileTestGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "rbacprofiles",
}

// newRBACProfileMgmtClient returns a fake management cluster client with the
// RBACProfile GVR registered, pre-populated with the given objects.
func newRBACProfileMgmtClient(objs ...runtime.Object) *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{rbacProfileTestGVR}, objs...)
}

// newRBACProfileLocalClient returns a fake local cluster client with the
// RBACProfile GVR registered (empty).
func newRBACProfileLocalClient() *dynamicfake.FakeDynamicClient {
	return newFakeDynamicClientWithGVRs([]schema.GroupVersionResource{rbacProfileTestGVR})
}

// buildConductorTenantProfile builds an unstructured conductor-tenant RBACProfile CR.
func buildConductorTenantProfile(namespace string, specMap map[string]interface{}) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "Rbacprofile",
			"metadata": map[string]interface{}{
				"name":      "conductor-tenant",
				"namespace": namespace,
			},
			"spec": specMap,
		},
	}
	obj.SetGroupVersionKind(rbacProfileTestGVR.GroupVersion().WithKind("Rbacprofile"))
	return obj
}

// ── constructor ───────────────────────────────────────────────────────────────

// TestRBACProfilePullLoop_ConstructsWithoutPanic verifies that NewRBACProfilePullLoop
// returns a non-nil loop without panicking.
func TestRBACProfilePullLoop_ConstructsWithoutPanic(t *testing.T) {
	loop := agent.NewRBACProfilePullLoop(
		newRBACProfileMgmtClient(), newRBACProfileLocalClient(),
		"ccs-dev", "seam-tenant-ccs-dev", "ont-system",
	)
	if loop == nil {
		t.Fatal("expected non-nil RBACProfilePullLoop")
	}
}

// ── tick behaviour ────────────────────────────────────────────────────────────

// TestRBACProfilePullLoop_AbsentOnMgmt_NoLocalPatch verifies that when
// conductor-tenant is absent on the management cluster, no Patch action is
// issued on the local client. Non-fatal connectivity/absence handling.
func TestRBACProfilePullLoop_AbsentOnMgmt_NoLocalPatch(t *testing.T) {
	mgmtClient := newRBACProfileMgmtClient() // no conductor-tenant present
	localClient := newRBACProfileLocalClient()

	loop := agent.NewRBACProfilePullLoop(
		mgmtClient, localClient,
		"ccs-dev", "seam-tenant-ccs-dev", "ont-system",
	)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	loop.Run(ctx, 0)

	for _, a := range localClient.Actions() {
		if _, ok := a.(k8stesting.PatchAction); ok {
			t.Errorf("unexpected Patch on local client when mgmt profile absent: %v", a)
		}
	}
}

// TestRBACProfilePullLoop_PresentOnMgmt_IssuesApplyPatch verifies that when
// conductor-tenant exists on the management cluster, a Patch(ApplyPatchType)
// targeting rbacprofiles/conductor-tenant in ont-system is issued on the local
// client. Decision C, CONDUCTOR-BL-TENANT-ROLE-RBACPROFILE-DISTRIBUTION.
func TestRBACProfilePullLoop_PresentOnMgmt_IssuesApplyPatch(t *testing.T) {
	spec := map[string]interface{}{
		"permissionDeclarations": []interface{}{
			map[string]interface{}{"permissionSetRef": "management-maximum"},
		},
	}
	profile := buildConductorTenantProfile("seam-tenant-ccs-dev", spec)

	mgmtClient := newRBACProfileMgmtClient()
	if _, err := mgmtClient.Resource(rbacProfileTestGVR).Namespace("seam-tenant-ccs-dev").Create(
		context.Background(), profile, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-populate management RBACProfile: %v", err)
	}

	localClient := newRBACProfileLocalClient()

	loop := agent.NewRBACProfilePullLoop(
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
		if pa.GetResource() != rbacProfileTestGVR {
			continue
		}
		if pa.GetName() != "conductor-tenant" {
			continue
		}
		if pa.GetNamespace() != "ont-system" {
			continue
		}
		found = true
	}
	if !found {
		t.Errorf("expected SSA Patch(ApplyPatchType) on rbacprofiles/conductor-tenant in ont-system; actions: %v",
			localClient.Actions())
	}
}

// TestRBACProfilePullLoop_PatchPayloadContainsSpec verifies that the SSA patch
// body includes the spec field copied from the management cluster profile.
func TestRBACProfilePullLoop_PatchPayloadContainsSpec(t *testing.T) {
	spec := map[string]interface{}{
		"permissionDeclarations": []interface{}{
			map[string]interface{}{"permissionSetRef": "cluster-maximum"},
		},
	}
	profile := buildConductorTenantProfile("seam-tenant-ccs-dev", spec)

	mgmtClient := newRBACProfileMgmtClient()
	if _, err := mgmtClient.Resource(rbacProfileTestGVR).Namespace("seam-tenant-ccs-dev").Create(
		context.Background(), profile, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-populate management RBACProfile: %v", err)
	}

	localClient := newRBACProfileLocalClient()

	loop := agent.NewRBACProfilePullLoop(
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
