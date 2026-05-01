package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// rbacPolicyGVR is the GroupVersionResource for RBACPolicy CRs.
// Defined by the Guardian operator in security.ontai.dev.
var rbacPolicyGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "rbacpolicies",
}

// clusterPolicyName is the name of the tenant-scoped RBACPolicy written by guardian
// ClusterRBACPolicyReconciler into seam-tenant-{clusterRef} for the cluster.
// Decision C, guardian-schema.md §7.
const clusterPolicyName = "cluster-policy"

// rbacPolicyFieldManager is the SSA field manager identity used by conductor-agent
// when writing the cluster-policy RBACPolicy to the local cluster.
const rbacPolicyFieldManager = "conductor-agent"

// RBACPolicyPullLoop pulls the cluster-policy RBACPolicy from the management
// cluster (seam-tenant-{clusterRef} namespace) and SSA-patches it into ont-system
// on the local cluster on each tick. This delivers the cluster RBAC policy to the
// target cluster so tenant-cluster Guardian (if deployed) can reference it.
// Decision C, T-17.
type RBACPolicyPullLoop struct {
	mgmtClient dynamic.Interface
	localClient dynamic.Interface
	clusterRef  string
	mgmtNS      string // seam-tenant-{clusterRef} on management cluster
	localNS     string // ont-system on local cluster
}

// NewRBACPolicyPullLoop constructs an RBACPolicyPullLoop for a target cluster.
// mgmtClient reads from the management cluster; localClient writes to the local cluster.
// mgmtNS is the management-side tenant namespace (seam-tenant-{clusterRef}).
// localNS is the local write target (ont-system).
func NewRBACPolicyPullLoop(
	mgmtClient, localClient dynamic.Interface,
	clusterRef, mgmtNS, localNS string,
) *RBACPolicyPullLoop {
	return &RBACPolicyPullLoop{
		mgmtClient:  mgmtClient,
		localClient: localClient,
		clusterRef:  clusterRef,
		mgmtNS:      mgmtNS,
		localNS:     localNS,
	}
}

// Run runs the pull loop until ctx is cancelled. Fires once immediately then
// repeats on interval. interval must be positive (> 0).
func (l *RBACPolicyPullLoop) Run(ctx context.Context, interval time.Duration) {
	l.tickOnce(ctx)

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.tickOnce(ctx)
		}
	}
}

// tickOnce performs a single pull cycle: GET cluster-policy from the management
// cluster and SSA-patch it into ont-system on the local cluster. Non-fatal on
// connectivity loss or management-side absence — retried on next tick.
func (l *RBACPolicyPullLoop) tickOnce(ctx context.Context) {
	policy, err := l.mgmtClient.Resource(rbacPolicyGVR).Namespace(l.mgmtNS).Get(
		ctx, clusterPolicyName, metav1.GetOptions{},
	)
	if err != nil {
		fmt.Printf("rbacpolicy pull loop: cluster=%q get %s/%s: %v\n",
			l.clusterRef, l.mgmtNS, clusterPolicyName, err)
		return
	}

	spec, _, _ := unstructuredNestedMap(policy.Object, "spec")

	payload := map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACPolicy",
		"metadata": map[string]interface{}{
			"name":      clusterPolicyName,
			"namespace": l.localNS,
		},
		"spec": spec,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("rbacpolicy pull loop: cluster=%q marshal RBACPolicy: %v\n", l.clusterRef, err)
		return
	}

	force := true
	if _, err := l.localClient.Resource(rbacPolicyGVR).Namespace(l.localNS).Patch(
		ctx, clusterPolicyName, types.ApplyPatchType, data, metav1.PatchOptions{
			FieldManager: rbacPolicyFieldManager,
			Force:        &force,
		},
	); err != nil {
		fmt.Printf("rbacpolicy pull loop: cluster=%q SSA-patch cluster-policy into %s: %v\n",
			l.clusterRef, l.localNS, err)
		return
	}

	fmt.Printf("rbacpolicy pull loop: cluster=%q cluster-policy applied to %s\n",
		l.clusterRef, l.localNS)
}
