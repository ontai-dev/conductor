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

// rbacProfileGVR is the GroupVersionResource for RBACProfile CRs.
// Defined by the Guardian operator in security.ontai.dev.
var rbacProfileGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "rbacprofiles",
}

// conductorTenantRBACProfileName is the name of the RBACProfile written by guardian
// ClusterRBACPolicyReconciler into seam-tenant-{clusterRef} for the tenant Conductor.
// Decision C, guardian-schema.md §19.
const conductorTenantRBACProfileName = "conductor-tenant"

// rbacProfileFieldManager is the SSA field manager identity used by conductor-agent
// when writing the conductor-tenant RBACProfile to the local cluster.
const rbacProfileFieldManager = "conductor-agent"

// RBACProfilePullLoop pulls the conductor-tenant RBACProfile from the management
// cluster (seam-tenant-{clusterRef} namespace) and SSA-patches it into ont-system
// on the local cluster on each tick. This delivers the Conductor agent's RBAC
// identity declaration to the target cluster without guardian running there.
// Decision C, CONDUCTOR-BL-TENANT-ROLE-RBACPROFILE-DISTRIBUTION.
type RBACProfilePullLoop struct {
	mgmtClient dynamic.Interface
	localClient dynamic.Interface
	clusterRef  string
	mgmtNS      string // seam-tenant-{clusterRef} on management cluster
	localNS     string // ont-system on local cluster
}

// NewRBACProfilePullLoop constructs an RBACProfilePullLoop for a target cluster.
// mgmtClient reads from the management cluster; localClient writes to the local cluster.
// mgmtNS is the management-side tenant namespace (seam-tenant-{clusterRef}).
// localNS is the local write target (ont-system).
func NewRBACProfilePullLoop(
	mgmtClient, localClient dynamic.Interface,
	clusterRef, mgmtNS, localNS string,
) *RBACProfilePullLoop {
	return &RBACProfilePullLoop{
		mgmtClient:  mgmtClient,
		localClient: localClient,
		clusterRef:  clusterRef,
		mgmtNS:      mgmtNS,
		localNS:     localNS,
	}
}

// Run runs the pull loop until ctx is cancelled. Fires once immediately then
// repeats on interval. interval must be positive (> 0).
func (l *RBACProfilePullLoop) Run(ctx context.Context, interval time.Duration) {
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

// tickOnce performs a single pull cycle: GET conductor-tenant from the management
// cluster and SSA-patch it into ont-system on the local cluster. Non-fatal on
// connectivity loss or management-side absence — retried on next tick.
func (l *RBACProfilePullLoop) tickOnce(ctx context.Context) {
	profile, err := l.mgmtClient.Resource(rbacProfileGVR).Namespace(l.mgmtNS).Get(
		ctx, conductorTenantRBACProfileName, metav1.GetOptions{},
	)
	if err != nil {
		fmt.Printf("rbacprofile pull loop: cluster=%q get %s/%s: %v\n",
			l.clusterRef, l.mgmtNS, conductorTenantRBACProfileName, err)
		return
	}

	spec, _, _ := unstructuredNestedMap(profile.Object, "spec")

	payload := map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACProfile",
		"metadata": map[string]interface{}{
			"name":      conductorTenantRBACProfileName,
			"namespace": l.localNS,
		},
		"spec": spec,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		fmt.Printf("rbacprofile pull loop: cluster=%q marshal RBACProfile: %v\n", l.clusterRef, err)
		return
	}

	force := true
	if _, err := l.localClient.Resource(rbacProfileGVR).Namespace(l.localNS).Patch(
		ctx, conductorTenantRBACProfileName, types.ApplyPatchType, data, metav1.PatchOptions{
			FieldManager: rbacProfileFieldManager,
			Force:        &force,
		},
	); err != nil {
		fmt.Printf("rbacprofile pull loop: cluster=%q SSA-patch conductor-tenant into %s: %v\n",
			l.clusterRef, l.localNS, err)
		return
	}

	fmt.Printf("rbacprofile pull loop: cluster=%q conductor-tenant applied to %s\n",
		l.clusterRef, l.localNS)
}
