// Package capability — Platform cluster lifecycle capability implementations.
// bootstrap, cluster-reset. conductor-schema.md §6.
package capability

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// talosClusterGVR is the GroupVersionResource for TalosCluster.
// platform.ontai.dev/v1alpha1/talosclusters — platform-schema.md §5.
var talosClusterGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "talosclusters",
}

// clusterResetGVR is the GroupVersionResource for ClusterReset.
// platform.ontai.dev/v1alpha1/clusterresets — platform-schema.md §5.
var clusterResetGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "clusterresets",
}

// bootstrapHandler implements the bootstrap named capability.
// Reads the TalosCluster CR, applies initial machine configs, and bootstraps
// the etcd cluster on the first control plane node. platform-schema.md §5.
type bootstrapHandler struct{}

func (h *bootstrapHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityBootstrap, now, runnerlib.ValidationFailure,
			"bootstrap requires TalosClient and DynamicClient"), nil
	}

	// Read the TalosCluster CR to confirm the bootstrap target.
	// For the management cluster, TalosCluster lives in ont-system.
	// For target clusters, it lives in seam-tenant-{clusterRef}.
	// The execute-mode Job receives the ClusterRef; derive namespace.
	ns := params.Namespace // ont-system for management cluster Jobs
	crList, err := params.DynamicClient.Resource(talosClusterGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityBootstrap, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list TalosCluster in %s: %v", ns, err)), nil
	}

	found := false
	for _, item := range crList.Items {
		name, _, _ := unstructuredString(item.Object, "metadata", "name")
		if name == params.ClusterRef {
			found = true
			break
		}
	}
	if !found {
		return failureResult(runnerlib.CapabilityBootstrap, now, runnerlib.ValidationFailure,
			fmt.Sprintf("TalosCluster %q not found in namespace %s", params.ClusterRef, ns)), nil
	}

	var steps []runnerlib.StepResult

	// Step 1 — Bootstrap etcd on the first control plane node.
	// The talos client is already pointed at the first control plane node via the
	// mounted talosconfig. Bootstrap must be called exactly once.
	step1Start := time.Now().UTC()
	if err := params.TalosClient.Bootstrap(ctx); err != nil {
		return failureResult(runnerlib.CapabilityBootstrap, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("Bootstrap: %v", err)), nil
	}
	steps = append(steps, runnerlib.StepResult{
		Name: "bootstrap-etcd", Status: runnerlib.ResultSucceeded,
		StartedAt: step1Start, CompletedAt: time.Now().UTC(),
		Message: fmt.Sprintf("etcd bootstrapped on first control plane node of %s", params.ClusterRef),
	})

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityBootstrap,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// clusterResetHandler implements the cluster-reset named capability.
// Validates the human approval gate annotation on the ClusterReset CR, then
// issues a factory reset to all cluster nodes via the Talos machine API.
// platform-schema.md §5, CLAUDE.md INV-007.
type clusterResetHandler struct{}

// resetApprovalAnnotation is the annotation that must be set to "true" on a
// ClusterReset CR before the conductor will execute the reset. INV-007.
const resetApprovalAnnotation = "ontai.dev/reset-approved"

func (h *clusterResetHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityClusterReset, now, runnerlib.ValidationFailure,
			"cluster-reset requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(clusterResetGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityClusterReset, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list ClusterReset in %s: %v", ns, err)), nil
	}

	var approved bool
	var gracefulDrain bool
	// Human approval gate: ontai.dev/reset-approved=true must be present.
	// INV-007 -- Destructive operations require affirmative CR with human approval gate.
	if len(crList.Items) > 0 {
		item := crList.Items[0]
		if annMap, ok := item.Object["metadata"].(map[string]interface{}); ok {
			if annBlock, ok := annMap["annotations"].(map[string]interface{}); ok {
				v, _ := annBlock[resetApprovalAnnotation].(string)
				approved = v == "true"
			}
		}
		drainStr, _, _ := unstructuredString(item.Object, "spec", "drainGracePeriodSeconds")
		gracefulDrain = drainStr != "0"
	}

	if !approved {
		return failureResult(runnerlib.CapabilityClusterReset, now, runnerlib.ValidationFailure,
			fmt.Sprintf("cluster-reset blocked: annotation %q=true not present on ClusterReset CR in %s",
				resetApprovalAnnotation, ns)), nil
	}

	// Issue factory reset via Talos API. graceful=true drains workloads first.
	stepStart := time.Now().UTC()
	if err := params.TalosClient.Reset(ctx, gracefulDrain); err != nil {
		return failureResult(runnerlib.CapabilityClusterReset, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("Reset: %v", err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityClusterReset,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{
				Name: "cluster-reset", Status: runnerlib.ResultSucceeded,
				StartedAt: stepStart, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("factory reset issued for cluster %s (graceful=%v)", params.ClusterRef, gracefulDrain),
			},
		},
	}, nil
}
