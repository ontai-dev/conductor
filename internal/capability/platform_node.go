// Package capability — Platform node operation capability implementations.
// node-patch, node-scale-up, node-decommission, node-reboot. conductor-schema.md §6.
package capability

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// nodeMaintenanceGVR is the GroupVersionResource for NodeMaintenance.
// platform.ontai.dev/v1alpha1/nodemaintenances — platform-schema.md §5.
var nodeMaintenanceGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "nodemaintenances",
}

// nodeOperationGVR is the GroupVersionResource for NodeOperation.
// platform.ontai.dev/v1alpha1/nodeoperations — platform-schema.md §5.
var nodeOperationGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "nodeoperations",
}

// nodePatchHandler implements the node-patch named capability.
// Reads the NodeMaintenance CR for a machine config patch and applies it via
// the Talos ApplyConfiguration API. platform-schema.md §5.
type nodePatchHandler struct{}

func (h *nodePatchHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.KubeClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityNodePatch, now, runnerlib.ValidationFailure,
			"node-patch requires TalosClient, KubeClient, and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(nodeMaintenanceGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityNodePatch, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list NodeMaintenance in %s: %v", ns, err)), nil
	}

	var patchSecretName, patchMode string
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "patch" {
			continue
		}
		patchSecretName, _, _ = unstructuredString(item.Object, "spec", "patchSecretRef", "name")
		patchMode, _, _ = unstructuredString(item.Object, "spec", "patchMode")
		if patchMode == "" {
			patchMode = "no-reboot"
		}
		break
	}

	if patchSecretName == "" {
		return failureResult(runnerlib.CapabilityNodePatch, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no NodeMaintenance CR with operation=patch and patchSecretRef found in %s", ns)), nil
	}

	// Read the machine config patch from the Secret.
	secret, err := params.KubeClient.CoreV1().Secrets(ns).Get(ctx, patchSecretName, metav1.GetOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityNodePatch, now, runnerlib.ExternalDependencyFailure,
			fmt.Sprintf("get patch Secret %s/%s: %v", ns, patchSecretName, err)), nil
	}

	patchBytes, ok := secret.Data["machineconfig"]
	if !ok || len(patchBytes) == 0 {
		return failureResult(runnerlib.CapabilityNodePatch, now, runnerlib.ValidationFailure,
			fmt.Sprintf("patch Secret %s/%s missing 'machineconfig' key", ns, patchSecretName)), nil
	}

	// Apply the machine config patch via Talos API.
	stepStart := time.Now().UTC()
	if err := params.TalosClient.ApplyConfiguration(ctx, patchBytes, patchMode); err != nil {
		return failureResult(runnerlib.CapabilityNodePatch, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("ApplyConfiguration (mode=%s): %v", patchMode, err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityNodePatch,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{Name: "apply-configuration", Status: runnerlib.ResultSucceeded, StartedAt: stepStart, CompletedAt: time.Now().UTC()},
		},
	}, nil
}

// nodeScaleUpHandler implements the node-scale-up named capability.
// Reads the NodeOperation CR for the target replica count and provisions
// additional Talos nodes. Only applies to capi.enabled=false clusters.
// platform-schema.md §5 (NodeOperation.spec.operation=scale-up).
type nodeScaleUpHandler struct{}

func (h *nodeScaleUpHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.KubeClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityNodeScaleUp, now, runnerlib.ValidationFailure,
			"node-scale-up requires KubeClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(nodeOperationGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityNodeScaleUp, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list NodeOperation in %s: %v", ns, err)), nil
	}

	var replicaCount int64
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "scale-up" {
			continue
		}
		rc, _, _ := unstructuredInt64(item.Object, "spec", "replicaCount")
		replicaCount = rc
		break
	}

	if replicaCount == 0 {
		return failureResult(runnerlib.CapabilityNodeScaleUp, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no NodeOperation CR with operation=scale-up and replicaCount>0 found in %s", ns)), nil
	}

	// Verify the target replica count is achievable: count existing nodes.
	nodes, err := params.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("ontai.dev/cluster=%s", params.ClusterRef),
	})
	if err != nil {
		// Fall back to listing all nodes if label not present.
		nodes, err = params.KubeClient.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
		if err != nil {
			return failureResult(runnerlib.CapabilityNodeScaleUp, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("list Nodes: %v", err)), nil
		}
	}
	currentCount := int64(len(nodes.Items))
	if currentCount >= replicaCount {
		// Already at or above target replica count — success, nothing to do.
		return runnerlib.OperationResultSpec{
			Capability:  runnerlib.CapabilityNodeScaleUp,
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   now,
			CompletedAt: time.Now().UTC(),
			Artifacts:   []runnerlib.ArtifactRef{},
			Steps: []runnerlib.StepResult{
				{
					Name: "scale-up", Status: runnerlib.ResultSucceeded,
					StartedAt: now, CompletedAt: time.Now().UTC(),
					Message: fmt.Sprintf("current node count %d already meets target %d", currentCount, replicaCount),
				},
			},
		}, nil
	}

	// Hardware provisioning for additional nodes is infrastructure-specific and
	// performed by the Seam Infrastructure Provider (platform-schema.md §4).
	// This executor signals the required count to the infrastructure plane
	// by patching a Node provisioning request ConfigMap in the tenant namespace.
	cmName := fmt.Sprintf("node-scale-up-%s", params.ClusterRef)
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      cmName,
			Namespace: ns,
			Labels:    map[string]string{"ontai.dev/capability": "node-scale-up"},
		},
		Data: map[string]string{
			"targetReplicaCount": fmt.Sprintf("%d", replicaCount),
		},
	}
	if _, createErr := params.KubeClient.CoreV1().ConfigMaps(ns).Create(ctx, cm, metav1.CreateOptions{}); createErr != nil {
		if k8serrors.IsAlreadyExists(createErr) {
			// Update existing.
			existing, getErr := params.KubeClient.CoreV1().ConfigMaps(ns).Get(ctx, cmName, metav1.GetOptions{})
			if getErr == nil {
				existing.Data = cm.Data
				_, createErr = params.KubeClient.CoreV1().ConfigMaps(ns).Update(ctx, existing, metav1.UpdateOptions{})
			}
		}
		if createErr != nil {
			return failureResult(runnerlib.CapabilityNodeScaleUp, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("write scale-up request ConfigMap: %v", createErr)), nil
		}
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityNodeScaleUp,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts: []runnerlib.ArtifactRef{
			{Name: "scale-up-request", Kind: "ConfigMap", Reference: fmt.Sprintf("%s/%s", ns, cmName)},
		},
		Steps: []runnerlib.StepResult{
			{
				Name: "emit-scale-up-request", Status: runnerlib.ResultSucceeded,
				StartedAt: now, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("scale-up request written for %d target replicas (current: %d)", replicaCount, currentCount),
			},
		},
	}, nil
}

// nodeDecommissionHandler implements the node-decommission named capability.
// Drains workloads from the target nodes via the Kubernetes API and resets
// them via the Talos machine API. platform-schema.md §5.
type nodeDecommissionHandler struct{}

func (h *nodeDecommissionHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.KubeClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityNodeDecommission, now, runnerlib.ValidationFailure,
			"node-decommission requires TalosClient, KubeClient, and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(nodeOperationGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityNodeDecommission, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list NodeOperation in %s: %v", ns, err)), nil
	}

	var targetNodes []string
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "decommission" {
			continue
		}
		nodesRaw, _, _ := unstructuredList(item.Object, "spec", "targetNodes")
		for _, n := range nodesRaw {
			if s, ok := n.(string); ok {
				targetNodes = append(targetNodes, s)
			}
		}
		break
	}

	if len(targetNodes) == 0 {
		return failureResult(runnerlib.CapabilityNodeDecommission, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no NodeOperation CR with operation=decommission and targetNodes found in %s", ns)), nil
	}

	var steps []runnerlib.StepResult

	// Cordon and drain each target node.
	for _, nodeName := range targetNodes {
		stepStart := time.Now().UTC()

		node, getErr := params.KubeClient.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if getErr != nil {
			return failureResult(runnerlib.CapabilityNodeDecommission, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("get node %s: %v", nodeName, getErr)), nil
		}

		// Cordon the node.
		node.Spec.Unschedulable = true
		if _, updateErr := params.KubeClient.CoreV1().Nodes().Update(ctx, node, metav1.UpdateOptions{}); updateErr != nil {
			return failureResult(runnerlib.CapabilityNodeDecommission, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("cordon node %s: %v", nodeName, updateErr)), nil
		}
		steps = append(steps, runnerlib.StepResult{
			Name: fmt.Sprintf("cordon:%s", nodeName), Status: runnerlib.ResultSucceeded,
			StartedAt: stepStart, CompletedAt: time.Now().UTC(),
		})
	}

	// Reset each node via Talos (graceful=true drains remaining pods).
	for _, nodeName := range targetNodes {
		stepStart := time.Now().UTC()
		if err := params.TalosClient.Reset(ctx, true); err != nil {
			return failureResult(runnerlib.CapabilityNodeDecommission, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("reset node %s: %v", nodeName, err)), nil
		}
		steps = append(steps, runnerlib.StepResult{
			Name: fmt.Sprintf("reset:%s", nodeName), Status: runnerlib.ResultSucceeded,
			StartedAt: stepStart, CompletedAt: time.Now().UTC(),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityNodeDecommission,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// nodeRebootHandler implements the node-reboot named capability.
// Reboots target nodes via the Talos machine API. platform-schema.md §5.
type nodeRebootHandler struct{}

func (h *nodeRebootHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityNodeReboot, now, runnerlib.ValidationFailure,
			"node-reboot requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(nodeOperationGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityNodeReboot, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list NodeOperation in %s: %v", ns, err)), nil
	}

	var targetNodes []string
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "reboot" {
			continue
		}
		nodesRaw, _, _ := unstructuredList(item.Object, "spec", "targetNodes")
		for _, n := range nodesRaw {
			if s, ok := n.(string); ok {
				targetNodes = append(targetNodes, s)
			}
		}
		break
	}

	if len(targetNodes) == 0 {
		return failureResult(runnerlib.CapabilityNodeReboot, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no NodeOperation CR with operation=reboot and targetNodes found in %s", ns)), nil
	}

	var steps []runnerlib.StepResult
	for _, nodeName := range targetNodes {
		stepStart := time.Now().UTC()
		if err := params.TalosClient.Reboot(ctx); err != nil {
			return failureResult(runnerlib.CapabilityNodeReboot, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("reboot node %s: %v", nodeName, err)), nil
		}
		steps = append(steps, runnerlib.StepResult{
			Name: fmt.Sprintf("reboot:%s", nodeName), Status: runnerlib.ResultSucceeded,
			StartedAt: stepStart, CompletedAt: time.Now().UTC(),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityNodeReboot,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// unstructuredInt64 extracts an int64 from nested unstructured maps.
func unstructuredInt64(obj map[string]interface{}, keys ...string) (int64, bool, error) {
	cur := obj
	for i, k := range keys {
		v, ok := cur[k]
		if !ok {
			return 0, false, nil
		}
		if i == len(keys)-1 {
			switch n := v.(type) {
			case int64:
				return n, true, nil
			case float64:
				return int64(n), true, nil
			case int:
				return int64(n), true, nil
			}
			return 0, false, nil
		}
		cur, ok = v.(map[string]interface{})
		if !ok {
			return 0, false, fmt.Errorf("expected map at key %q", k)
		}
	}
	return 0, false, nil
}

// Ensure corev1 is used (for node cordon via Nodes().Update()).
