package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

var packExecutionGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurepackexecutions",
}

// DriftSignalHandler runs on conductor role=management. On each cycle it:
//  1. Lists DriftSignal CRs in all seam-tenant-* namespaces.
//  2. For each signal in state=pending:
//     a. If escalationCounter >= escalationThreshold: writes TerminalDrift condition and skips.
//     b. Otherwise: finds the PackExecution for the affected ClusterPack, deletes it so
//        wrapper recreates and retriggers the pack-deploy Job. Sets state=queued.
//
// The retrigger model is delete-PackExecution: wrapper immediately reconciles and
// creates a new PackExecution with gates cleared (all conditions already met).
// Decision H, conductor-schema.md §7.9.
type DriftSignalHandler struct {
	client dynamic.Interface // management cluster client
}

// NewDriftSignalHandler constructs a DriftSignalHandler for the management cluster.
func NewDriftSignalHandler(client dynamic.Interface) *DriftSignalHandler {
	return &DriftSignalHandler{client: client}
}

// Run runs the handler until ctx is cancelled. Fires once immediately then repeats.
func (h *DriftSignalHandler) Run(ctx context.Context, interval time.Duration) {
	h.handleOnce(ctx)
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
			h.handleOnce(ctx)
		}
	}
}

// handleOnce processes all pending DriftSignals across seam-tenant-* namespaces.
func (h *DriftSignalHandler) handleOnce(ctx context.Context) {
	// List across all namespaces; filter to seam-tenant-* below.
	list, err := h.client.Resource(driftSignalGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may not be installed yet — not fatal.
		return
	}

	for _, item := range list.Items {
		ns := item.GetNamespace()
		if !strings.HasPrefix(ns, "seam-tenant-") {
			continue
		}

		spec, _, _ := unstructuredNestedMap(item.Object, "spec")
		state, _ := spec["state"].(string)
		if state != "pending" {
			continue
		}

		signalName := item.GetName()
		counter, _ := spec["escalationCounter"].(int64)

		// InfrastructureTalosCluster version drift signals are handled by platform's
		// DriftSignalReconciler (TCOR write + observedTalosVersion patch). Skip here.
		affectedRef, _, _ := unstructuredNestedMap(spec, "affectedCRRef")
		if kind, _ := affectedRef["kind"].(string); kind == "InfrastructureTalosCluster" {
			continue
		}

		if int32(counter) >= escalationThreshold {
			fmt.Printf("drift handler: signal=%q escalation threshold reached — marking TerminalDrift\n",
				signalName)
			h.setTerminalDrift(ctx, ns, signalName, "escalation threshold reached without resolution")
			continue
		}

		driftReason, _ := spec["driftReason"].(string)

		// Derive cluster name from namespace: seam-tenant-{clusterName}
		clusterName := strings.TrimPrefix(ns, "seam-tenant-")

		// Find and retrigger the PackExecution for this cluster namespace.
		retriggered, jobRef := h.retriggerPackExecution(ctx, ns, clusterName)
		if !retriggered {
			fmt.Printf("drift handler: cluster=%q signal=%q no PackExecution found to retrigger\n",
				clusterName, signalName)
		}

		// Advance state to queued.
		h.advanceToQueued(ctx, ns, signalName, jobRef)

		fmt.Printf("drift handler: cluster=%q signal=%q retriggered (escalation=%d, affected=%v, reason=%q)\n",
			clusterName, signalName, counter, affectedRef, driftReason)
	}
}

// retriggerPackExecution deletes the PackExecution for the given cluster so
// wrapper recreates it and submits a new pack-deploy Job. Returns (true, jobName)
// on success, (false, "") if no PackExecution is found.
func (h *DriftSignalHandler) retriggerPackExecution(ctx context.Context, tenantNS, clusterName string) (bool, string) {
	list, err := h.client.Resource(packExecutionGVR).Namespace(tenantNS).List(ctx, metav1.ListOptions{})
	if err != nil || len(list.Items) == 0 {
		return false, ""
	}

	// Delete the first PackExecution found (the one that delivered the drifted pack).
	pe := list.Items[0]
	spec, _, _ := unstructuredNestedMap(pe.Object, "spec")
	packName, _ := spec["packRef"].(string)
	peName := pe.GetName()

	if delErr := h.client.Resource(packExecutionGVR).Namespace(tenantNS).Delete(
		ctx, peName, metav1.DeleteOptions{},
	); delErr != nil && !k8serrors.IsNotFound(delErr) {
		fmt.Printf("drift handler: delete PackExecution %s/%s: %v\n", tenantNS, peName, delErr)
		return false, ""
	}
	fmt.Printf("drift handler: deleted PackExecution %s/%s (pack=%q) for retrigger\n",
		tenantNS, peName, packName)
	return true, peName
}

// advanceToQueued patches the DriftSignal state from pending to queued and records
// the correctionJobRef if provided. conductor-schema.md §7.9.
func (h *DriftSignalHandler) advanceToQueued(ctx context.Context, ns, signalName, jobRef string) {
	spec := map[string]interface{}{
		"state": "queued",
	}
	if jobRef != "" {
		spec["correctionJobRef"] = jobRef
	}
	patch := map[string]interface{}{"spec": spec}
	data, err := json.Marshal(patch)
	if err != nil {
		fmt.Printf("drift handler: marshal queued patch for %s/%s: %v\n", ns, signalName, err)
		return
	}
	if _, pErr := h.client.Resource(driftSignalGVR).Namespace(ns).Patch(
		ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
	); pErr != nil {
		fmt.Printf("drift handler: advance to queued %s/%s: %v\n", ns, signalName, pErr)
	}
}

// setTerminalDrift writes a TerminalDrift status condition on the DriftSignal and
// stops retriggering. Human intervention is required to resolve. Decision H.
func (h *DriftSignalHandler) setTerminalDrift(ctx context.Context, ns, signalName, reason string) {
	now := time.Now().UTC().Format(time.RFC3339)
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":               "TerminalDrift",
					"status":             "True",
					"reason":             "EscalationThresholdReached",
					"message":            reason,
					"lastTransitionTime": now,
				},
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return
	}
	if _, pErr := h.client.Resource(driftSignalGVR).Namespace(ns).Patch(
		ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{}, "status",
	); pErr != nil {
		fmt.Printf("drift handler: set TerminalDrift on %s/%s: %v\n", ns, signalName, pErr)
	}
}

// packExecutionGVRLocal is identical to packExecutionGVR but declared locally to avoid
// redeclaration if this file is merged with other GVR declarations later.
var _ = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurepackexecutions",
}
