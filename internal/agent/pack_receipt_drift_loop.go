package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// driftSignalGVR is the GroupVersionResource for DriftSignal CRs.
// Written to seam-tenant-{cluster} on the management cluster by conductor role=tenant.
// Reconciled by conductor role=management. conductor-schema.md §7.9.
var driftSignalGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "driftsignals",
}

// clusterPackMgmtGVR is the GroupVersionResource for ClusterPack CRs on the
// management cluster. Used by the drift loop to reconstruct the signed message
// for packSignature verification. conductor-schema.md §7.3.
var clusterPackMgmtGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructureclusterpacks",
}

// escalationThreshold is the maximum number of drift re-emit cycles before the
// drift signal is treated as terminal. conductor-schema.md §7.9.
const escalationThreshold int32 = 3

// PackReceiptDriftLoop runs on conductor role=tenant. On each cycle it:
//  1. Lists InfrastructurePackReceipts in ont-system on the local (tenant) cluster.
//  2. For each receipt with status.verified=false (set by the packinstance pull loop
//     after Ed25519 verification failure): logs a warning and skips drift checking.
//  3. For each receipt with status.verified=true or absent (executor-created receipts):
//     checks that every deployedResource still exists on the local cluster.
//     On any missing resource, writes a DriftSignal to seam-tenant-{clusterName}
//     on the management cluster.
//  4. DriftSignal escalation: increments EscalationCounter on each re-emit cycle.
//     At escalationThreshold the loop stops emitting for that receipt.
//
// Signature verification is handled exclusively by the packinstance pull loop (INV-026).
// The drift loop reads status.verified to gate drift checking. Decision H, conductor-schema.md §7.9.
type PackReceiptDriftLoop struct {
	localClient  dynamic.Interface // tenant cluster client
	mgmtClient   dynamic.Interface // management cluster client
	clusterName  string            // this cluster's name, used for DriftSignal namespace
	namespace    string            // local namespace for PackReceipt CRs (ont-system)
	mgmtTenantNS string            // seam-tenant-{clusterName} on management cluster
}

// NewPackReceiptDriftLoop constructs a PackReceiptDriftLoop. Signature verification
// is delegated to the packinstance pull loop (INV-026); the drift loop reads
// status.verified to gate drift checking.
func NewPackReceiptDriftLoop(localClient, mgmtClient dynamic.Interface, clusterName, namespace string) *PackReceiptDriftLoop {
	return &PackReceiptDriftLoop{
		localClient:  localClient,
		mgmtClient:   mgmtClient,
		clusterName:  clusterName,
		namespace:    namespace,
		mgmtTenantNS: "seam-tenant-" + clusterName,
	}
}

// Run runs the drift loop until ctx is cancelled. Fires once immediately then repeats.
func (l *PackReceiptDriftLoop) Run(ctx context.Context, interval time.Duration) {
	l.runOnce(ctx)
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
			l.runOnce(ctx)
		}
	}
}

// runOnce processes all PackReceipts in one cycle.
func (l *PackReceiptDriftLoop) runOnce(ctx context.Context) {
	list, err := l.localClient.Resource(packReceiptGVR).Namespace(l.namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		// CRD may not be installed yet — not fatal.
		return
	}

	for _, item := range list.Items {
		spec, _, _ := unstructuredNestedMap(item.Object, "spec")
		receiptName := item.GetName()

		// Orphan check: if the referenced ClusterPack no longer exists on the management
		// cluster, tear down all deployed resources and delete this PackReceipt. The
		// conductor owns cleanup of its cluster's resources when the governance record is
		// revoked. Decision H, conductor-schema.md §7.9.
		clusterPackRef, _ := spec["clusterPackRef"].(string)
		if clusterPackRef != "" {
			_, cpErr := l.mgmtClient.Resource(clusterPackMgmtGVR).Namespace(l.mgmtTenantNS).Get(
				ctx, clusterPackRef, metav1.GetOptions{},
			)
			if k8serrors.IsNotFound(cpErr) {
				fmt.Printf("drift loop: cluster=%q receipt=%q ClusterPack %q deleted — orphan teardown\n",
					l.clusterName, receiptName, clusterPackRef)
				l.teardownOrphanedReceipt(ctx, spec, receiptName)
				continue
			}
			if cpErr != nil {
				// Transient connectivity error — do not tear down; retry next cycle.
				fmt.Printf("drift loop: cluster=%q receipt=%q get ClusterPack %q: %v\n",
					l.clusterName, receiptName, clusterPackRef, cpErr)
				continue
			}
		}

		// Gate drift checking on signature verification result written by the
		// packinstance pull loop. status.verified=false means the management cluster
		// signature check failed — skip drift until the packinstance pull loop resolves it.
		// status.verified absent (executor-created receipts) → trusted; proceed to drift check.
		status, _, _ := unstructuredNestedMap(item.Object, "status")
		if verifiedRaw, hasVerified := status["verified"]; hasVerified {
			if verified, _ := verifiedRaw.(bool); !verified {
				fmt.Printf("drift loop: cluster=%q receipt=%q signature not verified — skipping drift check\n",
					l.clusterName, receiptName)
				continue
			}
		}

		if !l.checkDrift(ctx, spec, receiptName) {
			l.resolveSignalIfHealthy(ctx, receiptName)
		}
	}
}

// teardownOrphanedReceipt is called when the ClusterPack referenced by a PackReceipt
// no longer exists on the management cluster. It deletes:
//  1. All cluster-scoped resources listed in spec.deployedResources.
//  2. All namespaces that housed namespace-scoped deployedResources (cascade-deletes
//     everything inside them without needing to enumerate each resource individually).
//  3. The PackReceipt itself.
//  4. Any associated DriftSignal on the management cluster.
//
// Decision H, conductor-schema.md §7.9.
func (l *PackReceiptDriftLoop) teardownOrphanedReceipt(ctx context.Context, spec map[string]interface{}, receiptName string) {
	rawItems, _ := spec["deployedResources"].([]interface{})

	// Collect unique pack-owned namespaces (non-empty namespace field) and
	// cluster-scoped resources (empty namespace field) from deployed resources.
	packNamespaces := map[string]struct{}{}
	type clusterScopedResource struct {
		apiVersion, kind, name string
	}
	var clusterScoped []clusterScopedResource

	for _, raw := range rawItems {
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		ns, _ := item["namespace"].(string)
		apiVersion, _ := item["apiVersion"].(string)
		kind, _ := item["kind"].(string)
		name, _ := item["name"].(string)
		if apiVersion == "" || kind == "" || name == "" {
			continue
		}
		if ns != "" {
			packNamespaces[ns] = struct{}{}
		} else {
			clusterScoped = append(clusterScoped, clusterScopedResource{apiVersion, kind, name})
		}
	}

	// Delete cluster-scoped resources (ClusterRole, ClusterRoleBinding, IngressClass, etc.).
	for _, r := range clusterScoped {
		gvr, err := gvrFromAPIVersionKind(r.apiVersion, r.kind)
		if err != nil {
			fmt.Printf("drift loop: cluster=%q orphan teardown: GVR for %s/%s: %v\n",
				l.clusterName, r.apiVersion, r.kind, err)
			continue
		}
		if delErr := l.localClient.Resource(gvr).Delete(ctx, r.name, metav1.DeleteOptions{}); delErr != nil && !k8serrors.IsNotFound(delErr) {
			fmt.Printf("drift loop: cluster=%q orphan teardown: delete %s %s: %v\n",
				l.clusterName, r.kind, r.name, delErr)
		} else {
			fmt.Printf("drift loop: cluster=%q orphan teardown: deleted %s %s\n",
				l.clusterName, r.kind, r.name)
		}
	}

	// Delete each pack-owned namespace. This cascade-deletes all namespace-scoped
	// resources (Deployments, Services, ConfigMaps, etc.) without enumerating them.
	nsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	for ns := range packNamespaces {
		if delErr := l.localClient.Resource(nsGVR).Delete(ctx, ns, metav1.DeleteOptions{}); delErr != nil && !k8serrors.IsNotFound(delErr) {
			fmt.Printf("drift loop: cluster=%q orphan teardown: delete namespace %s: %v\n",
				l.clusterName, ns, delErr)
		} else {
			fmt.Printf("drift loop: cluster=%q orphan teardown: deleted namespace %s\n",
				l.clusterName, ns)
		}
	}

	if delErr := l.localClient.Resource(packReceiptGVR).Namespace(l.namespace).Delete(
		ctx, receiptName, metav1.DeleteOptions{},
	); delErr != nil && !k8serrors.IsNotFound(delErr) {
		fmt.Printf("drift loop: cluster=%q orphan teardown: delete PackReceipt %s: %v\n",
			l.clusterName, receiptName, delErr)
	} else {
		fmt.Printf("drift loop: cluster=%q orphan teardown: deleted PackReceipt %s\n",
			l.clusterName, receiptName)
	}

	signalName := "drift-" + receiptName
	if delErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Delete(
		ctx, signalName, metav1.DeleteOptions{},
	); delErr != nil && !k8serrors.IsNotFound(delErr) {
		fmt.Printf("drift loop: cluster=%q orphan teardown: delete DriftSignal %s: %v\n",
			l.clusterName, signalName, delErr)
	}
}

// checkDrift reads the deployedResources inventory from the PackReceipt and verifies
// each resource still exists on the local cluster. Returns true when drift is detected
// (first missing resource found), false when all resources exist. Decision H, conductor-schema.md §7.9.
func (l *PackReceiptDriftLoop) checkDrift(ctx context.Context, spec map[string]interface{}, receiptName string) bool {
	rawItems, _ := spec["deployedResources"].([]interface{})
	if len(rawItems) == 0 {
		return false
	}

	clusterPackRef, _ := spec["clusterPackRef"].(string)

	for _, raw := range rawItems {
		item, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		apiVersion, _ := item["apiVersion"].(string)
		kind, _ := item["kind"].(string)
		ns, _ := item["namespace"].(string)
		name, _ := item["name"].(string)

		if apiVersion == "" || kind == "" || name == "" {
			continue
		}

		gvr, err := gvrFromAPIVersionKind(apiVersion, kind)
		if err != nil {
			continue
		}

		var getErr error
		if ns != "" {
			_, getErr = l.localClient.Resource(gvr).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
		} else {
			_, getErr = l.localClient.Resource(gvr).Get(ctx, name, metav1.GetOptions{})
		}

		if k8serrors.IsNotFound(getErr) {
			// If the resource's namespace is Terminating, the resource disappearance is
			// caused by active deletion -- wait for finalizers to release before counting
			// as unresolved drift. Incrementing the counter here would prematurely exhaust
			// the escalation budget before the corrective Job can run.
			if ns != "" && l.namespaceTerminating(ctx, ns) {
				fmt.Printf("drift loop: cluster=%q receipt=%q namespace %q terminating — deferring drift for %s/%s\n",
					l.clusterName, receiptName, ns, kind, name)
				continue
			}
			reason := fmt.Sprintf("resource %s %s/%s/%s missing from cluster", apiVersion, kind, ns, name)
			fmt.Printf("drift loop: cluster=%q receipt=%q drift detected: %s\n",
				l.clusterName, receiptName, reason)
			l.emitDriftSignal(ctx, clusterPackRef, receiptName, apiVersion, kind, ns, name, reason)
			return true // emit one signal per cycle per receipt; recheck next cycle
		}
		if getErr != nil {
			fmt.Printf("drift loop: cluster=%q receipt=%q get %s/%s: %v\n",
				l.clusterName, receiptName, kind, name, getErr)
		}
	}
	return false
}

// namespaceTerminating returns true when the named namespace has a DeletionTimestamp
// set, meaning it is actively being deleted and waiting for finalizers. Resources
// inside a Terminating namespace appear absent before finalizers release; the drift
// loop must not count this transient state as unresolved drift.
func (l *PackReceiptDriftLoop) namespaceTerminating(ctx context.Context, ns string) bool {
	nsGVR := schema.GroupVersionResource{Group: "", Version: "v1", Resource: "namespaces"}
	obj, err := l.localClient.Resource(nsGVR).Get(ctx, ns, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return obj.GetDeletionTimestamp() != nil
}

// emitDriftSignal writes or updates a DriftSignal in seam-tenant-{clusterName} on the
// management cluster. Uses the receiptName as the DriftSignal name for idempotency.
// Increments EscalationCounter on each re-emit. Stops at escalationThreshold.
// Decision H, conductor-schema.md §7.9.
func (l *PackReceiptDriftLoop) emitDriftSignal(
	ctx context.Context,
	clusterPackRef, receiptName, apiVersion, kind, ns, name, reason string,
) {
	signalName := "drift-" + receiptName

	existing, err := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Get(
		ctx, signalName, metav1.GetOptions{},
	)
	if err != nil && !k8serrors.IsNotFound(err) {
		fmt.Printf("drift loop: cluster=%q get DriftSignal %s: %v\n",
			l.clusterName, signalName, err)
		return
	}

	if err == nil {
		// Signal already exists. Check state and escalation counter.
		spec, _, _ := unstructuredNestedMap(existing.Object, "spec")
		state, _ := spec["state"].(string)
		if state == "confirmed" {
			// Previously resolved — delete so fresh drift starts a new signal.
			_ = l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Delete(
				ctx, signalName, metav1.DeleteOptions{},
			)
			return
		}
		if state == "queued" {
			// Retrigger was issued but drift persists — increment counter and re-submit.
			counter, _ := spec["escalationCounter"].(int64)
			if int32(counter) >= escalationThreshold {
				fmt.Printf("drift loop: cluster=%q receipt=%q escalation threshold reached — terminal drift\n",
					l.clusterName, receiptName)
				return
			}
			newCounter := counter + 1
			rePatch := map[string]interface{}{
				"spec": map[string]interface{}{
					"escalationCounter": newCounter,
					"state":             "pending",
				},
			}
			reData, _ := json.Marshal(rePatch)
			if _, pErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
				ctx, signalName, types.MergePatchType, reData, metav1.PatchOptions{},
			); pErr != nil {
				fmt.Printf("drift loop: cluster=%q re-escalate DriftSignal %s: %v\n",
					l.clusterName, signalName, pErr)
			}
			return
		}
		counter, _ := spec["escalationCounter"].(int64)
		if int32(counter) >= escalationThreshold {
			fmt.Printf("drift loop: cluster=%q receipt=%q escalation threshold reached — terminal drift\n",
				l.clusterName, receiptName)
			return
		}
		// Re-emit: increment counter, keep state=pending.
		newCounter := counter + 1
		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"escalationCounter": newCounter,
				"state":             "pending",
			},
		}
		data, _ := json.Marshal(patch)
		if _, patchErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
			ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
		); patchErr != nil {
			fmt.Printf("drift loop: cluster=%q update DriftSignal %s: %v\n",
				l.clusterName, signalName, patchErr)
		}
		return
	}

	// Create new DriftSignal.
	signal := map[string]interface{}{
		"apiVersion": "infrastructure.ontai.dev/v1alpha1",
		"kind":       "DriftSignal",
		"metadata": map[string]interface{}{
			"name":      signalName,
			"namespace": l.mgmtTenantNS,
		},
		"spec": map[string]interface{}{
			"state":             "pending",
			"correlationID":     newCorrelationID(),
			"observedAt":        time.Now().UTC().Format(time.RFC3339),
			"escalationCounter": int32(0),
			"driftReason":       reason,
			"affectedCRRef": map[string]interface{}{
				"group":     groupFromAPIVersion(apiVersion),
				"kind":      kind,
				"namespace": ns,
				"name":      name,
			},
		},
	}
	data, err := json.Marshal(signal)
	if err != nil {
		fmt.Printf("drift loop: cluster=%q marshal DriftSignal: %v\n", l.clusterName, err)
		return
	}
	u := unstructuredFromRaw(data)
	if _, createErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Create(
		ctx,
		&u,
		metav1.CreateOptions{},
	); createErr != nil {
		fmt.Printf("drift loop: cluster=%q create DriftSignal %s: %v\n",
			l.clusterName, signalName, createErr)
		return
	}
	fmt.Printf("drift loop: cluster=%q emitted DriftSignal %s (reason: %s)\n",
		l.clusterName, signalName, reason)
}

// resolveSignalIfHealthy checks whether a DriftSignal for this receipt exists in
// queued state and, if so, sets it to confirmed. Called when checkDrift finds no
// missing resources — the retrigger succeeded and drift is resolved. Decision H.
func (l *PackReceiptDriftLoop) resolveSignalIfHealthy(ctx context.Context, receiptName string) {
	signalName := "drift-" + receiptName
	existing, err := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Get(
		ctx, signalName, metav1.GetOptions{},
	)
	if k8serrors.IsNotFound(err) {
		return
	}
	if err != nil {
		return
	}
	spec, _, _ := unstructuredNestedMap(existing.Object, "spec")
	state, _ := spec["state"].(string)
	if state != "queued" {
		return
	}
	// correlationID is cleared when confirming: the drift event correlation chain is
	// closed once remediation is verified. DriftSignal invariant: confirmed signals carry
	// no correlationID. conductor-schema.md §7.9.
	patch := map[string]interface{}{"spec": map[string]interface{}{
		"state":         "confirmed",
		"correlationID": "",
	}}
	data, _ := json.Marshal(patch)
	if _, pErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
		ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
	); pErr != nil {
		fmt.Printf("drift loop: cluster=%q resolve DriftSignal %s: %v\n",
			l.clusterName, signalName, pErr)
		return
	}
	fmt.Printf("drift loop: cluster=%q receipt=%q drift resolved — DriftSignal confirmed, correlationID cleared\n",
		l.clusterName, receiptName)
}

// gvrFromAPIVersionKind converts an APIVersion + Kind string into a GVR for use
// with the dynamic client. Handles core group (no slash) and named groups.
// This is a best-effort mapping used for resource existence checks only.
func gvrFromAPIVersionKind(apiVersion, kind string) (schema.GroupVersionResource, error) {
	// Derive the plural resource name by lowercasing the kind and appending "s".
	// This covers the common case. Irregular plurals (e.g., Ingress→ingresses) are
	// handled by the trailing switch below.
	resource := pluralizeKind(kind)

	var group, version string
	switch apiVersion {
	case "v1":
		group = ""
		version = "v1"
	default:
		// Named group: "apps/v1", "networking.k8s.io/v1", etc.
		for i := len(apiVersion) - 1; i >= 0; i-- {
			if apiVersion[i] == '/' {
				group = apiVersion[:i]
				version = apiVersion[i+1:]
				break
			}
		}
		if version == "" {
			return schema.GroupVersionResource{}, fmt.Errorf("cannot parse apiVersion %q", apiVersion)
		}
	}

	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}, nil
}

// pluralizeKind returns the lowercase plural form of a Kubernetes Kind.
func pluralizeKind(kind string) string {
	if kind == "" {
		return ""
	}
	lower := strings.ToLower(kind)
	// Handle common irregular plurals.
	switch lower {
	case "ingress":
		return "ingresses"
	case "networkpolicy":
		return "networkpolicies"
	case "storageclass":
		return "storageclasses"
	case "ingressclass":
		return "ingressclasses"
	case "endpointslice":
		return "endpointslices"
	default:
		return lower + "s"
	}
}

// groupFromAPIVersion extracts the API group from an apiVersion string.
// Returns empty string for core group (e.g., "v1").
func groupFromAPIVersion(apiVersion string) string {
	for i := len(apiVersion) - 1; i >= 0; i-- {
		if apiVersion[i] == '/' {
			return apiVersion[:i]
		}
	}
	return ""
}

// newCorrelationID generates a simple correlation ID using the current timestamp.
// A full UUID v4 would require an external package; timestamp provides sufficient
// deduplication for the federation delivery model. conductor-schema.md §7.9.
func newCorrelationID() string {
	return fmt.Sprintf("drift-%d", time.Now().UnixNano())
}

// unstructuredFromRaw constructs an unstructured.Unstructured from raw JSON bytes.
func unstructuredFromRaw(data []byte) unstructured.Unstructured {
	var obj unstructured.Unstructured
	_ = json.Unmarshal(data, &obj.Object)
	return obj
}
