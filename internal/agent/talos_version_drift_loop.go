package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sunstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// nodeGVR is the GroupVersionResource for Kubernetes Node objects.
var nodeGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "nodes"}

// talosOSImageRE matches the Talos version string embedded in node.status.nodeInfo.osImage.
// Talos reports: "Talos (v1.7.4)" -- the version is the second capture group.
var talosOSImageRE = regexp.MustCompile(`(?i)talos\s+\((v[\d]+\.[\d]+\.[\d]+[^\)]*)\)`)

// versionDriftSignalName is the DriftSignal name for talos version drift signals.
// One signal per cluster, written into seam-tenant-{clusterRef} on the management cluster.
const versionDriftSignalPrefix = "drift-version-"

// TalosVersionDriftLoop runs on conductor role=tenant. On each cycle it:
//  1. Reads the local InfrastructureTalosCluster in ont-system to get spec.talosVersion.
//  2. Lists Kubernetes Nodes on the tenant cluster and parses the Talos version from
//     node.status.nodeInfo.osImage ("Talos (v1.7.4)").
//  3. If ALL nodes report the same version AND it differs from spec.talosVersion:
//     emits an InfrastructureTalosCluster DriftSignal to the management cluster.
//     Platform's DriftSignalReconciler records the out-of-band change in the TCOR and
//     updates TalosCluster.status.observedTalosVersion.
//  4. If observed == spec.talosVersion: confirms any existing version drift signal.
//
// This loop is the only path through which conductor communicates out-of-band Talos
// version changes (upgrades performed outside ONT awareness) to platform on the
// management cluster. conductor-schema.md §7.10, Decision H.
type TalosVersionDriftLoop struct {
	localClient  dynamic.Interface // tenant cluster client (nodes + InfrastructureTalosCluster)
	mgmtClient   dynamic.Interface // management cluster client (DriftSignal writes)
	clusterRef   string
	namespace    string // ont-system on the tenant cluster
	mgmtTenantNS string // seam-tenant-{clusterRef} on management cluster
}

// NewTalosVersionDriftLoop constructs a TalosVersionDriftLoop.
func NewTalosVersionDriftLoop(localClient, mgmtClient dynamic.Interface, clusterRef, namespace string) *TalosVersionDriftLoop {
	return &TalosVersionDriftLoop{
		localClient:  localClient,
		mgmtClient:   mgmtClient,
		clusterRef:   clusterRef,
		namespace:    namespace,
		mgmtTenantNS: "seam-tenant-" + clusterRef,
	}
}

// Run runs the loop until ctx is cancelled. Fires once immediately then repeats.
func (l *TalosVersionDriftLoop) Run(ctx context.Context, interval time.Duration) {
	l.checkOnce(ctx)
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
			l.checkOnce(ctx)
		}
	}
}

// checkOnce performs one version drift check cycle.
func (l *TalosVersionDriftLoop) checkOnce(ctx context.Context) {
	specVersion, err := l.readSpecTalosVersion(ctx)
	if err != nil {
		fmt.Printf("version drift loop: cluster=%q read spec talosVersion: %v\n", l.clusterRef, err)
		return
	}
	if specVersion == "" {
		// TalosCluster spec.talosVersion not yet set -- skip.
		return
	}

	observedVersion, err := l.readObservedTalosVersion(ctx)
	if err != nil {
		fmt.Printf("version drift loop: cluster=%q read observed version from nodes: %v\n", l.clusterRef, err)
		return
	}
	if observedVersion == "" {
		// Could not determine a consistent version across all nodes -- mid-upgrade or no nodes.
		return
	}

	signalName := versionDriftSignalPrefix + l.clusterRef

	if observedVersion == specVersion {
		// No drift -- confirm any existing signal.
		l.confirmSignalIfPresent(ctx, signalName)
		return
	}

	// Drift detected: emit DriftSignal to management cluster.
	driftReason := fmt.Sprintf("talos version drift: spec=%s observed=%s", specVersion, observedVersion)
	l.emitVersionDriftSignal(ctx, signalName, observedVersion, specVersion, driftReason)
}

// readSpecTalosVersion reads spec.talosVersion from the local InfrastructureTalosCluster.
func (l *TalosVersionDriftLoop) readSpecTalosVersion(ctx context.Context) (string, error) {
	obj, err := l.localClient.Resource(talosClusterGVR).Namespace(l.namespace).Get(ctx, l.clusterRef, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get InfrastructureTalosCluster %s/%s: %w", l.namespace, l.clusterRef, err)
	}
	spec, _, _ := unstructuredNestedMap(obj.Object, "spec")
	version, _ := spec["talosVersion"].(string)
	return version, nil
}

// readObservedTalosVersion lists all nodes and returns the Talos version if all nodes
// agree on the same version. Returns "" if nodes are mixed (mid-upgrade) or absent.
func (l *TalosVersionDriftLoop) readObservedTalosVersion(ctx context.Context) (string, error) {
	list, err := l.localClient.Resource(nodeGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", fmt.Errorf("list nodes: %w", err)
	}
	if len(list.Items) == 0 {
		return "", nil
	}

	var agreed string
	for _, item := range list.Items {
		status, _, _ := unstructuredNestedMap(item.Object, "status")
		nodeInfo, _, _ := unstructuredNestedMap(status, "nodeInfo")
		osImage, _ := nodeInfo["osImage"].(string)
		version := ParseTalosVersionFromOSImage(osImage)
		if version == "" {
			// Node does not report a Talos version -- not a Talos node, skip.
			continue
		}
		if agreed == "" {
			agreed = version
		} else if agreed != version {
			// Mixed versions -- mid-upgrade in progress. Do not signal.
			return "", nil
		}
	}
	return agreed, nil
}

// ParseTalosVersionFromOSImage extracts the Talos version from node.status.nodeInfo.osImage.
// Talos reports: "Talos (v1.7.4)". Returns "" if the format does not match.
// Exported for testing.
func ParseTalosVersionFromOSImage(osImage string) string {
	m := talosOSImageRE.FindStringSubmatch(osImage)
	if len(m) < 2 {
		return ""
	}
	return m[1]
}

// emitVersionDriftSignal writes or updates the InfrastructureTalosCluster DriftSignal
// on the management cluster. Idempotent: creates if absent, increments counter if present.
func (l *TalosVersionDriftLoop) emitVersionDriftSignal(ctx context.Context, signalName, observedVersion, specVersion, driftReason string) {
	now := time.Now().UTC().Format(time.RFC3339)

	existing, err := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Get(ctx, signalName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		fmt.Printf("version drift loop: cluster=%q get DriftSignal: %v\n", l.clusterRef, err)
		return
	}

	if k8serrors.IsNotFound(err) {
		// First emission: create the signal.
		obj := map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata":   map[string]interface{}{"name": signalName, "namespace": l.mgmtTenantNS},
			"spec": map[string]interface{}{
				"state":         "pending",
				"correlationID": fmt.Sprintf("version-%s-%d", l.clusterRef, time.Now().UnixNano()),
				"observedAt":    now,
				"driftReason":   driftReason,
				"affectedCRRef": map[string]interface{}{
					"group":     "infrastructure.ontai.dev",
					"kind":      "InfrastructureTalosCluster",
					"namespace": l.namespace,
					"name":      l.clusterRef,
				},
				"escalationCounter": int64(0),
			},
		}
		if _, cErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Create(
			ctx, &k8sunstructured.Unstructured{Object: obj}, metav1.CreateOptions{},
		); cErr != nil {
			fmt.Printf("version drift loop: cluster=%q create DriftSignal: %v\n", l.clusterRef, cErr)
		}
		fmt.Printf("version drift loop: cluster=%q emitted version drift signal (spec=%s observed=%s)\n",
			l.clusterRef, specVersion, observedVersion)
		return
	}

	// Signal already present -- check current state.
	spec, _, _ := unstructuredNestedMap(existing.Object, "spec")
	state, _ := spec["state"].(string)
	counter, _ := spec["escalationCounter"].(int64)

	if int32(counter) >= escalationThreshold {
		// Terminal -- stop emitting.
		return
	}

	if state == "confirmed" {
		// Previous drift was confirmed as resolved; drift returned -- reset and re-emit.
		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"state":             "pending",
				"driftReason":       driftReason,
				"correlationID":     fmt.Sprintf("version-%s-%d", l.clusterRef, time.Now().UnixNano()),
				"observedAt":        now,
				"escalationCounter": int64(0),
			},
		}
		data, _ := json.Marshal(patch)
		if _, pErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
			ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
		); pErr != nil {
			fmt.Printf("version drift loop: cluster=%q reset confirmed DriftSignal: %v\n", l.clusterRef, pErr)
		}
		return
	}

	if state == "queued" {
		// Management has accepted but drift persists -- increment counter and reset to pending.
		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"state":             "pending",
				"driftReason":       driftReason,
				"escalationCounter": counter + 1,
			},
		}
		data, _ := json.Marshal(patch)
		if _, pErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
			ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
		); pErr != nil {
			fmt.Printf("version drift loop: cluster=%q increment version drift counter: %v\n", l.clusterRef, pErr)
		}
	}
	// state == "pending": already pending, management has not yet processed it -- do nothing.
}

// confirmSignalIfPresent advances the version drift signal to confirmed if it exists.
func (l *TalosVersionDriftLoop) confirmSignalIfPresent(ctx context.Context, signalName string) {
	existing, err := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Get(ctx, signalName, metav1.GetOptions{})
	if err != nil {
		return
	}
	spec, _, _ := unstructuredNestedMap(existing.Object, "spec")
	state, _ := spec["state"].(string)
	if state == "confirmed" || state == "" {
		return
	}
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"state":         "confirmed",
			"correlationID": "",
		},
	}
	data, _ := json.Marshal(patch)
	if _, pErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
		ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
	); pErr != nil {
		fmt.Printf("version drift loop: cluster=%q confirm DriftSignal: %v\n", l.clusterRef, pErr)
	}
}

// extractObservedVersionFromDriftReason parses the observed talos version out of a
// driftReason string produced by TalosVersionDriftLoop. Format:
// "talos version drift: spec={x} observed={y}"
// Returns "" if the format does not match.
func extractObservedVersionFromDriftReason(driftReason string) string {
	for _, part := range strings.Split(driftReason, " ") {
		if strings.HasPrefix(part, "observed=") {
			return strings.TrimPrefix(part, "observed=")
		}
	}
	return ""
}
