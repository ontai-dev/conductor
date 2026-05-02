package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sunstructured "k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
)

// k8sVersionDriftSignalPrefix is the DriftSignal name prefix for Kubernetes version drift signals.
const k8sVersionDriftSignalPrefix = "drift-k8s-version-"

// KubernetesVersionDriftLoop runs on conductor role=tenant. On each cycle it:
//  1. Reads the local InfrastructureTalosCluster in ont-system to get spec.kubernetesVersion.
//  2. Lists Kubernetes Nodes and reads node.status.nodeInfo.kubeletVersion ("v1.32.3").
//  3. If ALL nodes report the same version AND it differs from spec.kubernetesVersion:
//     emits an InfrastructureTalosCluster DriftSignal to the management cluster.
//     Platform's DriftSignalReconciler creates a corrective kube-upgrade UpgradePolicy.
//  4. If observed == spec.kubernetesVersion: confirms any existing K8s version drift signal.
//
// Mixed-version nodes (mid-upgrade) result in no signal being emitted -- the loop
// waits until all nodes converge before signalling. conductor-schema.md §7.10.
type KubernetesVersionDriftLoop struct {
	localClient  dynamic.Interface // tenant cluster client (nodes + InfrastructureTalosCluster)
	mgmtClient   dynamic.Interface // management cluster client (DriftSignal writes)
	clusterRef   string
	namespace    string // ont-system on the tenant cluster
	mgmtTenantNS string // seam-tenant-{clusterRef} on management cluster
}

// NewKubernetesVersionDriftLoop constructs a KubernetesVersionDriftLoop.
func NewKubernetesVersionDriftLoop(localClient, mgmtClient dynamic.Interface, clusterRef, namespace string) *KubernetesVersionDriftLoop {
	return &KubernetesVersionDriftLoop{
		localClient:  localClient,
		mgmtClient:   mgmtClient,
		clusterRef:   clusterRef,
		namespace:    namespace,
		mgmtTenantNS: "seam-tenant-" + clusterRef,
	}
}

// Run runs the loop until ctx is cancelled. Fires once immediately then repeats.
func (l *KubernetesVersionDriftLoop) Run(ctx context.Context, interval time.Duration) {
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

// checkOnce performs one Kubernetes version drift check cycle.
func (l *KubernetesVersionDriftLoop) checkOnce(ctx context.Context) {
	specVersion, err := l.readSpecKubernetesVersion(ctx)
	if err != nil {
		fmt.Printf("k8s version drift loop: cluster=%q read spec kubernetesVersion: %v\n", l.clusterRef, err)
		return
	}
	if specVersion == "" {
		return
	}

	observedVersion, err := l.readObservedKubernetesVersion(ctx)
	if err != nil {
		fmt.Printf("k8s version drift loop: cluster=%q read observed version from nodes: %v\n", l.clusterRef, err)
		return
	}
	if observedVersion == "" {
		// Could not determine a consistent version across all nodes -- mid-upgrade or no nodes.
		return
	}

	signalName := k8sVersionDriftSignalPrefix + l.clusterRef

	if observedVersion == specVersion {
		l.confirmSignalIfPresent(ctx, signalName)
		return
	}

	driftReason := fmt.Sprintf("kubernetes version drift: spec=%s observed=%s", specVersion, observedVersion)
	l.emitDriftSignal(ctx, signalName, observedVersion, specVersion, driftReason)
}

// readSpecKubernetesVersion reads spec.kubernetesVersion from the local InfrastructureTalosCluster.
func (l *KubernetesVersionDriftLoop) readSpecKubernetesVersion(ctx context.Context) (string, error) {
	obj, err := l.localClient.Resource(talosClusterGVR).Namespace(l.namespace).Get(ctx, l.clusterRef, metav1.GetOptions{})
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get InfrastructureTalosCluster %s/%s: %w", l.namespace, l.clusterRef, err)
	}
	spec, _, _ := unstructuredNestedMap(obj.Object, "spec")
	version, _ := spec["kubernetesVersion"].(string)
	return version, nil
}

// readObservedKubernetesVersion lists all nodes and returns the Kubernetes version if all
// nodes agree on the same kubeletVersion. Returns "" if nodes are mixed or absent.
// node.status.nodeInfo.kubeletVersion reports "v1.32.3"; the leading "v" is stripped
// before comparison with spec.kubernetesVersion which uses bare semver ("1.32.3").
func (l *KubernetesVersionDriftLoop) readObservedKubernetesVersion(ctx context.Context) (string, error) {
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
		kubeletVersion, _ := nodeInfo["kubeletVersion"].(string)
		version := strings.TrimPrefix(kubeletVersion, "v")
		if version == "" {
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

// emitDriftSignal writes or updates the InfrastructureTalosCluster K8s DriftSignal
// on the management cluster. Idempotent: creates if absent, increments counter if present.
func (l *KubernetesVersionDriftLoop) emitDriftSignal(ctx context.Context, signalName, observedVersion, specVersion, driftReason string) {
	now := time.Now().UTC().Format(time.RFC3339)

	existing, err := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Get(ctx, signalName, metav1.GetOptions{})
	if err != nil && !k8serrors.IsNotFound(err) {
		fmt.Printf("k8s version drift loop: cluster=%q get DriftSignal: %v\n", l.clusterRef, err)
		return
	}

	if k8serrors.IsNotFound(err) {
		obj := map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata":   map[string]interface{}{"name": signalName, "namespace": l.mgmtTenantNS},
			"spec": map[string]interface{}{
				"state":         "pending",
				"correlationID": fmt.Sprintf("k8s-version-%s-%d", l.clusterRef, time.Now().UnixNano()),
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
			fmt.Printf("k8s version drift loop: cluster=%q create DriftSignal: %v\n", l.clusterRef, cErr)
		}
		fmt.Printf("k8s version drift loop: cluster=%q emitted k8s version drift signal (spec=%s observed=%s)\n",
			l.clusterRef, specVersion, observedVersion)
		return
	}

	spec, _, _ := unstructuredNestedMap(existing.Object, "spec")
	state, _ := spec["state"].(string)
	counter, _ := spec["escalationCounter"].(int64)

	if int32(counter) >= escalationThreshold {
		return
	}

	if state == "confirmed" {
		patch := map[string]interface{}{
			"spec": map[string]interface{}{
				"state":             "pending",
				"driftReason":       driftReason,
				"correlationID":     fmt.Sprintf("k8s-version-%s-%d", l.clusterRef, time.Now().UnixNano()),
				"observedAt":        now,
				"escalationCounter": int64(0),
			},
		}
		data, _ := json.Marshal(patch)
		if _, pErr := l.mgmtClient.Resource(driftSignalGVR).Namespace(l.mgmtTenantNS).Patch(
			ctx, signalName, types.MergePatchType, data, metav1.PatchOptions{},
		); pErr != nil {
			fmt.Printf("k8s version drift loop: cluster=%q reset confirmed DriftSignal: %v\n", l.clusterRef, pErr)
		}
		return
	}

	if state == "queued" {
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
			fmt.Printf("k8s version drift loop: cluster=%q increment k8s drift counter: %v\n", l.clusterRef, pErr)
		}
	}
	// state == "pending": already pending -- do nothing.
}

// confirmSignalIfPresent advances the K8s version drift signal to confirmed if it exists.
func (l *KubernetesVersionDriftLoop) confirmSignalIfPresent(ctx context.Context, signalName string) {
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
		fmt.Printf("k8s version drift loop: cluster=%q confirm DriftSignal: %v\n", l.clusterRef, pErr)
	}
}
