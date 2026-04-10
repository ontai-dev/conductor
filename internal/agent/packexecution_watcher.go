// Package agent — PackExecutionWatcher creates PackExecution CRs from RunnerConfigs
// that carry the infra.ontai.dev/pack label. Runs on the management cluster Conductor
// only (management role). wrapper-schema.md §9 delivery chain. WS3.
package agent

import (
	"context"
	"fmt"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic"
)

var packWatcherRunnerConfigGVR = schema.GroupVersionResource{
	Group:    "runner.ontai.dev",
	Version:  "v1alpha1",
	Resource: "runnerconfigs",
}

var packExecutionGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "packexecutions",
}

// PackExecutionWatcher watches RunnerConfigs labeled infra.ontai.dev/pack across
// all seam-tenant-* namespaces and creates a corresponding PackExecution CR for
// each RunnerConfig that does not already have one. This is the management cluster
// responsibility declared in wrapper-schema.md §9 (delivery chain).
//
// One PackExecution per RunnerConfig, named {packName}-{clusterName}, in the same
// seam-tenant-{clusterName} namespace. Idempotent: skips if PackExecution exists.
type PackExecutionWatcher struct {
	dynamicClient dynamic.Interface
}

// NewPackExecutionWatcher creates a PackExecutionWatcher backed by the given
// dynamic client. Should only be instantiated on the management cluster.
func NewPackExecutionWatcher(dynamicClient dynamic.Interface) *PackExecutionWatcher {
	return &PackExecutionWatcher{dynamicClient: dynamicClient}
}

// Run starts the PackExecution creation loop. Runs at the given interval until
// leaderCtx is cancelled. Each tick lists all RunnerConfigs labeled
// infra.ontai.dev/pack across all seam-tenant-* namespaces and creates a
// PackExecution for any RunnerConfig that lacks one.
func (w *PackExecutionWatcher) Run(leaderCtx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-leaderCtx.Done():
			return
		case <-ticker.C:
			if err := w.reconcile(leaderCtx); err != nil {
				fmt.Printf("conductor agent: PackExecutionWatcher reconcile error: %v\n", err)
			}
		}
	}
}

// reconcile lists RunnerConfigs with the infra.ontai.dev/pack label across all
// namespaces, filters to seam-tenant-* namespaces, and creates PackExecution CRs
// for those that lack one.
func (w *PackExecutionWatcher) reconcile(ctx context.Context) error {
	rcList, err := w.dynamicClient.Resource(packWatcherRunnerConfigGVR).Namespace("").List(ctx, metav1.ListOptions{
		LabelSelector: "infra.ontai.dev/pack",
	})
	if err != nil {
		return fmt.Errorf("list RunnerConfigs: %w", err)
	}

	for _, rc := range rcList.Items {
		ns := rc.GetNamespace()
		if !strings.HasPrefix(ns, "seam-tenant-") {
			continue
		}

		labels := rc.GetLabels()
		packName := labels["infra.ontai.dev/pack"]
		packVersion := labels["infra.ontai.dev/pack-version"]
		clusterName := labels["platform.ontai.dev/cluster"]
		if packName == "" || clusterName == "" {
			continue
		}

		peNamespace := ns // seam-tenant-{clusterName}
		peName := packName + "-" + clusterName

		// Skip if PackExecution already exists.
		_, err := w.dynamicClient.Resource(packExecutionGVR).Namespace(peNamespace).Get(ctx, peName, metav1.GetOptions{})
		if err == nil {
			continue
		}
		if !k8serrors.IsNotFound(err) {
			fmt.Printf("conductor agent: PackExecutionWatcher: get PackExecution %s/%s: %v\n", peNamespace, peName, err)
			continue
		}

		// Create PackExecution from RunnerConfig. The admissionProfileRef defaults to
		// the cluster name (convention: one RBACProfile per cluster, named after it).
		pe := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": "infra.ontai.dev/v1alpha1",
			"kind":       "PackExecution",
			"metadata": map[string]interface{}{
				"name":      peName,
				"namespace": peNamespace,
				"labels": map[string]interface{}{
					"infra.ontai.dev/pack":        packName,
					"platform.ontai.dev/cluster":  clusterName,
					"infra.ontai.dev/runner-config": rc.GetName(),
				},
			},
			"spec": map[string]interface{}{
				"targetClusterRef":    clusterName,
				"admissionProfileRef": clusterName,
				"clusterPackRef": map[string]interface{}{
					"name":    packName,
					"version": packVersion,
				},
			},
		}}

		if _, err := w.dynamicClient.Resource(packExecutionGVR).Namespace(peNamespace).Create(ctx, pe, metav1.CreateOptions{}); err != nil {
			if k8serrors.IsAlreadyExists(err) {
				continue
			}
			fmt.Printf("conductor agent: PackExecutionWatcher: create PackExecution %s/%s: %v\n", peNamespace, peName, err)
			continue
		}
		fmt.Printf("conductor agent: PackExecutionWatcher: created PackExecution %s/%s (pack=%s version=%s cluster=%s)\n",
			peNamespace, peName, packName, packVersion, clusterName)
	}
	return nil
}
