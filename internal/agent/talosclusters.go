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

var talosClusterGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructuretalosclusters",
}

// SetTalosClusterReady patches the Ready condition to True on the
// InfrastructureTalosCluster named clusterName in namespace. Called by
// conductor role=tenant after winning leader election to signal that the
// tenant conductor is operational. seam-core/conditions ConditionTypeReady.
func SetTalosClusterReady(ctx context.Context, dynamicClient dynamic.Interface, namespace, clusterName string) error {
	now := time.Now().UTC().Format(time.RFC3339)
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"conditions": []interface{}{
				map[string]interface{}{
					"type":               "Ready",
					"status":             "True",
					"reason":             "ClusterReady",
					"message":            "Conductor role=tenant is operational.",
					"lastTransitionTime": now,
				},
			},
		},
	}
	data, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal TalosCluster status patch: %w", err)
	}
	_, err = dynamicClient.Resource(talosClusterGVR).Namespace(namespace).Patch(
		ctx,
		clusterName,
		types.MergePatchType,
		data,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		return fmt.Errorf("patch TalosCluster %s/%s status: %w", namespace, clusterName, err)
	}
	return nil
}
