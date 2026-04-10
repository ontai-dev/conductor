// Package capability — Wrapper domain capability implementations.
// pack-deploy: fetches a ClusterPack OCI artifact and applies it to the target
// cluster via server-side apply. conductor-schema.md §6, wrapper-schema.md §4.
package capability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// packExecutionGVR is the GroupVersionResource for PackExecution.
// infra.ontai.dev/v1alpha1/packexecutions — wrapper-schema.md §4.
var packExecutionGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "packexecutions",
}

// clusterPackGVR is the GroupVersionResource for ClusterPack.
// infra.ontai.dev/v1alpha1/clusterpacks — wrapper-schema.md §4.
var clusterPackGVR = schema.GroupVersionResource{
	Group:    "infra.ontai.dev",
	Version:  "v1alpha1",
	Resource: "clusterpacks",
}

// packDeployHandler implements the pack-deploy named capability.
// Fetches the ClusterPack OCI artifact, verifies the content checksum, and
// applies all manifests to the target cluster via server-side apply.
// conductor-schema.md §6, wrapper-schema.md §4.
type packDeployHandler struct{}

func (h *packDeployHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.OCIClient == nil || params.KubeClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			"pack-deploy requires OCIClient, KubeClient, and DynamicClient"), nil
	}

	// Read the PackExecution CR to get the ClusterPack reference.
	// PackExecutions live in seam-tenant-{clusterRef}. wrapper-schema.md §4.
	peTenantNS := "seam-tenant-" + params.ClusterRef
	peList, err := params.DynamicClient.Resource(packExecutionGVR).Namespace(peTenantNS).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list PackExecution in %s: %v", peTenantNS, err)), nil
	}

	var clusterPackName, clusterPackVersion string
	for _, item := range peList.Items {
		targetCluster, _, _ := unstructuredString(item.Object, "spec", "targetClusterRef")
		if targetCluster != params.ClusterRef {
			continue
		}
		clusterPackName, _, _ = unstructuredString(item.Object, "spec", "clusterPackRef", "name")
		clusterPackVersion, _, _ = unstructuredString(item.Object, "spec", "clusterPackRef", "version")
		break
	}

	if clusterPackName == "" {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no PackExecution CR targeting cluster %q found in %s", params.ClusterRef, peTenantNS)), nil
	}

	// Read the ClusterPack to get the OCI registry reference and checksum.
	cpList, err := params.DynamicClient.Resource(clusterPackGVR).Namespace("infra-system").
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list ClusterPack in infra-system: %v", err)), nil
	}

	var ociRef, expectedChecksum string
	for _, item := range cpList.Items {
		name, _, _ := unstructuredString(item.Object, "metadata", "name")
		version, _, _ := unstructuredString(item.Object, "spec", "version")
		if name != clusterPackName || version != clusterPackVersion {
			continue
		}
		ociRef, _, _ = unstructuredString(item.Object, "spec", "registryRef", "digest")
		if ociRef == "" {
			ociRef, _, _ = unstructuredString(item.Object, "spec", "registryRef", "url")
		}
		expectedChecksum, _, _ = unstructuredString(item.Object, "spec", "checksum")
		break
	}

	if ociRef == "" {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
			fmt.Sprintf("ClusterPack %s/%s has no registryRef", clusterPackName, clusterPackVersion)), nil
	}

	// Step 1 — Fetch manifests from OCI registry.
	step1Start := time.Now().UTC()
	manifests, err := params.OCIClient.PullManifests(ctx, ociRef)
	if err != nil {
		return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExternalDependencyFailure,
			fmt.Sprintf("PullManifests(%s): %v", ociRef, err)), nil
	}
	step1End := time.Now().UTC()

	// Verify content checksum if provided by the ClusterPack CR.
	if expectedChecksum != "" {
		actualChecksum := computeManifestChecksum(manifests)
		if actualChecksum != expectedChecksum {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ValidationFailure,
				fmt.Sprintf("checksum mismatch: expected %s, got %s", expectedChecksum, actualChecksum)), nil
		}
	}

	// Step 2 — Apply manifests via server-side apply.
	step2Start := time.Now().UTC()
	appliedCount := 0
	for i, manifest := range manifests {
		if len(bytes.TrimSpace(manifest)) == 0 {
			continue
		}
		// Extract the GVK and name from the manifest to determine the API endpoint.
		var obj map[string]interface{}
		if err := json.Unmarshal(manifest, &obj); err != nil {
			// Try YAML — not supported in pure Go without a YAML library.
			// The schema guarantees ClusterPack artifacts are JSON (conductor-schema.md §9).
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("unmarshal manifest[%d]: %v", i, err)), nil
		}

		apiVersion, _, _ := unstructuredStringFromMap(obj, "apiVersion")
		kind, _, _ := unstructuredStringFromMap(obj, "kind")
		meta, _ := obj["metadata"].(map[string]interface{})
		name, _ := meta["name"].(string)
		namespace, _ := meta["namespace"].(string)

		if apiVersion == "" || kind == "" || name == "" {
			continue
		}

		// Server-side apply via the dynamic client. This is the standard pattern
		// for applying pre-rendered manifests without client-side merge logic.
		gvr := gvrFromAPIVersionKind(apiVersion, kind)
		patch, _ := json.Marshal(obj)

		var applyErr error
		if namespace != "" {
			_, applyErr = params.DynamicClient.Resource(gvr).Namespace(namespace).
				Patch(ctx, name, types.ApplyPatchType, patch, metav1.PatchOptions{
					FieldManager: "conductor-pack-deploy",
				})
		} else {
			_, applyErr = params.DynamicClient.Resource(gvr).
				Patch(ctx, name, types.ApplyPatchType, patch, metav1.PatchOptions{
					FieldManager: "conductor-pack-deploy",
				})
		}
		if applyErr != nil {
			return failureResult(runnerlib.CapabilityPackDeploy, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("apply manifest[%d] %s %s/%s: %v", i, kind, namespace, name, applyErr)), nil
		}
		appliedCount++
	}
	step2End := time.Now().UTC()

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityPackDeploy,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts: []runnerlib.ArtifactRef{
			{
				Name:      "cluster-pack",
				Kind:      "OCIImage",
				Reference: ociRef,
				Checksum:  expectedChecksum,
			},
		},
		Steps: []runnerlib.StepResult{
			{Name: "pull-manifests", Status: runnerlib.ResultSucceeded, StartedAt: step1Start, CompletedAt: step1End, Message: fmt.Sprintf("%d manifests fetched", len(manifests))},
			{Name: "apply-manifests", Status: runnerlib.ResultSucceeded, StartedAt: step2Start, CompletedAt: step2End, Message: fmt.Sprintf("%d manifests applied", appliedCount)},
		},
	}, nil
}

// computeManifestChecksum computes a simple content-addressed checksum for
// a set of manifest bytes. This is a placeholder — production implementation
// uses the same algorithm as the Compiler (sha256 of concatenated manifests).
func computeManifestChecksum(manifests [][]byte) string {
	// The real checksum must match what the Compiler produces. Actual
	// implementation requires the shared checksum function from runnerlib.
	// For now, return a sentinel that will only match if expectedChecksum is empty.
	_ = manifests
	return ""
}

// gvrFromAPIVersionKind derives a schema.GroupVersionResource from the
// apiVersion and kind strings in a Kubernetes manifest. This is a best-effort
// derivation: it lowercases the kind and appends 's' for the resource name.
// For resources where this convention doesn't hold (e.g., "endpoints"),
// the server-side apply call will fail with a 404 and be reported as an error.
func gvrFromAPIVersionKind(apiVersion, kind string) schema.GroupVersionResource {
	group, version := splitAPIVersion(apiVersion)
	resource := lowercasePlural(kind)
	return schema.GroupVersionResource{Group: group, Version: version, Resource: resource}
}

// splitAPIVersion splits "group/version" into (group, version).
// Core API resources (e.g., "v1") return ("", "v1").
func splitAPIVersion(apiVersion string) (string, string) {
	for i, c := range apiVersion {
		if c == '/' {
			return apiVersion[:i], apiVersion[i+1:]
		}
	}
	return "", apiVersion
}

// lowercasePlural converts a Kind (e.g., "Deployment") to its lowercase plural
// resource name (e.g., "deployments") using the conventional 's' suffix.
func lowercasePlural(kind string) string {
	if len(kind) == 0 {
		return kind
	}
	lower := make([]byte, len(kind))
	for i := 0; i < len(kind); i++ {
		c := kind[i]
		if c >= 'A' && c <= 'Z' {
			lower[i] = c + 32
		} else {
			lower[i] = c
		}
	}
	return string(lower) + "s"
}
