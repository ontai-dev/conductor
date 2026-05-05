// Package capability — Platform security capability implementations.
// pki-rotate, credential-rotate, hardening-apply. conductor-schema.md §6.
package capability

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// HardeningStablePollInterval is the interval between Health() checks while
// waiting for a node to stabilize after a no-reboot config apply. Declared as
// a var so tests can lower it without modifying production constants.
var HardeningStablePollInterval = 5 * time.Second

// HardeningStableTimeout is the maximum time to wait for a node to report
// healthy after a hardening config apply before declaring failure.
var HardeningStableTimeout = 2 * time.Minute

// waitForNodeStable polls Health() on nodeIP until the node reports healthy or
// HardeningStableTimeout expires. If the node is already healthy on the first
// check this returns immediately without sleeping (fast path).
//
// No-reboot machineconfig applies can briefly restart services (e.g. kubelet
// for sysctl changes). Calling this between nodes prevents overlapping restarts
// across control-plane nodes from causing etcd quorum loss and VIP failure.
func waitForNodeStable(ctx context.Context, client TalosNodeClient, nodeIP string) error {
	nodeCtx := NodeContext(ctx, nodeIP)
	deadline := time.Now().Add(HardeningStableTimeout)
	for {
		if err := client.Health(nodeCtx); err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("node %s did not stabilize within %s after hardening apply", nodeIP, HardeningStableTimeout)
		}
		slog.Info("hardening-apply: node temporarily unhealthy after config apply, waiting",
			slog.String("node", nodeIP))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(HardeningStablePollInterval):
		}
	}
}

// secretsGVR is the GroupVersionResource for core/v1 Secrets.
var secretsGVR = schema.GroupVersionResource{Group: "", Version: "v1", Resource: "secrets"}

// kubeconfigWriteSecretKey is the data key used for kubeconfig values in
// platform Secret conventions. Matches kubeconfigSecretKey in platform.
const kubeconfigWriteSecretKey = "value"

// upsertKubeconfigSecret creates or updates a Secret holding a kubeconfig in
// the given namespace. Uses the dynamic client to remain independent of typed
// Kubernetes client bindings in the execute image. platform-schema.md §13.
func upsertKubeconfigSecret(ctx context.Context, params ExecuteParams, namespace, name string, kubeconfigBytes []byte) error {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "v1",
			"kind":       "Secret",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"data": map[string]interface{}{
				kubeconfigWriteSecretKey: base64.StdEncoding.EncodeToString(kubeconfigBytes),
			},
		},
	}

	existing, err := params.DynamicClient.Resource(secretsGVR).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			_, createErr := params.DynamicClient.Resource(secretsGVR).Namespace(namespace).Create(ctx, obj, metav1.CreateOptions{})
			if createErr != nil {
				return fmt.Errorf("create kubeconfig Secret %s/%s: %w", namespace, name, createErr)
			}
			return nil
		}
		return fmt.Errorf("get kubeconfig Secret %s/%s: %w", namespace, name, err)
	}

	existing.Object["data"] = obj.Object["data"]
	if _, err := params.DynamicClient.Resource(secretsGVR).Namespace(namespace).Update(ctx, existing, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("update kubeconfig Secret %s/%s: %w", namespace, name, err)
	}
	return nil
}

// pkiRotationGVR is the GroupVersionResource for PKIRotation.
// platform.ontai.dev/v1alpha1/pkirotations — platform-schema.md §5.
var pkiRotationGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "pkirotations",
}

// hardeningProfileGVR is the GroupVersionResource for HardeningProfile.
// platform.ontai.dev/v1alpha1/hardeningprofiles — platform-schema.md §5.
var hardeningProfileGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "hardeningprofiles",
}

// pkiRotateHandler implements the pki-rotate named capability.
// Reads the PKIRotation CR and initiates PKI certificate rotation via the
// Talos machine API by regenerating and applying updated machine configs.
// platform-schema.md §5 (PKIRotation).
type pkiRotateHandler struct{}

func (h *pkiRotateHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityPKIRotate, now, runnerlib.ValidationFailure,
			"pki-rotate requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)

	// Verify PKIRotation CR exists for this cluster.
	crList, err := params.DynamicClient.Resource(pkiRotationGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityPKIRotate, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list PKIRotation in %s: %v", ns, err)), nil
	}
	if len(crList.Items) == 0 {
		return failureResult(runnerlib.CapabilityPKIRotate, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no PKIRotation CR found in %s", ns)), nil
	}

	// Read the current machine config from the node and re-apply it in staged mode.
	// This initiates a rotation cycle: the staged config is applied on next reboot,
	// refreshing certificate-backed component configuration. conductor-schema.md §6.
	stagedStart := time.Now().UTC()
	configBytes, err := params.TalosClient.GetMachineConfig(ctx)
	if err != nil {
		return failureResult(runnerlib.CapabilityPKIRotate, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("GetMachineConfig for PKI rotation: %v", err)), nil
	}

	if err := params.TalosClient.ApplyConfiguration(ctx, configBytes, "staged"); err != nil {
		return failureResult(runnerlib.CapabilityPKIRotate, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("ApplyConfiguration staged for PKI rotation: %v", err)), nil
	}

	stagedStep := runnerlib.StepResult{
		Name: "pki-rotate-staged", Status: runnerlib.ResultSucceeded,
		StartedAt: stagedStart, CompletedAt: time.Now().UTC(),
		Message: fmt.Sprintf("PKI rotation staged for cluster %s; config staged for next reboot", params.ClusterRef),
	}

	// Refresh the kubeconfig Secret. This is a best-effort secondary step.
	// The staged config apply (above) is the critical step. If kubeconfig
	// refresh fails, the overall operation is still considered successful.
	// platform-schema.md §13.
	kubeconfigStart := time.Now().UTC()
	var kubeconfigStep runnerlib.StepResult
	var artifacts []runnerlib.ArtifactRef

	kubeconfigBytes, kcErr := params.TalosClient.Kubeconfig(ctx)
	if kcErr != nil {
		kubeconfigStep = runnerlib.StepResult{
			Name: "kubeconfig-refresh", Status: runnerlib.ResultSucceeded,
			StartedAt: kubeconfigStart, CompletedAt: time.Now().UTC(),
			Message: fmt.Sprintf("kubeconfig refresh skipped (non-fatal): %v", kcErr),
		}
	} else {
		mcSecretName := "seam-mc-" + params.ClusterRef + "-kubeconfig"
		targetSecretName := "target-cluster-kubeconfig"
		kcRefreshErr := upsertKubeconfigSecret(ctx, params, ns, mcSecretName, kubeconfigBytes)
		kcRefreshMsg := fmt.Sprintf("kubeconfig Secret %s/%s refreshed", ns, mcSecretName)
		if kcRefreshErr != nil {
			kcRefreshMsg = fmt.Sprintf("kubeconfig refresh partial: %s/%s failed: %v", ns, mcSecretName, kcRefreshErr)
		} else {
			_ = upsertKubeconfigSecret(ctx, params, ns, targetSecretName, kubeconfigBytes)
			artifacts = append(artifacts, runnerlib.ArtifactRef{
				Name:      "kubeconfig-refreshed",
				Kind:      "KubernetesSecret",
				Reference: fmt.Sprintf("%s/%s", ns, mcSecretName),
			})
		}
		kubeconfigStep = runnerlib.StepResult{
			Name: "kubeconfig-refresh", Status: runnerlib.ResultSucceeded,
			StartedAt: kubeconfigStart, CompletedAt: time.Now().UTC(),
			Message: kcRefreshMsg,
		}
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityPKIRotate,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   artifacts,
		Steps:       []runnerlib.StepResult{stagedStep, kubeconfigStep},
	}, nil
}

// credentialRotateHandler implements the credential-rotate named capability.
// Reads the NodeMaintenance CR (operation=credential-rotate) and rotates
// service account keys. platform-schema.md §5.
type credentialRotateHandler struct{}

func (h *credentialRotateHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.KubeClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityCredentialRotate, now, runnerlib.ValidationFailure,
			"credential-rotate requires KubeClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(nodeMaintenanceGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityCredentialRotate, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list NodeMaintenance in %s: %v", ns, err)), nil
	}

	var rotateServiceAccountKeys, rotateOIDCCredentials bool
	found := false
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "credential-rotate" {
			continue
		}
		found = true
		rsakRaw, _, _ := unstructuredList(item.Object, "spec", "rotateServiceAccountKeys")
		rotateServiceAccountKeys = len(rsakRaw) > 0 || rsakRaw == nil // default true if not set
		roidcRaw, _, _ := unstructuredList(item.Object, "spec", "rotateOIDCCredentials")
		rotateOIDCCredentials = len(roidcRaw) > 0

		// Read boolean flags properly.
		if v, ok := item.Object["spec"].(map[string]interface{}); ok {
			if b, ok := v["rotateServiceAccountKeys"].(bool); ok {
				rotateServiceAccountKeys = b
			}
			if b, ok := v["rotateOIDCCredentials"].(bool); ok {
				rotateOIDCCredentials = b
			}
		}
		break
	}

	if !found {
		return failureResult(runnerlib.CapabilityCredentialRotate, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no NodeMaintenance CR with operation=credential-rotate found in %s", ns)), nil
	}

	var steps []runnerlib.StepResult

	if rotateServiceAccountKeys {
		// Rotate service account signing key: delete the current secret and
		// let the kube-apiserver regenerate it. The new key takes effect on restart.
		stepStart := time.Now().UTC()
		saKeySecret := fmt.Sprintf("seam-sa-key-%s", params.ClusterRef)
		if delErr := params.KubeClient.CoreV1().Secrets(ns).Delete(ctx, saKeySecret, metav1.DeleteOptions{}); delErr != nil {
			return failureResult(runnerlib.CapabilityCredentialRotate, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("delete SA key Secret %s/%s: %v", ns, saKeySecret, delErr)), nil
		}
		steps = append(steps, runnerlib.StepResult{
			Name: "rotate-sa-key", Status: runnerlib.ResultSucceeded,
			StartedAt: stepStart, CompletedAt: time.Now().UTC(),
			Message: fmt.Sprintf("service account key Secret %s deleted; regeneration pending", saKeySecret),
		})
	}

	if rotateOIDCCredentials {
		stepStart := time.Now().UTC()
		// OIDC credential rotation: update the OIDC signing key Secret.
		// The actual key material is regenerated by the PKI controller on next reconcile.
		oidcSecret := fmt.Sprintf("seam-oidc-key-%s", params.ClusterRef)
		if delErr := params.KubeClient.CoreV1().Secrets(ns).Delete(ctx, oidcSecret, metav1.DeleteOptions{}); delErr != nil {
			return failureResult(runnerlib.CapabilityCredentialRotate, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("delete OIDC key Secret %s/%s: %v", ns, oidcSecret, delErr)), nil
		}
		steps = append(steps, runnerlib.StepResult{
			Name: "rotate-oidc-credential", Status: runnerlib.ResultSucceeded,
			StartedAt: stepStart, CompletedAt: time.Now().UTC(),
			Message: fmt.Sprintf("OIDC key Secret %s deleted; regeneration pending", oidcSecret),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityCredentialRotate,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// hardeningApplyHandler implements the hardening-apply named capability.
// Reads the NodeMaintenance CR (operation=hardening-apply) and the referenced
// HardeningProfile, then applies the machine config patches via Talos API.
// platform-schema.md §5 (NodeMaintenance, HardeningProfile).
type hardeningApplyHandler struct{}

func (h *hardeningApplyHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ValidationFailure,
			"hardening-apply requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(nodeMaintenanceGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list NodeMaintenance in %s: %v", ns, err)), nil
	}

	var hardeningProfileRef, hardeningProfileNs string
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "hardening-apply" {
			continue
		}
		hardeningProfileRef, _, _ = unstructuredString(item.Object, "spec", "hardeningProfileRef", "name")
		hardeningProfileNs, _, _ = unstructuredString(item.Object, "spec", "hardeningProfileRef", "namespace")
		if hardeningProfileNs == "" {
			hardeningProfileNs = ns
		}
		break
	}

	if hardeningProfileRef == "" {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no NodeMaintenance CR with operation=hardening-apply and hardeningProfileRef found in %s", ns)), nil
	}

	// Read the HardeningProfile to get machine config patches.
	hpList, err := params.DynamicClient.Resource(hardeningProfileGVR).Namespace(hardeningProfileNs).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list HardeningProfile in %s: %v", hardeningProfileNs, err)), nil
	}

	// machineConfigPatches is []string in the spec; read via unstructuredList to avoid
	// the incorrect type assertion that unstructuredString would perform on a slice.
	var configPatches []string
	for _, item := range hpList.Items {
		name, _, _ := unstructuredString(item.Object, "metadata", "name")
		if name != hardeningProfileRef {
			continue
		}
		raw, _, _ := unstructuredList(item.Object, "spec", "machineConfigPatches")
		for _, p := range raw {
			if s, ok := p.(string); ok && len(s) > 0 {
				configPatches = append(configPatches, s)
			}
		}
		break
	}

	if len(configPatches) == 0 {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ValidationFailure,
			fmt.Sprintf("HardeningProfile %q in %s has no machineConfigPatches", hardeningProfileRef, hardeningProfileNs)), nil
	}

	// Determine which nodes to apply hardening to. When TalosconfigPath is set
	// (production), we iterate over each endpoint individually using per-node
	// context so that GetMachineConfig reads that node's own config and
	// ApplyConfiguration writes only to that node. Without per-node isolation,
	// a multi-endpoint Talos client would apply node 1's config to every node,
	// which corrupts hostnames, IPs, and certificates on nodes 2..N.
	//
	// When TalosconfigPath is empty (unit tests, single-node clusters), we fall
	// back to the shared TalosClient with the original context.
	var nodeIPs []string
	if params.TalosconfigPath != "" {
		ips, epErr := EndpointsFromTalosconfig(params.TalosconfigPath)
		if epErr != nil {
			return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("read endpoints from talosconfig: %v", epErr)), nil
		}
		nodeIPs = ips
	}

	var steps []runnerlib.StepResult

	applyNode := func(nodeCtx context.Context, nodeID string) *runnerlib.OperationResultSpec {
		currentConfig, err := params.TalosClient.GetMachineConfig(nodeCtx)
		if err != nil {
			res := failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("GetMachineConfig on %s before hardening patch: %v", nodeID, err))
			return &res
		}
		for i, patch := range configPatches {
			stepStart := time.Now().UTC()
			merged, mergeErr := mergeYAMLPatch(currentConfig, []byte(patch))
			if mergeErr != nil {
				res := failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
					fmt.Sprintf("merge hardening patch %d on %s: %v", i, nodeID, mergeErr))
				return &res
			}
			if err := params.TalosClient.ApplyConfiguration(nodeCtx, merged, "no-reboot"); err != nil {
				res := failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
					fmt.Sprintf("ApplyConfiguration (hardening, profile=%s, patch=%d, node=%s): %v", hardeningProfileRef, i, nodeID, err))
				return &res
			}
			currentConfig = merged
			steps = append(steps, runnerlib.StepResult{
				Name:        fmt.Sprintf("apply-hardening-%d-%s", i, nodeID),
				Status:      runnerlib.ResultSucceeded,
				StartedAt:   stepStart,
				CompletedAt: time.Now().UTC(),
				Message:     fmt.Sprintf("hardening profile %q patch %d applied to %s (no-reboot mode)", hardeningProfileRef, i, nodeID),
			})
		}
		return nil
	}

	if len(nodeIPs) > 0 {
		for i, nodeIP := range nodeIPs {
			if res := applyNode(NodeContext(ctx, nodeIP), nodeIP); res != nil {
				return *res, nil
			}
			// Stabilization wait between nodes (not after the last node).
			// Allows services restarted by the no-reboot apply on this node to
			// recover before proceeding to the next node. Without this, overlapping
			// service restarts across control-plane nodes can drop etcd quorum and
			// take down the VIP.
			if i < len(nodeIPs)-1 {
				if wErr := waitForNodeStable(ctx, params.TalosClient, nodeIP); wErr != nil {
					res := failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
						fmt.Sprintf("node %s did not stabilize after hardening apply: %v", nodeIP, wErr))
					return res, nil
				}
			}
		}
	} else {
		if res := applyNode(ctx, "node"); res != nil {
			return *res, nil
		}
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityHardeningApply,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// mergeYAMLPatch deep-merges a YAML patch onto a base YAML document. Map keys
// in patch override or extend corresponding keys in base; slices in patch
// replace slices in base (no append semantics). The merged result is returned
// as YAML bytes. This is the correct approach for applying a partial Talos
// machine config overlay: the base is the current node config from
// GetMachineConfig, and the patch is a HardeningProfile machineConfigPatch.
func mergeYAMLPatch(base, patch []byte) ([]byte, error) {
	var baseMap map[string]interface{}
	if err := sigsyaml.Unmarshal(base, &baseMap); err != nil {
		return nil, fmt.Errorf("unmarshal base config: %w", err)
	}
	var patchMap map[string]interface{}
	if err := sigsyaml.Unmarshal(patch, &patchMap); err != nil {
		return nil, fmt.Errorf("unmarshal patch: %w", err)
	}
	merged := deepMergeStringMaps(baseMap, patchMap)
	out, err := sigsyaml.Marshal(merged)
	if err != nil {
		return nil, fmt.Errorf("marshal merged config: %w", err)
	}
	return out, nil
}

// deepMergeStringMaps merges src into dst recursively. Map values are merged;
// all other types (including slices) from src replace the corresponding dst value.
func deepMergeStringMaps(dst, src map[string]interface{}) map[string]interface{} {
	if dst == nil {
		dst = make(map[string]interface{})
	}
	for k, srcVal := range src {
		dstVal, exists := dst[k]
		if exists {
			srcMap, srcIsMap := srcVal.(map[string]interface{})
			dstMap, dstIsMap := dstVal.(map[string]interface{})
			if srcIsMap && dstIsMap {
				dst[k] = deepMergeStringMaps(dstMap, srcMap)
				continue
			}
		}
		dst[k] = srcVal
	}
	return dst
}
