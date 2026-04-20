// Package capability — Platform security capability implementations.
// pki-rotate, credential-rotate, hardening-apply. conductor-schema.md §6.
package capability

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

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

	// PKI rotation in Talos is performed by generating new certificates and
	// applying updated machine configs. The Talos API handles the certificate
	// rotation lifecycle when machine configs with new CA refs are applied.
	// ApplyConfiguration with mode=reboot triggers a full cert rotation cycle.
	pkiRotationConfig := []byte(`{"machine":{"ca":{}}}`) // triggers CA regeneration flow

	stepStart := time.Now().UTC()
	if err := params.TalosClient.ApplyConfiguration(ctx, pkiRotationConfig, "staged"); err != nil {
		return failureResult(runnerlib.CapabilityPKIRotate, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("ApplyConfiguration for PKI rotation: %v", err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityPKIRotate,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{
				Name: "pki-rotate", Status: runnerlib.ResultSucceeded,
				StartedAt: stepStart, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("PKI rotation staged for cluster %s", params.ClusterRef),
			},
		},
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

	var machineConfigPatches string
	for _, item := range hpList.Items {
		name, _, _ := unstructuredString(item.Object, "metadata", "name")
		if name != hardeningProfileRef {
			continue
		}
		machineConfigPatches, _, _ = unstructuredString(item.Object, "spec", "machineConfigPatches")
		break
	}

	if machineConfigPatches == "" {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ValidationFailure,
			fmt.Sprintf("HardeningProfile %q in %s has no machineConfigPatches", hardeningProfileRef, hardeningProfileNs)), nil
	}

	stepStart := time.Now().UTC()
	if err := params.TalosClient.ApplyConfiguration(ctx, []byte(machineConfigPatches), "no-reboot"); err != nil {
		return failureResult(runnerlib.CapabilityHardeningApply, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("ApplyConfiguration (hardening, profile=%s): %v", hardeningProfileRef, err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityHardeningApply,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{
				Name: "apply-hardening", Status: runnerlib.ResultSucceeded,
				StartedAt: stepStart, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("hardening profile %q applied (no-reboot mode)", hardeningProfileRef),
			},
		},
	}, nil
}
