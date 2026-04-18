// Package capability — Platform upgrade capability implementations.
// talos-upgrade, kube-upgrade, stack-upgrade. conductor-schema.md §6.
// These capabilities only run as direct Conductor Jobs when TalosCluster
// spec.capi.enabled=false (management cluster path). platform-schema.md §5.
package capability

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// upgradePolicyGVR is the GroupVersionResource for UpgradePolicy.
// platform.ontai.dev/v1alpha1/upgradepolicies — platform-schema.md §5.
var upgradePolicyGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "upgradepolicies",
}

// talosUpgradeHandler implements the talos-upgrade named capability.
// Reads the UpgradePolicy CR for the target Talos image and calls the
// Talos Upgrade API per node. platform-schema.md §5.
type talosUpgradeHandler struct{}

func (h *talosUpgradeHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ValidationFailure,
			"talos-upgrade requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(upgradePolicyGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list UpgradePolicy in %s: %v", ns, err)), nil
	}

	var upgradeImage string
	for _, item := range crList.Items {
		ut, _, _ := unstructuredString(item.Object, "spec", "upgradeType")
		if ut != "talos" {
			continue
		}
		upgradeImage, _, _ = unstructuredString(item.Object, "spec", "targetTalosVersion")
		break
	}

	if upgradeImage == "" {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no UpgradePolicy CR with upgradeType=talos and targetTalosVersion found in %s", ns)), nil
	}

	stepStart := time.Now().UTC()
	// stage=true stages the upgrade for next reboot rather than applying immediately.
	if err := params.TalosClient.Upgrade(ctx, upgradeImage, true); err != nil {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("Upgrade to %s: %v", upgradeImage, err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityTalosUpgrade,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{
				Name: "talos-upgrade", Status: runnerlib.ResultSucceeded,
				StartedAt: stepStart, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("staged upgrade to %s", upgradeImage),
			},
		},
	}, nil
}

// kubeUpgradeHandler implements the kube-upgrade named capability.
// Reads the UpgradePolicy CR for the target Kubernetes version and applies
// updated kubelet configuration patches via the Talos machine API.
// platform-schema.md §5 (UpgradePolicy.spec.upgradeType=kubernetes).
type kubeUpgradeHandler struct{}

func (h *kubeUpgradeHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ValidationFailure,
			"kube-upgrade requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(upgradePolicyGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list UpgradePolicy in %s: %v", ns, err)), nil
	}

	var targetKubeVersion string
	for _, item := range crList.Items {
		ut, _, _ := unstructuredString(item.Object, "spec", "upgradeType")
		if ut != "kubernetes" {
			continue
		}
		targetKubeVersion, _, _ = unstructuredString(item.Object, "spec", "targetKubernetesVersion")
		break
	}

	if targetKubeVersion == "" {
		return failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no UpgradePolicy CR with upgradeType=kubernetes and targetKubernetesVersion found in %s", ns)), nil
	}

	// Build the kubelet configuration patch for the target version.
	// Talos manages Kubernetes component upgrades via machine config.
	kubeletPatch := []byte(fmt.Sprintf(
		`{"machine":{"kubelet":{"image":"ghcr.io/siderolabs/kubelet:%s"}}}`,
		targetKubeVersion,
	))

	stepStart := time.Now().UTC()
	if err := params.TalosClient.ApplyConfiguration(ctx, kubeletPatch, "staged"); err != nil {
		return failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("ApplyConfiguration for kube upgrade to %s: %v", targetKubeVersion, err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityKubeUpgrade,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{
				Name: "kube-upgrade", Status: runnerlib.ResultSucceeded,
				StartedAt: stepStart, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("staged kubelet upgrade to %s", targetKubeVersion),
			},
		},
	}, nil
}

// stackUpgradeHandler implements the stack-upgrade named capability.
// Combines talos-upgrade and kube-upgrade into a sequenced upgrade of both
// the Talos OS and Kubernetes components. platform-schema.md §5.
type stackUpgradeHandler struct{}

func (h *stackUpgradeHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ValidationFailure,
			"stack-upgrade requires TalosClient and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(upgradePolicyGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list UpgradePolicy in %s: %v", ns, err)), nil
	}

	var talosImage, kubeVersion string
	for _, item := range crList.Items {
		ut, _, _ := unstructuredString(item.Object, "spec", "upgradeType")
		if ut != "stack" {
			continue
		}
		talosImage, _, _ = unstructuredString(item.Object, "spec", "targetTalosVersion")
		kubeVersion, _, _ = unstructuredString(item.Object, "spec", "targetKubernetesVersion")
		break
	}

	if talosImage == "" || kubeVersion == "" {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ValidationFailure,
			fmt.Sprintf("UpgradePolicy with upgradeType=stack must specify both targetTalosVersion and targetKubernetesVersion in %s", ns)), nil
	}

	var steps []runnerlib.StepResult

	// Step 1 — Stage Talos upgrade.
	step1Start := time.Now().UTC()
	if err := params.TalosClient.Upgrade(ctx, talosImage, true); err != nil {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("Upgrade (Talos) to %s: %v", talosImage, err)), nil
	}
	steps = append(steps, runnerlib.StepResult{
		Name: "talos-upgrade", Status: runnerlib.ResultSucceeded,
		StartedAt: step1Start, CompletedAt: time.Now().UTC(),
		Message: fmt.Sprintf("staged Talos upgrade to %s", talosImage),
	})

	// Step 2 — Stage Kubernetes upgrade.
	step2Start := time.Now().UTC()
	kubeletPatch := []byte(fmt.Sprintf(
		`{"machine":{"kubelet":{"image":"ghcr.io/siderolabs/kubelet:%s"}}}`,
		kubeVersion,
	))
	if err := params.TalosClient.ApplyConfiguration(ctx, kubeletPatch, "staged"); err != nil {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("ApplyConfiguration (Kubernetes) to %s: %v", kubeVersion, err)), nil
	}
	steps = append(steps, runnerlib.StepResult{
		Name: "kube-upgrade", Status: runnerlib.ResultSucceeded,
		StartedAt: step2Start, CompletedAt: time.Now().UTC(),
		Message: fmt.Sprintf("staged Kubernetes upgrade to %s", kubeVersion),
	})

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityStackUpgrade,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}
