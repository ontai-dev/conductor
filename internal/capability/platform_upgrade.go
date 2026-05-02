// Package capability — Platform upgrade capability implementations.
// talos-upgrade, kube-upgrade, stack-upgrade. conductor-schema.md §6.
// These capabilities only run as direct Conductor Jobs when TalosCluster
// spec.capi.enabled=false (management cluster path). platform-schema.md §5.
package capability

import (
	"context"
	"fmt"
	"log/slog"
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

// NodeRebootPollInterval is the interval between Health() checks while waiting
// for a node to complete its reboot cycle after an upgrade. Exported so tests
// can lower it without modifying production constants.
var NodeRebootPollInterval = 10 * time.Second


// talosUpgradeHandler implements the talos-upgrade named capability.
// Performs a rolling sequential upgrade of all nodes: each node is upgraded
// with stage=false (immediate reboot), then we wait for it to return healthy
// before moving to the next. platform-schema.md §5.
type talosUpgradeHandler struct{}

func (h *talosUpgradeHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ValidationFailure,
			"talos-upgrade requires TalosClient and DynamicClient"), nil
	}

	nodes := params.TalosClient.Nodes()
	if len(nodes) == 0 {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ValidationFailure,
			"talos-upgrade: no nodes available from talosconfig"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(upgradePolicyGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list UpgradePolicy in %s: %v", ns, err)), nil
	}

	var targetVersion string
	for _, item := range crList.Items {
		ut, _, _ := unstructuredString(item.Object, "spec", "upgradeType")
		if ut != "talos" {
			continue
		}
		targetVersion, _, _ = unstructuredString(item.Object, "spec", "targetTalosVersion")
		break
	}

	if targetVersion == "" {
		return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no UpgradePolicy CR with upgradeType=talos and targetTalosVersion found in %s", ns)), nil
	}
	upgradeImage := "ghcr.io/siderolabs/installer:" + targetVersion

	steps := make([]runnerlib.StepResult, 0, len(nodes))
	for i, nodeIP := range nodes {
		stepStart := time.Now().UTC()
		nodeCtx := NodeContext(ctx, nodeIP)

		slog.Info("talos-upgrade: starting node upgrade",
			slog.Int("node_index", i+1), slog.Int("node_total", len(nodes)),
			slog.String("node", nodeIP), slog.String("image", upgradeImage))

		if uErr := params.TalosClient.Upgrade(nodeCtx, upgradeImage, false); uErr != nil {
			slog.Info("talos-upgrade: upgrade call failed",
				slog.String("node", nodeIP), slog.String("error", uErr.Error()))
			return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("upgrade node %s to %s: %v", nodeIP, upgradeImage, uErr)), nil
		}

		slog.Info("talos-upgrade: upgrade initiated, waiting for node reboot",
			slog.String("node", nodeIP), slog.String("image", upgradeImage))

		if wErr := waitForNodeReboot(ctx, params.TalosClient, nodeIP); wErr != nil {
			slog.Info("talos-upgrade: node did not recover after reboot",
				slog.String("node", nodeIP), slog.String("error", wErr.Error()))
			return failureResult(runnerlib.CapabilityTalosUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("node %s did not recover after upgrade to %s: %v", nodeIP, upgradeImage, wErr)), nil
		}

		slog.Info("talos-upgrade: node ready after reboot",
			slog.String("node", nodeIP), slog.String("image", upgradeImage))

		steps = append(steps, runnerlib.StepResult{
			Name:        "talos-upgrade-" + nodeIP,
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   stepStart,
			CompletedAt: time.Now().UTC(),
			Message:     fmt.Sprintf("upgraded node %s to %s", nodeIP, upgradeImage),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityTalosUpgrade,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// waitForNodeReboot waits for nodeIP to complete its reboot cycle after upgrade.
//
// Phase 1 (up to 2 minutes): polls Health until the node goes offline, confirming
// the reboot started. If the node never goes offline (upgrade completed faster than
// the poll interval, or test stub), returns nil immediately.
//
// Phase 2 (up to 8 minutes): polls Health until the node comes back online.
func waitForNodeReboot(ctx context.Context, client TalosNodeClient, nodeIP string) error {
	nodeCtx := NodeContext(ctx, nodeIP)

	// Phase 1: detect node going offline.
	downDeadline := time.Now().Add(2 * time.Minute)
	wentDown := false
	for time.Now().Before(downDeadline) {
		if err := client.Health(nodeCtx); err != nil {
			slog.Info("talos-upgrade: node offline, reboot in progress", slog.String("node", nodeIP))
			wentDown = true
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(NodeRebootPollInterval):
		}
	}

	if !wentDown {
		// Never went offline -- node may have rebooted faster than the poll interval.
		// Return nil; the TalosVersionDriftLoop will detect persistence if needed.
		slog.Info("talos-upgrade: node did not go offline within phase-1 window, assuming complete",
			slog.String("node", nodeIP))
		return nil
	}

	// Phase 2: wait for node to come back online.
	upDeadline := time.Now().Add(8 * time.Minute)
	for time.Now().Before(upDeadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(NodeRebootPollInterval):
		}
		if err := client.Health(nodeCtx); err == nil {
			slog.Info("talos-upgrade: node back online after reboot", slog.String("node", nodeIP))
			return nil
		}
	}
	return fmt.Errorf("node %s did not become ready after reboot within %s", nodeIP, 8*time.Minute)
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
