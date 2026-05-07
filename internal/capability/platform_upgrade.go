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

	kubeletImagePatch := []byte(fmt.Sprintf(
		`{"machine":{"kubelet":{"image":"ghcr.io/siderolabs/kubelet:v%s"}}}`,
		targetKubeVersion,
	))

	// Enumerate nodes from talosconfig; fall back to single-context when absent.
	var nodeIPs []string
	if params.TalosconfigPath != "" {
		ips, epErr := EndpointsFromTalosconfig(params.TalosconfigPath)
		if epErr != nil {
			return failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("read endpoints from talosconfig: %v", epErr)), nil
		}
		nodeIPs = ips
	}

	applyKubelet := func(nodeCtx context.Context, nodeID string) *runnerlib.OperationResultSpec {
		existing, err := params.TalosClient.GetMachineConfig(nodeCtx)
		if err != nil {
			res := failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("GetMachineConfig on %s: %v", nodeID, err))
			return &res
		}
		merged, err := mergeYAMLPatch(existing, kubeletImagePatch)
		if err != nil {
			res := failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("merge kubelet patch on %s: %v", nodeID, err))
			return &res
		}
		if err := params.TalosClient.ApplyConfiguration(nodeCtx, merged, "no-reboot"); err != nil {
			res := failureResult(runnerlib.CapabilityKubeUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("ApplyConfiguration for kube upgrade to %s on %s: %v", targetKubeVersion, nodeID, err))
			return &res
		}
		return nil
	}

	var steps []runnerlib.StepResult
	if len(nodeIPs) > 0 {
		for _, nodeIP := range nodeIPs {
			stepStart := time.Now().UTC()
			if res := applyKubelet(NodeContext(ctx, nodeIP), nodeIP); res != nil {
				return *res, nil
			}
			steps = append(steps, runnerlib.StepResult{
				Name: "kube-upgrade-" + nodeIP, Status: runnerlib.ResultSucceeded,
				StartedAt: stepStart, CompletedAt: time.Now().UTC(),
				Message: fmt.Sprintf("kubelet upgraded to %s on %s", targetKubeVersion, nodeIP),
			})
		}
	} else {
		stepStart := time.Now().UTC()
		if res := applyKubelet(ctx, "node"); res != nil {
			return *res, nil
		}
		steps = append(steps, runnerlib.StepResult{
			Name: "kube-upgrade", Status: runnerlib.ResultSucceeded,
			StartedAt: stepStart, CompletedAt: time.Now().UTC(),
			Message: fmt.Sprintf("kubelet upgraded to %s", targetKubeVersion),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityKubeUpgrade,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}

// stackUpgradeHandler implements the stack-upgrade named capability.
// Combines talos-upgrade and kube-upgrade into a sequenced rolling upgrade of
// both the Talos OS and Kubernetes kubelet. Per node: stage the kubelet image
// change, then trigger the Talos upgrade with immediate reboot (stage=false).
// The node reboots once and applies both changes together. Wait for each node
// to recover before proceeding to the next. platform-schema.md §5.
type stackUpgradeHandler struct{}

func (h *stackUpgradeHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ValidationFailure,
			"stack-upgrade requires TalosClient and DynamicClient"), nil
	}

	nodes := params.TalosClient.Nodes()
	if len(nodes) == 0 {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ValidationFailure,
			"stack-upgrade: no nodes available from talosconfig"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(upgradePolicyGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list UpgradePolicy in %s: %v", ns, err)), nil
	}

	var talosVersion, kubeVersion string
	for _, item := range crList.Items {
		ut, _, _ := unstructuredString(item.Object, "spec", "upgradeType")
		if ut != "stack" {
			continue
		}
		talosVersion, _, _ = unstructuredString(item.Object, "spec", "targetTalosVersion")
		kubeVersion, _, _ = unstructuredString(item.Object, "spec", "targetKubernetesVersion")
		break
	}

	if talosVersion == "" || kubeVersion == "" {
		return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ValidationFailure,
			fmt.Sprintf("UpgradePolicy with upgradeType=stack must specify both targetTalosVersion and targetKubernetesVersion in %s", ns)), nil
	}
	talosImage := "ghcr.io/siderolabs/installer:" + talosVersion

	kubeletImagePatch := []byte(fmt.Sprintf(
		`{"machine":{"kubelet":{"image":"ghcr.io/siderolabs/kubelet:v%s"}}}`,
		kubeVersion,
	))

	var steps []runnerlib.StepResult

	// Rolling per-node upgrade: stage kubelet then trigger immediate Talos reboot.
	// The node reboots once and co-applies both the Talos installer and the kubelet
	// image change in the same reboot cycle.
	for i, nodeIP := range nodes {
		stepStart := time.Now().UTC()
		nodeCtx := NodeContext(ctx, nodeIP)

		slog.Info("stack-upgrade: staging kubelet image change",
			slog.Int("node_index", i+1), slog.Int("node_total", len(nodes)),
			slog.String("node", nodeIP), slog.String("kubeVersion", kubeVersion))

		existing, err := params.TalosClient.GetMachineConfig(nodeCtx)
		if err != nil {
			return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("GetMachineConfig on %s: %v", nodeIP, err)), nil
		}
		merged, err := mergeYAMLPatch(existing, kubeletImagePatch)
		if err != nil {
			return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("merge kubelet patch on %s: %v", nodeIP, err)), nil
		}
		if err := params.TalosClient.ApplyConfiguration(nodeCtx, merged, "staged"); err != nil {
			return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("ApplyConfiguration (Kubernetes %s) on %s: %v", kubeVersion, nodeIP, err)), nil
		}

		slog.Info("stack-upgrade: triggering Talos upgrade with immediate reboot",
			slog.String("node", nodeIP), slog.String("image", talosImage))

		if uErr := params.TalosClient.Upgrade(nodeCtx, talosImage, false); uErr != nil {
			return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("Upgrade (Talos %s) on %s: %v", talosImage, nodeIP, uErr)), nil
		}

		if wErr := waitForNodeReboot(ctx, params.TalosClient, nodeIP); wErr != nil {
			return failureResult(runnerlib.CapabilityStackUpgrade, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("node %s did not recover after stack upgrade: %v", nodeIP, wErr)), nil
		}

		slog.Info("stack-upgrade: node ready after reboot",
			slog.String("node", nodeIP), slog.String("talos", talosImage), slog.String("kube", kubeVersion))

		steps = append(steps, runnerlib.StepResult{
			Name:        "stack-upgrade-" + nodeIP,
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   stepStart,
			CompletedAt: time.Now().UTC(),
			Message:     fmt.Sprintf("upgraded node %s to Talos %s + Kubernetes %s", nodeIP, talosVersion, kubeVersion),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityStackUpgrade,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       steps,
	}, nil
}
