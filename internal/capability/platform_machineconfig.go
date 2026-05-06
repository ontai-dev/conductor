// Package capability -- Platform machine config capability implementations.
// machineconfig-backup. conductor-schema.md §6, platform-schema.md §11.
package capability

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// machineConfigBackupGVR is the GroupVersionResource for TalosMachineConfigBackup.
// platform.ontai.dev/v1alpha1/talosmachineconfigbackups -- platform-schema.md §11.
var machineConfigBackupGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "talosmachineconfigbackups",
}

// machineConfigBackupHandler implements the machineconfig-backup named capability.
// Reads each node's running machine config via the Talos API and uploads it to
// S3 at {cluster}/machineconfigs/{TIMESTAMP}/{hostname}.yaml.
// TalosMachineConfigBackup CR must exist in seam-tenant-{cluster}.
// platform-schema.md §11.
type machineConfigBackupHandler struct{}

func (h *machineConfigBackupHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.StorageClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityMachineConfigBackup, now, runnerlib.ValidationFailure,
			"machineconfig-backup requires TalosClient, StorageClient, and DynamicClient"), nil
	}

	// Read TalosMachineConfigBackup CR to get s3Destination.
	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(machineConfigBackupGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityMachineConfigBackup, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list TalosMachineConfigBackup in %s: %v", ns, err)), nil
	}

	var s3Bucket string
	for _, item := range crList.Items {
		s3Bucket, _, _ = unstructuredString(item.Object, "spec", "s3Destination", "bucket")
		if s3Bucket != "" {
			break
		}
	}

	if s3Bucket == "" {
		return failureResult(runnerlib.CapabilityMachineConfigBackup, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no TalosMachineConfigBackup CR with s3Destination.bucket found in %s", ns)), nil
	}

	// Determine which nodes to back up. When TalosconfigPath is set (production),
	// iterate each endpoint individually so GetMachineConfig reads that node's own
	// config. Without per-node isolation a multi-endpoint Talos client would read
	// node 1's config for every call.
	var nodeIPs []string
	if params.TalosconfigPath != "" {
		ips, epErr := EndpointsFromTalosconfig(params.TalosconfigPath)
		if epErr != nil {
			return failureResult(runnerlib.CapabilityMachineConfigBackup, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("read endpoints from talosconfig: %v", epErr)), nil
		}
		nodeIPs = ips
	} else {
		// Unit test / single-node fallback: use a single node sentinel.
		nodeIPs = []string{"node"}
	}

	ts := now.Format("20060102T150405Z")
	var steps []runnerlib.StepResult
	var artifacts []runnerlib.ArtifactRef

	for _, nodeIP := range nodeIPs {
		nodeCtx := ctx
		if nodeIP != "node" {
			nodeCtx = NodeContext(ctx, nodeIP)
		}

		stepStart := time.Now().UTC()

		configBytes, err := params.TalosClient.GetMachineConfig(nodeCtx)
		if err != nil {
			return failureResult(runnerlib.CapabilityMachineConfigBackup, now, runnerlib.ExecutionFailure,
				fmt.Sprintf("GetMachineConfig on %s: %v", nodeIP, err)), nil
		}

		// Extract hostname from machine config to form a readable S3 key.
		var cfg minimalMachineConfigHostname
		hostname := sanitizeHostname(nodeIP)
		if err := sigsyaml.Unmarshal(configBytes, &cfg); err == nil && cfg.Machine.Network.Hostname != "" {
			hostname = cfg.Machine.Network.Hostname
		}

		s3Key := fmt.Sprintf("%s/machineconfigs/%s/%s.yaml", params.ClusterRef, ts, hostname)

		if err := params.StorageClient.Upload(nodeCtx, s3Bucket, s3Key, bytes.NewReader(configBytes)); err != nil {
			return failureResult(runnerlib.CapabilityMachineConfigBackup, now, runnerlib.ExternalDependencyFailure,
				fmt.Sprintf("upload %s to s3://%s/%s: %v", hostname, s3Bucket, s3Key, err)), nil
		}

		steps = append(steps, runnerlib.StepResult{
			Name:        fmt.Sprintf("backup-%s", hostname),
			Status:      runnerlib.ResultSucceeded,
			StartedAt:   stepStart,
			CompletedAt: time.Now().UTC(),
			Message:     fmt.Sprintf("machine config for %s uploaded to s3://%s/%s", hostname, s3Bucket, s3Key),
		})
		artifacts = append(artifacts, runnerlib.ArtifactRef{
			Name:      fmt.Sprintf("machineconfig-%s", hostname),
			Kind:      "S3Object",
			Reference: fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key),
		})
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityMachineConfigBackup,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   artifacts,
		Steps:       steps,
	}, nil
}

// sanitizeHostname converts a node IP address into a filesystem-safe hostname
// string used as a fallback when the machine config hostname field is absent.
// Colons (IPv6) become hyphens; dots (IPv4) become hyphens.
func sanitizeHostname(nodeIP string) string {
	r := strings.NewReplacer(":", "-", ".", "-")
	return r.Replace(nodeIP)
}

