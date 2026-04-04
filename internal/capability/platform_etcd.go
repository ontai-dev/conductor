// Package capability — Platform etcd capability implementations.
// etcd-backup, etcd-maintenance, etcd-restore. conductor-schema.md §6.
package capability

import (
	"bytes"
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// etcdMaintenanceGVR is the GroupVersionResource for EtcdMaintenance.
// platform.ontai.dev/v1alpha1/etcdmaintenances — platform-schema.md §5.
var etcdMaintenanceGVR = schema.GroupVersionResource{
	Group:    "platform.ontai.dev",
	Version:  "v1alpha1",
	Resource: "etcdmaintenances",
}

// etcdBackupHandler implements the etcd-backup named capability.
// Takes an etcd snapshot via the Talos machine API and uploads it to object
// storage. EtcdMaintenance spec.operation must be "backup". platform-schema.md §5.
type etcdBackupHandler struct{}

func (h *etcdBackupHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.StorageClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityEtcdBackup, now, runnerlib.ValidationFailure,
			"etcd-backup requires TalosClient, StorageClient, and DynamicClient"), nil
	}

	// Read EtcdMaintenance CR to get s3Destination.
	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(etcdMaintenanceGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityEtcdBackup, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list EtcdMaintenance in %s: %v", ns, err)), nil
	}

	var s3Bucket, s3Key string
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "backup" {
			continue
		}
		s3Bucket, _, _ = unstructuredString(item.Object, "spec", "s3Destination", "bucket")
		s3Key, _, _ = unstructuredString(item.Object, "spec", "s3Destination", "key")
		if s3Key == "" {
			s3Key = fmt.Sprintf("etcd-backup-%s-%s.snapshot", params.ClusterRef, now.Format("20060102T150405Z"))
		}
		break
	}

	if s3Bucket == "" {
		return failureResult(runnerlib.CapabilityEtcdBackup, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no EtcdMaintenance CR with operation=backup found in %s", ns)), nil
	}

	// Take etcd snapshot via Talos API.
	step1Start := time.Now().UTC()
	var buf bytes.Buffer
	if err := params.TalosClient.EtcdSnapshot(ctx, &buf); err != nil {
		return failureResult(runnerlib.CapabilityEtcdBackup, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("EtcdSnapshot: %v", err)), nil
	}
	step1End := time.Now().UTC()

	// Upload snapshot to object storage.
	step2Start := time.Now().UTC()
	if err := params.StorageClient.Upload(ctx, s3Bucket, s3Key, &buf); err != nil {
		return failureResult(runnerlib.CapabilityEtcdBackup, now, runnerlib.ExternalDependencyFailure,
			fmt.Sprintf("upload snapshot to s3://%s/%s: %v", s3Bucket, s3Key, err)), nil
	}
	step2End := time.Now().UTC()

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityEtcdBackup,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts: []runnerlib.ArtifactRef{
			{
				Name:      "etcd-snapshot",
				Kind:      "S3Object",
				Reference: fmt.Sprintf("s3://%s/%s", s3Bucket, s3Key),
			},
		},
		Steps: []runnerlib.StepResult{
			{Name: "etcd-snapshot", Status: runnerlib.ResultSucceeded, StartedAt: step1Start, CompletedAt: step1End},
			{Name: "s3-upload", Status: runnerlib.ResultSucceeded, StartedAt: step2Start, CompletedAt: step2End},
		},
	}, nil
}

// etcdMaintenanceHandler implements the etcd-maintenance (defrag) named capability.
// Runs EtcdDefragment on the etcd cluster members via the Talos machine API.
// platform-schema.md §5 (EtcdMaintenance.spec.operation=defrag).
type etcdMaintenanceHandler struct{}

func (h *etcdMaintenanceHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil {
		return failureResult(runnerlib.CapabilityEtcdMaintenance, now, runnerlib.ValidationFailure,
			"etcd-maintenance requires TalosClient"), nil
	}

	stepStart := time.Now().UTC()
	if err := params.TalosClient.EtcdDefragment(ctx); err != nil {
		return failureResult(runnerlib.CapabilityEtcdMaintenance, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("EtcdDefragment: %v", err)), nil
	}

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityEtcdMaintenance,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{Name: "etcd-defragment", Status: runnerlib.ResultSucceeded, StartedAt: stepStart, CompletedAt: time.Now().UTC()},
		},
	}, nil
}

// etcdRestoreHandler implements the etcd-restore named capability.
// Downloads a snapshot from object storage and calls EtcdRecover via the
// Talos machine API. platform-schema.md §5 (EtcdMaintenance.spec.operation=restore).
type etcdRestoreHandler struct{}

func (h *etcdRestoreHandler) Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error) {
	now := time.Now().UTC()

	if params.TalosClient == nil || params.StorageClient == nil || params.DynamicClient == nil {
		return failureResult(runnerlib.CapabilityEtcdRestore, now, runnerlib.ValidationFailure,
			"etcd-restore requires TalosClient, StorageClient, and DynamicClient"), nil
	}

	ns := tenantNamespace(params.ClusterRef)
	crList, err := params.DynamicClient.Resource(etcdMaintenanceGVR).Namespace(ns).
		List(ctx, metav1.ListOptions{})
	if err != nil {
		return failureResult(runnerlib.CapabilityEtcdRestore, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("list EtcdMaintenance in %s: %v", ns, err)), nil
	}

	var s3Bucket, s3Key string
	for _, item := range crList.Items {
		op, _, _ := unstructuredString(item.Object, "spec", "operation")
		if op != "restore" {
			continue
		}
		s3Bucket, _, _ = unstructuredString(item.Object, "spec", "s3SnapshotPath", "bucket")
		s3Key, _, _ = unstructuredString(item.Object, "spec", "s3SnapshotPath", "key")
		break
	}

	if s3Bucket == "" || s3Key == "" {
		return failureResult(runnerlib.CapabilityEtcdRestore, now, runnerlib.ValidationFailure,
			fmt.Sprintf("no EtcdMaintenance CR with operation=restore and s3SnapshotPath found in %s", ns)), nil
	}

	// Download snapshot from object storage.
	step1Start := time.Now().UTC()
	reader, err := params.StorageClient.Download(ctx, s3Bucket, s3Key)
	if err != nil {
		return failureResult(runnerlib.CapabilityEtcdRestore, now, runnerlib.ExternalDependencyFailure,
			fmt.Sprintf("download snapshot s3://%s/%s: %v", s3Bucket, s3Key, err)), nil
	}
	defer reader.Close()
	step1End := time.Now().UTC()

	// Recover etcd from the snapshot via Talos API.
	step2Start := time.Now().UTC()
	if err := params.TalosClient.EtcdRecover(ctx, reader); err != nil {
		return failureResult(runnerlib.CapabilityEtcdRestore, now, runnerlib.ExecutionFailure,
			fmt.Sprintf("EtcdRecover: %v", err)), nil
	}
	step2End := time.Now().UTC()

	return runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityEtcdRestore,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps: []runnerlib.StepResult{
			{Name: "s3-download", Status: runnerlib.ResultSucceeded, StartedAt: step1Start, CompletedAt: step1End},
			{Name: "etcd-recover", Status: runnerlib.ResultSucceeded, StartedAt: step2Start, CompletedAt: step2End},
		},
	}, nil
}

// tenantNamespace returns the Kubernetes namespace where tenant CRDs live.
// Management cluster CRDs are in ont-system; target cluster CRDs are in
// tenant-{clusterRef}. platform-schema.md §2 (Namespace Conventions).
func tenantNamespace(clusterRef string) string {
	// The management cluster uses ont-system. Target clusters use tenant-{name}.
	// Since the execute-mode Job runs on the management cluster, the target's
	// tenant namespace is tenant-{clusterRef}.
	return fmt.Sprintf("tenant-%s", clusterRef)
}
