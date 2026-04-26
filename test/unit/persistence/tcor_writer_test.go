package persistence_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/internal/persistence"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

func buildTCORScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := seamv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("AddToScheme seamv1alpha1: %v", err)
	}
	return s
}

func preTCOR(clusterRef, talosVersion string) *seamv1alpha1.InfrastructureTalosClusterOperationResult {
	return &seamv1alpha1.InfrastructureTalosClusterOperationResult{
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterRef,
			Namespace: "seam-tenant-" + clusterRef,
		},
		Spec: seamv1alpha1.InfrastructureTalosClusterOperationResultSpec{
			ClusterRef:   clusterRef,
			TalosVersion: talosVersion,
			Revision:     1,
		},
	}
}

// TestTCOR_AppendOperationRecord_Succeeded verifies that AppendOperationRecord patches
// the per-cluster TCOR to add a Succeeded operation record keyed by jobRef.
func TestTCOR_AppendOperationRecord_Succeeded(t *testing.T) {
	scheme := buildTCORScheme(t)
	tcor := preTCOR("ccs-test", "v1.9.3")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tcor).Build()
	writer := persistence.NewKubeTalosClusterResultWriter(fakeClient)

	now := time.Now().UTC()
	result := runnerlib.OperationResultSpec{
		Capability:  "pki-rotate",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   now,
		CompletedAt: now,
	}
	if err := writer.AppendOperationRecord(context.Background(), "ccs-test", "pki-rotate-job-1", result); err != nil {
		t.Fatalf("AppendOperationRecord: %v", err)
	}

	got := &seamv1alpha1.InfrastructureTalosClusterOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Name: "ccs-test", Namespace: "seam-tenant-ccs-test"},
		got); err != nil {
		t.Fatalf("get TCOR: %v", err)
	}

	rec, ok := got.Spec.Operations["pki-rotate-job-1"]
	if !ok {
		t.Fatal("operation record not found in TCOR Operations map")
	}
	if rec.Status != seamv1alpha1.TalosClusterResultSucceeded {
		t.Errorf("Status = %q, want Succeeded", rec.Status)
	}
	if rec.Capability != "pki-rotate" {
		t.Errorf("Capability = %q, want pki-rotate", rec.Capability)
	}
	if rec.JobRef != "pki-rotate-job-1" {
		t.Errorf("JobRef = %q, want pki-rotate-job-1", rec.JobRef)
	}
	if rec.FailureReason != nil {
		t.Error("FailureReason should be nil for Succeeded result")
	}
}

// TestTCOR_AppendOperationRecord_Failed verifies that a Failed result records
// the FailureReason category and reason.
func TestTCOR_AppendOperationRecord_Failed(t *testing.T) {
	scheme := buildTCORScheme(t)
	tcor := preTCOR("ccs-fail", "v1.9.3")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tcor).Build()
	writer := persistence.NewKubeTalosClusterResultWriter(fakeClient)

	result := runnerlib.OperationResultSpec{
		Capability: "talos-upgrade",
		Status:     runnerlib.ResultFailed,
		FailureReason: &runnerlib.FailureReason{
			Category: runnerlib.ExecutionFailure,
			Reason:   "upgrade binary not found",
		},
	}
	if err := writer.AppendOperationRecord(context.Background(), "ccs-fail", "talos-upgrade-job-2", result); err != nil {
		t.Fatalf("AppendOperationRecord: %v", err)
	}

	got := &seamv1alpha1.InfrastructureTalosClusterOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Name: "ccs-fail", Namespace: "seam-tenant-ccs-fail"},
		got); err != nil {
		t.Fatalf("get TCOR: %v", err)
	}

	rec, ok := got.Spec.Operations["talos-upgrade-job-2"]
	if !ok {
		t.Fatal("operation record not found in TCOR Operations map")
	}
	if rec.Status != seamv1alpha1.TalosClusterResultFailed {
		t.Errorf("Status = %q, want Failed", rec.Status)
	}
	if rec.FailureReason == nil {
		t.Fatal("FailureReason should not be nil for Failed result")
	}
	if rec.FailureReason.Reason != "upgrade binary not found" {
		t.Errorf("FailureReason.Reason = %q, want %q", rec.FailureReason.Reason, "upgrade binary not found")
	}
	if rec.FailureReason.Category != string(runnerlib.ExecutionFailure) {
		t.Errorf("FailureReason.Category = %q, want %q", rec.FailureReason.Category, runnerlib.ExecutionFailure)
	}
}

// TestTCOR_AppendOperationRecord_SecondWrite verifies that a second write to the
// same TCOR appends a new record alongside the existing one (no overwrites).
func TestTCOR_AppendOperationRecord_SecondWrite(t *testing.T) {
	scheme := buildTCORScheme(t)
	tcor := preTCOR("ccs-multi", "v1.9.3")
	tcor.Spec.Operations = map[string]seamv1alpha1.TalosClusterOperationRecord{
		"first-job": {Capability: "etcd-backup", JobRef: "first-job", Status: seamv1alpha1.TalosClusterResultSucceeded},
	}
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tcor).Build()
	writer := persistence.NewKubeTalosClusterResultWriter(fakeClient)

	result := runnerlib.OperationResultSpec{
		Capability: "pki-rotate",
		Status:     runnerlib.ResultSucceeded,
	}
	if err := writer.AppendOperationRecord(context.Background(), "ccs-multi", "second-job", result); err != nil {
		t.Fatalf("AppendOperationRecord: %v", err)
	}

	got := &seamv1alpha1.InfrastructureTalosClusterOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Name: "ccs-multi", Namespace: "seam-tenant-ccs-multi"},
		got); err != nil {
		t.Fatalf("get TCOR: %v", err)
	}

	if len(got.Spec.Operations) != 2 {
		t.Errorf("Operations len = %d, want 2", len(got.Spec.Operations))
	}
	if _, ok := got.Spec.Operations["first-job"]; !ok {
		t.Error("first-job record missing after second write")
	}
	if _, ok := got.Spec.Operations["second-job"]; !ok {
		t.Error("second-job record not appended")
	}
}

// TestTCOR_AppendOperationRecord_TCORNotFound verifies that AppendOperationRecord
// returns an error when the per-cluster TCOR has not been pre-created.
func TestTCOR_AppendOperationRecord_TCORNotFound(t *testing.T) {
	scheme := buildTCORScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	writer := persistence.NewKubeTalosClusterResultWriter(fakeClient)

	result := runnerlib.OperationResultSpec{
		Capability: "etcd-backup",
		Status:     runnerlib.ResultSucceeded,
	}
	err := writer.AppendOperationRecord(context.Background(), "ccs-missing", "job-x", result)
	if err == nil {
		t.Error("expected error when TCOR not found; got nil")
	}
}

// TestNoopTalosClusterResultWriter_ReturnsNil verifies the noop implementation.
func TestNoopTalosClusterResultWriter_ReturnsNil(t *testing.T) {
	var w persistence.NoopTalosClusterResultWriter
	result := runnerlib.OperationResultSpec{
		Capability: "pki-rotate",
		Status:     runnerlib.ResultSucceeded,
	}
	if err := w.AppendOperationRecord(context.Background(), "any-cluster", "any-job", result); err != nil {
		t.Errorf("NoopTalosClusterResultWriter.AppendOperationRecord expected nil; got %v", err)
	}
}

// TestTCOR_AppendOperationRecord_SetsTimestamps verifies that StartedAt and CompletedAt
// are populated in the operation record when provided.
func TestTCOR_AppendOperationRecord_SetsTimestamps(t *testing.T) {
	scheme := buildTCORScheme(t)
	tcor := preTCOR("ccs-ts", "v1.9.3")
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(tcor).Build()
	writer := persistence.NewKubeTalosClusterResultWriter(fakeClient)

	start := time.Date(2026, 4, 25, 10, 0, 0, 0, time.UTC)
	end := time.Date(2026, 4, 25, 10, 5, 0, 0, time.UTC)
	result := runnerlib.OperationResultSpec{
		Capability:  "etcd-backup",
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   start,
		CompletedAt: end,
	}
	if err := writer.AppendOperationRecord(context.Background(), "ccs-ts", "ts-job", result); err != nil {
		t.Fatalf("AppendOperationRecord: %v", err)
	}

	got := &seamv1alpha1.InfrastructureTalosClusterOperationResult{}
	if err := fakeClient.Get(context.Background(),
		ctrlclient.ObjectKey{Name: "ccs-ts", Namespace: "seam-tenant-ccs-ts"},
		got); err != nil {
		t.Fatalf("get TCOR: %v", err)
	}

	rec := got.Spec.Operations["ts-job"]
	if rec.StartedAt == nil || !rec.StartedAt.Time.Equal(start) {
		t.Errorf("StartedAt = %v, want %v", rec.StartedAt, start)
	}
	if rec.CompletedAt == nil || !rec.CompletedAt.Time.Equal(end) {
		t.Errorf("CompletedAt = %v, want %v", rec.CompletedAt, end)
	}
}

// Ensure types.NamespacedName is available for record lookup.
var _ = types.NamespacedName{}
