package persistence_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ontai-dev/conductor/internal/persistence"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestConfigMapWriter_CreatesConfigMapWithResultKey verifies that WriteResult
// creates a ConfigMap with the result JSON under persistence.ResultDataKey.
// conductor-schema.md §8.
func TestConfigMapWriter_CreatesConfigMapWithResultKey(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	writer := persistence.NewKubeConfigMapWriter(fakeClient)

	result := runnerlib.OperationResultSpec{
		Capability:  runnerlib.CapabilityNodePatch,
		Status:      runnerlib.ResultSucceeded,
		StartedAt:   time.Now().UTC(),
		CompletedAt: time.Now().UTC(),
		Artifacts:   []runnerlib.ArtifactRef{},
		Steps:       []runnerlib.StepResult{},
	}

	if err := writer.WriteResult(context.Background(), "ont-system", "node-patch-result", result); err != nil {
		t.Fatalf("WriteResult returned unexpected error: %v", err)
	}

	cm, err := fakeClient.CoreV1().ConfigMaps("ont-system").Get(
		context.Background(), "node-patch-result", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMap not found after WriteResult: %v", err)
	}

	raw, ok := cm.Data[persistence.ResultDataKey]
	if !ok {
		t.Fatalf("ConfigMap missing key %q", persistence.ResultDataKey)
	}

	var got runnerlib.OperationResultSpec
	if err := json.Unmarshal([]byte(raw), &got); err != nil {
		t.Fatalf("cannot unmarshal result from ConfigMap: %v", err)
	}

	if got.Capability != runnerlib.CapabilityNodePatch {
		t.Errorf("expected Capability %q; got %q", runnerlib.CapabilityNodePatch, got.Capability)
	}
	if got.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected Status %q; got %q", runnerlib.ResultSucceeded, got.Status)
	}
}

// TestConfigMapWriter_UpdatesExistingConfigMap verifies that WriteResult updates
// an already-existing ConfigMap rather than failing with AlreadyExists.
// conductor-schema.md §8.
func TestConfigMapWriter_UpdatesExistingConfigMap(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	writer := persistence.NewKubeConfigMapWriter(fakeClient)

	first := runnerlib.OperationResultSpec{
		Capability: runnerlib.CapabilityNodePatch,
		Status:     runnerlib.ResultFailed,
		Artifacts:  []runnerlib.ArtifactRef{},
		Steps:      []runnerlib.StepResult{},
	}
	if err := writer.WriteResult(context.Background(), "ont-system", "node-patch-result", first); err != nil {
		t.Fatalf("first WriteResult: %v", err)
	}

	second := runnerlib.OperationResultSpec{
		Capability: runnerlib.CapabilityNodePatch,
		Status:     runnerlib.ResultSucceeded,
		Artifacts:  []runnerlib.ArtifactRef{},
		Steps:      []runnerlib.StepResult{},
	}
	if err := writer.WriteResult(context.Background(), "ont-system", "node-patch-result", second); err != nil {
		t.Fatalf("second WriteResult: %v", err)
	}

	cm, err := fakeClient.CoreV1().ConfigMaps("ont-system").Get(
		context.Background(), "node-patch-result", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("ConfigMap not found: %v", err)
	}

	var got runnerlib.OperationResultSpec
	if err := json.Unmarshal([]byte(cm.Data[persistence.ResultDataKey]), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected updated Status %q; got %q", runnerlib.ResultSucceeded, got.Status)
	}
}

// TestConfigMapWriter_CorrectNamespaceAndName verifies that the ConfigMap is
// created in the exact namespace and with the exact name provided.
func TestConfigMapWriter_CorrectNamespaceAndName(t *testing.T) {
	fakeClient := fake.NewSimpleClientset()
	writer := persistence.NewKubeConfigMapWriter(fakeClient)

	result := runnerlib.OperationResultSpec{
		Capability: runnerlib.CapabilityEtcdBackup,
		Status:     runnerlib.ResultSucceeded,
		Artifacts:  []runnerlib.ArtifactRef{},
	}
	if err := writer.WriteResult(context.Background(), "ont-system", "etcd-backup-cm", result); err != nil {
		t.Fatalf("WriteResult: %v", err)
	}

	// ConfigMap must exist in the specified namespace with the specified name.
	if _, err := fakeClient.CoreV1().ConfigMaps("ont-system").Get(
		context.Background(), "etcd-backup-cm", metav1.GetOptions{}); err != nil {
		t.Errorf("ConfigMap not found in expected namespace/name: %v", err)
	}
	// Must not exist in a different namespace.
	if _, err := fakeClient.CoreV1().ConfigMaps("default").Get(
		context.Background(), "etcd-backup-cm", metav1.GetOptions{}); err == nil {
		t.Error("ConfigMap must not exist in namespace 'default'")
	}
}

// TestNoopConfigMapWriter_WriteResultReturnsNil verifies that the noop writer
// accepts any input and always returns nil without writing anything.
func TestNoopConfigMapWriter_WriteResultReturnsNil(t *testing.T) {
	var w persistence.NoopConfigMapWriter
	result := runnerlib.OperationResultSpec{
		Capability: runnerlib.CapabilityNodePatch,
		Status:     runnerlib.ResultSucceeded,
	}
	if err := w.WriteResult(context.Background(), "ont-system", "result-cm", result); err != nil {
		t.Errorf("NoopConfigMapWriter.WriteResult expected nil; got %v", err)
	}
}
