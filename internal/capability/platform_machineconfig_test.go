package capability

import (
	"bytes"
	"context"
	"io"
	"testing"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// stubTalosClientMC is a minimal TalosNodeClient for machineconfig-backup tests.
type stubTalosClientMC struct {
	configYAML []byte
	configErr  error
}

func (s *stubTalosClientMC) Bootstrap(_ context.Context) error                              { return nil }
func (s *stubTalosClientMC) ApplyConfiguration(_ context.Context, _ []byte, _ string) error { return nil }
func (s *stubTalosClientMC) Upgrade(_ context.Context, _ string, _ bool) error              { return nil }
func (s *stubTalosClientMC) Reboot(_ context.Context) error                                 { return nil }
func (s *stubTalosClientMC) Reset(_ context.Context, _ bool) error                          { return nil }
func (s *stubTalosClientMC) EtcdSnapshot(_ context.Context, _ io.Writer) error              { return nil }
func (s *stubTalosClientMC) EtcdRecover(_ context.Context, _ io.Reader) error               { return nil }
func (s *stubTalosClientMC) EtcdDefragment(_ context.Context) error                         { return nil }
func (s *stubTalosClientMC) Kubeconfig(_ context.Context) ([]byte, error)                   { return nil, nil }
func (s *stubTalosClientMC) Nodes() []string                                                { return nil }
func (s *stubTalosClientMC) Health(_ context.Context) error                                 { return nil }
func (s *stubTalosClientMC) Close() error                                                   { return nil }

func (s *stubTalosClientMC) GetMachineConfig(_ context.Context) ([]byte, error) {
	return s.configYAML, s.configErr
}

// stubStorageMC records Upload calls and can return an error.
type stubStorageMC struct {
	uploads   []stubUploadCall
	uploadErr error
}

type stubUploadCall struct {
	bucket string
	key    string
	data   []byte
}

func (s *stubStorageMC) Upload(_ context.Context, bucket, key string, r io.Reader) error {
	if s.uploadErr != nil {
		return s.uploadErr
	}
	data, _ := io.ReadAll(r)
	s.uploads = append(s.uploads, stubUploadCall{bucket: bucket, key: key, data: data})
	return nil
}

func (s *stubStorageMC) Download(_ context.Context, _, _ string) (io.ReadCloser, error) {
	return nil, nil
}

// buildMachineConfigBackupScheme returns a scheme with TalosMachineConfigBackup registered.
func buildMachineConfigBackupScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "platform.ontai.dev", Version: "v1alpha1", Kind: "TalosMachineConfigBackup"},
		&unstructured.Unstructured{},
	)
	s.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "platform.ontai.dev", Version: "v1alpha1", Kind: "TalosMachineConfigBackupList"},
		&unstructured.UnstructuredList{},
	)
	return s
}

// makeMachineConfigBackupCR builds an unstructured TalosMachineConfigBackup.
func makeMachineConfigBackupCR(namespace, name, bucket string) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "platform.ontai.dev",
		Version: "v1alpha1",
		Kind:    "TalosMachineConfigBackup",
	})
	obj.SetNamespace(namespace)
	obj.SetName(name)
	_ = unstructured.SetNestedField(obj.Object, bucket, "spec", "s3Destination", "bucket")
	return obj
}

func TestMachineConfigBackupHandler_NilClients(t *testing.T) {
	h := &machineConfigBackupHandler{}
	params := ExecuteParams{
		ClusterRef: "ccs-dev",
		// All clients nil.
	}
	result, err := h.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed, got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ValidationFailure {
		t.Errorf("expected ValidationFailure category, got %v", result.FailureReason)
	}
}

func TestMachineConfigBackupHandler_NoCR(t *testing.T) {
	scheme := buildMachineConfigBackupScheme()
	dynClient := fake.NewSimpleDynamicClient(scheme)

	h := &machineConfigBackupHandler{}
	params := ExecuteParams{
		ClusterRef: "ccs-dev",
		ExecuteClients: ExecuteClients{
			TalosClient:     &stubTalosClientMC{configYAML: []byte("machine:\n  network:\n    hostname: node1\n")},
			StorageClient:   &stubStorageMC{},
			DynamicClient:   dynClient,
			TalosconfigPath: "",
		},
	}
	result, err := h.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed when no CR present, got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ValidationFailure {
		t.Errorf("expected ValidationFailure, got %v", result.FailureReason)
	}
}

func TestMachineConfigBackupHandler_Success_SingleNode(t *testing.T) {
	scheme := buildMachineConfigBackupScheme()
	ns := "seam-tenant-ccs-dev"
	cr := makeMachineConfigBackupCR(ns, "mcb-test", "my-bucket")
	dynClient := fake.NewSimpleDynamicClient(scheme, cr)

	configYAML := []byte("machine:\n  network:\n    hostname: cp1\n")
	storage := &stubStorageMC{}

	h := &machineConfigBackupHandler{}
	params := ExecuteParams{
		ClusterRef: "ccs-dev",
		ExecuteClients: ExecuteClients{
			TalosClient:     &stubTalosClientMC{configYAML: configYAML},
			StorageClient:   storage,
			DynamicClient:   dynClient,
			TalosconfigPath: "", // single-node fallback path
		},
	}

	result, err := h.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected Succeeded, got %q: steps=%v", result.Status, result.Steps)
	}
	if len(storage.uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(storage.uploads))
	}
	if storage.uploads[0].bucket != "my-bucket" {
		t.Errorf("bucket: expected %q, got %q", "my-bucket", storage.uploads[0].bucket)
	}
	if !bytes.Equal(storage.uploads[0].data, configYAML) {
		t.Error("upload data does not match machine config YAML")
	}
	// Key format: {cluster}/machineconfigs/{ts}/{hostname}.yaml
	key := storage.uploads[0].key
	if !findSubstring(key, "ccs-dev/machineconfigs/") {
		t.Errorf("expected key to contain %q, got %q", "ccs-dev/machineconfigs/", key)
	}
	if !findSubstring(key, "cp1.yaml") {
		t.Errorf("expected key to contain hostname cp1.yaml, got %q", key)
	}
	// Artifacts must reference the S3 object.
	if len(result.Artifacts) != 1 {
		t.Fatalf("expected 1 artifact, got %d", len(result.Artifacts))
	}
	if result.Artifacts[0].Kind != "S3Object" {
		t.Errorf("artifact Kind: expected S3Object, got %q", result.Artifacts[0].Kind)
	}
}

func TestMachineConfigBackupHandler_StorageUploadFailure(t *testing.T) {
	scheme := buildMachineConfigBackupScheme()
	ns := "seam-tenant-ccs-dev"
	cr := makeMachineConfigBackupCR(ns, "mcb-test", "my-bucket")
	dynClient := fake.NewSimpleDynamicClient(scheme, cr)

	storage := &stubStorageMC{uploadErr: io.ErrUnexpectedEOF}

	h := &machineConfigBackupHandler{}
	params := ExecuteParams{
		ClusterRef: "ccs-dev",
		ExecuteClients: ExecuteClients{
			TalosClient:     &stubTalosClientMC{configYAML: []byte("machine:\n  network:\n    hostname: cp1\n")},
			StorageClient:   storage,
			DynamicClient:   dynClient,
			TalosconfigPath: "",
		},
	}

	result, err := h.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed on upload error, got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ExternalDependencyFailure {
		t.Errorf("expected ExternalDependencyFailure category, got %v", result.FailureReason)
	}
}

func TestMachineConfigBackupHandler_HostnameFallback_NoHostnameInConfig(t *testing.T) {
	scheme := buildMachineConfigBackupScheme()
	ns := "seam-tenant-ccs-dev"
	cr := makeMachineConfigBackupCR(ns, "mcb-test", "my-bucket")
	dynClient := fake.NewSimpleDynamicClient(scheme, cr)

	// Machine config has no hostname field.
	storage := &stubStorageMC{}
	h := &machineConfigBackupHandler{}
	params := ExecuteParams{
		ClusterRef: "ccs-dev",
		ExecuteClients: ExecuteClients{
			TalosClient:     &stubTalosClientMC{configYAML: []byte("machine:\n  install:\n    disk: /dev/sda\n")},
			StorageClient:   storage,
			DynamicClient:   dynClient,
			TalosconfigPath: "",
		},
	}

	result, err := h.Execute(context.Background(), params)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected Succeeded even without hostname in config, got %q", result.Status)
	}
	if len(storage.uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(storage.uploads))
	}
	// Key must fall back to sanitized nodeIP "node".
	if !findSubstring(storage.uploads[0].key, "node.yaml") {
		t.Errorf("expected fallback key containing 'node.yaml', got %q", storage.uploads[0].key)
	}
}

// findSubstring returns true when sub appears anywhere in s.
func findSubstring(s, sub string) bool {
	if len(sub) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
