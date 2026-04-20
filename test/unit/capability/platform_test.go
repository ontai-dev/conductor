package capability_test

import (
	"context"
	"io"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// ---------------------------------------------------------------------------
// Fake dynamic client helpers
// ---------------------------------------------------------------------------

// platformKindToResource maps canonical Kind names to their GVR resource names.
// Required because resource names are irregular plurals (e.g., policy→policies).
var platformKindToResource = map[string]string{
	"TalosCluster":     "talosclusters",
	"UpgradePolicy":    "upgradepolicies",
	"NodeOperation":    "nodeoperations",
	"NodeMaintenance":  "nodemaintenances",
	"EtcdMaintenance":  "etcdmaintenances",
	"PKIRotation":      "pkirotations",
	"ClusterReset":     "clusterresets",
	"HardeningProfile": "hardeningprofiles",
}

// newPlatformDynClient builds a fake dynamic client with all platform GVRs
// registered and pre-creates the provided objects using the canonical Kind names.
func newPlatformDynClient(objects ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	s := runtime.NewScheme()
	for kind, resource := range platformKindToResource {
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: "platform.ontai.dev", Version: "v1alpha1", Kind: kind},
			&unstructured.Unstructured{},
		)
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: "platform.ontai.dev", Version: "v1alpha1", Kind: kind + "List"},
			&unstructured.UnstructuredList{},
		)
		_ = resource
	}
	client := dynamicfake.NewSimpleDynamicClient(s)
	for _, obj := range objects {
		kind := obj.GetKind()
		resource, ok := platformKindToResource[kind]
		if !ok {
			continue
		}
		gvr := schema.GroupVersionResource{
			Group: "platform.ontai.dev", Version: "v1alpha1", Resource: resource,
		}
		ns := obj.GetNamespace()
		if ns == "" {
			_, _ = client.Resource(gvr).Create(context.Background(), obj, metav1.CreateOptions{})
		} else {
			_, _ = client.Resource(gvr).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
		}
	}
	return client
}

// ---------------------------------------------------------------------------
// stubTalosClient — records calls and returns configured errors.
// ---------------------------------------------------------------------------

type stubTalosClient struct {
	bootstrapErr      error
	applyConfigErr    error
	upgradeErr        error
	rebootErr         error
	resetErr          error
	etcdSnapshotErr   error
	etcdRecoverErr    error
	etcdDefragErr     error
	bootstrapCalled   bool
	upgradeCalled     bool
	rebootCalled      bool
	resetCalled       bool
	defragmentCalled  bool
}

func (s *stubTalosClient) Bootstrap(_ context.Context) error {
	s.bootstrapCalled = true
	return s.bootstrapErr
}
func (s *stubTalosClient) ApplyConfiguration(_ context.Context, _ []byte, _ string) error {
	return s.applyConfigErr
}
func (s *stubTalosClient) Upgrade(_ context.Context, _ string, _ bool) error {
	s.upgradeCalled = true
	return s.upgradeErr
}
func (s *stubTalosClient) Reboot(_ context.Context) error {
	s.rebootCalled = true
	return s.rebootErr
}
func (s *stubTalosClient) Reset(_ context.Context, _ bool) error {
	s.resetCalled = true
	return s.resetErr
}
func (s *stubTalosClient) EtcdSnapshot(_ context.Context, _ io.Writer) error {
	return s.etcdSnapshotErr
}
func (s *stubTalosClient) EtcdRecover(_ context.Context, _ io.Reader) error {
	return s.etcdRecoverErr
}
func (s *stubTalosClient) EtcdDefragment(_ context.Context) error {
	s.defragmentCalled = true
	return s.etcdDefragErr
}
func (s *stubTalosClient) Close() error { return nil }

// assertValidationFailure asserts Status=Failed and Category=ValidationFailure.
func assertValidationFailure(t *testing.T, result runnerlib.OperationResultSpec, label string) {
	t.Helper()
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("%s: expected ResultFailed; got %q", label, result.Status)
	}
	if result.FailureReason == nil {
		t.Errorf("%s: expected non-nil FailureReason", label)
		return
	}
	if result.FailureReason.Category != runnerlib.ValidationFailure {
		t.Errorf("%s: expected ValidationFailure; got %q", label, result.FailureReason.Category)
	}
}

// ---------------------------------------------------------------------------
// bootstrap
// ---------------------------------------------------------------------------

func TestBootstrap_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityBootstrap)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityBootstrap,
		ClusterRef: "ccs-test",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "bootstrap nil clients")
}

func TestBootstrap_NoTalosClusterCRReturnsFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityBootstrap)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityBootstrap,
		ClusterRef: "ccs-test",
		Namespace:  "ont-system",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			DynamicClient: newPlatformDynClient(), // empty
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed; got %q", result.Status)
	}
}

// TestBootstrap_CallsBootstrapAPIWhenClusterExists verifies the bootstrap path.
// conductor-schema.md §6.
func TestBootstrap_CallsBootstrapAPIWhenClusterExists(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityBootstrap)

	cluster := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "platform.ontai.dev/v1alpha1",
			"kind": "TalosCluster",
			"metadata":   map[string]interface{}{"name": "ccs-test", "namespace": "ont-system"},
			"spec":       map[string]interface{}{},
		},
	}
	talos := &stubTalosClient{}

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityBootstrap,
		ClusterRef: "ccs-test",
		Namespace:  "ont-system",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(cluster),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.bootstrapCalled {
		t.Error("expected Bootstrap() to be called")
	}
}

// ---------------------------------------------------------------------------
// talos-upgrade
// ---------------------------------------------------------------------------

func TestTalosUpgrade_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityTalosUpgrade)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityTalosUpgrade})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "talos-upgrade nil clients")
}

func TestTalosUpgrade_CallsUpgradeAPI(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityTalosUpgrade)

	talos := &stubTalosClient{}
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityTalosUpgrade,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(upgradePolicyCR("ccs-test", "talos", "v1.12.0", "")),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.upgradeCalled {
		t.Error("expected Upgrade() to be called")
	}
}

// ---------------------------------------------------------------------------
// kube-upgrade
// ---------------------------------------------------------------------------

func TestKubeUpgrade_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityKubeUpgrade)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityKubeUpgrade})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "kube-upgrade nil clients")
}

func TestKubeUpgrade_AppliesKubeletConfig(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityKubeUpgrade)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityKubeUpgrade,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			DynamicClient: newPlatformDynClient(upgradePolicyCR("ccs-test", "kubernetes", "", "v1.32.0")),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
}

// ---------------------------------------------------------------------------
// stack-upgrade
// ---------------------------------------------------------------------------

func TestStackUpgrade_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityStackUpgrade)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityStackUpgrade})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "stack-upgrade nil clients")
}

func TestStackUpgrade_RunsBothUpgradeSteps(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityStackUpgrade)

	talos := &stubTalosClient{}
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityStackUpgrade,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(upgradePolicyCR("ccs-test", "stack", "v1.12.0", "v1.32.0")),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.upgradeCalled {
		t.Error("expected Upgrade() to be called for talos step")
	}
	if len(result.Steps) < 2 {
		t.Errorf("expected ≥2 steps; got %d", len(result.Steps))
	}
}

// ---------------------------------------------------------------------------
// node-reboot
// ---------------------------------------------------------------------------

func TestNodeReboot_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodeReboot)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityNodeReboot})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "node-reboot nil clients")
}

func TestNodeReboot_CallsRebootAPI(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodeReboot)

	talos := &stubTalosClient{}
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityNodeReboot,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(nodeOperationCR("ccs-test", "reboot", []string{"worker-1"}, 0)),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.rebootCalled {
		t.Error("expected Reboot() to be called")
	}
}

// ---------------------------------------------------------------------------
// node-patch
// ---------------------------------------------------------------------------

func TestNodePatch_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodePatch)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityNodePatch})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "node-patch nil clients")
}

func TestNodePatch_AppliesConfigFromSecret(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodePatch)

	clusterRef := "ccs-test"
	ns := "tenant-" + clusterRef
	kubeClient := fake.NewSimpleClientset()
	_, _ = kubeClient.CoreV1().Secrets(ns).Create(context.Background(), &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "mc-patch-secret", Namespace: ns},
		Data:       map[string][]byte{"machineconfig": []byte(`{"machine":{}}`)},
	}, metav1.CreateOptions{})

	talos := &stubTalosClient{}
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityNodePatch,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			KubeClient:    kubeClient,
			DynamicClient: newPlatformDynClient(nodeMaintenanceCR(clusterRef, "patch", "mc-patch-secret")),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
}

// ---------------------------------------------------------------------------
// node-decommission
// ---------------------------------------------------------------------------

func TestNodeDecommission_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodeDecommission)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityNodeDecommission})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "node-decommission nil clients")
}

func TestNodeDecommission_CordonsAndResetsNode(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodeDecommission)

	clusterRef := "ccs-test"
	kubeClient := fake.NewSimpleClientset()
	_, _ = kubeClient.CoreV1().Nodes().Create(context.Background(), &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: "worker-1"},
	}, metav1.CreateOptions{})

	talos := &stubTalosClient{}
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityNodeDecommission,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			KubeClient:    kubeClient,
			DynamicClient: newPlatformDynClient(nodeOperationCR(clusterRef, "decommission", []string{"worker-1"}, 0)),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.resetCalled {
		t.Error("expected Reset() to be called")
	}
	n, _ := kubeClient.CoreV1().Nodes().Get(context.Background(), "worker-1", metav1.GetOptions{})
	if n != nil && !n.Spec.Unschedulable {
		t.Error("expected node to be cordoned after decommission")
	}
}

// ---------------------------------------------------------------------------
// node-scale-up
// ---------------------------------------------------------------------------

func TestNodeScaleUp_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodeScaleUp)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityNodeScaleUp})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "node-scale-up nil clients")
}

func TestNodeScaleUp_WritesRequestConfigMap(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityNodeScaleUp)

	clusterRef := "ccs-test"
	ns := "tenant-" + clusterRef
	kubeClient := fake.NewSimpleClientset()

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityNodeScaleUp,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			KubeClient:    kubeClient,
			DynamicClient: newPlatformDynClient(nodeOperationCR(clusterRef, "scale-up", nil, 5)),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	cm, cmErr := kubeClient.CoreV1().ConfigMaps(ns).Get(
		context.Background(), "node-scale-up-"+clusterRef, metav1.GetOptions{})
	if cmErr != nil {
		t.Fatalf("scale-up request ConfigMap not found: %v", cmErr)
	}
	if cm.Data["targetReplicaCount"] != "5" {
		t.Errorf("expected targetReplicaCount=5; got %q", cm.Data["targetReplicaCount"])
	}
}

// ---------------------------------------------------------------------------
// etcd-defrag
// ---------------------------------------------------------------------------

func TestEtcdMaintenance_NilClientReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdDefrag)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityEtcdDefrag})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "etcd-defrag nil client")
}

func TestEtcdMaintenance_CallsDefragment(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdDefrag)

	talos := &stubTalosClient{}
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityEtcdDefrag,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{TalosClient: talos},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.defragmentCalled {
		t.Error("expected EtcdDefragment() to be called")
	}
}

// ---------------------------------------------------------------------------
// pki-rotate
// ---------------------------------------------------------------------------

func TestPKIRotate_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPKIRotate)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityPKIRotate})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "pki-rotate nil clients")
}

func TestPKIRotate_StagesConfigWhenCRPresent(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPKIRotate)

	clusterRef := "ccs-test"
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPKIRotate,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			DynamicClient: newPlatformDynClient(pkiRotationCR(clusterRef)),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
}

// ---------------------------------------------------------------------------
// credential-rotate
// ---------------------------------------------------------------------------

func TestCredentialRotate_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityCredentialRotate)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityCredentialRotate})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "credential-rotate nil clients")
}

// ---------------------------------------------------------------------------
// hardening-apply
// ---------------------------------------------------------------------------

func TestHardeningApply_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityHardeningApply)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityHardeningApply})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "hardening-apply nil clients")
}

// ---------------------------------------------------------------------------
// cluster-reset
// ---------------------------------------------------------------------------

func TestClusterReset_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityClusterReset)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityClusterReset})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "cluster-reset nil clients")
}

// TestClusterReset_BlockedWithoutApprovalAnnotation verifies the human gate.
// CLAUDE.md INV-007: destructive operations require human approval annotation.
func TestClusterReset_BlockedWithoutApprovalAnnotation(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityClusterReset)

	talos := &stubTalosClient{}
	clusterRef := "ccs-test"
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityClusterReset,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(clusterResetCR(clusterRef, false)),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed without approval; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ValidationFailure {
		t.Errorf("expected ValidationFailure; got %+v", result.FailureReason)
	}
	if talos.resetCalled {
		t.Error("Reset() must NOT be called without approval annotation — INV-007")
	}
}

func TestClusterReset_ExecutesWhenApproved(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityClusterReset)

	talos := &stubTalosClient{}
	clusterRef := "ccs-test"
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityClusterReset,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(clusterResetCR(clusterRef, true)),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !talos.resetCalled {
		t.Error("expected Reset() to be called when approval annotation is present")
	}
}

// ---------------------------------------------------------------------------
// CR builder helpers
// ---------------------------------------------------------------------------

func upgradePolicyCR(clusterRef, upgradeType, talosVersion, kubeVersion string) *unstructured.Unstructured {
	spec := map[string]interface{}{"upgradeType": upgradeType}
	if talosVersion != "" {
		spec["targetTalosVersion"] = talosVersion
	}
	if kubeVersion != "" {
		spec["targetKubernetesVersion"] = kubeVersion
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind": "UpgradePolicy",
		"metadata":   map[string]interface{}{"name": "up-" + clusterRef, "namespace": "tenant-" + clusterRef},
		"spec":       spec,
	}}
}

func nodeOperationCR(clusterRef, operation string, targetNodes []string, replicaCount int64) *unstructured.Unstructured {
	spec := map[string]interface{}{"operation": operation}
	if len(targetNodes) > 0 {
		nodes := make([]interface{}, len(targetNodes))
		for i, n := range targetNodes {
			nodes[i] = n
		}
		spec["targetNodes"] = nodes
	}
	if replicaCount > 0 {
		spec["replicaCount"] = replicaCount
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind": "NodeOperation",
		"metadata":   map[string]interface{}{"name": "nop-" + clusterRef, "namespace": "tenant-" + clusterRef},
		"spec":       spec,
	}}
}

func nodeMaintenanceCR(clusterRef, operation, patchSecretName string) *unstructured.Unstructured {
	spec := map[string]interface{}{"operation": operation}
	if patchSecretName != "" {
		spec["patchSecretRef"] = map[string]interface{}{"name": patchSecretName}
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind": "NodeMaintenance",
		"metadata":   map[string]interface{}{"name": "nm-" + clusterRef, "namespace": "tenant-" + clusterRef},
		"spec":       spec,
	}}
}

func pkiRotationCR(clusterRef string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind": "PKIRotation",
		"metadata":   map[string]interface{}{"name": "pkir-" + clusterRef, "namespace": "tenant-" + clusterRef},
		"spec":       map[string]interface{}{},
	}}
}

func clusterResetCR(clusterRef string, approved bool) *unstructured.Unstructured {
	annotations := map[string]interface{}{}
	if approved {
		annotations["ontai.dev/reset-approved"] = "true"
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind": "ClusterReset",
		"metadata": map[string]interface{}{
			"name": "crst-" + clusterRef, "namespace": "tenant-" + clusterRef,
			"annotations": annotations,
		},
		"spec": map[string]interface{}{"drainGracePeriodSeconds": "30"},
	}}
}
