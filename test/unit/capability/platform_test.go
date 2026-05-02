package capability_test

import (
	"bytes"
	"context"
	"fmt"
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

// platformKindToResource maps canonical Kind names to their GVR resource names
// under platform.ontai.dev. Required because resource names are irregular plurals.
var platformKindToResource = map[string]string{
	"UpgradePolicy":    "upgradepolicies",
	"NodeOperation":    "nodeoperations",
	"NodeMaintenance":  "nodemaintenances",
	"EtcdMaintenance":  "etcdmaintenances",
	"PKIRotation":      "pkirotations",
	"ClusterReset":     "clusterresets",
	"HardeningProfile": "hardeningprofiles",
}

// infraKindToResource maps seam-core infrastructure.ontai.dev Kind names to GVR resources.
// TalosCluster migrated to infrastructure.ontai.dev in the seam rebranding.
var infraKindToResource = map[string]string{
	"InfrastructureTalosCluster": "infrastructuretalosclusters",
}

// newPlatformDynClient builds a fake dynamic client with all platform and infrastructure
// GVRs registered and pre-creates the provided objects using their canonical Kind names.
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
	for kind, resource := range infraKindToResource {
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: kind},
			&unstructured.Unstructured{},
		)
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: kind + "List"},
			&unstructured.UnstructuredList{},
		)
		_ = resource
	}
	client := dynamicfake.NewSimpleDynamicClient(s)
	for _, obj := range objects {
		kind := obj.GetKind()
		if resource, ok := platformKindToResource[kind]; ok {
			gvr := schema.GroupVersionResource{
				Group: "platform.ontai.dev", Version: "v1alpha1", Resource: resource,
			}
			ns := obj.GetNamespace()
			if ns == "" {
				_, _ = client.Resource(gvr).Create(context.Background(), obj, metav1.CreateOptions{})
			} else {
				_, _ = client.Resource(gvr).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
			}
		} else if resource, ok := infraKindToResource[kind]; ok {
			gvr := schema.GroupVersionResource{
				Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: resource,
			}
			ns := obj.GetNamespace()
			if ns == "" {
				_, _ = client.Resource(gvr).Create(context.Background(), obj, metav1.CreateOptions{})
			} else {
				_, _ = client.Resource(gvr).Namespace(ns).Create(context.Background(), obj, metav1.CreateOptions{})
			}
		}
	}
	return client
}

// ---------------------------------------------------------------------------
// stubTalosClient — records calls and returns configured errors.
// ---------------------------------------------------------------------------

type stubTalosClient struct {
	bootstrapErr          error
	applyConfigErr        error
	upgradeErr            error
	rebootErr             error
	resetErr              error
	etcdSnapshotErr       error
	etcdRecoverErr        error
	etcdDefragErr         error
	getMachineConfigErr   error
	getMachineConfigBytes []byte
	kubeconfigErr         error
	kubeconfigBytes       []byte
	bootstrapCalled       bool
	upgradeCalled         bool
	rebootCalled          bool
	resetCalled           bool
	defragmentCalled      bool
	applyConfigCalls      []applyConfigCall
}

type applyConfigCall struct {
	configBytes []byte
	mode        string
}

func (s *stubTalosClient) Bootstrap(_ context.Context) error {
	s.bootstrapCalled = true
	return s.bootstrapErr
}
func (s *stubTalosClient) ApplyConfiguration(_ context.Context, configBytes []byte, mode string) error {
	s.applyConfigCalls = append(s.applyConfigCalls, applyConfigCall{configBytes: configBytes, mode: mode})
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
func (s *stubTalosClient) GetMachineConfig(_ context.Context) ([]byte, error) {
	return s.getMachineConfigBytes, s.getMachineConfigErr
}
func (s *stubTalosClient) Kubeconfig(_ context.Context) ([]byte, error) {
	return s.kubeconfigBytes, s.kubeconfigErr
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
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureTalosCluster",
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
	ns := "seam-tenant-" + clusterRef
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
	ns := "seam-tenant-" + clusterRef
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

// TestHardeningApply_SinglePatchApplied verifies that a HardeningProfile with one
// machineConfigPatch causes a single ApplyConfiguration call in no-reboot mode.
func TestHardeningApply_SinglePatchApplied(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityHardeningApply)

	clusterRef := "ccs-test"
	profileName := "seam-hardening-base"
	patch := "machine:\n  sysctls:\n    net.ipv4.ip_forward: \"1\"\n"

	nm := nodeMaintenanceHardeningCR(clusterRef, profileName, "")
	hp := hardeningProfileCR(clusterRef, profileName, []string{patch})
	talos := &stubTalosClient{}

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityHardeningApply,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(nm, hp),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q: %+v", result.Status, result.FailureReason)
	}
	if len(talos.applyConfigCalls) != 1 {
		t.Fatalf("expected 1 ApplyConfiguration call; got %d", len(talos.applyConfigCalls))
	}
	if talos.applyConfigCalls[0].mode != "no-reboot" {
		t.Errorf("expected mode=no-reboot; got %q", talos.applyConfigCalls[0].mode)
	}
	if string(talos.applyConfigCalls[0].configBytes) != patch {
		t.Errorf("config bytes mismatch; got %q want %q",
			string(talos.applyConfigCalls[0].configBytes), patch)
	}
	if len(result.Steps) != 1 {
		t.Errorf("expected 1 step; got %d", len(result.Steps))
	}
}

// TestHardeningApply_MultiPatchApplied verifies that multiple machineConfigPatches
// cause one ApplyConfiguration call per patch, in order.
func TestHardeningApply_MultiPatchApplied(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityHardeningApply)

	clusterRef := "ccs-test"
	profileName := "seam-hardening-multi"
	patches := []string{
		"machine:\n  sysctls:\n    net.ipv4.ip_forward: \"1\"\n",
		"machine:\n  sysctls:\n    net.ipv4.conf.all.rp_filter: \"1\"\n",
	}

	nm := nodeMaintenanceHardeningCR(clusterRef, profileName, "")
	hp := hardeningProfileCR(clusterRef, profileName, patches)
	talos := &stubTalosClient{}

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityHardeningApply,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(nm, hp),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q", result.Status)
	}
	if len(talos.applyConfigCalls) != 2 {
		t.Fatalf("expected 2 ApplyConfiguration calls; got %d", len(talos.applyConfigCalls))
	}
	for i, call := range talos.applyConfigCalls {
		if call.mode != "no-reboot" {
			t.Errorf("call %d: expected mode=no-reboot; got %q", i, call.mode)
		}
		if string(call.configBytes) != patches[i] {
			t.Errorf("call %d: config bytes mismatch", i)
		}
	}
	if len(result.Steps) != 2 {
		t.Errorf("expected 2 steps; got %d", len(result.Steps))
	}
}

// TestHardeningApply_EmptyPatchesValidationFailure verifies that a HardeningProfile
// with no machineConfigPatches returns a ValidationFailure.
func TestHardeningApply_EmptyPatchesValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityHardeningApply)

	clusterRef := "ccs-test"
	profileName := "seam-hardening-empty"

	nm := nodeMaintenanceHardeningCR(clusterRef, profileName, "")
	hp := hardeningProfileCR(clusterRef, profileName, nil)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityHardeningApply,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			DynamicClient: newPlatformDynClient(nm, hp),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "hardening-apply empty patches")
}

// TestHardeningApply_NoHardeningProfileRefValidationFailure verifies that a
// NodeMaintenance without a hardeningProfileRef returns a ValidationFailure.
func TestHardeningApply_NoHardeningProfileRefValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityHardeningApply)

	clusterRef := "ccs-test"
	// NodeMaintenance with operation=hardening-apply but no hardeningProfileRef.
	nm := nodeMaintenanceCR(clusterRef, "hardening-apply", "")

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityHardeningApply,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			DynamicClient: newPlatformDynClient(nm),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "hardening-apply no profileRef")
}

// TestHardeningApply_ApplyErrorReturnsExecutionFailure verifies that a Talos API
// error during patch application returns an ExecutionFailure.
func TestHardeningApply_ApplyErrorReturnsExecutionFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityHardeningApply)

	clusterRef := "ccs-test"
	profileName := "seam-hardening-base"
	patch := "machine:\n  sysctls:\n    net.ipv4.ip_forward: \"1\"\n"

	nm := nodeMaintenanceHardeningCR(clusterRef, profileName, "")
	hp := hardeningProfileCR(clusterRef, profileName, []string{patch})
	talos := &stubTalosClient{applyConfigErr: fmt.Errorf("talos connection refused")}

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityHardeningApply,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   talos,
			DynamicClient: newPlatformDynClient(nm, hp),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.Category != runnerlib.ExecutionFailure {
		t.Errorf("expected ExecutionFailure; got %+v", result.FailureReason)
	}
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
		"metadata":   map[string]interface{}{"name": "up-" + clusterRef, "namespace": "seam-tenant-" + clusterRef},
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
		"metadata":   map[string]interface{}{"name": "nop-" + clusterRef, "namespace": "seam-tenant-" + clusterRef},
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
		"metadata":   map[string]interface{}{"name": "nm-" + clusterRef, "namespace": "seam-tenant-" + clusterRef},
		"spec":       spec,
	}}
}

func pkiRotationCR(clusterRef string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind": "PKIRotation",
		"metadata":   map[string]interface{}{"name": "pkir-" + clusterRef, "namespace": "seam-tenant-" + clusterRef},
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
			"name": "crst-" + clusterRef, "namespace": "seam-tenant-" + clusterRef,
			"annotations": annotations,
		},
		"spec": map[string]interface{}{"drainGracePeriodSeconds": "30"},
	}}
}

func etcdMaintenanceCR(clusterRef, operation string, extraSpec map[string]interface{}) *unstructured.Unstructured {
	spec := map[string]interface{}{"operation": operation}
	for k, v := range extraSpec {
		spec[k] = v
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind":       "EtcdMaintenance",
		"metadata":   map[string]interface{}{"name": "em-" + clusterRef, "namespace": "seam-tenant-" + clusterRef},
		"spec":       spec,
	}}
}

// stubStorageClient records Upload/Download calls and returns configured errors.
type stubStorageClient struct {
	uploadErr    error
	downloadErr  error
	downloadData []byte
	uploadCalled bool
	lastBucket   string
	lastKey      string
}

func (s *stubStorageClient) Upload(_ context.Context, bucket, key string, _ io.Reader) error {
	s.uploadCalled = true
	s.lastBucket = bucket
	s.lastKey = key
	return s.uploadErr
}

func (s *stubStorageClient) Download(_ context.Context, bucket, key string) (io.ReadCloser, error) {
	s.lastBucket = bucket
	s.lastKey = key
	if s.downloadErr != nil {
		return nil, s.downloadErr
	}
	return io.NopCloser(bytes.NewReader(s.downloadData)), nil
}

// ---------------------------------------------------------------------------
// etcd-backup
// ---------------------------------------------------------------------------

func TestEtcdBackup_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdBackup)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityEtcdBackup})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "etcd-backup nil clients")
}

func TestEtcdBackup_MissingCRReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdBackup)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityEtcdBackup,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			StorageClient: &stubStorageClient{},
			DynamicClient: newPlatformDynClient(),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "etcd-backup no CR")
}

func TestEtcdBackup_UploadSnapshotToS3(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdBackup)

	clusterRef := "ccs-test"
	storage := &stubStorageClient{}
	cr := etcdMaintenanceCR(clusterRef, "backup", map[string]interface{}{
		"s3Destination": map[string]interface{}{"bucket": "ont-backups", "key": "snapshot.db"},
	})
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityEtcdBackup,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			StorageClient: storage,
			DynamicClient: newPlatformDynClient(cr),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if !storage.uploadCalled {
		t.Error("expected StorageClient.Upload() to be called")
	}
	if storage.lastBucket != "ont-backups" {
		t.Errorf("expected bucket=ont-backups; got %q", storage.lastBucket)
	}
	if len(result.Artifacts) == 0 {
		t.Error("expected at least one artifact in result")
	}
}

// ---------------------------------------------------------------------------
// etcd-restore
// ---------------------------------------------------------------------------

func TestEtcdRestore_NilClientsReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdRestore)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{Capability: runnerlib.CapabilityEtcdRestore})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "etcd-restore nil clients")
}

func TestEtcdRestore_MissingCRReturnsValidationFailure(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdRestore)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityEtcdRestore,
		ClusterRef: "ccs-test",
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			StorageClient: &stubStorageClient{},
			DynamicClient: newPlatformDynClient(),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "etcd-restore no CR")
}

func TestEtcdRestore_DownloadsAndRecoversFromS3(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityEtcdRestore)

	clusterRef := "ccs-test"
	storage := &stubStorageClient{downloadData: []byte("etcd-snapshot-data")}
	cr := etcdMaintenanceCR(clusterRef, "restore", map[string]interface{}{
		"s3SnapshotPath": map[string]interface{}{"bucket": "ont-backups", "key": "snapshot.db"},
	})
	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityEtcdRestore,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			TalosClient:   &stubTalosClient{},
			StorageClient: storage,
			DynamicClient: newPlatformDynClient(cr),
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected ResultSucceeded; got %q (reason: %v)", result.Status, result.FailureReason)
	}
	if storage.lastBucket != "ont-backups" {
		t.Errorf("expected bucket=ont-backups; got %q", storage.lastBucket)
	}
	if storage.lastKey != "snapshot.db" {
		t.Errorf("expected key=snapshot.db; got %q", storage.lastKey)
	}
}

// nodeMaintenanceHardeningCR builds a NodeMaintenance unstructured object for
// operation=hardening-apply. profileName is the hardeningProfileRef.name field.
// profileNS, when non-empty, overrides the hardeningProfileRef.namespace field;
// the conductor handler defaults to the tenant namespace when empty.
func nodeMaintenanceHardeningCR(clusterRef, profileName, profileNS string) *unstructured.Unstructured {
	profileRef := map[string]interface{}{"name": profileName}
	if profileNS != "" {
		profileRef["namespace"] = profileNS
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind":       "NodeMaintenance",
		"metadata": map[string]interface{}{
			"name":      "nm-harden-" + clusterRef,
			"namespace": "seam-tenant-" + clusterRef,
		},
		"spec": map[string]interface{}{
			"operation":           "hardening-apply",
			"hardeningProfileRef": profileRef,
		},
	}}
}

// hardeningProfileCR builds a HardeningProfile unstructured object with the given
// machineConfigPatches slice. An empty or nil slice produces a profile with no patches.
func hardeningProfileCR(clusterRef, name string, patches []string) *unstructured.Unstructured {
	patchList := make([]interface{}, len(patches))
	for i, p := range patches {
		patchList[i] = p
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "platform.ontai.dev/v1alpha1",
		"kind":       "HardeningProfile",
		"metadata": map[string]interface{}{
			"name":      name,
			"namespace": "seam-tenant-" + clusterRef,
		},
		"spec": map[string]interface{}{
			"machineConfigPatches": patchList,
		},
	}}
}

