// Package capability_test -- unit tests for pack-deploy two-layer OCI split path.
//
// Tests verify that when ClusterPack.spec.rbacDigest is set, pack-deploy:
//  1. Calls GuardianIntakeClient.SubmitPackRBACLayer before applying workload.
//  2. Does not apply RBAC manifests directly via SSA (INV-004).
//  3. Returns ValidationFailure when rbacDigest is set but GuardianClient is nil.
//  4. Uses the legacy single-layer path when rbacDigest is absent.
//  5. Returns failure when guardian intake returns an error.
//  6. Applies workload manifests after successful guardian intake.
//
// INV-004, wrapper-schema.md §4, conductor-schema.md §6.
package capability_test

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// ---------------------------------------------------------------------------
// Stub GuardianIntakeClient
// ---------------------------------------------------------------------------

type stubGuardianClient struct {
	submitErr   error
	waitErr     error
	submitted   []string // YAML manifests received
	componentName string
	targetCluster string
}

func (s *stubGuardianClient) SubmitPackRBACLayer(_ context.Context, componentName string, manifests []string, targetCluster string) (int, error) {
	s.componentName = componentName
	s.targetCluster = targetCluster
	s.submitted = manifests
	if s.submitErr != nil {
		return 0, s.submitErr
	}
	return len(manifests), nil
}

func (s *stubGuardianClient) WaitForRBACProfileProvisioned(_ context.Context, _, _ string) error {
	return s.waitErr
}

var _ capability.GuardianIntakeClient = (*stubGuardianClient)(nil)

// ---------------------------------------------------------------------------
// Split path ClusterPack builder
// ---------------------------------------------------------------------------

func clusterPackSplitCR(clusterRef, name, version, registryURL, rbacDigest, workloadDigest string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "infra.ontai.dev/v1alpha1",
		"kind":       "ClusterPack",
		"metadata":   map[string]interface{}{"name": name, "namespace": "seam-tenant-" + clusterRef},
		"spec": map[string]interface{}{
			"version": version,
			"registryRef": map[string]interface{}{
				"url": registryURL,
			},
			"rbacDigest":     rbacDigest,
			"workloadDigest": workloadDigest,
		},
	}}
}

// rbacLayerTarGz builds a tar.gz blob containing a single RBAC YAML manifest.
func rbacLayerTarGz(t *testing.T) []byte {
	t.Helper()
	const clusterRoleYAML = `apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ingress-nginx`
	return makeTarGz(t, map[string][]byte{"rbac.yaml": []byte(clusterRoleYAML)})
}

// workloadLayerTarGz builds a tar.gz blob containing a single workload YAML manifest.
func workloadLayerTarGz(t *testing.T) []byte {
	t.Helper()
	const deploymentYAML = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingress-nginx-controller
  namespace: ingress-nginx`
	return makeTarGz(t, map[string][]byte{"workload.yaml": []byte(deploymentYAML)})
}

// newSplitOCIClient returns a stub OCI client that returns rbac blob for the
// rbacDigest reference and workload blob for the workloadDigest reference.
type splitStubOCIClient struct {
	rbacBlob     []byte
	workloadBlob []byte
	rbacDigest   string
	workloadDigest string
}

func (s *splitStubOCIClient) PullManifests(_ context.Context, ref string) ([][]byte, error) {
	if s.rbacDigest != "" && containsSuffix(ref, s.rbacDigest) {
		return [][]byte{s.rbacBlob}, nil
	}
	if s.workloadDigest != "" && containsSuffix(ref, s.workloadDigest) {
		return [][]byte{s.workloadBlob}, nil
	}
	return nil, nil
}

func containsSuffix(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

var _ capability.OCIRegistryClient = (*splitStubOCIClient)(nil)

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestPackDeploy_SplitPath_CallsGuardianIntakeBeforeWorkload verifies that when
// rbacDigest is set, GuardianIntakeClient.SubmitPackRBACLayer is called with the
// RBAC manifests extracted from the RBAC OCI layer. INV-004, wrapper-schema.md §4.
func TestPackDeploy_SplitPath_CallsGuardianIntakeBeforeWorkload(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	guardian := &stubGuardianClient{}
	oci := &splitStubOCIClient{
		rbacBlob:       rbacLayerTarGz(t),
		workloadBlob:   workloadLayerTarGz(t),
		rbacDigest:     "sha256:rbacabc",
		workloadDigest: "sha256:workloadabc",
	}
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	cp := clusterPackSplitCR(clusterRef, "nginx-ingress", "v1.0.0",
		"registry.example.com/nginx-ingress", "sha256:rbacabc", "sha256:workloadabc")
	dynClient := newWrapperDynClient(pe, cp)

	_, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: guardian,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(guardian.submitted) == 0 {
		t.Error("expected SubmitPackRBACLayer to be called with RBAC manifests; got none submitted")
	}
	if guardian.componentName != "nginx-ingress" {
		t.Errorf("componentName: got %q want %q", guardian.componentName, "nginx-ingress")
	}
	if guardian.targetCluster != clusterRef {
		t.Errorf("targetCluster: got %q want %q", guardian.targetCluster, clusterRef)
	}
}

// TestPackDeploy_SplitPath_GuardianClientRequiredWhenRBACDigestPresent verifies
// that ValidationFailure is returned when rbacDigest is set but GuardianClient
// is nil. pack-deploy must not apply RBAC directly. INV-004.
func TestPackDeploy_SplitPath_GuardianClientRequiredWhenRBACDigestPresent(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	cp := clusterPackSplitCR(clusterRef, "nginx-ingress", "v1.0.0",
		"registry.example.com/nginx-ingress", "sha256:rbacabc", "sha256:workloadabc")
	dynClient := newWrapperDynClient(pe, cp)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:     &stubOCIClient{manifests: [][]byte{rbacLayerTarGz(t)}},
			KubeClient:    fake.NewSimpleClientset(),
			DynamicClient: dynClient,
			// GuardianClient deliberately nil
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assertValidationFailure(t, result, "split path without guardian client")
}

// TestPackDeploy_SplitPath_SkipsWorkloadWhenIntakeFails verifies that the workload
// layer is never fetched or applied when guardian intake returns an error.
func TestPackDeploy_SplitPath_SkipsWorkloadWhenIntakeFails(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	guardian := &stubGuardianClient{submitErr: errors.New("guardian unreachable")}
	oci := &splitStubOCIClient{
		rbacBlob:       rbacLayerTarGz(t),
		workloadBlob:   workloadLayerTarGz(t),
		rbacDigest:     "sha256:rbacabc",
		workloadDigest: "sha256:workloadabc",
	}
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	cp := clusterPackSplitCR(clusterRef, "nginx-ingress", "v1.0.0",
		"registry.example.com/nginx-ingress", "sha256:rbacabc", "sha256:workloadabc")
	dynClient := newWrapperDynClient(pe, cp)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: guardian,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultFailed {
		t.Errorf("expected ResultFailed when intake fails; got %q", result.Status)
	}
	if result.FailureReason == nil || result.FailureReason.FailedStep != "rbac-intake" {
		t.Errorf("expected FailedStep=rbac-intake; got %+v", result.FailureReason)
	}
	// Verify workload layer was not fetched: the workload step should be absent.
	for _, step := range result.Steps {
		if step.Name == "apply-workload" {
			t.Error("apply-workload step must not appear when intake fails")
		}
	}
}

// TestPackDeploy_SplitPath_LayerRefsUsesBaseURLWhenRegistryRefDigestSet verifies
// that when a ClusterPack carries spec.registryRef.digest (helm-compiled packs),
// the RBAC and workload layer OCI refs are constructed as baseURL@layerDigest and
// NOT as baseURL@registryRefDigest@layerDigest. The double-digest form is invalid
// and causes HTTP 404 when the conductor pull step fetches the manifest.
// conductor PR #17, COMPILER-HELM-E2E.
func TestPackDeploy_SplitPath_LayerRefsUsesBaseURLWhenRegistryRefDigestSet(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	const (
		baseURL        = "registry.example.com/nginx-ingress"
		registryDigest = "sha256:workload111" // registryRef.digest (workload)
		rbacDigest     = "sha256:rbacaaa"
		workloadDigest = "sha256:workload111"
	)

	// Track every ref PullManifests is called with.
	var pulledRefs []string
	oci := &recordingOCIClient{
		rbacBlob:     rbacLayerTarGz(t),
		workloadBlob: workloadLayerTarGz(t),
		rbacDigest:   rbacDigest,
		onPull:       func(ref string) { pulledRefs = append(pulledRefs, ref) },
	}

	// ClusterPack with registryRef.digest set (helm-compiled pack).
	cp := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "infra.ontai.dev/v1alpha1",
		"kind":       "ClusterPack",
		"metadata":   map[string]interface{}{"name": "nginx-ingress", "namespace": "seam-tenant-" + clusterRef},
		"spec": map[string]interface{}{
			"version": "v1.0.0",
			"registryRef": map[string]interface{}{
				"url":    baseURL,
				"digest": registryDigest,
			},
			"rbacDigest":     rbacDigest,
			"workloadDigest": workloadDigest,
		},
	}}
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	dynClient := newWrapperDynClient(pe, cp)

	_, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: &stubGuardianClient{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Every pulled ref must be baseURL@digest — never baseURL@digest1@digest2.
	wantRBAC := baseURL + "@" + rbacDigest
	wantWorkload := baseURL + "@" + workloadDigest
	for _, ref := range pulledRefs {
		if ref != wantRBAC && ref != wantWorkload {
			t.Errorf("pulled unexpected OCI ref %q; want %q or %q (double-digest would be a bug)", ref, wantRBAC, wantWorkload)
		}
		if len(ref) > len(baseURL)+1+len(registryDigest)+1+len(rbacDigest) {
			t.Errorf("OCI ref %q looks like a double-digest ref (too long)", ref)
		}
	}
}

// recordingOCIClient records all PullManifests refs and delegates to a split stub.
type recordingOCIClient struct {
	rbacBlob     []byte
	workloadBlob []byte
	rbacDigest   string
	onPull       func(string)
}

func (r *recordingOCIClient) PullManifests(_ context.Context, ref string) ([][]byte, error) {
	if r.onPull != nil {
		r.onPull(ref)
	}
	if r.rbacDigest != "" && containsSuffix(ref, r.rbacDigest) {
		return [][]byte{r.rbacBlob}, nil
	}
	return [][]byte{r.workloadBlob}, nil
}

var _ capability.OCIRegistryClient = (*recordingOCIClient)(nil)

// TestPackDeploy_LegacyPath_SkipsGuardianIntakeWhenNoRBACDigest verifies that the
// existing single-layer path is used when rbacDigest is absent. GuardianClient
// must NOT be called in this case. Backward compatibility, wrapper-schema.md §4.
func TestPackDeploy_LegacyPath_SkipsGuardianIntakeWhenNoRBACDigest(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-dev"
	guardian := &stubGuardianClient{} // must NOT be called
	pe := packExecutionCR(clusterRef, "cilium-pack", "v1.0.0")
	// Legacy ClusterPack: no rbacDigest
	cp := clusterPackCR(clusterRef, "cilium-pack", "v1.0.0", "registry.example.com/cilium@sha256:abc123")
	dynClient := newWrapperDynClient(pe, cp)

	oci := &stubOCIClient{manifests: [][]byte{makeTarGz(t, map[string][]byte{
		"manifest.yaml": []byte("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: cilium\n  namespace: kube-system"),
	})}}

	_, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: guardian,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(guardian.submitted) > 0 {
		t.Errorf("expected SubmitPackRBACLayer NOT to be called on legacy path; got %d manifests submitted", len(guardian.submitted))
	}
}

// TestPackDeploy_SplitPath_StepNamesRecordedInResult verifies that the OperationResult
// steps slice records the expected step names for a successful split-path execution.
func TestPackDeploy_SplitPath_StepNamesRecordedInResult(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	guardian := &stubGuardianClient{}
	oci := &splitStubOCIClient{
		rbacBlob:       rbacLayerTarGz(t),
		workloadBlob:   workloadLayerTarGz(t),
		rbacDigest:     "sha256:rbacabc",
		workloadDigest: "sha256:workloadabc",
	}
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	cp := clusterPackSplitCR(clusterRef, "nginx-ingress", "v1.0.0",
		"registry.example.com/nginx-ingress", "sha256:rbacabc", "sha256:workloadabc")
	dynClient := newWrapperDynClient(pe, cp)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: guardian,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	stepNames := make(map[string]bool)
	for _, s := range result.Steps {
		stepNames[s.Name] = true
	}
	for _, want := range []string{"pull-rbac-layer", "rbac-intake", "wait-rbac-profile"} {
		if !stepNames[want] {
			t.Errorf("expected step %q in result; got steps: %v", want, result.Steps)
		}
	}
}

// TestPackDeploy_SplitPath_RBACIntakeCalledWithCorrectManifestCount verifies that
// the YAML strings submitted to guardian match the count of RBAC manifests
// extracted from the RBAC OCI layer.
func TestPackDeploy_SplitPath_RBACIntakeCalledWithCorrectManifestCount(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	const rbacYAML = `apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ingress-nginx
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ingress-nginx
  namespace: ingress-nginx`
	// Pack both documents into a single file -- ParseManifests handles multi-doc YAML.
	rbacBlob := makeTarGz(t, map[string][]byte{"rbac.yaml": []byte(rbacYAML)})

	clusterRef := "ccs-mgmt"
	guardian := &stubGuardianClient{}
	oci := &splitStubOCIClient{
		rbacBlob:       rbacBlob,
		workloadBlob:   workloadLayerTarGz(t),
		rbacDigest:     "sha256:rbacmulti",
		workloadDigest: "sha256:workloadabc",
	}
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	cp := clusterPackSplitCR(clusterRef, "nginx-ingress", "v1.0.0",
		"registry.example.com/nginx-ingress", "sha256:rbacmulti", "sha256:workloadabc")
	dynClient := newWrapperDynClient(pe, cp)

	_, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: guardian,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The tar.gz has one file containing two YAML documents. After splitting on ---
	// separators, each document is submitted as an individual string to guardian.
	if len(guardian.submitted) != 2 {
		t.Errorf("expected 2 YAML strings submitted (one per document); got %d", len(guardian.submitted))
	}
}

// newThreeBucketDynClient returns a fake dynamic client that handles three-bucket
// pack apply. It registers GVRs for Deployment and MutatingWebhookConfiguration
// and prepends a Reactor that accepts any Patch call without error (SSA upsert
// semantics -- the fake client returns "not found" by default for objects that
// don't exist, which would fail applyParsedManifest in unit tests).
func newThreeBucketDynClient(objects ...*unstructured.Unstructured) *dynamicfake.FakeDynamicClient {
	kinds := map[string]schema.GroupVersionResource{
		"PackExecution":                {Group: "infra.ontai.dev", Version: "v1alpha1", Resource: "packexecutions"},
		"ClusterPack":                  {Group: "infra.ontai.dev", Version: "v1alpha1", Resource: "clusterpacks"},
		"Deployment":                   {Group: "apps", Version: "v1", Resource: "deployments"},
		"MutatingWebhookConfiguration": {Group: "admissionregistration.k8s.io", Version: "v1", Resource: "mutatingwebhookconfigurations"},
		"Namespace":                    {Group: "", Version: "v1", Resource: "namespaces"},
	}
	s := runtime.NewScheme()
	for kind, gvr := range kinds {
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: kind},
			&unstructured.Unstructured{},
		)
		s.AddKnownTypeWithName(
			schema.GroupVersionKind{Group: gvr.Group, Version: gvr.Version, Kind: kind + "List"},
			&unstructured.UnstructuredList{},
		)
	}
	client := dynamicfake.NewSimpleDynamicClient(s)
	// Accept any Patch call without error. The fake client rejects Patch on objects
	// that don't exist; this reactor makes Patch behave like an SSA upsert.
	client.PrependReactor("patch", "*", func(action k8stesting.Action) (bool, runtime.Object, error) {
		return true, &unstructured.Unstructured{}, nil
	})
	wrapperGVRs := map[string]schema.GroupVersionResource{
		"PackExecution": {Group: "infra.ontai.dev", Version: "v1alpha1", Resource: "packexecutions"},
		"ClusterPack":   {Group: "infra.ontai.dev", Version: "v1alpha1", Resource: "clusterpacks"},
	}
	for _, obj := range objects {
		gvr, ok := wrapperGVRs[obj.GetKind()]
		if !ok {
			continue
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

// clusterPackThreeBucketCR builds a ClusterPack with rbacDigest, clusterScopedDigest,
// and workloadDigest all set (three-bucket pack, e.g., cert-manager).
func clusterPackThreeBucketCR(clusterRef, name, version, registryURL, rbacDigest, clusterScopedDigest, workloadDigest string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "infra.ontai.dev/v1alpha1",
		"kind":       "ClusterPack",
		"metadata":   map[string]interface{}{"name": name, "namespace": "seam-tenant-" + clusterRef},
		"spec": map[string]interface{}{
			"version": version,
			"registryRef": map[string]interface{}{
				"url": registryURL,
			},
			"rbacDigest":          rbacDigest,
			"clusterScopedDigest": clusterScopedDigest,
			"workloadDigest":      workloadDigest,
		},
	}}
}

// simpleWorkloadLayerTarGz builds a tar.gz with a ConfigMap-only workload.
// ConfigMap has no readiness check, so waitForStageReady skips it entirely.
// Used in tests that need to verify a Succeeded result status without a live cluster.
func simpleWorkloadLayerTarGz(t *testing.T) []byte {
	t.Helper()
	const cmYAML = `apiVersion: v1
kind: ConfigMap
metadata:
  name: pack-config
  namespace: cert-manager`
	return makeTarGz(t, map[string][]byte{"workload.yaml": []byte(cmYAML)})
}

// clusterScopedLayerTarGz builds a tar.gz blob containing a MutatingWebhookConfiguration.
func clusterScopedLayerTarGz(t *testing.T) []byte {
	t.Helper()
	const webhookYAML = `apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: cert-manager-webhook`
	return makeTarGz(t, map[string][]byte{"cluster-scoped.yaml": []byte(webhookYAML)})
}

// threeWayOCIClient serves rbac, cluster-scoped, and workload blobs by digest suffix.
type threeWayOCIClient struct {
	rbacBlob          []byte
	clusterScopedBlob []byte
	workloadBlob      []byte
	rbacDigest        string
	clusterScopedDigest string
	workloadDigest    string
	pulledRefs        []string
}

func (c *threeWayOCIClient) PullManifests(_ context.Context, ref string) ([][]byte, error) {
	c.pulledRefs = append(c.pulledRefs, ref)
	if c.rbacDigest != "" && containsSuffix(ref, c.rbacDigest) {
		return [][]byte{c.rbacBlob}, nil
	}
	if c.clusterScopedDigest != "" && containsSuffix(ref, c.clusterScopedDigest) {
		return [][]byte{c.clusterScopedBlob}, nil
	}
	if c.workloadDigest != "" && containsSuffix(ref, c.workloadDigest) {
		return [][]byte{c.workloadBlob}, nil
	}
	return nil, nil
}

var _ capability.OCIRegistryClient = (*threeWayOCIClient)(nil)

// TestPackDeploy_SplitPath_Step6_AppliesClusterScopedBeforeWorkload verifies that
// when clusterScopedDigest is present, the apply-cluster-scoped step appears in
// the result and is recorded before apply-workload. Governor ruling 2026-04-22.
func TestPackDeploy_SplitPath_Step6_AppliesClusterScopedBeforeWorkload(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	oci := &threeWayOCIClient{
		rbacBlob:            rbacLayerTarGz(t),
		clusterScopedBlob:   clusterScopedLayerTarGz(t),
		workloadBlob:        simpleWorkloadLayerTarGz(t),
		rbacDigest:          "sha256:rbac001",
		clusterScopedDigest: "sha256:cs001",
		workloadDigest:      "sha256:wl001",
	}
	pe := packExecutionCR(clusterRef, "cert-manager", "v1.14.0-r1")
	cp := clusterPackThreeBucketCR(clusterRef, "cert-manager", "v1.14.0-r1",
		"registry.example.com/cert-manager", "sha256:rbac001", "sha256:cs001", "sha256:wl001")
	dynClient := newThreeBucketDynClient(pe, cp)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: &stubGuardianClient{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify step order: apply-cluster-scoped must appear before apply-workload.
	stepNames := make([]string, 0, len(result.Steps))
	for _, s := range result.Steps {
		stepNames = append(stepNames, s.Name)
	}
	csIdx := -1
	wlIdx := -1
	for i, n := range stepNames {
		if n == "apply-cluster-scoped" {
			csIdx = i
		}
		if n == "apply-workload" {
			wlIdx = i
		}
	}
	if csIdx < 0 {
		t.Errorf("apply-cluster-scoped step missing; got steps: %v", stepNames)
	}
	if wlIdx < 0 {
		t.Errorf("apply-workload step missing; got steps: %v", stepNames)
	}
	if csIdx >= 0 && wlIdx >= 0 && csIdx > wlIdx {
		t.Errorf("apply-cluster-scoped (index %d) must precede apply-workload (index %d)", csIdx, wlIdx)
	}

	// Verify the cluster-scoped layer was actually pulled.
	foundCS := false
	for _, ref := range oci.pulledRefs {
		if containsSuffix(ref, "sha256:cs001") {
			foundCS = true
		}
	}
	if !foundCS {
		t.Errorf("cluster-scoped layer digest sha256:cs001 was never pulled; pulled refs: %v", oci.pulledRefs)
	}
}

// TestPackDeploy_SplitPath_Step6_SkippedWhenNoClusterScopedDigest verifies that when
// clusterScopedDigest is absent (nginx-like pack), the apply-cluster-scoped step
// is not recorded and the pack still succeeds. wrapper-schema.md §4.
func TestPackDeploy_SplitPath_Step6_SkippedWhenNoClusterScopedDigest(t *testing.T) {
	reg := capability.NewRegistry()
	capability.RegisterAll(reg)
	h, _ := reg.Resolve(runnerlib.CapabilityPackDeploy)

	clusterRef := "ccs-mgmt"
	oci := &splitStubOCIClient{
		rbacBlob:       rbacLayerTarGz(t),
		workloadBlob:   simpleWorkloadLayerTarGz(t),
		rbacDigest:     "sha256:rbac002",
		workloadDigest: "sha256:wl002",
	}
	pe := packExecutionCR(clusterRef, "nginx-ingress", "v1.0.0")
	cp := clusterPackSplitCR(clusterRef, "nginx-ingress", "v1.0.0",
		"registry.example.com/nginx-ingress", "sha256:rbac002", "sha256:wl002")
	dynClient := newThreeBucketDynClient(pe, cp)

	result, err := h.Execute(context.Background(), capability.ExecuteParams{
		Capability: runnerlib.CapabilityPackDeploy,
		ClusterRef: clusterRef,
		ExecuteClients: capability.ExecuteClients{
			OCIClient:      oci,
			KubeClient:     fake.NewSimpleClientset(),
			DynamicClient:  dynClient,
			GuardianClient: &stubGuardianClient{},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Status != runnerlib.ResultSucceeded {
		t.Errorf("expected Succeeded; got %q", result.Status)
	}
	for _, s := range result.Steps {
		if s.Name == "apply-cluster-scoped" {
			t.Error("apply-cluster-scoped step must not appear when clusterScopedDigest is absent")
		}
	}
}
