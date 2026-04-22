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

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/kubernetes/fake"

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
