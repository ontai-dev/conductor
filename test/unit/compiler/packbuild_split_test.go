// Package compiler_test -- packbuild RBAC/workload split contract tests.
//
// Tests verify that SplitRBACAndWorkload correctly partitions Kubernetes
// manifests into RBAC and workload slices, and that ParseManifests correctly
// handles multi-document YAML. wrapper-schema.md §4.
package compiler_test

import (
	"strings"
	"testing"

	"github.com/ontai-dev/conductor/internal/packbuild"
)

// nginxLikeManifests is a minimal set of manifests resembling nginx-ingress
// chart output: ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment, Service.
const nginxLikeManifests = `apiVersion: v1
kind: ServiceAccount
metadata:
  name: ingress-nginx
  namespace: ingress-nginx
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ingress-nginx
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ingress-nginx
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingress-nginx-controller
  namespace: ingress-nginx
---
apiVersion: v1
kind: Service
metadata:
  name: ingress-nginx-controller
  namespace: ingress-nginx`

// TestSplit_RBACResourcesLandInRBACSlice verifies that ServiceAccount, ClusterRole,
// and ClusterRoleBinding manifests land in the rbac slice from a nginx-like input.
func TestSplit_RBACResourcesLandInRBACSlice(t *testing.T) {
	manifests, err := packbuild.ParseManifests(nginxLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, _ := packbuild.SplitRBACAndWorkload(manifests)

	rbacKinds := map[string]bool{}
	for _, m := range rbac {
		rbacKinds[m.Kind] = true
	}

	for _, want := range []string{"ServiceAccount", "ClusterRole", "ClusterRoleBinding"} {
		if !rbacKinds[want] {
			t.Errorf("RBAC slice missing kind %q; got kinds: %v", want, collectKinds(rbac))
		}
	}
	if len(rbac) != 3 {
		t.Errorf("expected 3 RBAC manifests; got %d: %v", len(rbac), collectKinds(rbac))
	}
}

// TestSplit_WorkloadResourcesLandInWorkloadSlice verifies that Deployment and Service
// manifests land in the workload slice from a nginx-like input. The injected
// Namespace manifest for ingress-nginx is also expected (namespace injection).
func TestSplit_WorkloadResourcesLandInWorkloadSlice(t *testing.T) {
	manifests, err := packbuild.ParseManifests(nginxLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	_, workload := packbuild.SplitRBACAndWorkload(manifests)

	workloadKinds := map[string]bool{}
	for _, m := range workload {
		workloadKinds[m.Kind] = true
	}

	for _, want := range []string{"Deployment", "Service", "Namespace"} {
		if !workloadKinds[want] {
			t.Errorf("workload slice missing kind %q; got kinds: %v", want, collectKinds(workload))
		}
	}
	// 3: Namespace (injected) + Deployment + Service
	if len(workload) != 3 {
		t.Errorf("expected 3 workload manifests (Namespace+Deployment+Service); got %d: %v", len(workload), collectKinds(workload))
	}
}

// TestSplit_NamespaceInjectedForRBACNamespace verifies that when a ServiceAccount
// references a namespace not declared by any Namespace manifest, a synthetic
// Namespace manifest is prepended to the workload slice. This ensures the
// target cluster has the namespace before guardian rbac-intake applies the
// ServiceAccount into it. wrapper-schema.md §4, Governor-approved session/13.
func TestSplit_NamespaceInjectedForRBACNamespace(t *testing.T) {
	manifests, err := packbuild.ParseManifests(nginxLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	_, workload := packbuild.SplitRBACAndWorkload(manifests)

	if len(workload) == 0 {
		t.Fatal("workload slice is empty; expected injected Namespace manifest")
	}
	// Injected Namespace must be the FIRST manifest in the workload slice so it
	// is applied before other workload resources.
	first := workload[0]
	if first.Kind != "Namespace" {
		t.Errorf("first workload manifest: got Kind=%q; want Namespace", first.Kind)
	}
	if first.Name != "ingress-nginx" {
		t.Errorf("injected Namespace: got Name=%q; want ingress-nginx", first.Name)
	}
}

// TestSplit_ExplicitNamespaceNotDuplicated verifies that when the full manifest set
// already includes an explicit Namespace manifest for a namespace referenced by
// RBAC resources, no second Namespace manifest is injected.
func TestSplit_ExplicitNamespaceNotDuplicated(t *testing.T) {
	const withExplicitNS = `apiVersion: v1
kind: Namespace
metadata:
  name: ingress-nginx
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ingress-nginx
  namespace: ingress-nginx
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingress-nginx-controller
  namespace: ingress-nginx`

	manifests, err := packbuild.ParseManifests(withExplicitNS)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	_, workload := packbuild.SplitRBACAndWorkload(manifests)

	nsCount := 0
	for _, m := range workload {
		if m.Kind == "Namespace" {
			nsCount++
		}
	}
	if nsCount != 1 {
		t.Errorf("expected exactly 1 Namespace manifest in workload; got %d (kinds: %v)", nsCount, collectKinds(workload))
	}
}

// TestSplit_EmptyRBACSliceWhenNoRBACPresent verifies that a manifest set with no
// RBAC resources produces an empty RBAC slice.
func TestSplit_EmptyRBACSliceWhenNoRBACPresent(t *testing.T) {
	const workloadOnly = `apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
---
apiVersion: v1
kind: Service
metadata:
  name: my-app`

	manifests, err := packbuild.ParseManifests(workloadOnly)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, workload := packbuild.SplitRBACAndWorkload(manifests)

	if len(rbac) != 0 {
		t.Errorf("expected empty RBAC slice; got %d: %v", len(rbac), collectKinds(rbac))
	}
	if len(workload) != 2 {
		t.Errorf("expected 2 workload manifests; got %d", len(workload))
	}
}

// TestSplit_AllRBACKindsRecognized verifies that all five RBAC kinds are correctly
// routed to the RBAC slice.
func TestSplit_AllRBACKindsRecognized(t *testing.T) {
	const allRBACKinds = `apiVersion: v1
kind: ServiceAccount
metadata:
  name: sa
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: role
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cr
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: rb
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: crb`

	manifests, err := packbuild.ParseManifests(allRBACKinds)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, workload := packbuild.SplitRBACAndWorkload(manifests)

	if len(rbac) != 5 {
		t.Errorf("expected 5 RBAC manifests; got %d: %v", len(rbac), collectKinds(rbac))
	}
	if len(workload) != 0 {
		t.Errorf("expected empty workload slice; got %d: %v", len(workload), collectKinds(workload))
	}
}

// TestSplit_IsPure verifies that SplitRBACAndWorkload does not modify the input
// slice: the original slice length and element order are preserved after a call.
func TestSplit_IsPure(t *testing.T) {
	manifests, err := packbuild.ParseManifests(nginxLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	originalLen := len(manifests)
	originalKinds := collectKinds(manifests)

	// Call twice to check idempotency.
	packbuild.SplitRBACAndWorkload(manifests)
	packbuild.SplitRBACAndWorkload(manifests)

	if len(manifests) != originalLen {
		t.Errorf("input slice length changed: was %d, now %d", originalLen, len(manifests))
	}
	afterKinds := collectKinds(manifests)
	if afterKinds != originalKinds {
		t.Errorf("input slice order changed: was %q, now %q", originalKinds, afterKinds)
	}
}

// TestSplit_ParseManifests_BlankDocumentsSkipped verifies that blank documents
// between "---" separators are skipped gracefully.
func TestSplit_ParseManifests_BlankDocumentsSkipped(t *testing.T) {
	const withBlanks = `apiVersion: v1
kind: ServiceAccount
metadata:
  name: sa
---

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app`

	manifests, err := packbuild.ParseManifests(withBlanks)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}
	if len(manifests) != 2 {
		t.Errorf("expected 2 manifests (blank skipped); got %d", len(manifests))
	}
}

// certManagerLikeManifests is a minimal set of manifests resembling cert-manager
// chart output: CRD, MutatingWebhookConfiguration, Deployment, ClusterRole, SA.
const certManagerLikeManifests = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: certificates.cert-manager.io
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: cert-manager-webhook
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cert-manager
  namespace: cert-manager
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cert-manager-controller
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cert-manager
  namespace: cert-manager`

// TestSplitManifests_MutatingWebhookLandsInClusterScopedSlice verifies that a
// MutatingWebhookConfiguration manifest is routed to the clusterScoped bucket,
// not the workload bucket. Governor ruling 2026-04-22 three-bucket split.
func TestSplitManifests_MutatingWebhookLandsInClusterScopedSlice(t *testing.T) {
	manifests, err := packbuild.ParseManifests(certManagerLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	_, clusterScoped, workload := packbuild.SplitManifests(manifests)

	csKinds := map[string]bool{}
	for _, m := range clusterScoped {
		csKinds[m.Kind] = true
	}
	if !csKinds["MutatingWebhookConfiguration"] {
		t.Errorf("clusterScoped slice missing MutatingWebhookConfiguration; got: %v", collectKinds(clusterScoped))
	}
	for _, m := range workload {
		if m.Kind == "MutatingWebhookConfiguration" {
			t.Errorf("workload slice must not contain MutatingWebhookConfiguration")
		}
	}
}

// TestSplitManifests_CRDLandsInClusterScopedSlice verifies that a
// CustomResourceDefinition manifest is routed to the clusterScoped bucket.
func TestSplitManifests_CRDLandsInClusterScopedSlice(t *testing.T) {
	manifests, err := packbuild.ParseManifests(certManagerLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	_, clusterScoped, _ := packbuild.SplitManifests(manifests)

	csKinds := map[string]bool{}
	for _, m := range clusterScoped {
		csKinds[m.Kind] = true
	}
	if !csKinds["CustomResourceDefinition"] {
		t.Errorf("clusterScoped slice missing CustomResourceDefinition; got: %v", collectKinds(clusterScoped))
	}
}

// TestSplitManifests_DeploymentLandsInWorkloadSlice verifies that a Deployment
// manifest is routed to the workload bucket, not clusterScoped or rbac.
func TestSplitManifests_DeploymentLandsInWorkloadSlice(t *testing.T) {
	manifests, err := packbuild.ParseManifests(certManagerLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, clusterScoped, workload := packbuild.SplitManifests(manifests)

	for _, m := range rbac {
		if m.Kind == "Deployment" {
			t.Error("Deployment must not land in rbac slice")
		}
	}
	for _, m := range clusterScoped {
		if m.Kind == "Deployment" {
			t.Error("Deployment must not land in clusterScoped slice")
		}
	}
	found := false
	for _, m := range workload {
		if m.Kind == "Deployment" {
			found = true
		}
	}
	if !found {
		t.Errorf("Deployment not found in workload slice; got: %v", collectKinds(workload))
	}
}

// TestSplitManifests_ClusterRoleLandsInRBACSlice verifies that ClusterRole is
// routed to the rbac bucket, not the clusterScoped bucket.
func TestSplitManifests_ClusterRoleLandsInRBACSlice(t *testing.T) {
	manifests, err := packbuild.ParseManifests(certManagerLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, clusterScoped, _ := packbuild.SplitManifests(manifests)

	rbacKinds := map[string]bool{}
	for _, m := range rbac {
		rbacKinds[m.Kind] = true
	}
	if !rbacKinds["ClusterRole"] {
		t.Errorf("rbac slice missing ClusterRole; got: %v", collectKinds(rbac))
	}
	for _, m := range clusterScoped {
		if m.Kind == "ClusterRole" {
			t.Error("ClusterRole must not land in clusterScoped slice")
		}
	}
}

// TestSplitManifests_AllClusterScopedKindsRecognized verifies that all eight
// cluster-scoped non-RBAC kinds route to the clusterScoped bucket.
func TestSplitManifests_AllClusterScopedKindsRecognized(t *testing.T) {
	const allCS = `apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: mwc
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingWebhookConfiguration
metadata:
  name: vwc
---
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: crd
---
apiVersion: apiregistration.k8s.io/v1
kind: APIService
metadata:
  name: apix
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata:
  name: pc
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: sc
---
apiVersion: networking.k8s.io/v1
kind: IngressClass
metadata:
  name: ic
---
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: ci`

	manifests, err := packbuild.ParseManifests(allCS)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, clusterScoped, workload := packbuild.SplitManifests(manifests)

	if len(rbac) != 0 {
		t.Errorf("expected empty rbac slice; got %d: %v", len(rbac), collectKinds(rbac))
	}
	if len(workload) != 0 {
		t.Errorf("expected empty workload slice; got %d: %v", len(workload), collectKinds(workload))
	}
	if len(clusterScoped) != 8 {
		t.Errorf("expected 8 cluster-scoped manifests; got %d: %v", len(clusterScoped), collectKinds(clusterScoped))
	}
}

// TestSplitManifests_NginxHasNoClusterScopedManifests verifies that a manifest
// set resembling nginx-ingress (no webhooks or CRDs) produces an empty
// clusterScoped slice. Ensures the three-bucket split is backward compatible.
func TestSplitManifests_NginxHasNoClusterScopedManifests(t *testing.T) {
	manifests, err := packbuild.ParseManifests(nginxLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	_, clusterScoped, _ := packbuild.SplitManifests(manifests)

	if len(clusterScoped) != 0 {
		t.Errorf("nginx manifests should have no cluster-scoped resources; got %d: %v", len(clusterScoped), collectKinds(clusterScoped))
	}
}

// TestSplitManifests_LegacyTwoBucketPreserved verifies that SplitRBACAndWorkload
// still works after the three-bucket refactor: cluster-scoped resources merge
// into the workload slice for backward-compatible callers.
func TestSplitManifests_LegacyTwoBucketPreserved(t *testing.T) {
	manifests, err := packbuild.ParseManifests(certManagerLikeManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}

	rbac, workload := packbuild.SplitRBACAndWorkload(manifests)

	// CRD and MutatingWebhookConfiguration must appear in workload (merged from cs).
	workloadKinds := map[string]bool{}
	for _, m := range workload {
		workloadKinds[m.Kind] = true
	}
	if !workloadKinds["CustomResourceDefinition"] {
		t.Error("legacy SplitRBACAndWorkload: CRD missing from workload slice")
	}
	if !workloadKinds["MutatingWebhookConfiguration"] {
		t.Error("legacy SplitRBACAndWorkload: MutatingWebhookConfiguration missing from workload slice")
	}
	// ClusterRole must remain in rbac slice.
	rbacKinds := map[string]bool{}
	for _, m := range rbac {
		rbacKinds[m.Kind] = true
	}
	if !rbacKinds["ClusterRole"] {
		t.Error("legacy SplitRBACAndWorkload: ClusterRole missing from rbac slice")
	}
}

// certManagerWithHookManifests extends certManagerLikeManifests with a
// cert-manager-startupapicheck Job annotated as a Helm post-install hook.
// The hook Job must be excluded from ParseManifests output; the remaining
// four resources must pass through unchanged.
const certManagerWithHookManifests = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: certificates.cert-manager.io
---
apiVersion: admissionregistration.k8s.io/v1
kind: MutatingWebhookConfiguration
metadata:
  name: cert-manager-webhook
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: cert-manager
  namespace: cert-manager
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: cert-manager-controller
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: cert-manager
  namespace: cert-manager
---
apiVersion: batch/v1
kind: Job
metadata:
  name: cert-manager-startupapicheck
  namespace: cert-manager
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-weight: "1"
    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded`

// TestParseManifests_HelmHookExcluded verifies that a manifest carrying a
// helm.sh/hook annotation is excluded from the ParseManifests output.
// Helm lifecycle hook Jobs (startupapicheck, tests) must never enter OCI layers.
func TestParseManifests_HelmHookExcluded(t *testing.T) {
	const hookOnly = `apiVersion: batch/v1
kind: Job
metadata:
  name: cert-manager-startupapicheck
  namespace: cert-manager
  annotations:
    helm.sh/hook: post-install,post-upgrade`

	manifests, err := packbuild.ParseManifests(hookOnly)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}
	if len(manifests) != 0 {
		t.Errorf("expected 0 manifests (hook excluded); got %d: %v", len(manifests), collectKinds(manifests))
	}
}

// TestParseManifests_HelmHookExcludedFromMixedSet verifies that in a mixed
// manifest set, all helm.sh/hook-annotated resources are excluded and all
// non-hook resources pass through unchanged.
func TestParseManifests_HelmHookExcludedFromMixedSet(t *testing.T) {
	manifests, err := packbuild.ParseManifests(certManagerWithHookManifests)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}
	// 5 resources pass through: CRD, MutatingWebhookConfiguration, Deployment,
	// ClusterRole, ServiceAccount. The hook Job is excluded.
	if len(manifests) != 5 {
		t.Errorf("expected 5 manifests (hook excluded); got %d: %v", len(manifests), collectKinds(manifests))
	}
	for _, m := range manifests {
		if m.Name == "cert-manager-startupapicheck" {
			t.Errorf("hook Job cert-manager-startupapicheck must not appear in ParseManifests output")
		}
	}
}

// TestParseManifests_NonHookJobIncluded verifies that a Job without a
// helm.sh/hook annotation is NOT excluded -- only hook-annotated resources
// are filtered, not all Jobs.
func TestParseManifests_NonHookJobIncluded(t *testing.T) {
	const regularJob = `apiVersion: batch/v1
kind: Job
metadata:
  name: data-migration
  namespace: myapp`

	manifests, err := packbuild.ParseManifests(regularJob)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}
	if len(manifests) != 1 {
		t.Errorf("expected 1 manifest (non-hook Job included); got %d", len(manifests))
	}
	if manifests[0].Kind != "Job" {
		t.Errorf("expected Kind=Job; got %q", manifests[0].Kind)
	}
}

// TestParseManifests_MultipleHooksAllExcluded verifies that when multiple
// manifests in the same document set carry helm.sh/hook annotations, all of
// them are excluded regardless of hook type value.
func TestParseManifests_MultipleHooksAllExcluded(t *testing.T) {
	const multiHook = `apiVersion: batch/v1
kind: Job
metadata:
  name: pre-install-job
  annotations:
    helm.sh/hook: pre-install
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: real-app
  namespace: myapp
---
apiVersion: batch/v1
kind: Job
metadata:
  name: post-install-job
  annotations:
    helm.sh/hook: post-install`

	manifests, err := packbuild.ParseManifests(multiHook)
	if err != nil {
		t.Fatalf("ParseManifests: %v", err)
	}
	if len(manifests) != 1 {
		t.Errorf("expected 1 manifest (both hook Jobs excluded); got %d: %v", len(manifests), collectKinds(manifests))
	}
	if manifests[0].Kind != "Deployment" {
		t.Errorf("expected the surviving manifest to be Deployment; got %q", manifests[0].Kind)
	}
}

// collectKinds returns a comma-separated string of manifest kinds for test diagnostics.
func collectKinds(manifests []packbuild.Manifest) string {
	kinds := make([]string, len(manifests))
	for i, m := range manifests {
		kinds[i] = m.Kind
	}
	return strings.Join(kinds, ",")
}
