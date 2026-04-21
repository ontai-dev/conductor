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

// collectKinds returns a comma-separated string of manifest kinds for test diagnostics.
func collectKinds(manifests []packbuild.Manifest) string {
	kinds := make([]string, len(manifests))
	for i, m := range manifests {
		kinds[i] = m.Kind
	}
	return strings.Join(kinds, ",")
}
