package agent

// Unit tests for TenantBootstrapSweep: audit sweep + component profile
// creation. Uses fake clients — no live cluster.
//
// Covers conductor-schema.md §15 bootstrap sweep invariants:
//   - Roles, RoleBindings, ClusterRoles, ClusterRoleBindings are observed (logged) only --
//     never annotated. Patching RBAC resources triggers Kubernetes escalation checks.
//   - ServiceAccounts ARE annotated (no escalation risk).
//   - kube-system, kube-public, kube-node-lease are never swept.
//   - Namespaces with exempt label are skipped.
//   - system: prefix ClusterRoles/ClusterRoleBindings are not swept.
//   - Already-annotated resources are not re-patched (idempotent).
//   - Component profiles created in ont-system (not component namespace).
//   - Component profiles skipped when SA absent.
//   - Security CRD absence is handled gracefully.
//   - Gate never transitions to strict -- conductor in tenant never enforces.

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/ontai-dev/conductor/internal/webhook"
)

// --- helpers ---

func newSweepSuite(t *testing.T) (*TenantBootstrapSweep, *kubefake.Clientset, *dynamicfake.FakeDynamicClient, *webhook.EnforcementGate) {
	t.Helper()
	s := runtime.NewScheme()
	registerSecurityCRDs(s)

	kube := kubefake.NewClientset()
	dyn := dynamicfake.NewSimpleDynamicClient(s)
	gate := webhook.NewEnforcementGate()
	sweep := &TenantBootstrapSweep{
		KubeClient:    kube,
		DynamicClient: dyn,
		Gate:          gate,
	}
	return sweep, kube, dyn, gate
}

// registerSecurityCRDs adds PermissionSet, RBACPolicy, RBACProfile list types
// to the scheme so the fake dynamic client can handle them.
func registerSecurityCRDs(s *runtime.Scheme) {
	for _, kind := range []string{"PermissionSet", "RBACPolicy", "RBACProfile"} {
		gvk := schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: kind}
		listGVK := schema.GroupVersionKind{Group: "security.ontai.dev", Version: "v1alpha1", Kind: kind + "List"}
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(listGVK, &unstructured.UnstructuredList{})
	}
}

func addNamespace(t *testing.T, kube *kubefake.Clientset, name string, labels map[string]string) {
	t.Helper()
	ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels}}
	if _, err := kube.CoreV1().Namespaces().Create(context.Background(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatalf("add namespace %q: %v", name, err)
	}
}

func addRole(t *testing.T, kube *kubefake.Clientset, ns, name string, annotations map[string]string) {
	t.Helper()
	role := &rbacv1.Role{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: annotations}}
	if _, err := kube.RbacV1().Roles(ns).Create(context.Background(), role, metav1.CreateOptions{}); err != nil {
		t.Fatalf("add Role %s/%s: %v", ns, name, err)
	}
}

func addRoleBinding(t *testing.T, kube *kubefake.Clientset, ns, name string, annotations map[string]string) {
	t.Helper()
	rb := &rbacv1.RoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: annotations}}
	if _, err := kube.RbacV1().RoleBindings(ns).Create(context.Background(), rb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("add RoleBinding %s/%s: %v", ns, name, err)
	}
}

func addServiceAccount(t *testing.T, kube *kubefake.Clientset, ns, name string, annotations map[string]string) {
	t.Helper()
	sa := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Annotations: annotations}}
	if _, err := kube.CoreV1().ServiceAccounts(ns).Create(context.Background(), sa, metav1.CreateOptions{}); err != nil {
		t.Fatalf("add ServiceAccount %s/%s: %v", ns, name, err)
	}
}

func addClusterRole(t *testing.T, kube *kubefake.Clientset, name string, annotations map[string]string) {
	t.Helper()
	cr := &rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}
	if _, err := kube.RbacV1().ClusterRoles().Create(context.Background(), cr, metav1.CreateOptions{}); err != nil {
		t.Fatalf("add ClusterRole %q: %v", name, err)
	}
}

func getRoleAnnotation(t *testing.T, kube *kubefake.Clientset, ns, name, key string) string {
	t.Helper()
	r, err := kube.RbacV1().Roles(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get Role %s/%s: %v", ns, name, err)
	}
	return r.Annotations[key]
}

func getServiceAccountAnnotation(t *testing.T, kube *kubefake.Clientset, ns, name, key string) string {
	t.Helper()
	sa, err := kube.CoreV1().ServiceAccounts(ns).Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ServiceAccount %s/%s: %v", ns, name, err)
	}
	return sa.Annotations[key]
}

func getClusterRoleAnnotation(t *testing.T, kube *kubefake.Clientset, name, key string) string {
	t.Helper()
	cr, err := kube.RbacV1().ClusterRoles().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get ClusterRole %q: %v", name, err)
	}
	return cr.Annotations[key]
}

// --- sweep: namespace-scoped resources ---

// TestSweep_StampsOwnershipInAppNamespace verifies the audit-mode sweep:
// Roles and RoleBindings are observed (logged) but NOT annotated.
// ServiceAccounts ARE annotated (not subject to RBAC escalation checks).
func TestSweep_StampsOwnershipInAppNamespace(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)
	ctx := context.Background()

	addNamespace(t, kube, "app-ns", nil)
	addRole(t, kube, "app-ns", "app-role", nil)
	addRoleBinding(t, kube, "app-ns", "app-rb", nil)
	addServiceAccount(t, kube, "app-ns", "app-sa", nil)

	if err := sweep.sweepAllNamespaces(ctx); err != nil {
		t.Fatalf("sweepAllNamespaces: %v", err)
	}

	// Roles and RoleBindings are observed-only -- no annotation patch.
	if got := getRoleAnnotation(t, kube, "app-ns", "app-role", annotationRBACOwner); got != "" {
		t.Errorf("Role should NOT be annotated in audit mode, got %q", got)
	}
	rb, _ := kube.RbacV1().RoleBindings("app-ns").Get(ctx, "app-rb", metav1.GetOptions{})
	if rb.Annotations[annotationRBACOwner] != "" {
		t.Errorf("RoleBinding should NOT be annotated in audit mode")
	}

	// ServiceAccounts are still annotated (no escalation risk).
	if got := getServiceAccountAnnotation(t, kube, "app-ns", "app-sa", annotationRBACOwner); got != annotationRBACOwnerValue {
		t.Errorf("ServiceAccount annotation: got %q, want %q", got, annotationRBACOwnerValue)
	}
}

// TestSweep_KubeSystemSkipped verifies that kube-system is never swept.
func TestSweep_KubeSystemSkipped(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "kube-system", nil)
	addRole(t, kube, "kube-system", "system-role", nil)

	if err := sweep.sweepAllNamespaces(context.Background()); err != nil {
		t.Fatalf("sweepAllNamespaces: %v", err)
	}

	if got := getRoleAnnotation(t, kube, "kube-system", "system-role", annotationRBACOwner); got != "" {
		t.Errorf("kube-system Role should not be annotated, got %q", got)
	}
}

// TestSweep_KubePublicSkipped verifies that kube-public is never swept.
func TestSweep_KubePublicSkipped(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "kube-public", nil)
	addServiceAccount(t, kube, "kube-public", "default", nil)

	if err := sweep.sweepAllNamespaces(context.Background()); err != nil {
		t.Fatalf("sweepAllNamespaces: %v", err)
	}

	if got := getServiceAccountAnnotation(t, kube, "kube-public", "default", annotationRBACOwner); got != "" {
		t.Errorf("kube-public ServiceAccount should not be annotated, got %q", got)
	}
}

// TestSweep_KubeNodeLeaseSkipped verifies that kube-node-lease is never swept.
func TestSweep_KubeNodeLeaseSkipped(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "kube-node-lease", nil)
	addRole(t, kube, "kube-node-lease", "node-lease-role", nil)

	if err := sweep.sweepAllNamespaces(context.Background()); err != nil {
		t.Fatalf("sweepAllNamespaces: %v", err)
	}

	if got := getRoleAnnotation(t, kube, "kube-node-lease", "node-lease-role", annotationRBACOwner); got != "" {
		t.Errorf("kube-node-lease Role should not be annotated, got %q", got)
	}
}

// TestSweep_ExemptLabeledNamespaceSkipped verifies that namespaces labeled
// seam.ontai.dev/webhook-mode=exempt are not swept.
func TestSweep_ExemptLabeledNamespaceSkipped(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "operator-ns", map[string]string{webhookExemptLabel: webhookExemptValue})
	addRole(t, kube, "operator-ns", "operator-role", nil)

	if err := sweep.sweepAllNamespaces(context.Background()); err != nil {
		t.Fatalf("sweepAllNamespaces: %v", err)
	}

	if got := getRoleAnnotation(t, kube, "operator-ns", "operator-role", annotationRBACOwner); got != "" {
		t.Errorf("exempt-labeled namespace Role should not be annotated, got %q", got)
	}
}

// TestSweep_AlreadyAnnotatedRoleNotRepatched verifies that resources already
// carrying the ownership annotation are not patched again (idempotent).
func TestSweep_AlreadyAnnotatedRoleNotRepatched(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "app-ns", nil)
	addRole(t, kube, "app-ns", "pre-owned", map[string]string{
		annotationRBACOwner: annotationRBACOwnerValue,
	})

	// Count patches: fake client tracks actions.
	if err := sweep.sweepAllNamespaces(context.Background()); err != nil {
		t.Fatalf("sweepAllNamespaces: %v", err)
	}

	actions := kube.Actions()
	for _, a := range actions {
		if a.GetVerb() == "patch" && a.GetResource().Resource == "roles" {
			t.Errorf("already-annotated Role should not be patched, got patch action: %v", a)
		}
	}
}

// --- sweep: cluster-scoped resources ---

// TestSweep_SystemPrefixClusterRoleNotAnnotated verifies that ClusterRoles with
// the system: name prefix are not swept — Kubernetes built-ins must be left
// untouched even though they land in non-system namespaces (cluster-scoped).
func TestSweep_SystemPrefixClusterRoleNotAnnotated(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addClusterRole(t, kube, "system:node", nil)
	addClusterRole(t, kube, "system:controller:node-controller", nil)

	if err := sweep.sweepClusterScoped(context.Background()); err != nil {
		t.Fatalf("sweepClusterScoped: %v", err)
	}

	if got := getClusterRoleAnnotation(t, kube, "system:node", annotationRBACOwner); got != "" {
		t.Errorf("system:node ClusterRole should not be annotated, got %q", got)
	}
}

// TestSweep_NonSystemClusterRoleObserved verifies that ClusterRoles without
// the system: prefix are observed (logged) but NOT annotated in audit mode.
// Annotation patching of ClusterRoles triggers Kubernetes RBAC escalation checks.
func TestSweep_NonSystemClusterRoleObserved(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addClusterRole(t, kube, "my-app-admin", nil)

	if err := sweep.sweepClusterScoped(context.Background()); err != nil {
		t.Fatalf("sweepClusterScoped: %v", err)
	}

	if got := getClusterRoleAnnotation(t, kube, "my-app-admin", annotationRBACOwner); got != "" {
		t.Errorf("non-system ClusterRole should NOT be annotated in audit mode, got %q", got)
	}
}

// --- component profile creation (SA-based discovery) ---

// TestSweep_ProfileCreation_SkipsAbsentSA verifies that a component whose
// ServiceAccount does not exist on the cluster is silently skipped.
// Namespace is irrelevant -- discovery is by SA name, not by namespace.
func TestSweep_ProfileCreation_SkipsAbsentSA(t *testing.T) {
	sweep, _, dyn, _ := newSweepSuite(t)

	// No SA for any known component exists.
	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles with absent SAs: %v", err)
	}

	// No PermissionSet should have been created in any namespace.
	list, _ := dyn.Resource(tenantPermissionSetGVR).Namespace("cert-manager").List(context.Background(), metav1.ListOptions{})
	if list != nil && len(list.Items) > 0 {
		t.Errorf("PermissionSet should not be created when SA is absent")
	}
}

// TestSweep_ProfileCreation_DiscoversByServiceAccountName verifies that the
// component's install namespace is discovered via SA name (not hardcoded), and
// that profiles are always created in ont-system regardless of install namespace.
func TestSweep_ProfileCreation_DiscoversByServiceAccountName(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	// Create cert-manager SA in a non-conventional namespace to prove
	// discovery is SA-based, not namespace-hardcoded.
	addNamespace(t, kube, "tools", nil)
	addServiceAccount(t, kube, "tools", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	// Profiles are always created in ont-system, not in the install namespace.
	ps, err := dyn.Resource(tenantPermissionSetGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-baseline", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("PermissionSet not created in ont-system: %v", err)
	}
	if ps.GetName() != "cert-manager-baseline" {
		t.Errorf("PermissionSet name: got %q, want cert-manager-baseline", ps.GetName())
	}

	policy, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-rbac-policy", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("RBACPolicy not created in ont-system: %v", err)
	}
	if policy.GetName() != "cert-manager-rbac-policy" {
		t.Errorf("RBACPolicy name: got %q", policy.GetName())
	}

	profile, err := dyn.Resource(tenantRBACProfileGVR).Namespace("ont-system").Get(
		context.Background(), "rbac-cert-manager", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("RBACProfile not created in ont-system: %v", err)
	}
	if profile.GetName() != "rbac-cert-manager" {
		t.Errorf("RBACProfile name: got %q", profile.GetName())
	}
}

// TestSweep_ProfileCreation_HintPreferredOnCollision verifies that when the
// SA name exists in multiple namespaces, the NamespaceHint namespace is used
// to build the principalRef — all profiles still land in ont-system.
func TestSweep_ProfileCreation_HintPreferredOnCollision(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	// cert-manager SA exists in both "cert-manager" (hint) and "other-ns".
	addNamespace(t, kube, "other-ns", nil)
	addServiceAccount(t, kube, "other-ns", "cert-manager", nil)
	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	// Profiles always in ont-system, not in the install namespace.
	_, err := dyn.Resource(tenantPermissionSetGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-baseline", metav1.GetOptions{},
	)
	if err != nil {
		t.Errorf("PermissionSet not found in ont-system: %v", err)
	}
	// Neither component namespace should carry profiles.
	for _, ns := range []string{"cert-manager", "other-ns"} {
		list, _ := dyn.Resource(tenantPermissionSetGVR).Namespace(ns).List(context.Background(), metav1.ListOptions{})
		if list != nil && len(list.Items) > 0 {
			t.Errorf("PermissionSet should NOT be created in component namespace %s", ns)
		}
	}
}

// TestSweep_ProfileCreation_PrincipalRefUsesDiscoveredNamespace verifies that
// the RBACProfile principalRef is built from the discovered install namespace,
// while the profile itself resides in ont-system.
func TestSweep_ProfileCreation_PrincipalRefUsesDiscoveredNamespace(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	addNamespace(t, kube, "custom-ns", nil)
	addServiceAccount(t, kube, "custom-ns", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	// Profile is in ont-system, not the component's install namespace.
	profile, err := dyn.Resource(tenantRBACProfileGVR).Namespace("ont-system").Get(
		context.Background(), "rbac-cert-manager", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("RBACProfile not found in ont-system: %v", err)
	}

	// PrincipalRef still uses the discovered install namespace.
	spec, _ := profile.Object["spec"].(map[string]interface{})
	principalRef, _ := spec["principalRef"].(string)
	wantPrincipal := "system:serviceaccount:custom-ns:cert-manager"
	if principalRef != wantPrincipal {
		t.Errorf("principalRef: got %q, want %q", principalRef, wantPrincipal)
	}
}

// TestSweep_ProfileCreation_IsIdempotent verifies that running createComponentProfiles
// twice does not fail or duplicate resources.
func TestSweep_ProfileCreation_IsIdempotent(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("first createComponentProfiles: %v", err)
	}
	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("second createComponentProfiles (idempotent): %v", err)
	}
}

// TestSweep_ProfileCreation_SkipsMissingCRDs verifies graceful handling when
// security.ontai.dev CRDs are not installed on the tenant cluster. The sweep
// must return nil (not an error) so the enforcement gate still transitions.
func TestSweep_ProfileCreation_SkipsMissingCRDs(t *testing.T) {
	s := runtime.NewScheme()
	kube := kubefake.NewClientset()
	dyn := dynamicfake.NewSimpleDynamicClient(s)
	gate := webhook.NewEnforcementGate()
	sweep := &TenantBootstrapSweep{
		KubeClient:    kube,
		DynamicClient: dyn,
		Gate:          gate,
	}

	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles with missing CRDs should not error: %v", err)
	}
}

// --- enforcement gate lifecycle ---

// TestRunOnce_GateRemainsAuditAfterSuccess verifies that the enforcement gate
// stays in audit mode even after a fully successful sweep. Conductor in tenant
// never enforces -- it observes only. The gate never transitions to strict.
func TestRunOnce_GateRemainsAuditAfterSuccess(t *testing.T) {
	sweep, kube, _, gate := newSweepSuite(t)

	addNamespace(t, kube, "app-ns", nil)
	addRole(t, kube, "app-ns", "app-role", nil)

	sweep.RunOnce(context.Background())

	if gate.IsStrict() {
		t.Error("gate should remain in audit mode after RunOnce -- tenant conductor never enforces")
	}
}

// TestRunOnce_GateRemainsAuditOnSweepFailure verifies that the enforcement
// gate stays in audit mode when the sweep fails (namespace list error, etc.).
// This prevents a broken sweep from locking out the cluster.
func TestRunOnce_GateRemainsAuditOnSweepFailure(t *testing.T) {
	gate := webhook.NewEnforcementGate()
	// Provide a nil KubeClient to force an immediate error path.
	sweep := &TenantBootstrapSweep{
		KubeClient:    nil,
		DynamicClient: nil,
		Gate:          gate,
	}

	sweep.RunOnce(context.Background())

	if gate.IsStrict() {
		t.Error("gate should remain in audit mode after failed RunOnce")
	}
}

// TestIsSystemNamespace verifies the helper correctly classifies all Kubernetes
// system namespaces that must be excluded from the annotation sweep.
func TestIsSystemNamespace(t *testing.T) {
	exempt := []string{"kube-system", "kube-public", "kube-node-lease"}
	for _, ns := range exempt {
		if !isSystemNamespace(ns) {
			t.Errorf("isSystemNamespace(%q) = false, want true", ns)
		}
	}

	governed := []string{"app-ns", "cert-manager", "kueue-system", "ont-system", "seam-system", ""}
	for _, ns := range governed {
		if isSystemNamespace(ns) {
			t.Errorf("isSystemNamespace(%q) = true, want false", ns)
		}
	}
}

// TestSweep_PermissionSetSpec verifies the PermissionSet spec fields are
// correctly populated: description, permissions with wildcard apiGroups/resources
// and all seven standard verbs.
func TestSweep_PermissionSetSpec(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)
	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	ps, err := dyn.Resource(tenantPermissionSetGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-baseline", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get PermissionSet: %v", err)
	}

	spec, ok := ps.Object["spec"].(map[string]interface{})
	if !ok {
		t.Fatal("PermissionSet spec absent or wrong type")
	}

	perms, ok := spec["permissions"].([]interface{})
	if !ok || len(perms) == 0 {
		t.Fatal("PermissionSet spec.permissions absent or empty")
	}

	rule, ok := perms[0].(map[string]interface{})
	if !ok {
		t.Fatal("permissions[0] wrong type")
	}

	verbs, ok := rule["verbs"].([]interface{})
	if !ok {
		t.Fatal("permissions[0].verbs absent")
	}
	if len(verbs) != 7 {
		t.Errorf("expected 7 verbs, got %d: %v", len(verbs), verbs)
	}

	apiGroups, ok := rule["apiGroups"].([]interface{})
	if !ok || len(apiGroups) == 0 || apiGroups[0] != "*" {
		t.Errorf("expected apiGroups=[*], got %v", apiGroups)
	}
}

// TestSweep_AllFiveComponentsCreated verifies that all five components in the
// catalog get PermissionSet+RBACPolicy+RBACProfile when their SAs are present.
// Each component SA is created in the conventional namespace for clarity.
func TestSweep_AllFiveComponentsCreated(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	// Map of component SA name → namespace for setup.
	compNS := map[string]string{
		"cert-manager":                       "cert-manager",
		"kueue-controller-manager":            "kueue-system",
		"cnpg-manager":                        "cnpg-system",
		"metallb-controller":                  "metallb-system",
		"local-path-provisioner-service-account": "local-path-storage",
	}
	for saName, ns := range compNS {
		addNamespace(t, kube, ns, nil)
		addServiceAccount(t, kube, ns, saName, nil)
	}

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	// All profiles land in ont-system regardless of install namespace.
	for _, comp := range tenantKnownComponents {
		if _, err := dyn.Resource(tenantPermissionSetGVR).Namespace("ont-system").Get(
			context.Background(), comp.PermissionSetName, metav1.GetOptions{},
		); apierrors.IsNotFound(err) {
			t.Errorf("PermissionSet %q not created for component %q", comp.PermissionSetName, comp.Name)
		}
		if _, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Get(
			context.Background(), comp.PolicyName, metav1.GetOptions{},
		); apierrors.IsNotFound(err) {
			t.Errorf("RBACPolicy %q not created for component %q", comp.PolicyName, comp.Name)
		}
		if _, err := dyn.Resource(tenantRBACProfileGVR).Namespace("ont-system").Get(
			context.Background(), comp.ProfileName, metav1.GetOptions{},
		); apierrors.IsNotFound(err) {
			t.Errorf("RBACProfile %q not created for component %q", comp.ProfileName, comp.Name)
		}
	}
}

// --- RBACPolicy annotation-driven enforcement escalation ---

// TestEnsureRBACPolicy_StampsAuditAnnotationOnCreate verifies that when conductor
// creates a new RBACPolicy, it stamps ontai.dev/rbac-enforcement-mode=audit on
// the metadata. This marks the policy as audit-mode so humans can identify it
// and optionally escalate via annotation. Decision H.
func TestEnsureRBACPolicy_StampsAuditAnnotationOnCreate(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	policy, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-rbac-policy", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get RBACPolicy: %v", err)
	}
	annotations, _, _ := unstructured.NestedStringMap(policy.Object, "metadata", "annotations")
	if annotations[annotationEnforcementMode] != annotationEnforcementAudit {
		t.Errorf("new RBACPolicy annotation %q: got %q, want %q",
			annotationEnforcementMode, annotations[annotationEnforcementMode], annotationEnforcementAudit)
	}
	spec, _, _ := unstructured.NestedStringMap(policy.Object, "spec")
	if spec["enforcementMode"] != "audit" {
		t.Errorf("new RBACPolicy enforcementMode: got %q, want audit", spec["enforcementMode"])
	}
}

// TestEnsureRBACPolicy_EnforcementAnnotation_EscalatesToStrict verifies that
// when a human sets ontai.dev/rbac-enforcement-mode=enforcement on an existing
// RBACPolicy, the sweep escalates enforcementMode to strict on the next cycle.
// This is the human-in-loop gate: conductor never self-escalates. Decision H.
func TestEnsureRBACPolicy_EnforcementAnnotation_EscalatesToStrict(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	// Pre-create the RBACPolicy with the enforcement annotation already set,
	// simulating a human who ran: kubectl annotate rbacpolicy/cert-manager-rbac-policy
	// ontai.dev/rbac-enforcement-mode=enforcement -n ont-system
	preExisting := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "RBACPolicy",
			"metadata": map[string]interface{}{
				"name":      "cert-manager-rbac-policy",
				"namespace": "ont-system",
				"annotations": map[string]interface{}{
					annotationEnforcementMode: annotationEnforcementStrict,
				},
			},
			"spec": map[string]interface{}{
				"enforcementMode": "audit",
			},
		},
	}
	if _, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Create(
		context.Background(), preExisting, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-create RBACPolicy: %v", err)
	}

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	updated, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-rbac-policy", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated RBACPolicy: %v", err)
	}
	spec, _, _ := unstructured.NestedStringMap(updated.Object, "spec")
	if spec["enforcementMode"] != "strict" {
		t.Errorf("enforcementMode after annotation escalation: got %q, want strict", spec["enforcementMode"])
	}
}

// TestEnsureRBACPolicy_AuditAnnotation_KeepsAudit verifies that an existing
// RBACPolicy carrying the default audit annotation is left unchanged (enforcementMode
// stays audit). Conductor never self-escalates.
func TestEnsureRBACPolicy_AuditAnnotation_KeepsAudit(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	preExisting := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "RBACPolicy",
			"metadata": map[string]interface{}{
				"name":      "cert-manager-rbac-policy",
				"namespace": "ont-system",
				"annotations": map[string]interface{}{
					annotationEnforcementMode: annotationEnforcementAudit,
				},
			},
			"spec": map[string]interface{}{
				"enforcementMode": "audit",
			},
		},
	}
	if _, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Create(
		context.Background(), preExisting, metav1.CreateOptions{},
	); err != nil {
		t.Fatalf("pre-create RBACPolicy: %v", err)
	}

	if err := sweep.createComponentProfiles(context.Background(), annotationEnforcementAudit); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	updated, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-rbac-policy", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get updated RBACPolicy: %v", err)
	}
	spec, _, _ := unstructured.NestedStringMap(updated.Object, "spec")
	if spec["enforcementMode"] != "audit" {
		t.Errorf("enforcementMode with audit annotation: got %q, want audit", spec["enforcementMode"])
	}
}

// --- management cluster enforcement escalation ---

// TestEnsureRBACPolicy_MgmtClusterAnnotation_EscalatesToStrict verifies that when
// admin annotates the InfrastructureTalosCluster in seam-system on the management
// cluster with ontai.dev/rbac-enforcement-mode=enforcement, the sweep escalates
// all RBACPolicies to strict. This is the management-side human-in-loop gate: admin
// annotates a governance object on the management cluster; conductor propagates the
// intent to the tenant cluster's RBACPolicies on the next sweep cycle. Decision H.
func TestEnsureRBACPolicy_MgmtClusterAnnotation_EscalatesToStrict(t *testing.T) {
	// Set up mgmt fake client with InfrastructureTalosCluster in seam-system.
	mgmtScheme := runtime.NewScheme()
	mgmtScheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureTalosCluster"},
		&unstructured.Unstructured{},
	)
	mgmtScheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureTalosClusterList"},
		&unstructured.UnstructuredList{},
	)

	// Admin has annotated the TC on the management cluster.
	tc := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureTalosCluster",
			"metadata": map[string]interface{}{
				"name":      "ccs-dev",
				"namespace": "seam-system",
				"annotations": map[string]interface{}{
					annotationEnforcementMode: annotationEnforcementStrict,
				},
			},
		},
	}
	mgmtDyn := dynamicfake.NewSimpleDynamicClient(mgmtScheme, tc)

	// Set up tenant sweep with mgmt client.
	tenantScheme := runtime.NewScheme()
	registerSecurityCRDs(tenantScheme)
	kube := kubefake.NewClientset()
	dyn := dynamicfake.NewSimpleDynamicClient(tenantScheme)
	sweep := &TenantBootstrapSweep{
		KubeClient:        kube,
		DynamicClient:     dyn,
		MgmtDynamicClient: mgmtDyn,
		ClusterName:       "ccs-dev",
		Gate:              webhook.NewEnforcementGate(),
	}

	addNamespace(t, kube, "cert-manager", nil)
	addServiceAccount(t, kube, "cert-manager", "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background(), sweep.readMgmtEnforcementMode(context.Background())); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	policy, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("ont-system").Get(
		context.Background(), "cert-manager-rbac-policy", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get RBACPolicy: %v", err)
	}
	spec, _, _ := unstructured.NestedStringMap(policy.Object, "spec")
	if spec["enforcementMode"] != "strict" {
		t.Errorf("enforcementMode after mgmt cluster annotation: got %q, want strict", spec["enforcementMode"])
	}
}
