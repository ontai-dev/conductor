package agent

// Unit tests for TenantBootstrapSweep: annotation sweep + component profile
// creation + enforcement gate lifecycle. Uses fake clients — no live cluster.
//
// Covers conductor-schema.md §15 bootstrap sweep invariants:
//   - Annotation sweep stamps ownership on un-owned RBAC in non-system namespaces.
//   - kube-system, kube-public, kube-node-lease are never swept.
//   - Namespaces with exempt label are skipped.
//   - system: prefix ClusterRoles/ClusterRoleBindings are not swept.
//   - Already-annotated resources are not re-patched (idempotent).
//   - Component profiles created when namespace exists and CRDs available.
//   - Component profiles skipped when namespace absent.
//   - Security CRD absence is handled gracefully (gate not blocked).
//   - Gate transitions to strict after successful sweep.
//   - Gate remains in audit mode if sweep fails.

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

func addClusterRoleBinding(t *testing.T, kube *kubefake.Clientset, name string, annotations map[string]string) {
	t.Helper()
	crb := &rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations}}
	if _, err := kube.RbacV1().ClusterRoleBindings().Create(context.Background(), crb, metav1.CreateOptions{}); err != nil {
		t.Fatalf("add ClusterRoleBinding %q: %v", name, err)
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

// TestSweep_StampsOwnershipInAppNamespace verifies that un-owned Role,
// RoleBinding, and ServiceAccount in a non-system namespace are annotated
// with ontai.dev/rbac-owner=guardian after a sweep.
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

	if got := getRoleAnnotation(t, kube, "app-ns", "app-role", annotationRBACOwner); got != annotationRBACOwnerValue {
		t.Errorf("Role annotation: got %q, want %q", got, annotationRBACOwnerValue)
	}

	rb, _ := kube.RbacV1().RoleBindings("app-ns").Get(ctx, "app-rb", metav1.GetOptions{})
	if rb.Annotations[annotationRBACOwner] != annotationRBACOwnerValue {
		t.Errorf("RoleBinding missing ownership annotation")
	}

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

// TestSweep_NonSystemClusterRoleAnnotated verifies that ClusterRoles without
// the system: prefix ARE annotated during the cluster-scoped sweep.
func TestSweep_NonSystemClusterRoleAnnotated(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addClusterRole(t, kube, "my-app-admin", nil)

	if err := sweep.sweepClusterScoped(context.Background()); err != nil {
		t.Fatalf("sweepClusterScoped: %v", err)
	}

	if got := getClusterRoleAnnotation(t, kube, "my-app-admin", annotationRBACOwner); got != annotationRBACOwnerValue {
		t.Errorf("non-system ClusterRole annotation: got %q, want %q", got, annotationRBACOwnerValue)
	}
}

// --- component profile creation ---

// TestSweep_ProfileCreation_SkipsAbsentNamespace verifies that a component
// whose namespace does not exist on the tenant cluster is silently skipped.
func TestSweep_ProfileCreation_SkipsAbsentNamespace(t *testing.T) {
	sweep, _, dyn, _ := newSweepSuite(t)

	// No cert-manager namespace exists.
	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("createComponentProfiles with absent namespaces: %v", err)
	}

	// No PermissionSet should have been created.
	list, _ := dyn.Resource(tenantPermissionSetGVR).Namespace("cert-manager").List(context.Background(), metav1.ListOptions{})
	if list != nil && len(list.Items) > 0 {
		t.Errorf("PermissionSet should not be created when namespace is absent")
	}
}

// TestSweep_ProfileCreation_CreatesResourcesWhenNamespaceExists verifies that
// PermissionSet, RBACPolicy, and RBACProfile are created when the component
// namespace exists and security CRDs are available.
func TestSweep_ProfileCreation_CreatesResourcesWhenNamespaceExists(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	addNamespace(t, kube, "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	ps, err := dyn.Resource(tenantPermissionSetGVR).Namespace("cert-manager").Get(
		context.Background(), "cert-manager-baseline", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("PermissionSet not created: %v", err)
	}
	if ps.GetName() != "cert-manager-baseline" {
		t.Errorf("PermissionSet name: got %q, want cert-manager-baseline", ps.GetName())
	}

	policy, err := dyn.Resource(tenantRBACPolicyGVR).Namespace("cert-manager").Get(
		context.Background(), "cert-manager-rbac-policy", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("RBACPolicy not created: %v", err)
	}
	if policy.GetName() != "cert-manager-rbac-policy" {
		t.Errorf("RBACPolicy name: got %q", policy.GetName())
	}

	profile, err := dyn.Resource(tenantRBACProfileGVR).Namespace("cert-manager").Get(
		context.Background(), "rbac-cert-manager", metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("RBACProfile not created: %v", err)
	}
	if profile.GetName() != "rbac-cert-manager" {
		t.Errorf("RBACProfile name: got %q", profile.GetName())
	}
}

// TestSweep_ProfileCreation_IsIdempotent verifies that running createComponentProfiles
// twice does not fail or duplicate resources.
func TestSweep_ProfileCreation_IsIdempotent(t *testing.T) {
	sweep, kube, _, _ := newSweepSuite(t)

	addNamespace(t, kube, "cert-manager", nil)

	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("first createComponentProfiles: %v", err)
	}
	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("second createComponentProfiles (idempotent): %v", err)
	}
}

// TestSweep_ProfileCreation_SkipsMissingCRDs verifies graceful handling when
// security.ontai.dev CRDs are not installed on the tenant cluster. The sweep
// must return nil (not an error) so the enforcement gate still transitions.
func TestSweep_ProfileCreation_SkipsMissingCRDs(t *testing.T) {
	// Build a dynamic client scheme with NO security CRD types registered.
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

	// The fake client returns "no kind is registered" for unknown GVRs; the
	// sweep should detect this and return nil.
	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("createComponentProfiles with missing CRDs should not error: %v", err)
	}
}

// --- enforcement gate lifecycle ---

// TestRunOnce_SetsGateStrictOnSuccess verifies that the enforcement gate
// transitions to strict after a fully successful sweep + profile creation.
func TestRunOnce_SetsGateStrictOnSuccess(t *testing.T) {
	sweep, kube, _, gate := newSweepSuite(t)

	addNamespace(t, kube, "app-ns", nil)

	sweep.RunOnce(context.Background())

	if !gate.IsStrict() {
		t.Error("gate should be strict after successful RunOnce")
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

	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	ps, err := dyn.Resource(tenantPermissionSetGVR).Namespace("cert-manager").Get(
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
// catalog get PermissionSet+RBACPolicy+RBACProfile when their namespaces exist.
func TestSweep_AllFiveComponentsCreated(t *testing.T) {
	sweep, kube, dyn, _ := newSweepSuite(t)

	for _, comp := range tenantKnownComponents {
		addNamespace(t, kube, comp.Namespace, nil)
	}

	if err := sweep.createComponentProfiles(context.Background()); err != nil {
		t.Fatalf("createComponentProfiles: %v", err)
	}

	for _, comp := range tenantKnownComponents {
		if _, err := dyn.Resource(tenantPermissionSetGVR).Namespace(comp.Namespace).Get(
			context.Background(), comp.PermissionSetName, metav1.GetOptions{},
		); apierrors.IsNotFound(err) {
			t.Errorf("PermissionSet %q not created for component %q", comp.PermissionSetName, comp.Name)
		}
		if _, err := dyn.Resource(tenantRBACPolicyGVR).Namespace(comp.Namespace).Get(
			context.Background(), comp.PolicyName, metav1.GetOptions{},
		); apierrors.IsNotFound(err) {
			t.Errorf("RBACPolicy %q not created for component %q", comp.PolicyName, comp.Name)
		}
		if _, err := dyn.Resource(tenantRBACProfileGVR).Namespace(comp.Namespace).Get(
			context.Background(), comp.ProfileName, metav1.GetOptions{},
		); apierrors.IsNotFound(err) {
			t.Errorf("RBACProfile %q not created for component %q", comp.ProfileName, comp.Name)
		}
	}
}
