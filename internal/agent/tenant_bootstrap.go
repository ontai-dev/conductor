package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apitypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	"github.com/ontai-dev/conductor/internal/webhook"
)

// Annotation constants for the tenant bootstrap sweep.
// Must match guardian/internal/webhook/decision.go exactly.
const (
	annotationRBACOwner          = "ontai.dev/rbac-owner"
	annotationRBACOwnerValue     = "guardian"
	annotationEnforcementMode    = "ontai.dev/rbac-enforcement-mode"
	annotationEnforcementAudit   = "audit"
	webhookExemptLabel           = "seam.ontai.dev/webhook-mode"
	webhookExemptValue           = "exempt"
)

// sweepAnnotationPatch is the JSON MergePatch applied to each un-owned RBAC
// resource during the bootstrap annotation sweep. Only touches metadata.annotations.
var sweepAnnotationPatch = mustBuildTenantSweepPatch()

func mustBuildTenantSweepPatch() []byte {
	type metaPatch struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	var p metaPatch
	p.Metadata.Annotations = map[string]string{
		annotationRBACOwner:       annotationRBACOwnerValue,
		annotationEnforcementMode: annotationEnforcementAudit,
	}
	b, err := json.Marshal(p)
	if err != nil {
		panic("conductor: failed to build tenant sweep annotation patch: " + err.Error())
	}
	return b
}

// tenantComponent describes a third-party component whose profile Conductor
// creates on tenant clusters. The install namespace is discovered at runtime
// by finding ServiceAccountName across all non-system namespaces — never
// hardcoded. Mirrors managementThirdPartyComponents in guardian. guardian-schema.md §6.
type tenantComponent struct {
	// Name is the human-readable component identifier.
	Name string

	// ServiceAccountName is the well-known SA name the component creates in its
	// install namespace. Used to discover the actual namespace at runtime.
	ServiceAccountName string

	// NamespaceHint is the conventional install namespace, used only as a
	// tiebreaker when the SA name matches in multiple non-system namespaces.
	NamespaceHint string

	ProfileName       string
	PolicyName        string
	PermissionSetName string
}

// tenantKnownComponents is the catalog of components for which Conductor creates
// profiles on tenant clusters. Namespaces are discovered via ServiceAccountName.
var tenantKnownComponents = []tenantComponent{
	{
		Name:               "cert-manager",
		ServiceAccountName: "cert-manager",
		NamespaceHint:      "cert-manager",
		ProfileName:        "rbac-cert-manager",
		PolicyName:         "cert-manager-rbac-policy",
		PermissionSetName:  "cert-manager-baseline",
	},
	{
		Name:               "kueue",
		ServiceAccountName: "kueue-controller-manager",
		ProfileName:        "rbac-kueue",
		PolicyName:         "kueue-rbac-policy",
		PermissionSetName:  "kueue-baseline",
	},
	{
		Name:               "cnpg",
		ServiceAccountName: "cnpg-manager",
		ProfileName:        "rbac-cnpg",
		PolicyName:         "cnpg-rbac-policy",
		PermissionSetName:  "cnpg-baseline",
	},
	{
		Name:               "metallb",
		ServiceAccountName: "metallb-controller",
		ProfileName:        "rbac-metallb",
		PolicyName:         "metallb-rbac-policy",
		PermissionSetName:  "metallb-baseline",
	},
	{
		Name:               "local-path-provisioner",
		ServiceAccountName: "local-path-provisioner-service-account",
		ProfileName:        "rbac-local-path-provisioner",
		PolicyName:         "local-path-provisioner-rbac-policy",
		PermissionSetName:  "local-path-provisioner-baseline",
	},
}

// Security CRD GVRs for dynamic client access to Guardian CRDs on tenant clusters.
var (
	tenantPermissionSetGVR = schema.GroupVersionResource{
		Group:    "security.ontai.dev",
		Version:  "v1alpha1",
		Resource: "permissionsets",
	}
	tenantRBACPolicyGVR = schema.GroupVersionResource{
		Group:    "security.ontai.dev",
		Version:  "v1alpha1",
		Resource: "rbacpolicies",
	}
	tenantRBACProfileGVR = schema.GroupVersionResource{
		Group:    "security.ontai.dev",
		Version:  "v1alpha1",
		Resource: "rbacprofiles",
	}
)

// TenantBootstrapSweep performs the bootstrap annotation sweep and component
// profile creation on tenant clusters. Mirrors Guardian's BootstrapAnnotationRunnable
// behavior. Runs once on startup and periodically to pick up newly deployed
// components (e.g., after ClusterPack deployments).
//
// Phase 1 (audit): annotates all pre-existing RBAC with ownership annotations.
// EnforcementGate remains in audit mode — webhook logs but does not reject.
//
// Phase 2 (profile creation): for each component whose namespace exists,
// creates PermissionSet, RBACPolicy, and RBACProfile via the dynamic client.
// Skips silently if the security CRDs are not installed on the tenant cluster.
//
// After both phases: sets EnforcementGate to strict. Webhook begins rejecting
// RBAC resources without the ownership annotation. conductor-schema.md §15,
// guardian-schema.md §3 Step 2, §6.
type TenantBootstrapSweep struct {
	KubeClient    kubernetes.Interface
	DynamicClient dynamic.Interface
	Gate          *webhook.EnforcementGate
}

// RunOnce performs the full bootstrap sweep and component profile creation.
// Should be started as a goroutine after leader election wins. Blocks until
// complete, then sets the enforcement gate to strict.
func (s *TenantBootstrapSweep) RunOnce(ctx context.Context) {
	if err := s.runOnce(ctx); err != nil {
		fmt.Printf("tenant bootstrap sweep: error: %v\n", err)
		// Non-fatal: gate remains in audit mode if sweep fails. Retried periodically.
		return
	}
	s.Gate.SetStrict()
	fmt.Printf("tenant bootstrap sweep: complete — enforcement mode set to strict\n")
}

// RunPeriodic runs the bootstrap sweep on startup then repeats on interval.
// Each cycle re-scans all namespaces to pick up newly deployed components.
// The gate transitions to strict after the first successful run.
func (s *TenantBootstrapSweep) RunPeriodic(ctx context.Context, interval time.Duration) {
	s.RunOnce(ctx)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Re-scan for newly deployed components. Annotation sweep is idempotent.
			// Gate is already strict from the first run; this only adds new profiles.
			if err := s.runOnce(ctx); err != nil {
				fmt.Printf("tenant bootstrap sweep: periodic error: %v\n", err)
			}
		}
	}
}

func (s *TenantBootstrapSweep) runOnce(ctx context.Context) error {
	if err := s.sweepAllNamespaces(ctx); err != nil {
		return fmt.Errorf("sweep namespaces: %w", err)
	}
	if err := s.createComponentProfiles(ctx); err != nil {
		return fmt.Errorf("create component profiles: %w", err)
	}
	return nil
}

// sweepAllNamespaces annotates all un-owned RBAC resources in all non-exempt
// namespaces and all cluster-scoped RBAC. kube-system is always skipped.
func (s *TenantBootstrapSweep) sweepAllNamespaces(ctx context.Context) error {
	if s.KubeClient == nil {
		return fmt.Errorf("KubeClient not initialized")
	}
	nsList, err := s.KubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}

	for _, ns := range nsList.Items {
		if isSystemNamespace(ns.Name) {
			continue
		}
		if ns.Labels[webhookExemptLabel] == webhookExemptValue {
			continue
		}
		if err := s.sweepNamespace(ctx, ns.Name); err != nil {
			return fmt.Errorf("sweep namespace %q: %w", ns.Name, err)
		}
	}

	return s.sweepClusterScoped(ctx)
}

// sweepNamespace annotates Roles, RoleBindings, and ServiceAccounts in ns.
func (s *TenantBootstrapSweep) sweepNamespace(ctx context.Context, ns string) error {
	roles, err := s.KubeClient.RbacV1().Roles(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, role := range roles.Items {
		if role.Annotations[annotationRBACOwner] == annotationRBACOwnerValue {
			continue
		}
		target := &rbacv1.Role{}
		target.Name = role.Name
		target.Namespace = role.Namespace
		if _, err := s.KubeClient.RbacV1().Roles(ns).Patch(
			ctx, role.Name, apitypes.MergePatchType, sweepAnnotationPatch, metav1.PatchOptions{},
		); err != nil {
			return fmt.Errorf("patch Role %s/%s: %w", ns, role.Name, err)
		}
	}

	rbs, err := s.KubeClient.RbacV1().RoleBindings(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, rb := range rbs.Items {
		if rb.Annotations[annotationRBACOwner] == annotationRBACOwnerValue {
			continue
		}
		if _, err := s.KubeClient.RbacV1().RoleBindings(ns).Patch(
			ctx, rb.Name, apitypes.MergePatchType, sweepAnnotationPatch, metav1.PatchOptions{},
		); err != nil {
			return fmt.Errorf("patch RoleBinding %s/%s: %w", ns, rb.Name, err)
		}
	}

	sas, err := s.KubeClient.CoreV1().ServiceAccounts(ns).List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, sa := range sas.Items {
		if sa.Annotations[annotationRBACOwner] == annotationRBACOwnerValue {
			continue
		}
		if _, err := s.KubeClient.CoreV1().ServiceAccounts(ns).Patch(
			ctx, sa.Name, apitypes.MergePatchType, sweepAnnotationPatch, metav1.PatchOptions{},
		); err != nil {
			return fmt.Errorf("patch ServiceAccount %s/%s: %w", ns, sa.Name, err)
		}
	}

	return nil
}

// sweepClusterScoped annotates ClusterRoles and ClusterRoleBindings.
// Skips resources whose name starts with "system:" — Kubernetes built-ins.
func (s *TenantBootstrapSweep) sweepClusterScoped(ctx context.Context) error {
	crs, err := s.KubeClient.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, cr := range crs.Items {
		if strings.HasPrefix(cr.Name, "system:") {
			continue
		}
		if cr.Annotations[annotationRBACOwner] == annotationRBACOwnerValue {
			continue
		}
		if _, err := s.KubeClient.RbacV1().ClusterRoles().Patch(
			ctx, cr.Name, apitypes.MergePatchType, sweepAnnotationPatch, metav1.PatchOptions{},
		); err != nil {
			return fmt.Errorf("patch ClusterRole %s: %w", cr.Name, err)
		}
	}

	crbs, err := s.KubeClient.RbacV1().ClusterRoleBindings().List(ctx, metav1.ListOptions{})
	if err != nil {
		return err
	}
	for _, crb := range crbs.Items {
		if strings.HasPrefix(crb.Name, "system:") {
			continue
		}
		if crb.Annotations[annotationRBACOwner] == annotationRBACOwnerValue {
			continue
		}
		if _, err := s.KubeClient.RbacV1().ClusterRoleBindings().Patch(
			ctx, crb.Name, apitypes.MergePatchType, sweepAnnotationPatch, metav1.PatchOptions{},
		); err != nil {
			return fmt.Errorf("patch ClusterRoleBinding %s: %w", crb.Name, err)
		}
	}

	return nil
}

// createComponentProfiles creates baseline PermissionSet, RBACPolicy, and RBACProfile
// for each known component discovered on the tenant cluster. Discovery finds the
// component's install namespace by matching ServiceAccountName across all non-system
// namespaces — no namespace is hardcoded. Skips silently if security CRDs are absent.
func (s *TenantBootstrapSweep) createComponentProfiles(ctx context.Context) error {
	for _, comp := range tenantKnownComponents {
		ns, principalRef, found := s.discoverComponentNamespace(ctx, comp)
		if !found {
			continue
		}

		if err := s.ensurePermissionSet(ctx, ns, comp); err != nil {
			if isSecurityCRDAbsent(err) {
				fmt.Printf("tenant bootstrap sweep: security CRDs not installed on this cluster, skipping profile creation\n")
				return nil
			}
			return fmt.Errorf("PermissionSet for %q: %w", comp.Name, err)
		}
		if err := s.ensureRBACPolicy(ctx, ns, comp); err != nil {
			if isSecurityCRDAbsent(err) {
				return nil
			}
			return fmt.Errorf("RBACPolicy for %q: %w", comp.Name, err)
		}
		if err := s.ensureRBACProfile(ctx, ns, principalRef, comp); err != nil {
			if isSecurityCRDAbsent(err) {
				return nil
			}
			return fmt.Errorf("RBACProfile for %q: %w", comp.Name, err)
		}

		fmt.Printf("tenant bootstrap sweep: component wrapped: %s in %s\n", comp.Name, ns)
	}
	return nil
}

// discoverComponentNamespace finds the namespace where a component is installed
// by listing all ServiceAccounts across all non-system namespaces and matching
// by ServiceAccountName. Returns namespace, principalRef, and whether found.
// When multiple namespaces have the same SA name, NamespaceHint is used as
// the preferred namespace; otherwise the first match is used.
func (s *TenantBootstrapSweep) discoverComponentNamespace(ctx context.Context, comp tenantComponent) (ns, principalRef string, found bool) {
	saList, err := s.KubeClient.CoreV1().ServiceAccounts(metav1.NamespaceAll).List(ctx, metav1.ListOptions{})
	if err != nil {
		return "", "", false
	}

	var candidates []string
	for _, sa := range saList.Items {
		if sa.Name == comp.ServiceAccountName && !isSystemNamespace(sa.Namespace) {
			candidates = append(candidates, sa.Namespace)
		}
	}

	switch len(candidates) {
	case 0:
		return "", "", false
	case 1:
		ns = candidates[0]
	default:
		ns = candidates[0]
		for _, c := range candidates {
			if c == comp.NamespaceHint {
				ns = c
				break
			}
		}
		fmt.Printf("tenant bootstrap sweep: multiple namespaces match SA %q, using %q\n",
			comp.ServiceAccountName, ns)
	}

	principalRef = fmt.Sprintf("system:serviceaccount:%s:%s", ns, comp.ServiceAccountName)
	return ns, principalRef, true
}

// isSecurityCRDAbsent returns true when the error indicates that the
// security.ontai.dev CRD group is not installed on this cluster.
func isSecurityCRDAbsent(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "no kind is registered") ||
		strings.Contains(msg, "no matches for kind") ||
		strings.Contains(msg, "the server could not find the requested resource")
}

// isSystemNamespace returns true for namespaces that must never be swept.
// Kubernetes writes RBAC to these during normal operations and upgrades;
// the sweep must not touch them. Matches the exemption set in the webhook.
func isSystemNamespace(ns string) bool {
	switch ns {
	case "kube-system", "kube-public", "kube-node-lease":
		return true
	}
	return false
}

func (s *TenantBootstrapSweep) ensurePermissionSet(ctx context.Context, ns string, comp tenantComponent) error {
	_, err := s.DynamicClient.Resource(tenantPermissionSetGVR).Namespace(ns).Get(
		ctx, comp.PermissionSetName, metav1.GetOptions{},
	)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "PermissionSet",
			"metadata": map[string]interface{}{
				"namespace": ns,
				"name":      comp.PermissionSetName,
				"labels": map[string]interface{}{
					"ontai.dev/managed-by":          "conductor",
					"ontai.dev/permission-set-type": "bootstrap",
					"ontai.dev/component":           comp.Name,
				},
			},
			"spec": map[string]interface{}{
				"description": comp.Name + " bootstrap baseline permissions (tenant cluster)",
				"permissions": []interface{}{
					map[string]interface{}{
						"apiGroups": []interface{}{"*"},
						"resources": []interface{}{"*"},
						"verbs": []interface{}{
							"get", "list", "watch", "create", "update", "patch", "delete",
						},
					},
				},
			},
		},
	}
	_, err = s.DynamicClient.Resource(tenantPermissionSetGVR).Namespace(ns).Create(
		ctx, obj, metav1.CreateOptions{},
	)
	return err
}

func (s *TenantBootstrapSweep) ensureRBACPolicy(ctx context.Context, ns string, comp tenantComponent) error {
	_, err := s.DynamicClient.Resource(tenantRBACPolicyGVR).Namespace(ns).Get(
		ctx, comp.PolicyName, metav1.GetOptions{},
	)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "RBACPolicy",
			"metadata": map[string]interface{}{
				"namespace": ns,
				"name":      comp.PolicyName,
				"labels": map[string]interface{}{
					"ontai.dev/managed-by": "conductor",
					"ontai.dev/component":  comp.Name,
				},
			},
			"spec": map[string]interface{}{
				"subjectScope":            "platform",
				"maximumPermissionSetRef": comp.PermissionSetName,
				"enforcementMode":         "strict",
			},
		},
	}
	_, err = s.DynamicClient.Resource(tenantRBACPolicyGVR).Namespace(ns).Create(
		ctx, obj, metav1.CreateOptions{},
	)
	return err
}

func (s *TenantBootstrapSweep) ensureRBACProfile(ctx context.Context, ns, principalRef string, comp tenantComponent) error {
	_, err := s.DynamicClient.Resource(tenantRBACProfileGVR).Namespace(ns).Get(
		ctx, comp.ProfileName, metav1.GetOptions{},
	)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "RBACProfile",
			"metadata": map[string]interface{}{
				"namespace": ns,
				"name":      comp.ProfileName,
				"labels": map[string]interface{}{
					"ontai.dev/managed-by":        "conductor",
					"ontai.dev/rbac-profile-type": "third-party",
					"ontai.dev/component":         comp.Name,
				},
			},
			"spec": map[string]interface{}{
				"principalRef":   principalRef,
				"targetClusters": []interface{}{"management"},
				"permissionDeclarations": []interface{}{
					map[string]interface{}{
						"permissionSetRef": comp.PermissionSetName,
						"scope":            "cluster",
					},
				},
				"rbacPolicyRef": comp.PolicyName,
			},
		},
	}
	_, err = s.DynamicClient.Resource(tenantRBACProfileGVR).Namespace(ns).Create(
		ctx, obj, metav1.CreateOptions{},
	)
	return err
}
