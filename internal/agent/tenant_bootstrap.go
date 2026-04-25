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

// tenantComponent describes a third-party component whose RBACProfile Guardian
// creates on management clusters. Conductor role=tenant mirrors this on tenant
// clusters when security CRDs are available. guardian-schema.md §6.
type tenantComponent struct {
	Name              string
	Namespace         string
	PrincipalRef      string
	ProfileName       string
	PolicyName        string
	PermissionSetName string
}

// tenantKnownComponents is the catalog of components for which Conductor creates
// profiles on tenant clusters when the component namespace exists.
// Mirrors managementThirdPartyComponents in guardian bootstrap_third_party_profiles.go.
var tenantKnownComponents = []tenantComponent{
	{
		Name:              "cert-manager",
		Namespace:         "cert-manager",
		PrincipalRef:      "system:serviceaccount:cert-manager:cert-manager",
		ProfileName:       "rbac-cert-manager",
		PolicyName:        "cert-manager-rbac-policy",
		PermissionSetName: "cert-manager-baseline",
	},
	{
		Name:              "kueue",
		Namespace:         "kueue-system",
		PrincipalRef:      "system:serviceaccount:kueue-system:kueue-controller-manager",
		ProfileName:       "rbac-kueue",
		PolicyName:        "kueue-rbac-policy",
		PermissionSetName: "kueue-baseline",
	},
	{
		Name:              "cnpg",
		Namespace:         "security-system",
		PrincipalRef:      "system:serviceaccount:security-system:cloudnative-pg",
		ProfileName:       "rbac-cnpg",
		PolicyName:        "cnpg-rbac-policy",
		PermissionSetName: "cnpg-baseline",
	},
	{
		Name:              "metallb",
		Namespace:         "metallb-system",
		PrincipalRef:      "system:serviceaccount:metallb-system:controller",
		ProfileName:       "rbac-metallb",
		PolicyName:        "metallb-rbac-policy",
		PermissionSetName: "metallb-baseline",
	},
	{
		Name:              "local-path-provisioner",
		Namespace:         "local-path-storage",
		PrincipalRef:      "system:serviceaccount:local-path-storage:local-path-provisioner",
		ProfileName:       "rbac-local-path-provisioner",
		PolicyName:        "local-path-provisioner-rbac-policy",
		PermissionSetName: "local-path-provisioner-baseline",
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
	nsList, err := s.KubeClient.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list namespaces: %w", err)
	}

	for _, ns := range nsList.Items {
		if ns.Name == "kube-system" {
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
// for each known component whose namespace exists on the tenant cluster.
// Skips silently if the security.ontai.dev CRDs are not installed on this cluster.
// Creation is idempotent — existing resources are left unchanged.
func (s *TenantBootstrapSweep) createComponentProfiles(ctx context.Context) error {
	for _, comp := range tenantKnownComponents {
		if _, err := s.KubeClient.CoreV1().Namespaces().Get(ctx, comp.Namespace, metav1.GetOptions{}); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return fmt.Errorf("check namespace %q: %w", comp.Namespace, err)
		}

		if err := s.ensurePermissionSet(ctx, comp); err != nil {
			if isSecurityCRDAbsent(err) {
				fmt.Printf("tenant bootstrap sweep: security CRDs not installed on this cluster, skipping profile creation\n")
				return nil
			}
			return fmt.Errorf("PermissionSet for %q: %w", comp.Name, err)
		}
		if err := s.ensureRBACPolicy(ctx, comp); err != nil {
			if isSecurityCRDAbsent(err) {
				return nil
			}
			return fmt.Errorf("RBACPolicy for %q: %w", comp.Name, err)
		}
		if err := s.ensureRBACProfile(ctx, comp); err != nil {
			if isSecurityCRDAbsent(err) {
				return nil
			}
			return fmt.Errorf("RBACProfile for %q: %w", comp.Name, err)
		}

		fmt.Printf("tenant bootstrap sweep: component wrapped: %s in %s\n", comp.Name, comp.Namespace)
	}
	return nil
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

func (s *TenantBootstrapSweep) ensurePermissionSet(ctx context.Context, comp tenantComponent) error {
	_, err := s.DynamicClient.Resource(tenantPermissionSetGVR).Namespace(comp.Namespace).Get(
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
				"namespace": comp.Namespace,
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
	_, err = s.DynamicClient.Resource(tenantPermissionSetGVR).Namespace(comp.Namespace).Create(
		ctx, obj, metav1.CreateOptions{},
	)
	return err
}

func (s *TenantBootstrapSweep) ensureRBACPolicy(ctx context.Context, comp tenantComponent) error {
	_, err := s.DynamicClient.Resource(tenantRBACPolicyGVR).Namespace(comp.Namespace).Get(
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
				"namespace": comp.Namespace,
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
	_, err = s.DynamicClient.Resource(tenantRBACPolicyGVR).Namespace(comp.Namespace).Create(
		ctx, obj, metav1.CreateOptions{},
	)
	return err
}

func (s *TenantBootstrapSweep) ensureRBACProfile(ctx context.Context, comp tenantComponent) error {
	_, err := s.DynamicClient.Resource(tenantRBACProfileGVR).Namespace(comp.Namespace).Get(
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
				"namespace": comp.Namespace,
				"name":      comp.ProfileName,
				"labels": map[string]interface{}{
					"ontai.dev/managed-by":        "conductor",
					"ontai.dev/rbac-profile-type": "third-party",
					"ontai.dev/component":         comp.Name,
				},
			},
			"spec": map[string]interface{}{
				"principalRef":   comp.PrincipalRef,
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
	_, err = s.DynamicClient.Resource(tenantRBACProfileGVR).Namespace(comp.Namespace).Create(
		ctx, obj, metav1.CreateOptions{},
	)
	return err
}
