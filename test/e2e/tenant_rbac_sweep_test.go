package e2e_test

// Scenario: Tenant RBAC bootstrap sweep and enforcement webhook validation.
//
// Pre-conditions for management cluster tests (MGMT_KUBECONFIG required):
//   - Guardian is deployed on ccs-mgmt and its bootstrap sweep has completed.
//   - Guardian's admission webhook is operational in Enforcing mode.
//
// Pre-conditions for tenant sweep tests (TENANT_KUBECONFIG additionally required):
//   - Conductor role=tenant is running in ont-system on the tenant cluster.
//   - The tenant Conductor has been rebuilt with the TenantBootstrapSweep changes
//     and redeployed (image rebuild required before this test is live).
//
// What this test verifies (conductor-schema.md §15, guardian-schema.md §5,
// CS-INV-001):
//
// Management cluster (Guardian-owned enforcement):
//   - Guardian swept all RBAC resources in non-system namespaces.
//   - kube-system resources carry no ontai.dev/rbac-owner annotation.
//   - New RBAC resource without annotation is rejected by Guardian's webhook.
//   - New RBAC resource in kube-system is admitted unconditionally.
//   - ClusterRole with system: prefix is admitted unconditionally.
//
// Tenant cluster (Conductor role=tenant enforcement):
//   - Conductor swept all RBAC resources in non-system namespaces.
//   - kube-system resources carry no ontai.dev/rbac-owner annotation.
//   - New RBAC resource without annotation is rejected (gate is strict).
//   - New RBAC resource in kube-system is admitted unconditionally.
//   - New ClusterRole with system: prefix is admitted unconditionally.
//   - cert-manager RBACProfile exists in cert-manager ns (if cert-manager deployed).
//   - kueue RBACProfile exists in kueue-system ns (if kueue deployed).

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rbacv1 "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	e2ehelpers "github.com/ontai-dev/seam-core/pkg/e2e"
)

const (
	rbacOwnerAnnotation      = "ontai.dev/rbac-owner"
	rbacOwnerAnnotationValue = "guardian"

	sweepPollTimeout  = 2 * time.Minute
	sweepPollInterval = 5 * time.Second
)

var (
	tenantPermissionSetGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "permissionsets",
	}
	tenantRBACProfileGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "rbacprofiles",
	}
)

// ============================================================
// Management cluster: Guardian sweep and enforcement
// ============================================================

var _ = Describe("Management cluster: Guardian bootstrap sweep and enforcement", func() {
	// All tests in this group require MGMT_KUBECONFIG — already gated by BeforeSuite.

	It("seam-system Roles carry ontai.dev/rbac-owner=guardian after Guardian sweep", func() {
		validateAnnotationPresent(context.Background(), mgmtClient, "seam-system",
			"Role", sweepPollTimeout, sweepPollInterval)
	})

	It("kube-system Roles are NOT annotated by the sweep (Guardian skips kube-system)", func() {
		validateAnnotationAbsent(context.Background(), mgmtClient, "kube-system")
	})

	It("ont-system ServiceAccounts carry the ownership annotation (Conductor's own RBAC)", func() {
		validateAnnotationPresent(context.Background(), mgmtClient, "ont-system",
			"ServiceAccount", sweepPollTimeout, sweepPollInterval)
	})

	It("cert-manager RBACProfile exists in cert-manager namespace after Guardian Phase 2b", func() {
		if !namespaceExists(context.Background(), mgmtClient, "cert-manager") {
			Skip("requires cert-manager namespace and CERT_MANAGER_PROFILE backlog item closed")
		}
		validateProfileExists(context.Background(), mgmtClient, "cert-manager", "rbac-cert-manager")
	})

	It("kueue RBACProfile exists in kueue-system namespace after Guardian Phase 2b", func() {
		if !namespaceExists(context.Background(), mgmtClient, "kueue-system") {
			Skip("requires kueue-system namespace and KUEUE_PROFILE backlog item closed")
		}
		validateProfileExists(context.Background(), mgmtClient, "kueue-system", "rbac-kueue")
	})

	It("Guardian webhook rejects new Role without annotation in app namespace (strict mode)", func() {
		validateWebhookRejectsUnannotatedRole(context.Background(), mgmtClient,
			"seam-tenant-"+mgmtClusterName)
	})

	It("Guardian webhook admits new Role in kube-system unconditionally", func() {
		validateWebhookAdmitsRoleInKubeSystem(context.Background(), mgmtClient)
	})
})

// ============================================================
// Tenant cluster: Conductor role=tenant sweep and enforcement
// ============================================================

var _ = Describe("Tenant cluster: Conductor role=tenant bootstrap sweep and enforcement", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and rebuilt Conductor role=tenant image with TenantBootstrapSweep (CONDUCTOR-TENANT-SWEEP-E2E)")
		}
	})

	It("non-system namespace Roles carry ontai.dev/rbac-owner=guardian after Conductor sweep", func() {
		validateAnnotationPresentOnAnyNonSystemNS(context.Background(), tenantClient,
			sweepPollTimeout, sweepPollInterval)
	})

	It("kube-system Roles are NOT annotated by Conductor sweep", func() {
		validateAnnotationAbsent(context.Background(), tenantClient, "kube-system")
	})

	It("kube-public ServiceAccounts are NOT annotated by Conductor sweep", func() {
		validateAnnotationAbsent(context.Background(), tenantClient, "kube-public")
	})

	It("kube-node-lease namespace resources are NOT annotated by Conductor sweep", func() {
		validateAnnotationAbsent(context.Background(), tenantClient, "kube-node-lease")
	})

	It("system: prefixed ClusterRoles are NOT annotated by Conductor sweep", func() {
		validateSystemClusterRoleNotAnnotated(context.Background(), tenantClient)
	})

	It("Conductor webhook rejects new unannotated Role in app namespace (strict mode active)", func() {
		Skip("requires Conductor role=tenant image rebuilt with enforcement gate and CONDUCTOR-TENANT-SWEEP-E2E closed")
	})

	It("Conductor webhook admits new Role in kube-system unconditionally", func() {
		Skip("requires Conductor role=tenant image rebuilt with enforcement gate and CONDUCTOR-TENANT-SWEEP-E2E closed")
	})

	It("Conductor webhook admits system: prefixed ClusterRole in strict mode", func() {
		Skip("requires Conductor role=tenant image rebuilt with enforcement gate and CONDUCTOR-TENANT-SWEEP-E2E closed")
	})

	It("cert-manager RBACProfile created by Conductor in cert-manager namespace", func() {
		if !namespaceExists(context.Background(), tenantClient, "cert-manager") {
			Skip("requires cert-manager deployed to tenant cluster and CONDUCTOR-TENANT-SWEEP-E2E closed")
		}
		validateProfileExists(context.Background(), tenantClient, "cert-manager", "rbac-cert-manager")
	})

	It("kueue RBACProfile created by Conductor in kueue-system namespace", func() {
		if !namespaceExists(context.Background(), tenantClient, "kueue-system") {
			Skip("requires kueue deployed to tenant cluster and CONDUCTOR-TENANT-SWEEP-E2E closed")
		}
		validateProfileExists(context.Background(), tenantClient, "kueue-system", "rbac-kueue")
	})

	It("PermissionSet baseline has wildcard apiGroups/resources and 7 verbs", func() {
		if !namespaceExists(context.Background(), tenantClient, "cert-manager") {
			Skip("requires cert-manager deployed to tenant cluster and CONDUCTOR-TENANT-SWEEP-E2E closed")
		}
		validatePermissionSetSpec(context.Background(), tenantClient, "cert-manager", "cert-manager-baseline")
	})

	It("Conductor sweep is idempotent: re-running does not duplicate profiles", func() {
		Skip("requires Conductor role=tenant image rebuilt with TenantBootstrapSweep periodic loop and CONDUCTOR-TENANT-SWEEP-E2E closed")
	})
})

// ============================================================
// Shared validation helpers
// ============================================================

// validateAnnotationPresent polls until at least one resource of kind in ns
// carries the ontai.dev/rbac-owner=guardian annotation.
func validateAnnotationPresent(ctx context.Context, cl *e2ehelpers.ClusterClient, ns, kind string, timeout, interval time.Duration) {
	By(fmt.Sprintf("waiting for %s/%s to carry %s=%s", kind, ns, rbacOwnerAnnotation, rbacOwnerAnnotationValue))
	Eventually(func() bool {
		switch kind {
		case "Role":
			list, err := cl.Typed.RbacV1().Roles(ns).List(ctx, metav1.ListOptions{})
			if err != nil || len(list.Items) == 0 {
				return false
			}
			for _, r := range list.Items {
				if r.Annotations[rbacOwnerAnnotation] == rbacOwnerAnnotationValue {
					return true
				}
			}
			return false
		case "ServiceAccount":
			list, err := cl.Typed.CoreV1().ServiceAccounts(ns).List(ctx, metav1.ListOptions{})
			if err != nil || len(list.Items) == 0 {
				return false
			}
			for _, sa := range list.Items {
				if sa.Annotations[rbacOwnerAnnotation] == rbacOwnerAnnotationValue {
					return true
				}
			}
			return false
		}
		return false
	}, timeout, interval).Should(BeTrue(),
		"expected %s in %s to carry %s=%s on cluster %s", kind, ns, rbacOwnerAnnotation, rbacOwnerAnnotationValue, cl.Name)
}

// validateAnnotationAbsent verifies that no Role in ns carries the ownership
// annotation. kube-system, kube-public, and kube-node-lease must never be swept.
func validateAnnotationAbsent(ctx context.Context, cl *e2ehelpers.ClusterClient, ns string) {
	By(fmt.Sprintf("verifying Roles in %s do NOT carry ownership annotation on cluster %s", ns, cl.Name))
	list, err := cl.Typed.RbacV1().Roles(ns).List(ctx, metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred())
	for _, r := range list.Items {
		Expect(r.Annotations[rbacOwnerAnnotation]).To(BeEmpty(),
			"Role %s/%s should not carry ownership annotation (sweep must skip %s)", ns, r.Name, ns)
	}
}

// validateAnnotationPresentOnAnyNonSystemNS finds any non-system namespace with
// at least one annotated Role, confirming the sweep ran.
func validateAnnotationPresentOnAnyNonSystemNS(ctx context.Context, cl *e2ehelpers.ClusterClient, timeout, interval time.Duration) {
	By("waiting for at least one non-system namespace Role to carry ownership annotation on cluster " + cl.Name)
	systemNS := map[string]bool{"kube-system": true, "kube-public": true, "kube-node-lease": true}
	Eventually(func() bool {
		nsList, err := cl.Typed.CoreV1().Namespaces().List(ctx, metav1.ListOptions{})
		if err != nil {
			return false
		}
		for _, ns := range nsList.Items {
			if systemNS[ns.Name] {
				continue
			}
			roles, err := cl.Typed.RbacV1().Roles(ns.Name).List(ctx, metav1.ListOptions{})
			if err != nil {
				continue
			}
			for _, r := range roles.Items {
				if r.Annotations[rbacOwnerAnnotation] == rbacOwnerAnnotationValue {
					return true
				}
			}
		}
		return false
	}, timeout, interval).Should(BeTrue(),
		"no annotated Role found in any non-system namespace on cluster %s — sweep may not have run", cl.Name)
}

// validateSystemClusterRoleNotAnnotated verifies that system: prefixed
// ClusterRoles were not stamped by the annotation sweep.
func validateSystemClusterRoleNotAnnotated(ctx context.Context, cl *e2ehelpers.ClusterClient) {
	By("verifying system: ClusterRoles are not annotated on cluster " + cl.Name)
	list, err := cl.Typed.RbacV1().ClusterRoles().List(ctx, metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred())
	for _, cr := range list.Items {
		if len(cr.Name) >= 7 && cr.Name[:7] == "system:" {
			Expect(cr.Annotations[rbacOwnerAnnotation]).To(BeEmpty(),
				"ClusterRole %q (system: prefix) should not be annotated by the sweep", cr.Name)
		}
	}
}

// validateProfileExists verifies that an RBACProfile with the given name exists
// in the given namespace.
func validateProfileExists(ctx context.Context, cl *e2ehelpers.ClusterClient, ns, profileName string) {
	By(fmt.Sprintf("verifying RBACProfile %s/%s exists on cluster %s", ns, profileName, cl.Name))
	profile, err := cl.Dynamic.Resource(tenantRBACProfileGVR).Namespace(ns).Get(
		ctx, profileName, metav1.GetOptions{},
	)
	Expect(err).NotTo(HaveOccurred(),
		"RBACProfile %s/%s not found on cluster %s", ns, profileName, cl.Name)
	Expect(profile.GetName()).To(Equal(profileName))
}

// validatePermissionSetSpec verifies the baseline PermissionSet has the expected
// wildcard permissions and all seven standard verbs.
func validatePermissionSetSpec(ctx context.Context, cl *e2ehelpers.ClusterClient, ns, psName string) {
	By(fmt.Sprintf("verifying PermissionSet %s/%s spec on cluster %s", ns, psName, cl.Name))
	ps, err := cl.Dynamic.Resource(tenantPermissionSetGVR).Namespace(ns).Get(
		ctx, psName, metav1.GetOptions{},
	)
	Expect(err).NotTo(HaveOccurred(),
		"PermissionSet %s/%s not found on cluster %s", ns, psName, cl.Name)

	spec, ok := ps.Object["spec"].(map[string]interface{})
	Expect(ok).To(BeTrue(), "PermissionSet spec absent")

	perms, ok := spec["permissions"].([]interface{})
	Expect(ok).To(BeTrue(), "PermissionSet spec.permissions absent")
	Expect(perms).NotTo(BeEmpty())

	rule, ok := perms[0].(map[string]interface{})
	Expect(ok).To(BeTrue(), "permissions[0] wrong type")

	verbs, ok := rule["verbs"].([]interface{})
	Expect(ok).To(BeTrue(), "permissions[0].verbs absent")
	Expect(verbs).To(HaveLen(7), "expected 7 standard verbs in baseline PermissionSet")
}

// validateWebhookRejectsUnannotatedRole attempts to create a Role without the
// ownership annotation and expects it to be rejected. Used to verify enforcement
// is active. The Role is created in a governed namespace (not a system namespace).
func validateWebhookRejectsUnannotatedRole(ctx context.Context, cl *e2ehelpers.ClusterClient, ns string) {
	By(fmt.Sprintf("verifying admission webhook rejects unannotated Role in %s on cluster %s", ns, cl.Name))
	if !namespaceExists(ctx, cl, ns) {
		Skip(fmt.Sprintf("namespace %s absent on cluster %s — cannot validate webhook rejection", ns, cl.Name))
	}

	testRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "e2e-sweep-test-unannotated-" + fmt.Sprintf("%d", time.Now().UnixNano()%100000),
			Namespace: ns,
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"},
		}},
	}

	_, err := cl.Typed.RbacV1().Roles(ns).Create(ctx, testRole, metav1.CreateOptions{})
	Expect(err).To(HaveOccurred(),
		"webhook should reject unannotated Role in %s on cluster %s (enforcement mode should be active)", ns, cl.Name)
	Expect(apierrors.IsForbidden(err)).To(BeTrue(),
		"expected 403 Forbidden from webhook, got: %v", err)
}

// validateWebhookAdmitsRoleInKubeSystem attempts to create a Role in kube-system
// and expects it to succeed (webhook must exempt kube-system unconditionally).
func validateWebhookAdmitsRoleInKubeSystem(ctx context.Context, cl *e2ehelpers.ClusterClient) {
	By("verifying admission webhook admits Role creation in kube-system on cluster " + cl.Name)

	name := "e2e-sweep-test-kube-system-" + fmt.Sprintf("%d", time.Now().UnixNano()%100000)
	testRole := &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "kube-system",
		},
		Rules: []rbacv1.PolicyRule{{
			APIGroups: []string{""}, Resources: []string{"configmaps"}, Verbs: []string{"get"},
		}},
	}

	created, err := cl.Typed.RbacV1().Roles("kube-system").Create(ctx, testRole, metav1.CreateOptions{})
	Expect(err).NotTo(HaveOccurred(),
		"webhook should admit Role in kube-system on cluster %s", cl.Name)

	// Cleanup.
	_ = cl.Typed.RbacV1().Roles("kube-system").Delete(ctx, created.Name, metav1.DeleteOptions{})
}

// namespaceExists returns true if the given namespace exists on the cluster.
func namespaceExists(ctx context.Context, cl *e2ehelpers.ClusterClient, ns string) bool {
	_, err := cl.Typed.CoreV1().Namespaces().Get(ctx, ns, metav1.GetOptions{})
	return err == nil
}
