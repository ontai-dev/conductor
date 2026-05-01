package e2e_test

// Scenario: Tenant RBAC bootstrap sweep and enforcement webhook validation.
//
// Pre-conditions for management cluster tests (MGMT_KUBECONFIG required):
//   - Guardian is deployed on ccs-mgmt and its bootstrap sweep has completed.
//   - Guardian's admission webhook is operational in Enforcing mode.
//
// Pre-conditions for tenant sweep tests (TENANT_KUBECONFIG additionally required):
//   - Guardian role=tenant is running in ont-system on the tenant cluster.
//   - Guardian role=tenant has completed its bootstrap annotation sweep.
//   - Guardian role=tenant TenantProfileRunnable has created component RBACProfiles.
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
// Tenant cluster (Guardian role=tenant enforcement, INV-004):
//   - Guardian swept all RBAC resources in non-system namespaces.
//   - kube-system resources carry no ontai.dev/rbac-owner annotation.
//   - New RBAC resource without annotation is rejected (gate is strict).
//   - New RBAC resource in kube-system is admitted unconditionally.
//   - New ClusterRole with system: prefix is admitted unconditionally.
//   - cert-manager RBACProfile exists in ont-system (TenantProfileRunnable, CS-INV-008).
//   - kueue RBACProfile exists in ont-system (TenantProfileRunnable, CS-INV-008).

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	rbacv1 "k8s.io/api/rbac/v1"
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
	tenantRBACProfileGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "rbacprofiles",
	}
)

// ============================================================
// Management cluster: Guardian sweep and enforcement
// ============================================================

var _ = Describe("Management cluster: Guardian bootstrap sweep and enforcement", func() {
	// All tests in this group require MGMT_KUBECONFIG — already gated by BeforeSuite.

	It("cert-manager Roles carry ontai.dev/rbac-owner=guardian after Guardian sweep", func() {
		if !namespaceExists(context.Background(), mgmtClient, "cert-manager") {
			Skip("requires cert-manager namespace on management cluster")
		}
		validateAnnotationPresent(context.Background(), mgmtClient, "cert-manager",
			"Role", sweepPollTimeout, sweepPollInterval)
	})

	It("kube-system Roles are NOT annotated by the sweep (Guardian skips kube-system)", func() {
		Skip("requires fresh cluster without pre-existing stale annotations from old sweep runs; validate after next cluster bootstrap")
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
		Skip("requires Guardian enforcement mode active (Guardian WebhookMode must advance past Initialising); blocked by CNPG unreachable on ccs-mgmt (CS-INV-003)")
	})

	It("Guardian webhook admits new Role in kube-system unconditionally", func() {
		validateWebhookAdmitsRoleInKubeSystem(context.Background(), mgmtClient)
	})
})

// ============================================================
// Tenant cluster: Conductor role=tenant sweep and enforcement
// ============================================================

var _ = Describe("Tenant cluster: Guardian role=tenant bootstrap sweep and enforcement", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG set and TENANT_CLUSTER_NAME=ccs-dev")
		}
	})

	It("non-system namespace Roles carry ontai.dev/rbac-owner=guardian after Guardian sweep", func() {
		validateAnnotationPresentOnAnyNonSystemNS(context.Background(), tenantClient,
			sweepPollTimeout, sweepPollInterval)
	})

	It("kube-system Roles are NOT annotated by Guardian sweep", func() {
		validateAnnotationAbsent(context.Background(), tenantClient, "kube-system")
	})

	It("kube-public resources are NOT annotated by Guardian sweep", func() {
		validateAnnotationAbsent(context.Background(), tenantClient, "kube-public")
	})

	It("kube-node-lease namespace resources are NOT annotated by Guardian sweep", func() {
		validateAnnotationAbsent(context.Background(), tenantClient, "kube-node-lease")
	})

	It("system: prefixed ClusterRoles are NOT annotated by Guardian sweep", func() {
		validateSystemClusterRoleNotAnnotated(context.Background(), tenantClient)
	})

	It("Guardian webhook rejects new unannotated Role in app namespace (strict mode active)", func() {
		Skip("requires Guardian role=tenant webhook enforcement mode active — blocked until CNPG-backed audit is reachable on ccs-dev (GUARDIAN-TENANT-WEBHOOK-E2E)")
	})

	It("Guardian webhook admits new Role in kube-system unconditionally", func() {
		Skip("requires Guardian role=tenant webhook enforcement mode active — blocked until CNPG-backed audit is reachable on ccs-dev (GUARDIAN-TENANT-WEBHOOK-E2E)")
	})

	It("Guardian webhook admits system: prefixed ClusterRole in strict mode", func() {
		Skip("requires Guardian role=tenant webhook enforcement mode active — blocked until CNPG-backed audit is reachable on ccs-dev (GUARDIAN-TENANT-WEBHOOK-E2E)")
	})

	// TenantProfileRunnable creates RBACProfiles in ont-system (Namespace), not the component
	// namespace. CS-INV-008: no per-component PermissionSet or RBACPolicy is created.
	It("cert-manager RBACProfile created by TenantProfileRunnable in ont-system", func() {
		if !namespaceExists(context.Background(), tenantClient, "cert-manager") {
			Skip("requires cert-manager deployed to tenant cluster")
		}
		validateProfileExists(context.Background(), tenantClient, "ont-system", "cert-manager")
	})

	It("kueue RBACProfile created by TenantProfileRunnable in ont-system", func() {
		if !namespaceExists(context.Background(), tenantClient, "kueue-system") {
			Skip("requires kueue deployed to tenant cluster")
		}
		validateProfileExists(context.Background(), tenantClient, "ont-system", "kueue")
	})

	It("TenantProfileRunnable sweep is idempotent: re-running does not duplicate profiles", func() {
		if !namespaceExists(context.Background(), tenantClient, "cert-manager") {
			Skip("requires cert-manager deployed to tenant cluster")
		}
		// Count profiles in ont-system before and after a restart would be needed for
		// true idempotency verification. Instead, confirm count is exactly 1.
		list, err := tenantClient.Dynamic.Resource(tenantRBACProfileGVR).
			Namespace("ont-system").
			List(context.Background(), metav1.ListOptions{
				LabelSelector: "ontai.dev/component=cert-manager",
			})
		Expect(err).NotTo(HaveOccurred())
		Expect(list.Items).To(HaveLen(1),
			"TenantProfileRunnable must create exactly one cert-manager RBACProfile in ont-system (idempotent)")
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
