package e2e_test

// rbacprofile_rbacpolicy_pull_loop_test.go -- live cluster verification of
// conductor role=tenant pull loops for RBACProfile and RBACPolicy on ccs-dev.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG and TENANT_KUBECONFIG set; TENANT_CLUSTER_NAME=ccs-dev.
//   - Conductor agent running in ont-system on ccs-dev with MGMT_KUBECONFIG_PATH set.
//   - Guardian running on ccs-mgmt with conductor-tenant RBACProfile in
//     seam-tenant-{tenantClusterName} on ccs-mgmt.
//   - Guardian running on ccs-dev with cluster-policy RBACPolicy in
//     seam-tenant-{tenantClusterName} on ccs-mgmt.
//
// What this test verifies (conductor-schema.md §11, Decision C, T-17):
//   - RBACProfilePullLoop SSA-patches conductor-tenant RBACProfile into ont-system
//     on the tenant cluster.
//   - RBACPolicyPullLoop SSA-patches cluster-policy RBACPolicy into ont-system
//     on the tenant cluster.
//   - Both resources exist with non-empty spec after conductor pull loops converge.
//   - Pull loops are idempotent: re-polling shows same resourceVersion.

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	pullLoopPollTimeout  = 4 * time.Minute
	pullLoopPollInterval = 5 * time.Second

	// conductorTenantProfileName is the RBACProfile that conductor role=management
	// distributes to tenant clusters via RBACProfilePullLoop.
	conductorTenantProfileName = "conductor-tenant"

	// clusterPolicyName is the RBACPolicy that conductor role=management
	// distributes to tenant clusters via RBACPolicyPullLoop.
	clusterPolicyName = "cluster-policy"

	// tenantOntSystem is the namespace where conductor writes pull-loop results.
	tenantOntSystem = "ont-system"
)

var (
	rbacProfileGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "rbacprofiles",
	}
	rbacPolicyGVR = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "rbacpolicies",
	}
)

// ============================================================
// RBACProfile pull loop
// ============================================================

var _ = Describe("Conductor role=tenant: RBACProfilePullLoop", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-BL-TENANT-ROLE-RBACPROFILE-DISTRIBUTION)")
		}
	})

	It("conductor-tenant RBACProfile exists in seam-tenant-{tenantClusterName} on management cluster (precondition)", func() {
		mgmtTenantNS := "seam-tenant-" + tenantClusterName
		By(fmt.Sprintf("verifying conductor-tenant RBACProfile in %s on %s (pull loop source)",
			mgmtTenantNS, mgmtClient.Name))

		Eventually(func() bool {
			_, err := mgmtClient.Dynamic.Resource(rbacProfileGVR).
				Namespace(mgmtTenantNS).
				Get(context.Background(), conductorTenantProfileName, metav1.GetOptions{})
			return err == nil
		}, pullLoopPollTimeout, pullLoopPollInterval).Should(BeTrue(),
			"conductor-tenant RBACProfile must exist in %s on %s before pull loop can deliver it",
			mgmtTenantNS, mgmtClient.Name)
	})

	It("conductor-tenant RBACProfile SSA-patched into ont-system on tenant cluster", func() {
		By(fmt.Sprintf("polling for RBACProfile %s/%s on %s",
			tenantOntSystem, conductorTenantProfileName, tenantClient.Name))

		Eventually(func() bool {
			_, err := tenantClient.Dynamic.Resource(rbacProfileGVR).
				Namespace(tenantOntSystem).
				Get(context.Background(), conductorTenantProfileName, metav1.GetOptions{})
			return err == nil
		}, pullLoopPollTimeout, pullLoopPollInterval).Should(BeTrue(),
			"RBACProfilePullLoop did not SSA-patch conductor-tenant RBACProfile into ont-system within %s; "+
				"ensure conductor agent has MGMT_KUBECONFIG_PATH and RBACProfile exists on management cluster",
			pullLoopPollTimeout)
	})

	It("conductor-tenant RBACProfile in ont-system carries non-empty spec", func() {
		profile, err := tenantClient.Dynamic.Resource(rbacProfileGVR).
			Namespace(tenantOntSystem).
			Get(context.Background(), conductorTenantProfileName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, ok := profile.Object["spec"].(map[string]interface{})
		Expect(ok).To(BeTrue(), "conductor-tenant RBACProfile must have spec subresource")
		Expect(spec).NotTo(BeEmpty(),
			"conductor-tenant RBACProfile spec must be non-empty after SSA patch from management cluster")
	})

	It("conductor-tenant RBACProfile pull loop is idempotent: resourceVersion stable", func() {
		profile1, err := tenantClient.Dynamic.Resource(rbacProfileGVR).
			Namespace(tenantOntSystem).
			Get(context.Background(), conductorTenantProfileName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		rv1 := profile1.GetResourceVersion()

		By("waiting one pull loop interval (65s) then re-reading")
		time.Sleep(65 * time.Second)

		profile2, err := tenantClient.Dynamic.Resource(rbacProfileGVR).
			Namespace(tenantOntSystem).
			Get(context.Background(), conductorTenantProfileName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		rv2 := profile2.GetResourceVersion()

		Expect(rv1).To(Equal(rv2),
			"RBACProfilePullLoop must not write when content is unchanged (idempotency via SSA)")
	})
})

// ============================================================
// RBACPolicy pull loop
// ============================================================

var _ = Describe("Conductor role=tenant: RBACPolicyPullLoop", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-RBACPOLICY-PULL-E2E)")
		}
	})

	It("cluster-policy RBACPolicy exists in seam-tenant-{tenantClusterName} on management cluster (precondition)", func() {
		mgmtTenantNS := "seam-tenant-" + tenantClusterName
		By(fmt.Sprintf("verifying cluster-policy RBACPolicy in %s on %s (pull loop source)",
			mgmtTenantNS, mgmtClient.Name))

		Eventually(func() bool {
			_, err := mgmtClient.Dynamic.Resource(rbacPolicyGVR).
				Namespace(mgmtTenantNS).
				Get(context.Background(), clusterPolicyName, metav1.GetOptions{})
			return err == nil
		}, pullLoopPollTimeout, pullLoopPollInterval).Should(BeTrue(),
			"cluster-policy RBACPolicy must exist in %s on %s before pull loop can deliver it",
			mgmtTenantNS, mgmtClient.Name)
	})

	It("cluster-policy RBACPolicy SSA-patched into ont-system on tenant cluster", func() {
		By(fmt.Sprintf("polling for RBACPolicy %s/%s on %s",
			tenantOntSystem, clusterPolicyName, tenantClient.Name))

		Eventually(func() bool {
			_, err := tenantClient.Dynamic.Resource(rbacPolicyGVR).
				Namespace(tenantOntSystem).
				Get(context.Background(), clusterPolicyName, metav1.GetOptions{})
			return err == nil
		}, pullLoopPollTimeout, pullLoopPollInterval).Should(BeTrue(),
			"RBACPolicyPullLoop did not SSA-patch cluster-policy RBACPolicy into ont-system within %s; "+
				"ensure conductor agent has MGMT_KUBECONFIG_PATH and RBACPolicy exists on management cluster",
			pullLoopPollTimeout)
	})

	It("cluster-policy RBACPolicy in ont-system carries non-empty spec", func() {
		policy, err := tenantClient.Dynamic.Resource(rbacPolicyGVR).
			Namespace(tenantOntSystem).
			Get(context.Background(), clusterPolicyName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, ok := policy.Object["spec"].(map[string]interface{})
		Expect(ok).To(BeTrue(), "cluster-policy RBACPolicy must have spec subresource")
		Expect(spec).NotTo(BeEmpty(),
			"cluster-policy RBACPolicy spec must be non-empty after SSA patch from management cluster")
	})

	It("cluster-policy RBACPolicy pull loop is idempotent: resourceVersion stable", func() {
		policy1, err := tenantClient.Dynamic.Resource(rbacPolicyGVR).
			Namespace(tenantOntSystem).
			Get(context.Background(), clusterPolicyName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		rv1 := policy1.GetResourceVersion()

		By("waiting one pull loop interval (65s) then re-reading")
		time.Sleep(65 * time.Second)

		policy2, err := tenantClient.Dynamic.Resource(rbacPolicyGVR).
			Namespace(tenantOntSystem).
			Get(context.Background(), clusterPolicyName, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		rv2 := policy2.GetResourceVersion()

		Expect(rv1).To(Equal(rv2),
			"RBACPolicyPullLoop must not write when content is unchanged (idempotency via SSA)")
	})
})
