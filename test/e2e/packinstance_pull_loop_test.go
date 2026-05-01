package e2e_test

// packinstance_pull_loop_test.go -- live cluster verification of conductor
// role=agent PackInstance pull loop on ccs-dev.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG set (management cluster).
//   - TENANT_KUBECONFIG set (ccs-dev, TENANT_CLUSTER_NAME=ccs-dev).
//   - Conductor agent running in ont-system on ccs-dev with MGMT_KUBECONFIG_PATH set.
//   - cert-manager deployed to ccs-dev via ClusterPack; signing loop on ccs-mgmt has
//     written seam-pack-signed-ccs-dev-cert-manager into seam-tenant-ccs-dev.
//
// What this test verifies (conductor-schema.md §6, INV-026):
//   - Tenant conductor pull loop discovers signed artifact Secrets on ccs-mgmt.
//   - Verified=True PackReceipt created in ont-system on ccs-dev.
//   - PackReceipt clusterPackRef is non-empty.
//   - Second read is idempotent (signature unchanged, no re-write).

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
	packReceiptPollTimeout  = 3 * time.Minute
	packReceiptPollInterval = 5 * time.Second
)

var (
	packReceiptGVR = schema.GroupVersionResource{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: "infrastructurepackreceipts",
	}
)

var _ = Describe("Conductor role=agent: PackInstance pull loop", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-TENANT-PACKRECEIPT-E2E)")
		}
	})

	It("PackReceipt cert-manager exists in ont-system on tenant cluster", func() {
		By(fmt.Sprintf("polling for InfrastructurePackReceipt cert-manager in ont-system on %s",
			tenantClient.Name))

		Eventually(func() bool {
			_, err := tenantClient.Dynamic.Resource(packReceiptGVR).
				Namespace("ont-system").
				Get(context.Background(), "cert-manager", metav1.GetOptions{})
			return err == nil
		}, packReceiptPollTimeout, packReceiptPollInterval).Should(BeTrue(),
			"PackInstance pull loop did not create PackReceipt cert-manager in ont-system within %s; "+
				"ensure conductor agent running with MGMT_KUBECONFIG_PATH and signing Secret exists on ccs-mgmt",
			packReceiptPollTimeout)
	})

	It("PackReceipt cert-manager has verified=true", func() {
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), "cert-manager", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		status, _ := receipt.Object["status"].(map[string]interface{})
		Expect(status).NotTo(BeNil(), "PackReceipt must have status subresource")

		verified, _ := status["verified"].(bool)
		Expect(verified).To(BeTrue(),
			"PackReceipt must have verified=true after conductor validates Ed25519 signature (INV-026)")
	})

	It("PackReceipt cert-manager has non-empty clusterPackRef", func() {
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), "cert-manager", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		Expect(spec).NotTo(BeNil())
		ref, _ := spec["clusterPackRef"].(string)
		Expect(ref).NotTo(BeEmpty(),
			"PackReceipt must carry clusterPackRef from the signed artifact")
	})

	It("PackReceipt verificationFailedReason is empty when verified=true", func() {
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), "cert-manager", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		status, _ := receipt.Object["status"].(map[string]interface{})
		reason, _ := status["verificationFailedReason"].(string)
		Expect(reason).To(BeEmpty(),
			"verificationFailedReason must be empty when signature verification passes")
	})

	It("PackReceipt is idempotent: second read after pull interval shows same verified=true", func() {
		receipt1, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), "cert-manager", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		status1, _ := receipt1.Object["status"].(map[string]interface{})
		sig1, _ := status1["signature"].(string)

		By("waiting one pull loop interval (65s) then re-reading")
		time.Sleep(65 * time.Second)

		receipt2, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), "cert-manager", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		status2, _ := receipt2.Object["status"].(map[string]interface{})
		sig2, _ := status2["signature"].(string)

		Expect(sig1).To(Equal(sig2),
			"PackReceipt signature must not change when artifact is unchanged (idempotency — INV-026)")

		verified2, _ := status2["verified"].(bool)
		Expect(verified2).To(BeTrue(), "verified must remain true on second read")
	})

	It("PackReceipt with corrupted signature would show verified=false", func() {
		Skip("requires injecting a corrupted signing Secret into seam-tenant-{clusterName} — CONDUCTOR-SNAPSHOT-INVALID-SIG-E2E")
	})
})
