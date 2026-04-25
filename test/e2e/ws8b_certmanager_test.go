package e2e_test

// Step 5: WS8b cert-manager Helm packbuild with three-bucket split.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG set (suite-level gate)
//   - Compiler has been run with cert-manager helm packbuild input
//   - cert-manager ClusterPack CR exists in seam-system
//   - Conductor execute job has completed for cert-manager pack-deploy
//   - PackReceipt exists in seam-tenant-{mgmtClusterName}
//
// Reusable: each validation function accepts a ClusterClient and cluster name.
// Verifies the end state on the cluster rather than invoking the compiler directly,
// so the same specs run against management and later tenant cluster without change.
//
// Covers management cluster validation gate Step 5 (GAP_TO_FILL.md).

import (
	"context"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	e2ehelpers "github.com/ontai-dev/seam-core/pkg/e2e"
)

var (
	ws8bClusterPackGVR = schema.GroupVersionResource{
		Group:    "infrastructure.ontai.dev",
		Version:  "v1alpha1",
		Resource: "infrastructureclusterpacks",
	}
	ws8bPackReceiptGVR = schema.GroupVersionResource{
		Group:    "infrastructure.ontai.dev",
		Version:  "v1alpha1",
		Resource: "infrastructurepackreceipts",
	}
)

const (
	ws8bPackName  = "cert-manager"
	ws8bPollTimeout  = 5 * time.Minute
	ws8bPollInterval = 5 * time.Second
)

var _ = Describe("Step 5: WS8b cert-manager Helm packbuild three-bucket split", func() {
	It("ClusterPack cert-manager has chartVersion, chartURL, chartName, helmVersion populated", func() {
		validateWS8bClusterPackHelmFields(context.Background(), mgmtClient)
	})

	It("ClusterPack cert-manager has rbacDigest and workloadDigest from three-bucket OCI split", func() {
		validateWS8bClusterPackDigests(context.Background(), mgmtClient)
	})

	It("PackReceipt for cert-manager carries rbacDigest and workloadDigest through from ClusterPack", func() {
		validateWS8bPackReceiptDigestCarryThrough(context.Background(), mgmtClient, mgmtClusterName, ws8bPollTimeout, ws8bPollInterval)
	})

	It("PackReceipt for cert-manager carries helm metadata fields through from ClusterPack", func() {
		validateWS8bPackReceiptHelmFields(context.Background(), mgmtClient, mgmtClusterName, ws8bPollTimeout, ws8bPollInterval)
	})
})

// validateWS8bClusterPackHelmFields verifies that ClusterPack cert-manager in
// seam-system has all four helm metadata fields populated.
func validateWS8bClusterPackHelmFields(ctx context.Context, cl *e2ehelpers.ClusterClient) {
	By("getting ClusterPack cert-manager from seam-system on cluster " + cl.Name)
	obj, err := cl.Dynamic.Resource(ws8bClusterPackGVR).Namespace("seam-system").
		Get(ctx, ws8bPackName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred(), "ClusterPack cert-manager not found in seam-system on %s", cl.Name)

	spec, ok := obj.Object["spec"].(map[string]interface{})
	Expect(ok).To(BeTrue(), "ClusterPack spec absent")

	for _, field := range []string{"chartVersion", "chartURL", "chartName", "helmVersion"} {
		val, present := spec[field]
		Expect(present).To(BeTrue(),
			"ClusterPack cert-manager spec.%s absent on %s -- Phase 2 (T-09) regression", field, cl.Name)
		Expect(fmt.Sprint(val)).NotTo(BeEmpty(),
			"ClusterPack cert-manager spec.%s is empty on %s", field, cl.Name)
	}
}

// validateWS8bClusterPackDigests verifies the three-bucket OCI split produced
// rbacDigest and workloadDigest in the ClusterPack spec.
func validateWS8bClusterPackDigests(ctx context.Context, cl *e2ehelpers.ClusterClient) {
	By("verifying rbacDigest and workloadDigest in ClusterPack cert-manager on cluster " + cl.Name)
	obj, err := cl.Dynamic.Resource(ws8bClusterPackGVR).Namespace("seam-system").
		Get(ctx, ws8bPackName, metav1.GetOptions{})
	Expect(err).NotTo(HaveOccurred())

	spec, ok := obj.Object["spec"].(map[string]interface{})
	Expect(ok).To(BeTrue(), "ClusterPack spec absent")

	for _, field := range []string{"rbacDigest", "workloadDigest"} {
		val, present := spec[field]
		Expect(present).To(BeTrue(),
			"ClusterPack cert-manager spec.%s absent on %s -- three-bucket split regression", field, cl.Name)
		Expect(fmt.Sprint(val)).NotTo(BeEmpty(),
			"ClusterPack cert-manager spec.%s is empty on %s", field, cl.Name)
	}
}

// validateWS8bPackReceiptDigestCarryThrough waits for the PackReceipt for
// cert-manager in seam-tenant-{clusterName} and verifies rbacDigest and
// workloadDigest are present (T-10 carry-through).
func validateWS8bPackReceiptDigestCarryThrough(
	ctx context.Context,
	cl *e2ehelpers.ClusterClient,
	clusterName string,
	timeout, interval time.Duration,
) {
	tenantNS := "seam-tenant-" + clusterName
	By(fmt.Sprintf("waiting for cert-manager PackReceipt in %s with digests on cluster %s", tenantNS, cl.Name))

	var receiptSpec map[string]interface{}
	Eventually(func() bool {
		list, err := cl.Dynamic.Resource(ws8bPackReceiptGVR).Namespace(tenantNS).
			List(ctx, metav1.ListOptions{
				LabelSelector: "infrastructure.ontai.dev/pack=" + ws8bPackName,
			})
		if err != nil || len(list.Items) == 0 {
			return false
		}
		receiptSpec, _ = list.Items[0].Object["spec"].(map[string]interface{})
		return receiptSpec != nil
	}, timeout, interval).Should(BeTrue(),
		"PackReceipt for cert-manager not found in %s on %s within %s", tenantNS, cl.Name, timeout)

	for _, field := range []string{"rbacDigest", "workloadDigest"} {
		val, present := receiptSpec[field]
		Expect(present).To(BeTrue(),
			"PackReceipt cert-manager spec.%s absent in %s on %s -- T-10 carry-through regression", field, tenantNS, cl.Name)
		Expect(fmt.Sprint(val)).NotTo(BeEmpty(),
			"PackReceipt cert-manager spec.%s is empty in %s on %s", field, tenantNS, cl.Name)
	}
}

// validateWS8bPackReceiptHelmFields waits for the PackReceipt and verifies the
// helm metadata fields are carried through from ClusterPack (T-10).
func validateWS8bPackReceiptHelmFields(
	ctx context.Context,
	cl *e2ehelpers.ClusterClient,
	clusterName string,
	timeout, interval time.Duration,
) {
	tenantNS := "seam-tenant-" + clusterName
	By(fmt.Sprintf("verifying helm metadata in cert-manager PackReceipt in %s on cluster %s", tenantNS, cl.Name))

	var receiptSpec map[string]interface{}
	Eventually(func() bool {
		list, err := cl.Dynamic.Resource(ws8bPackReceiptGVR).Namespace(tenantNS).
			List(ctx, metav1.ListOptions{
				LabelSelector: "infrastructure.ontai.dev/pack=" + ws8bPackName,
			})
		if err != nil || len(list.Items) == 0 {
			return false
		}
		receiptSpec, _ = list.Items[0].Object["spec"].(map[string]interface{})
		return receiptSpec != nil
	}, timeout, interval).Should(BeTrue(),
		"PackReceipt for cert-manager not found in %s on %s within %s", tenantNS, cl.Name, timeout)

	for _, field := range []string{"chartVersion", "chartURL", "chartName", "helmVersion"} {
		val, present := receiptSpec[field]
		Expect(present).To(BeTrue(),
			"PackReceipt cert-manager spec.%s absent in %s on %s -- helm metadata carry-through regression", field, tenantNS, cl.Name)
		Expect(fmt.Sprint(val)).NotTo(BeEmpty(),
			"PackReceipt cert-manager spec.%s is empty in %s on %s", field, tenantNS, cl.Name)
	}
}
