package e2e_test

// snapshot_pull_loop_test.go -- live cluster verification of conductor role=agent
// SnapshotPullLoop on ccs-dev.
//
// The SnapshotPullLoop runs on the tenant cluster. It pulls PermissionSnapshot CRs
// from the management cluster, verifies their Ed25519 signatures, and populates
// the local in-memory SnapshotStore for authorization decisions. On signature failure
// it patches DegradedSecurityState on the local PermissionSnapshotReceipt.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG and TENANT_KUBECONFIG set; TENANT_CLUSTER_NAME=ccs-dev.
//   - Conductor agent running in ont-system on ccs-dev with MGMT_KUBECONFIG_PATH set.
//   - Guardian management running on ccs-mgmt; PermissionSnapshot snapshot-{cluster}
//     exists in seam-system and is signed by conductor signing loop.
//
// What this test verifies (conductor-schema.md §10, INV-026, guardian-schema.md §7):
//   - Management cluster PermissionSnapshot for this tenant is signed (precondition).
//   - Local PermissionSnapshotReceipt in ont-system has no DegradedSecurityState.
//   - Conductor agent pod is running in ont-system on the tenant cluster.

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
	snapshotLoopPollTimeout  = 2 * time.Minute
	snapshotLoopPollInterval = 5 * time.Second

	// mgmtSignatureAnnotation is the annotation key written by the management conductor
	// signing loop on PermissionSnapshot CRs (INV-026).
	mgmtSignatureAnnotation = "infrastructure.ontai.dev/management-signature"
)

var _ = Describe("Conductor role=agent: SnapshotPullLoop", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-SNAPSHOT-PULL-E2E)")
		}
	})

	It("management cluster PermissionSnapshot is signed (conductor signing loop precondition)", func() {
		snapshotName := fmt.Sprintf("snapshot-%s", tenantClusterName)
		snap, err := mgmtClient.Dynamic.Resource(permissionSnapshotGVRc).
			Namespace("").
			List(context.Background(), metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())

		var found bool
		for _, item := range snap.Items {
			if item.GetName() != snapshotName {
				continue
			}
			found = true
			sig := item.GetAnnotations()[mgmtSignatureAnnotation]
			Expect(sig).NotTo(BeEmpty(),
				"PermissionSnapshot %s must carry %s before SnapshotPullLoop can verify it",
				snapshotName, mgmtSignatureAnnotation)
		}
		Expect(found).To(BeTrue(),
			"PermissionSnapshot %s not found on management cluster — signing loop must run first",
			snapshotName)
	})

	It("local PermissionSnapshotReceipt in ont-system has no DegradedSecurityState condition", func() {
		receiptName := "receipt-" + tenantClusterName
		By(fmt.Sprintf("verifying %s/%s has no DegradedSecurityState on %s",
			"ont-system", receiptName, tenantClient.Name))

		// Poll briefly to allow the pull loop to process the snapshot.
		Eventually(func() error {
			receipt, err := tenantClient.Dynamic.Resource(permissionSnapshotReceiptGVRc).
				Namespace("ont-system").
				Get(context.Background(), receiptName, metav1.GetOptions{})
			if err != nil {
				return err
			}
			conditions, _ := receipt.Object["status"].(map[string]interface{})
			if conditions == nil {
				return nil // no status at all means no degraded condition
			}
			condList, _ := conditions["conditions"].([]interface{})
			for _, raw := range condList {
				cond, _ := raw.(map[string]interface{})
				if cond == nil {
					continue
				}
				if cond["type"] == "DegradedSecurityState" && cond["status"] == "True" {
					return fmt.Errorf("DegradedSecurityState=True: %v", cond["message"])
				}
			}
			return nil
		}, snapshotLoopPollTimeout, snapshotLoopPollInterval).Should(Succeed(),
			"SnapshotPullLoop must not set DegradedSecurityState; "+
				"a True value means Ed25519 verification failed (INV-026)")
	})

	It("conductor agent pod is running in ont-system on tenant cluster", func() {
		By(fmt.Sprintf("verifying conductor pod exists and is Running on %s", tenantClient.Name))
		Eventually(func() bool {
			pods, err := tenantClient.Typed.CoreV1().Pods("ont-system").List(
				context.Background(), metav1.ListOptions{
					LabelSelector: "app.kubernetes.io/name=conductor",
				})
			if err != nil || len(pods.Items) == 0 {
				return false
			}
			for _, pod := range pods.Items {
				if string(pod.Status.Phase) == "Running" {
					return true
				}
			}
			return false
		}, snapshotLoopPollTimeout, snapshotLoopPollInterval).Should(BeTrue(),
			"conductor agent pod must be Running in ont-system on tenant cluster")
	})

	It("invalid snapshot signature results in DegradedSecurityState=True", func() {
		Skip("requires injecting a forged PermissionSnapshot signature into the management cluster — CONDUCTOR-SNAPSHOT-INVALID-SIG-E2E")
	})
})

// permissionSnapshotGVRc and permissionSnapshotReceiptGVRc are conductor-package aliases
// to avoid collision with guardian e2e package vars (different test binary).
var (
	permissionSnapshotGVRc = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "permissionsnapshots",
	}
	permissionSnapshotReceiptGVRc = schema.GroupVersionResource{
		Group: "security.ontai.dev", Version: "v1alpha1", Resource: "permissionsnapshotreceipts",
	}
)
