package e2e_test

// drift_injection_test.go -- live cluster verification of the full drift
// detection and corrective redeployment cycle on ccs-dev.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG and TENANT_KUBECONFIG set; TENANT_CLUSTER_NAME=ccs-dev.
//   - cert-manager deployed to ccs-dev via ClusterPack.
//   - PackReceipt cert-manager exists in ont-system on ccs-dev with non-empty
//     spec.deployedResources.
//   - Conductor agent role=tenant running in ont-system on ccs-dev.
//   - Conductor agent role=management running in ont-system on ccs-mgmt.
//   - PackReceiptDriftLoop enabled; DriftSignalHandler enabled on ccs-mgmt.
//
// What this test verifies (conductor-schema.md §14, Decision H):
//   - PackReceiptDriftLoop.checkDrift() detects a deleted resource.
//   - DriftSignal CR is written to seam-tenant-{tenantClusterName} on ccs-mgmt.
//   - DriftSignalHandler on ccs-mgmt advances DriftSignal from pending to queued.
//   - PackExecution is deleted and recreated by wrapper.
//   - After redeployment, DriftSignal reaches state=confirmed.
//   - Deleted resource is restored on the tenant cluster.
//
// The resource deletion is performed by the test on the tenant cluster using the
// first entry from PackReceipt spec.deployedResources. It is non-destructive in
// aggregate: the redeployment restores the resource within the test timeout.

import (
	"context"
	"fmt"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	driftPollTimeout  = 8 * time.Minute
	driftPollInterval = 5 * time.Second

	// driftSignalPollTimeout covers DriftSignal emission + handler reaction +
	// wrapper pack-deploy Job completion + resource restore.
	driftSignalPollTimeout = 12 * time.Minute
)

var driftSignalGVR = schema.GroupVersionResource{
	Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: "driftsignals",
}

var _ = Describe("Conductor drift detection: full injection cycle", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-DRIFT-INJECTION-E2E)")
		}
	})

	// driftReceiptName is the PackReceipt written by wrapper writePackReceipt() at pack-deploy time.
	// It carries deployedResources used by PackReceiptDriftLoop.checkDrift() to detect missing resources.
	// Named after the ClusterPack: {clusterPackName} = cert-manager-{clusterName}.
	driftReceiptName := func() string { return "cert-manager-" + tenantClusterName }

	It("PackReceipt deployedResources is non-empty (drift injection precondition)", func() {
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), driftReceiptName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		Expect(spec).NotTo(BeNil())

		drs, _ := spec["deployedResources"].([]interface{})
		Expect(drs).NotTo(BeEmpty(),
			"deployedResources must be non-empty before drift injection; "+
				"ensure PackInstancePullLoop has converged on ccs-dev")
	})

	It("DriftSignal appears on management cluster after resource deletion on tenant", func() {
		By("reading PackReceipt to find a deletable resource")
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), driftReceiptName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		drs, _ := spec["deployedResources"].([]interface{})
		Expect(drs).NotTo(BeEmpty())

		targetEntry := pickDriftTarget(drs)
		Expect(targetEntry).NotTo(BeNil(),
			"no deletable workload resource found in deployedResources; need a Namespace-scoped resource to inject drift")

		apiVersion, _ := targetEntry["apiVersion"].(string)
		kind, _ := targetEntry["kind"].(string)
		name, _ := targetEntry["name"].(string)
		ns, _ := targetEntry["namespace"].(string)
		gvr := apiVersionKindToGVR(apiVersion, kind)

		By(fmt.Sprintf("deleting %s/%s %s/%s from tenant cluster to inject drift", apiVersion, kind, ns, name))
		var deleteErr error
		if ns != "" {
			deleteErr = tenantClient.Dynamic.Resource(gvr).
				Namespace(ns).Delete(context.Background(), name, metav1.DeleteOptions{})
		} else {
			deleteErr = tenantClient.Dynamic.Resource(gvr).
				Delete(context.Background(), name, metav1.DeleteOptions{})
		}
		Expect(deleteErr).NotTo(HaveOccurred(),
			"failed to delete %s/%s %s/%s to inject drift", apiVersion, kind, ns, name)

		By("waiting for DriftSignal to appear in seam-tenant-{tenantClusterName} on management cluster")
		mgmtTenantNS := "seam-tenant-" + tenantClusterName
		// DriftSignal naming convention: drift-{receiptName}. DriftSignals carry no
		// cluster label -- poll by the deterministic name directly.
		signalName := "drift-" + driftReceiptName()
		Eventually(func() bool {
			signal, err := mgmtClient.Dynamic.Resource(driftSignalGVR).
				Namespace(mgmtTenantNS).
				Get(context.Background(), signalName, metav1.GetOptions{})
			if err != nil {
				return false
			}
			spec, _ := signal.Object["spec"].(map[string]interface{})
			if spec == nil {
				return false
			}
			// Accept any state: the signal may advance from pending→queued→confirmed
			// before this poll fires. driftReason carries the affected resource name.
			driftReason, _ := spec["driftReason"].(string)
			if strings.Contains(driftReason, name) {
				return true
			}
			state, _ := spec["state"].(string)
			return state == "pending" || state == "queued" || state == "confirmed"
		}, driftPollTimeout, driftPollInterval).Should(BeTrue(),
			"PackReceiptDriftLoop did not emit DriftSignal %s into %s on %s within %s; "+
				"ensure conductor agent role=tenant is polling deployedResources",
			signalName, mgmtTenantNS, mgmtClient.Name, driftPollTimeout)
	})

	It("DriftSignal advances to state=queued after DriftSignalHandler runs", func() {
		mgmtTenantNS := "seam-tenant-" + tenantClusterName

		By("waiting for at least one DriftSignal in pending or queued state in " + mgmtTenantNS)
		Eventually(func() bool {
			list, err := mgmtClient.Dynamic.Resource(driftSignalGVR).
				Namespace(mgmtTenantNS).
				List(context.Background(), metav1.ListOptions{})
			if err != nil || len(list.Items) == 0 {
				return false
			}
			for _, item := range list.Items {
				spec, _ := item.Object["spec"].(map[string]interface{})
				if spec == nil {
					continue
				}
				state, _ := spec["state"].(string)
				if state == "queued" || state == "confirmed" {
					return true
				}
			}
			return false
		}, driftPollTimeout, driftPollInterval).Should(BeTrue(),
			"DriftSignalHandler did not advance DriftSignal to queued state within %s; "+
				"ensure conductor role=management DriftSignalHandler is running on %s",
			driftPollTimeout, mgmtClient.Name)
	})

	It("deleted resource is restored on tenant cluster after drift remediation", func() {
		By("reading PackReceipt to find the previously deleted resource")
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), driftReceiptName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		drs, _ := spec["deployedResources"].([]interface{})
		Expect(drs).NotTo(BeEmpty())

		targetEntry := pickDriftTarget(drs)
		Expect(targetEntry).NotTo(BeNil())

		apiVersion, _ := targetEntry["apiVersion"].(string)
		kind, _ := targetEntry["kind"].(string)
		name, _ := targetEntry["name"].(string)
		ns, _ := targetEntry["namespace"].(string)
		gvr := apiVersionKindToGVR(apiVersion, kind)

		By(fmt.Sprintf("waiting for %s/%s %s/%s to be restored via pack redeploy", apiVersion, kind, ns, name))
		Eventually(func() bool {
			var err error
			if ns != "" {
				_, err = tenantClient.Dynamic.Resource(gvr).
					Namespace(ns).Get(context.Background(), name, metav1.GetOptions{})
			} else {
				_, err = tenantClient.Dynamic.Resource(gvr).
					Get(context.Background(), name, metav1.GetOptions{})
			}
			return err == nil
		}, driftSignalPollTimeout, driftPollInterval).Should(BeTrue(),
			"%s/%s %s/%s must be restored by the corrective pack-deploy job within %s",
			apiVersion, kind, ns, name, driftSignalPollTimeout)
	})
})

var _ = Describe("Conductor drift detection: DriftSignal state machine", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-DRIFT-STATEMACHINE-E2E)")
		}
	})

	It("DriftSignal reaches state=confirmed after pack redeployment completes", func() {
		mgmtTenantNS := "seam-tenant-" + tenantClusterName

		Eventually(func() bool {
			list, err := mgmtClient.Dynamic.Resource(driftSignalGVR).
				Namespace(mgmtTenantNS).
				List(context.Background(), metav1.ListOptions{})
			if err != nil {
				return false
			}
			for _, item := range list.Items {
				spec, _ := item.Object["spec"].(map[string]interface{})
				if spec == nil {
					continue
				}
				state, _ := spec["state"].(string)
				if state == "confirmed" {
					return true
				}
			}
			return false
		}, driftSignalPollTimeout, driftPollInterval).Should(BeTrue(),
			"DriftSignal did not reach state=confirmed within %s after pack redeployment; "+
				"conductor schema requires pending -> queued -> confirmed chain (Decision H)",
			driftSignalPollTimeout)
	})

	It("escalation to TerminalDrift does not occur within one drift cycle", func() {
		Skip("requires injecting repeated drift beyond escalationThreshold — " +
			"CONDUCTOR-DRIFT-TERMINAL-E2E")
	})
})

// pickDriftTarget selects the first namespace-scoped workload resource from deployedResources
// that is safe to delete and verify as restored. Skips cluster-management resources and
// all RBAC/auth kinds (guardian owns those and they may not exist as individual objects
// once guardian has taken ownership after bootstrap sweep).
func pickDriftTarget(drs []interface{}) map[string]interface{} {
	skipKinds := map[string]bool{
		"Namespace":                    true,
		"CustomResourceDefinition":     true,
		"ClusterRole":                  true,
		"ClusterRoleBinding":           true,
		"Role":                         true,
		"RoleBinding":                  true,
		"ServiceAccount":               true,
		"MutatingWebhookConfiguration": true,
		"ValidatingWebhookConfiguration": true,
	}

	for _, raw := range drs {
		entry, ok := raw.(map[string]interface{})
		if !ok {
			continue
		}
		kind, _ := entry["kind"].(string)
		if skipKinds[kind] {
			continue
		}
		ns, _ := entry["namespace"].(string)
		if ns == "" {
			continue
		}
		name, _ := entry["name"].(string)
		if name == "" {
			continue
		}
		return entry
	}
	return nil
}
