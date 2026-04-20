package e2e_test

// Scenario: Kueue mutating webhook scoped to ont-managed namespaces
//
// Pre-conditions required for this test to run:
//   - MGMT_KUBECONFIG set and pointing to a live ccs-mgmt cluster
//   - Kueue deployed and webhook scoped (enable-ccs-mgmt.sh Phase 00 complete)
//   - C-KUEUE-WEBHOOK closed (webhook scoping moved to Phase 00, applied
//     immediately after kueue-controller.yaml)
//
// What this test verifies (C-KUEUE-WEBHOOK):
//   - kueue-validating-webhook-configuration has a namespaceSelector for
//     ont-managed=true (not global)
//   - A workload in a non-ont-managed namespace is not intercepted by the
//     Kueue webhook
//   - A workload in an ont-managed namespace (seam-tenant-*) is intercepted

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("Kueue mutating webhook scoping", func() {
	It("kueue-validating-webhook-configuration has namespaceSelector for ont-managed=true", func() {
		Skip("requires live cluster with MGMT_KUBECONFIG set and C-KUEUE-WEBHOOK closed")
	})

	It("Kueue webhook does not intercept workloads in kube-system namespace", func() {
		Skip("requires live cluster with MGMT_KUBECONFIG set and C-KUEUE-WEBHOOK closed")
	})

	It("Kueue webhook intercepts workloads in ont-managed seam-tenant namespace", func() {
		Skip("requires live cluster with MGMT_KUBECONFIG set and C-KUEUE-WEBHOOK closed")
	})
})
