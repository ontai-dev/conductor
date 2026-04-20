package e2e_test

// Scenario: CoreDNS DSNS stanza applied automatically after phase 05
//
// Pre-conditions required for this test to run:
//   - MGMT_KUBECONFIG set and pointing to a live ccs-mgmt cluster
//   - enable-ccs-mgmt.sh Step 7a has completed (CoreDNS patched)
//   - seam.ontave.dev. zone present in CoreDNS Corefile
//   - C-COREDNS-PATCH closed (compiler no longer emits coredns-dsns-patch.sh;
//     enable-ccs-mgmt.sh Step 7a handles the patch automatically)
//
// What this test verifies (C-COREDNS-PATCH):
//   - CoreDNS coredns ConfigMap Corefile contains seam.ontave.dev. zone block
//   - CoreDNS deployment has the dsns-zone volume mount from dsns-zone ConfigMap
//   - DNS resolution of a DSNS record succeeds from within the cluster

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("CoreDNS DSNS stanza automated patch", func() {
	It("CoreDNS Corefile contains seam.ontave.dev zone block after enable", func() {
		Skip("requires live cluster with MGMT_KUBECONFIG set and C-COREDNS-PATCH closed")
	})

	It("CoreDNS deployment has dsns-zone volume mount", func() {
		Skip("requires live cluster with MGMT_KUBECONFIG set and C-COREDNS-PATCH closed")
	})

	It("DNS resolution succeeds for a DSNS-managed record", func() {
		Skip("requires live cluster with MGMT_KUBECONFIG set and C-COREDNS-PATCH closed")
	})
})
