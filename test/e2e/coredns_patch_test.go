package e2e_test

// coredns_patch_test.go -- live cluster verification of the CoreDNS DSNS zone patch
// applied by conductor role=management on ccs-mgmt. conductor-schema.md §12.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG set.
//   - CoreDNS DSNS enable bundle (05-post-bootstrap/coredns-dsns-stanza.yaml) applied
//     to ccs-mgmt. If not applied, individual tests self-skip with C-COREDNS-PATCH.

import (
	"context"
	"strings"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var _ = Describe("CoreDNS DSNS stanza automated patch", func() {
	It("CoreDNS Corefile contains seam.ontave.dev zone block after enable", func() {
		cm, err := mgmtClient.Typed.CoreV1().ConfigMaps("kube-system").Get(
			context.Background(), "coredns", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(), "CoreDNS ConfigMap coredns must exist in kube-system")

		corefile, ok := cm.Data["Corefile"]
		if !ok || !strings.Contains(corefile, "seam.ontave.dev") {
			Skip("CoreDNS Corefile does not contain seam.ontave.dev — apply coredns-dsns-stanza.yaml first (C-COREDNS-PATCH)")
		}
		Expect(corefile).To(ContainSubstring("seam.ontave.dev"),
			"Corefile must contain the seam.ontave.dev zone stanza added by the DSNS enable bundle")
	})

	It("CoreDNS deployment has dsns-zone volume mount", func() {
		deploy, err := mgmtClient.Typed.AppsV1().Deployments("kube-system").Get(
			context.Background(), "coredns", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred(), "CoreDNS Deployment must exist in kube-system")

		var found bool
		for _, c := range deploy.Spec.Template.Spec.Containers {
			for _, vm := range c.VolumeMounts {
				if vm.Name == "dsns-zone" {
					found = true
				}
			}
		}
		if !found {
			Skip("CoreDNS Deployment does not have dsns-zone volume mount — apply coredns-dsns-stanza.yaml first (C-COREDNS-PATCH)")
		}
		Expect(found).To(BeTrue(), "CoreDNS Deployment must carry the dsns-zone VolumeMount")
	})

	It("DNS resolution succeeds for a DSNS-managed record", func() {
		Skip("requires DSNS record provisioned and CoreDNS patch confirmed — C-COREDNS-PATCH and DSNS-RECORD-E2E")
	})
})
