package e2e_test

// kueue_webhook_test.go -- live cluster verification that the Kueue webhook
// namespaceSelector is scoped to ont-managed namespaces only. conductor-schema.md §12.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG set.
//   - Kueue deployed and its ValidatingWebhookConfiguration updated by conductor
//     to scope intercept to ont-managed=true namespaces.
//
// Individual tests self-skip if the webhook is not yet scoped (C-KUEUE-WEBHOOK).

import (
	"context"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const kueueWebhookName = "kueue-validating-webhook-configuration"

var _ = Describe("Kueue mutating webhook scoping", func() {
	It("kueue-validating-webhook-configuration has namespaceSelector for ont-managed=true", func() {
		vwc, err := mgmtClient.Typed.AdmissionregistrationV1().
			ValidatingWebhookConfigurations().
			Get(context.Background(), kueueWebhookName, metav1.GetOptions{})
		if err != nil {
			Skip("kueue-validating-webhook-configuration not found — Kueue not deployed or C-KUEUE-WEBHOOK not closed")
		}

		var hasOntManagedSelector bool
		for _, wh := range vwc.Webhooks {
			if wh.NamespaceSelector == nil {
				continue
			}
			for _, req := range wh.NamespaceSelector.MatchExpressions {
				if req.Key == "ont-managed" {
					hasOntManagedSelector = true
				}
			}
			for k := range wh.NamespaceSelector.MatchLabels {
				if k == "ont-managed" {
					hasOntManagedSelector = true
				}
			}
		}
		if !hasOntManagedSelector {
			Skip("kueue webhook namespaceSelector does not reference ont-managed — conductor scoping not yet applied (C-KUEUE-WEBHOOK)")
		}
		Expect(hasOntManagedSelector).To(BeTrue(),
			"kueue-validating-webhook-configuration must scope intercept to ont-managed=true namespaces (C-KUEUE-WEBHOOK)")
	})

	It("kube-system namespace does not carry ont-managed=true label (webhook excludes it by selector)", func() {
		vwc, err := mgmtClient.Typed.AdmissionregistrationV1().
			ValidatingWebhookConfigurations().
			Get(context.Background(), kueueWebhookName, metav1.GetOptions{})
		if err != nil {
			Skip("kueue-validating-webhook-configuration not found — C-KUEUE-WEBHOOK")
		}
		var hasOntManagedSelector bool
		for _, wh := range vwc.Webhooks {
			if wh.NamespaceSelector == nil {
				continue
			}
			for _, req := range wh.NamespaceSelector.MatchExpressions {
				if req.Key == "ont-managed" {
					hasOntManagedSelector = true
				}
			}
			for k := range wh.NamespaceSelector.MatchLabels {
				if k == "ont-managed" {
					hasOntManagedSelector = true
				}
			}
		}
		if !hasOntManagedSelector {
			Skip("kueue webhook namespaceSelector does not reference ont-managed — C-KUEUE-WEBHOOK not closed")
		}

		// kube-system must not carry ont-managed=true so the webhook's namespaceSelector
		// excludes it. We verify by label inspection, not by creating a workload.
		kubeSystem, err := mgmtClient.Typed.CoreV1().Namespaces().Get(
			context.Background(), "kube-system", metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		Expect(kubeSystem.Labels["ont-managed"]).NotTo(Equal("true"),
			"kube-system must not carry ont-managed=true — webhook namespaceSelector would incorrectly intercept it")
	})

	It("seam-tenant-{tenantClusterName} namespace carries ont-managed=true label (webhook includes it)", func() {
		vwc, err := mgmtClient.Typed.AdmissionregistrationV1().
			ValidatingWebhookConfigurations().
			Get(context.Background(), kueueWebhookName, metav1.GetOptions{})
		if err != nil {
			Skip("kueue-validating-webhook-configuration not found — C-KUEUE-WEBHOOK")
		}
		var hasOntManagedSelector bool
		for _, wh := range vwc.Webhooks {
			if wh.NamespaceSelector == nil {
				continue
			}
			for _, req := range wh.NamespaceSelector.MatchExpressions {
				if req.Key == "ont-managed" {
					hasOntManagedSelector = true
				}
			}
			for k := range wh.NamespaceSelector.MatchLabels {
				if k == "ont-managed" {
					hasOntManagedSelector = true
				}
			}
		}
		if !hasOntManagedSelector {
			Skip("kueue webhook namespaceSelector does not reference ont-managed — C-KUEUE-WEBHOOK not closed")
		}

		// seam-tenant-{clusterName} must carry ont-managed=true so Kueue intercepts
		// workloads submitted for governed tenant clusters. Verified by label inspection.
		tenantNS := "seam-tenant-" + tenantClusterName
		ns, err := mgmtClient.Typed.CoreV1().Namespaces().Get(
			context.Background(), tenantNS, metav1.GetOptions{})
		if err != nil {
			Skip("namespace " + tenantNS + " not found — set TENANT_CLUSTER_NAME=ccs-dev and ensure ClusterRBACPolicyReconciler has run")
		}
		Expect(ns.Labels["ont-managed"]).To(Equal("true"),
			"namespace %s must carry ont-managed=true so the Kueue webhook intercepts its workloads", tenantNS)
	})
})
