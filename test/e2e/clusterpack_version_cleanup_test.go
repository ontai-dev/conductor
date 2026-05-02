package e2e_test

// clusterpack_version_cleanup_test.go -- live cluster verification of
// CLUSTERPACK-BL-VERSION-CLEANUP: resources orphaned by a ClusterPack version
// upgrade are deleted from the tenant cluster by PackInstancePullLoop.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG and TENANT_KUBECONFIG set; TENANT_CLUSTER_NAME=ccs-dev.
//   - cert-manager deployed to ccs-dev via ClusterPack at version N.
//   - Conductor agent running in ont-system on ccs-dev.
//   - PackReceipt {helmReceiptName} exists in ont-system on ccs-dev with
//     spec.deployedResources populated (required for orphan diff).
//
// PackReceipt naming: conductor uses the PackInstance name as the PackReceipt name.
// PackInstance names follow the pattern {packbuild-type}-{clusterName}, e.g.
// cert-manager-helm-ccs-dev. Tests parameterize on tenantClusterName.
//
// What this test verifies (conductor-schema.md §8, CLUSTERPACK-BL-VERSION-CLEANUP):
//   - PackReceipt carries spec.deployedResources (non-empty).
//   - Each entry in deployedResources has apiVersion, kind, name fields.
//   - All resources listed in deployedResources actually exist on the cluster.
//   - PackReceipt carries rbacDigest and workloadDigest (written by wrapper).
//
// The full upgrade-then-orphan-delete cycle is a destructive test that requires
// a version increment on the ClusterPack CR and is deferred to a dedicated
// upgrade verification session. This test verifies the current-state invariants
// that make orphan cleanup safe.

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
	versionCleanupPollTimeout  = 3 * time.Minute
	versionCleanupPollInterval = 5 * time.Second
)

var _ = Describe("CLUSTERPACK-BL-VERSION-CLEANUP: PackReceipt deployedResources invariants", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CLUSTERPACK-BL-VERSION-CLEANUP)")
		}
	})

	// deployReceipt is the PackReceipt written by wrapper writePackReceipt() at pack-deploy time.
	// It carries deployedResources (needed by deleteOrphanedResources() on version upgrade).
	// The name follows {clusterPackName} pattern: cert-manager-{clusterName}.
	// Distinct from the pull-loop-written receipt (cert-manager-helm-{cluster}) which carries
	// verified=true but may not include deployedResources unless the PackInstance spec does.
	deployReceiptName := func() string { return "cert-manager-" + tenantClusterName }

	It("PackReceipt cert-manager-{tenantClusterName} has non-empty spec.deployedResources", func() {
		name := deployReceiptName()
		By(fmt.Sprintf("polling for non-empty deployedResources on PackReceipt %s in ont-system on %s",
			name, tenantClient.Name))

		Eventually(func() bool {
			receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
				Namespace("ont-system").
				Get(context.Background(), name, metav1.GetOptions{})
			if err != nil {
				return false
			}
			spec, _ := receipt.Object["spec"].(map[string]interface{})
			if spec == nil {
				return false
			}
			drs, _ := spec["deployedResources"].([]interface{})
			return len(drs) > 0
		}, versionCleanupPollTimeout, versionCleanupPollInterval).Should(BeTrue(),
			"PackReceipt %s must carry deployedResources after pull loop converges; "+
				"needed by deleteOrphanedResources() on version upgrade (CLUSTERPACK-BL-VERSION-CLEANUP)",
			name)
	})

	It("each deployedResources entry has apiVersion, kind, and name", func() {
		name := deployReceiptName()
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		Expect(spec).NotTo(BeNil())

		drs, _ := spec["deployedResources"].([]interface{})
		Expect(drs).NotTo(BeEmpty(), "deployedResources must be non-empty")

		for i, raw := range drs {
			entry, ok := raw.(map[string]interface{})
			Expect(ok).To(BeTrue(), "deployedResources[%d] must be a map", i)

			apiVersion, _ := entry["apiVersion"].(string)
			Expect(apiVersion).NotTo(BeEmpty(),
				"deployedResources[%d].apiVersion must not be empty (needed by deployedResourceKey())", i)

			kind, _ := entry["kind"].(string)
			Expect(kind).NotTo(BeEmpty(),
				"deployedResources[%d].kind must not be empty (needed by deployedResourceKey())", i)

			entryName, _ := entry["name"].(string)
			Expect(entryName).NotTo(BeEmpty(),
				"deployedResources[%d].name must not be empty (needed by deleteOrphanedResources())", i)
		}
	})

	It("resources listed in deployedResources exist on the tenant cluster", func() {
		name := deployReceiptName()
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		Expect(spec).NotTo(BeNil())

		drs, _ := spec["deployedResources"].([]interface{})
		Expect(drs).NotTo(BeEmpty())

		var missing []string
		for _, raw := range drs {
			entry, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			apiVersion, _ := entry["apiVersion"].(string)
			kind, _ := entry["kind"].(string)
			entryName, _ := entry["name"].(string)
			ns, _ := entry["namespace"].(string)
			if apiVersion == "" || kind == "" || entryName == "" {
				continue
			}

			gvr := apiVersionKindToGVR(apiVersion, kind)
			var getErr error
			if ns != "" {
				_, getErr = tenantClient.Dynamic.Resource(gvr).
					Namespace(ns).Get(context.Background(), entryName, metav1.GetOptions{})
			} else {
				_, getErr = tenantClient.Dynamic.Resource(gvr).
					Get(context.Background(), entryName, metav1.GetOptions{})
			}
			if getErr != nil {
				missing = append(missing, fmt.Sprintf("%s/%s %s/%s", apiVersion, kind, ns, entryName))
			}
		}

		Expect(missing).To(BeEmpty(),
			"all resources in PackReceipt deployedResources must exist on tenant cluster; "+
				"missing: %s (PackReceiptDriftLoop would have emitted DriftSignal)",
			strings.Join(missing, ", "))
	})

	It("PackReceipt carries rbacDigest and workloadDigest", func() {
		name := deployReceiptName()
		receipt, err := tenantClient.Dynamic.Resource(packReceiptGVR).
			Namespace("ont-system").
			Get(context.Background(), name, metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		spec, _ := receipt.Object["spec"].(map[string]interface{})
		Expect(spec).NotTo(BeNil())

		rbacDigest, _ := spec["rbacDigest"].(string)
		Expect(rbacDigest).NotTo(BeEmpty(),
			"PackReceipt must carry rbacDigest written by wrapper writePackReceipt() at pack deploy time")

		workloadDigest, _ := spec["workloadDigest"].(string)
		Expect(workloadDigest).NotTo(BeEmpty(),
			"PackReceipt must carry workloadDigest written by wrapper writePackReceipt() at pack deploy time")
	})
})

var _ = Describe("CLUSTERPACK-BL-VERSION-CLEANUP: destructive upgrade cycle", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CLUSTERPACK-BL-VERSION-CLEANUP)")
		}
	})

	It("orphan cleanup deletes resources removed in a version upgrade", func() {
		Skip("requires triggering a ClusterPack version increment on ccs-mgmt and observing " +
			"deleteOrphanedResources() removing the delta resources from ccs-dev; " +
			"deferred to dedicated upgrade verification session — CLUSTERPACK-BL-VERSION-CLEANUP-UPGRADE-E2E")
	})
})

// apiVersionKindToGVR derives a schema.GroupVersionResource from an apiVersion and kind.
// This is a best-effort heuristic: it lower-cases kind, pluralizes by appending 's',
// and splits apiVersion into group/version. Covers the common cases in cert-manager
// and Kubernetes core resources.
func apiVersionKindToGVR(apiVersion, kind string) schema.GroupVersionResource {
	lower := strings.ToLower(kind)
	resource := lower + "s"

	parts := strings.SplitN(apiVersion, "/", 2)
	if len(parts) == 1 {
		return schema.GroupVersionResource{Group: "", Version: parts[0], Resource: resource}
	}
	return schema.GroupVersionResource{Group: parts[0], Version: parts[1], Resource: resource}
}
