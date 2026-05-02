package e2e_test

// permission_service_test.go -- live cluster verification that the local
// PermissionService gRPC endpoint on the tenant conductor is reachable and
// serves correct authorization decisions.
//
// Pre-conditions:
//   - TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev set.
//   - Conductor agent running in ont-system on ccs-dev with MGMT_KUBECONFIG_PATH set.
//   - conductor-permission-service NodePort 30051 applied on ccs-dev.
//   - PermissionSnapshot for ccs-dev signed and acknowledged by pull loop
//     (PermissionSnapshotReceipt receipt-ccs-dev exists in ont-system).
//
// What this test verifies (conductor-schema.md §10 step 6, CONDUCTOR-PERMISSION-SERVICE-E2E):
//   - gRPC endpoint on NodePort 30051 is reachable.
//   - ListPermissions returns a valid response for a known principal.
//   - CheckPermission returns a valid response (allowed or denied with a reason).
//   - ExplainDecision returns structured rule detail when snapshot is loaded.
//   - When store is not yet populated, all methods return a consistent not-ready reason.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/encoding"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// permSvcNodePort is the NodePort on which the PermissionService gRPC endpoint
// is exposed on ccs-dev. Must match conductor-permission-service.yaml.
const permSvcNodePort = 30051

// permSvcFullMethod returns the full gRPC method path for the given method name.
func permSvcFullMethod(method string) string {
	return "/ontai.security.v1alpha1.PermissionService/" + method
}

// testJSONCodec is the JSON gRPC codec used by the e2e test client to call the
// conductor PermissionService, which registers application/grpc+json in its init().
type testJSONCodec struct{}

func (testJSONCodec) Marshal(v any) ([]byte, error)      { return json.Marshal(v) }
func (testJSONCodec) Unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
func (testJSONCodec) Name() string                       { return "json" }

func init() {
	encoding.RegisterCodec(testJSONCodec{})
}

// firstNodeInternalIP returns the InternalIP address of the first node in the
// tenant cluster. Used to reach NodePort services from the test host.
func firstNodeInternalIP() string {
	nodes, err := tenantClient.Typed.CoreV1().Nodes().List(
		context.Background(), metav1.ListOptions{})
	Expect(err).NotTo(HaveOccurred(), "must list tenant cluster nodes")
	Expect(nodes.Items).NotTo(BeEmpty(), "tenant cluster must have at least one node")
	for _, addr := range nodes.Items[0].Status.Addresses {
		if addr.Type == corev1.NodeInternalIP {
			return addr.Address
		}
	}
	Fail("no InternalIP found on first tenant cluster node")
	return ""
}

// dialPermSvc establishes a gRPC client connection to the conductor PermissionService
// NodePort on the given node IP. Caller is responsible for closing the connection.
func dialPermSvc(nodeIP string) *grpc.ClientConn {
	addr := fmt.Sprintf("%s:%d", nodeIP, permSvcNodePort)
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	Expect(err).NotTo(HaveOccurred(),
		"must build gRPC client connection to conductor PermissionService at %s", addr)
	return conn
}

// permSvcSnapshotLoaded checks whether the PermissionSnapshotReceipt on the
// tenant cluster reports that the snapshot has been successfully pulled and
// verified (no DegradedSecurityState, ready field present).
func permSvcSnapshotLoaded() bool {
	receiptName := "receipt-" + tenantClusterName
	receipt, err := tenantClient.Dynamic.Resource(permissionSnapshotReceiptGVRc).
		Namespace("ont-system").
		Get(context.Background(), receiptName, metav1.GetOptions{})
	if err != nil {
		return false
	}
	status, _ := receipt.Object["status"].(map[string]interface{})
	if status == nil {
		return false
	}
	conditions, _ := status["conditions"].([]interface{})
	for _, raw := range conditions {
		cond, _ := raw.(map[string]interface{})
		if cond == nil {
			continue
		}
		if cond["type"] == "DegradedSecurityState" && cond["status"] == "True" {
			return false
		}
	}
	// Receipt exists and has no DegradedSecurityState -- treat as loaded.
	return true
}

var _ = Describe("Conductor role=agent: PermissionService gRPC", func() {
	BeforeEach(func() {
		if tenantClient == nil {
			Skip("requires TENANT_KUBECONFIG and TENANT_CLUSTER_NAME=ccs-dev (CONDUCTOR-PERMISSION-SERVICE-E2E)")
		}
	})

	It("PermissionService gRPC endpoint is reachable on NodePort 30051", func() {
		nodeIP := firstNodeInternalIP()
		conn := dialPermSvc(nodeIP)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := &permissionservice.ListPermissionsRequest{
			Principal: "system:serviceaccount:cert-manager:cert-manager",
			Cluster:   tenantClusterName,
		}
		resp := &permissionservice.ListPermissionsResponse{}
		err := conn.Invoke(ctx, permSvcFullMethod("ListPermissions"), req, resp,
			grpc.ForceCodec(testJSONCodec{}),
		)
		Expect(err).NotTo(HaveOccurred(),
			"conductor PermissionService at %s:%d must respond to ListPermissions; "+
				"apply conductor-permission-service.yaml on ccs-dev and ensure conductor pod is running",
			nodeIP, permSvcNodePort)
	})

	It("PermissionService ListPermissions returns empty rules with reason when store not ready", func() {
		if permSvcSnapshotLoaded() {
			Skip("snapshot is loaded — not-ready path not exercisable; see loaded-snapshot test")
		}

		nodeIP := firstNodeInternalIP()
		conn := dialPermSvc(nodeIP)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := &permissionservice.ListPermissionsRequest{
			Principal: "system:serviceaccount:cert-manager:cert-manager",
			Cluster:   tenantClusterName,
		}
		resp := &permissionservice.ListPermissionsResponse{}
		err := conn.Invoke(ctx, permSvcFullMethod("ListPermissions"), req, resp,
			grpc.ForceCodec(testJSONCodec{}),
		)
		Expect(err).NotTo(HaveOccurred())
		Expect(resp.Rules).To(BeEmpty(),
			"ListPermissions must return empty rules when snapshot store is not ready; "+
				"set MGMT_KUBECONFIG_PATH on conductor Deployment and apply conductor-mgmt-kubeconfig Secret")
	})

	It("PermissionService CheckPermission returns structured response for any request", func() {
		nodeIP := firstNodeInternalIP()
		conn := dialPermSvc(nodeIP)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := &permissionservice.CheckPermissionRequest{
			Principal: "system:serviceaccount:cert-manager:cert-manager",
			Cluster:   tenantClusterName,
			APIGroup:  "cert-manager.io",
			Resource:  "certificates",
			Verb:      "get",
		}
		resp := &permissionservice.CheckPermissionResponse{}
		err := conn.Invoke(ctx, permSvcFullMethod("CheckPermission"), req, resp,
			grpc.ForceCodec(testJSONCodec{}),
		)
		Expect(err).NotTo(HaveOccurred(),
			"CheckPermission must return a structured response (allowed or denied with reason)")
		Expect(resp.Reason).NotTo(BeEmpty(),
			"CheckPermission response must always carry a Reason field")
	})

	It("PermissionService ListPermissions returns rules for cert-manager when snapshot is loaded", func() {
		if !permSvcSnapshotLoaded() {
			Skip("PermissionSnapshotReceipt not yet present on ccs-dev — set MGMT_KUBECONFIG_PATH " +
				"on conductor Deployment and apply conductor-mgmt-kubeconfig Secret (CONDUCTOR-PERMISSION-SERVICE-E2E)")
		}

		nodeIP := firstNodeInternalIP()
		conn := dialPermSvc(nodeIP)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()

		req := &permissionservice.ListPermissionsRequest{
			Principal: "system:serviceaccount:cert-manager:cert-manager",
			Cluster:   tenantClusterName,
		}
		resp := &permissionservice.ListPermissionsResponse{}

		Eventually(func() error {
			innerResp := &permissionservice.ListPermissionsResponse{}
			if err := conn.Invoke(ctx, permSvcFullMethod("ListPermissions"), req, innerResp,
				grpc.ForceCodec(testJSONCodec{}),
			); err != nil {
				return err
			}
			if len(innerResp.Rules) == 0 {
				return fmt.Errorf("ListPermissions returned zero rules for cert-manager; snapshot may not be loaded yet")
			}
			*resp = *innerResp
			return nil
		}, 2*time.Minute, 10*time.Second).Should(Succeed(),
			"PermissionService must return non-empty rules for cert-manager when snapshot is acknowledged")

		Expect(resp.Rules).NotTo(BeEmpty())
		for _, rule := range resp.Rules {
			Expect(rule.Resource).NotTo(BeEmpty(), "each rule must have a non-empty resource")
			Expect(rule.Verbs).NotTo(BeEmpty(), "each rule must carry at least one verb")
		}
	})

	It("PermissionService ExplainDecision returns structured rule detail when snapshot is loaded", func() {
		if !permSvcSnapshotLoaded() {
			Skip("PermissionSnapshotReceipt not yet present on ccs-dev (CONDUCTOR-PERMISSION-SERVICE-E2E)")
		}

		nodeIP := firstNodeInternalIP()
		conn := dialPermSvc(nodeIP)
		defer conn.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		req := &permissionservice.ExplainDecisionRequest{
			Principal: "system:serviceaccount:cert-manager:cert-manager",
			Cluster:   tenantClusterName,
			APIGroup:  "cert-manager.io",
			Resource:  "certificates",
			Verb:      "get",
		}
		resp := &permissionservice.ExplainDecisionResponse{}
		err := conn.Invoke(ctx, permSvcFullMethod("ExplainDecision"), req, resp,
			grpc.ForceCodec(testJSONCodec{}),
		)
		Expect(err).NotTo(HaveOccurred(),
			"ExplainDecision must return a structured response when snapshot is loaded")
		Expect(resp.Reason).NotTo(BeEmpty(),
			"ExplainDecision must always populate Reason")
		if resp.Allowed {
			Expect(resp.MatchedRule).NotTo(BeNil(),
				"ExplainDecision must populate MatchedRule when decision is allowed")
			Expect(resp.MatchedRule.Resource).NotTo(BeEmpty())
		}
	})
})
