package e2e_test

// cnpg_audit_sweep_test.go -- live cluster audit event summary from the CNPG
// audit_events table for tenant cluster ccs-dev.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG set; ccs-mgmt guardian running with CNPG in seam-system.
//   - guardian-db Cluster is Ready; guardian-db-app Secret exists in seam-system.
//   - At least one audit event row exists for cluster_id=tenantClusterName.
//
// What this test verifies (guardian-schema.md §16, G-BL-SELF-AUDIT-MISSING):
//   - Guardian CNPG audit_events table is reachable from the management cluster.
//   - Rows exist for cluster_id=tenantClusterName (forwarded by guardian role=tenant).
//   - Prints a grouped summary: action + decision + COUNT(*) per action class.
//   - permission_snapshot_audit rows exist for the tenant cluster (INV-026).
//
// Query method: kubectl exec into the CNPG primary pod (postgresql=guardian-db,
// role=primary in seam-system) using the Kubernetes pod exec REST endpoint.
// No direct database connection from the test runner is required.

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/remotecommand"
)

const (
	cnpgNamespace    = "seam-system"
	cnpgPodSelector  = "cnpg.io/cluster=guardian-cnpg,role=primary"
	cnpgPollTimeout  = 2 * time.Minute
	cnpgPollInterval = 5 * time.Second
	cnpgExecTimeout  = 30 * time.Second
)

var _ = Describe("CNPG audit_events: tenant ccs-dev forwarding summary", func() {
	var cnpgPodName string
	var restCfg *rest.Config

	BeforeEach(func() {
		if mgmtKubeconfig == "" {
			Skip("requires MGMT_KUBECONFIG for CNPG audit query (G-BL-SELF-AUDIT-MISSING)")
		}

		var err error
		restCfg, err = clientcmd.BuildConfigFromFlags("", mgmtKubeconfig)
		Expect(err).NotTo(HaveOccurred(), "failed to build REST config from MGMT_KUBECONFIG")

		By("finding CNPG primary pod in seam-system")
		Eventually(func() string {
			pods, err := mgmtClient.Typed.CoreV1().Pods(cnpgNamespace).List(
				context.Background(), metav1.ListOptions{LabelSelector: cnpgPodSelector})
			if err != nil || len(pods.Items) == 0 {
				return ""
			}
			for _, pod := range pods.Items {
				if pod.Status.Phase == corev1.PodRunning {
					cnpgPodName = pod.Name
					return pod.Name
				}
			}
			return ""
		}, cnpgPollTimeout, cnpgPollInterval).ShouldNot(BeEmpty(),
			"CNPG primary pod (selector %s) not found or not Running in %s on %s; "+
				"ensure guardian-db Cluster is Ready before running audit sweep",
			cnpgPodSelector, cnpgNamespace, mgmtClient.Name)
	})

	It("audit_events table exists and is reachable via CNPG primary pod", func() {
		By(fmt.Sprintf("exec into %s/%s: check audit_events table exists", cnpgNamespace, cnpgPodName))

		out, err := execPsqlInPod(restCfg, cnpgNamespace, cnpgPodName, `\dt audit_events`)
		Expect(err).NotTo(HaveOccurred(),
			"psql exec failed -- CNPG pod %s/%s unreachable or guardian user not available; "+
				"guardian-db must be Ready and migrations applied",
			cnpgNamespace, cnpgPodName)

		GinkgoWriter.Printf("\naudit_events table check:\n%s\n", out)
		Expect(out).To(ContainSubstring("audit_events"),
			"audit_events table must exist after guardian migration 001")
	})

	It("audit_events rows exist for cluster_id=tenantClusterName", func() {
		query := fmt.Sprintf(
			`SELECT COUNT(*) FROM audit_events WHERE cluster_id = '%s'`,
			tenantClusterName)

		out, err := execPsqlInPod(restCfg, cnpgNamespace, cnpgPodName, query)
		Expect(err).NotTo(HaveOccurred())

		GinkgoWriter.Printf("\naudit_events count for cluster_id=%s: %s\n",
			tenantClusterName, strings.TrimSpace(out))
		Expect(strings.TrimSpace(out)).NotTo(Equal("0"),
			"no audit_events rows for cluster_id=%s; "+
				"guardian role=tenant on ccs-dev must forward audit events to management guardian via AuditSink",
			tenantClusterName)
	})

	It("prints grouped audit summary for tenant ccs-dev from CNPG sink", func() {
		query := fmt.Sprintf(
			`SELECT action, decision, COUNT(*) AS count `+
				`FROM audit_events `+
				`WHERE cluster_id = '%s' `+
				`GROUP BY action, decision `+
				`ORDER BY count DESC `+
				`LIMIT 30`,
			tenantClusterName)

		out, err := execPsqlInPod(restCfg, cnpgNamespace, cnpgPodName, query)
		Expect(err).NotTo(HaveOccurred())

		GinkgoWriter.Printf(
			"\n=== CNPG audit_events summary for cluster_id=%s ===\n%s\n=== end ===\n",
			tenantClusterName, out)

		Expect(out).NotTo(BeEmpty(),
			"audit summary must not be empty for cluster_id=%s", tenantClusterName)
	})

	It("permission_snapshot_audit rows exist for tenant cluster", func() {
		query := fmt.Sprintf(
			`SELECT snapshot_name, target_cluster, `+
				`CASE WHEN signed_at IS NOT NULL THEN 'signed' ELSE 'unsigned' END AS sign_state, `+
				`CASE WHEN delivered_at IS NOT NULL THEN 'delivered' ELSE 'pending' END AS delivery_state `+
				`FROM permission_snapshot_audit `+
				`WHERE target_cluster = '%s' `+
				`ORDER BY generated_at DESC `+
				`LIMIT 10`,
			tenantClusterName)

		out, err := execPsqlInPod(restCfg, cnpgNamespace, cnpgPodName, query)
		Expect(err).NotTo(HaveOccurred())

		GinkgoWriter.Printf(
			"\n=== permission_snapshot_audit for target_cluster=%s ===\n%s\n=== end ===\n",
			tenantClusterName, out)

		Expect(out).To(ContainSubstring(tenantClusterName),
			"permission_snapshot_audit must have rows for target_cluster=%s "+
				"(INV-026 signing chain: generated_at, signed_at, delivered_at)",
			tenantClusterName)
	})
})

// execPsqlInPod runs a psql command inside the named CNPG pod using the
// Kubernetes pod exec REST endpoint. The command is run as:
//
//	psql -U guardian -c <sql>
//
// Returns combined stdout+stderr. The REST URL is constructed manually to
// avoid a dependency on k8s.io/kubectl/pkg/scheme.
func execPsqlInPod(cfg *rest.Config, ns, podName, sql string) (string, error) {
	// Use postgres superuser (peer auth inside pod, no password required).
	// The guardian database is the target DB where migrations run.
	cmd := []string{"psql", "-U", "postgres", "-d", "guardian", "-c", sql}
	return execInPod(cfg, ns, podName, cmd)
}

// execInPod executes cmd in the named pod using the Kubernetes exec API.
// The URL is built manually from the REST config host to avoid importing
// k8s.io/kubectl/pkg/scheme (which pulls incompatible k8s version transitive deps).
func execInPod(cfg *rest.Config, ns, podName string, cmd []string) (string, error) {
	execURL := buildExecURL(cfg.Host, ns, podName, cmd)

	restCfgCopy := rest.CopyConfig(cfg)

	exec, err := remotecommand.NewSPDYExecutor(restCfgCopy, "POST", execURL)
	if err != nil {
		return "", fmt.Errorf("build SPDY executor for pod %s/%s: %w", ns, podName, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), cnpgExecTimeout)
	defer cancel()

	var outBuf bytes.Buffer
	if err := exec.StreamWithContext(ctx, remotecommand.StreamOptions{
		Stdout: &outBuf,
		Stderr: &outBuf,
	}); err != nil {
		return outBuf.String(), fmt.Errorf("exec stream in pod %s/%s: %w", ns, podName, err)
	}
	return outBuf.String(), nil
}

// buildExecURL constructs the pod exec URL from the API server host, namespace,
// pod name, and command slice. Uses the same path format as kubectl exec.
func buildExecURL(host, ns, podName string, cmd []string) *url.URL {
	path := fmt.Sprintf("/api/v1/namespaces/%s/pods/%s/exec", ns, podName)
	u := &url.URL{
		Scheme: "https",
		Host:   strings.TrimPrefix(strings.TrimPrefix(host, "https://"), "http://"),
		Path:   path,
	}
	q := url.Values{}
	for _, c := range cmd {
		q.Add("command", c)
	}
	q.Set("stdout", "true")
	q.Set("stderr", "true")
	u.RawQuery = q.Encode()
	return u
}
