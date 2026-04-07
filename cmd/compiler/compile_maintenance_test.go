package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	platformv1alpha1 "github.com/ontai-dev/platform/api/v1alpha1"
)

// WS1 — Maintenance compile-only contract unit tests.
// compileMaintenanceCore is tested with a fake kubernetes.Interface so no
// live cluster is required. conductor-schema.md §9.

// newFakeK8sWithLeader builds a fake kubernetes client pre-populated with:
//   - a platform-leader Lease in seam-system with holderIdentity pointing to podName
//   - a Pod named podName scheduled on nodeName
func newFakeK8sWithLeader(podName, nodeName string) *fake.Clientset {
	holderIdentity := podName + "_test-uid"
	lease := &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "platform-leader",
			Namespace: "seam-system",
		},
		Spec: coordinationv1.LeaseSpec{
			HolderIdentity: &holderIdentity,
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: "seam-system",
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
		},
	}
	return fake.NewSimpleClientset(lease, pod)
}

// newFakeK8sWithLeaderAndS3 extends newFakeK8sWithLeader with the etcd backup
// S3 Secret required for etcd-backup operations.
func newFakeK8sWithLeaderAndS3(podName, nodeName string) *fake.Clientset {
	client := newFakeK8sWithLeader(podName, nodeName)
	s3Secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "seam-etcd-backup-config",
			Namespace: "seam-system",
		},
	}
	if err := client.Tracker().Add(s3Secret); err != nil {
		panic("newFakeK8sWithLeaderAndS3: add S3 secret: " + err.Error())
	}
	return client
}

// newFakeK8sWithLeaderS3AndNode extends newFakeK8sWithLeaderAndS3 with a
// Node object so validateTargetNodes can find it.
func newFakeK8sWithLeaderS3AndNode(podName, nodeName, targetNode string) *fake.Clientset {
	client := newFakeK8sWithLeaderAndS3(podName, nodeName)
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: targetNode},
	}
	if err := client.Tracker().Add(node); err != nil {
		panic("newFakeK8sWithLeaderS3AndNode: add node: " + err.Error())
	}
	return client
}

// TestMaintenance_DrainOperation_ProducesMaintenanceBundleYAML verifies that
// compileMaintenanceCore with a drain operation produces a MaintenanceBundle
// YAML in the output directory. No real cluster is contacted.
func TestMaintenance_DrainOperation_ProducesMaintenanceBundleYAML(t *testing.T) {
	k8s := newFakeK8sWithLeader("platform-pod-0", "control-plane-node-0")
	outDir := t.TempDir()

	err := compileMaintenanceCore(
		platformv1alpha1.MaintenanceBundleOperationDrain,
		nil,
		"ccs-test",
		"seam-system",
		k8s,
		outDir,
	)
	if err != nil {
		t.Fatalf("compileMaintenanceCore: %v", err)
	}

	outPath := filepath.Join(outDir, "ccs-test-drain.yaml")
	if _, statErr := os.Stat(outPath); os.IsNotExist(statErr) {
		t.Fatalf("expected output file %q; not found", outPath)
	}
}

// TestMaintenance_EtcdBackupOperation_TargetNodesInOutput verifies that an
// etcd-backup operation with explicit targets produces a MaintenanceBundle
// that embeds both the operation type and the target node list.
// conductor-schema.md §9: "assert output carries the correct operation type and target nodes."
func TestMaintenance_EtcdBackupOperation_TargetNodesInOutput(t *testing.T) {
	const leaderPod = "platform-pod-0"
	const leaderNode = "cp-node-0"
	targets := []string{"worker-1", "worker-2"}

	k8s := newFakeK8sWithLeaderS3AndNode(leaderPod, leaderNode, "worker-1")
	// Add worker-2 as well.
	node2 := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "worker-2"}}
	if err := k8s.Tracker().Add(node2); err != nil {
		t.Fatalf("add node worker-2: %v", err)
	}

	outDir := t.TempDir()
	err := compileMaintenanceCore(
		platformv1alpha1.MaintenanceBundleOperationEtcdBackup,
		targets,
		"ccs-test",
		"seam-system",
		k8s,
		outDir,
	)
	if err != nil {
		t.Fatalf("compileMaintenanceCore: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "ccs-test-etcd-backup.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "etcd-backup") {
		t.Errorf("output missing operation type 'etcd-backup':\n%s", content)
	}
	for _, node := range targets {
		if !strings.Contains(content, node) {
			t.Errorf("output missing target node %q:\n%s", node, content)
		}
	}
}

// TestMaintenance_OperationTypeAndClusterRefInOutput verifies that the
// MaintenanceBundle CR carries the correct operation and clusterRef.
// conductor-schema.md §9.
func TestMaintenance_OperationTypeAndClusterRefInOutput(t *testing.T) {
	k8s := newFakeK8sWithLeader("platform-pod-0", "cp-node-0")
	outDir := t.TempDir()

	err := compileMaintenanceCore(
		platformv1alpha1.MaintenanceBundleOperationUpgrade,
		nil,
		"ccs-prod",
		"seam-system",
		k8s,
		outDir,
	)
	if err != nil {
		t.Fatalf("compileMaintenanceCore: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "ccs-prod-upgrade.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	content := string(data)
	if !strings.Contains(content, "upgrade") {
		t.Errorf("output missing operation 'upgrade':\n%s", content)
	}
	if !strings.Contains(content, "ccs-prod") {
		t.Errorf("output missing clusterRef 'ccs-prod':\n%s", content)
	}
}

// TestMaintenance_LeaderNodeEmbeddedInOutput verifies that the leader node
// resolved from the Lease is embedded as operatorLeaderNode in the bundle.
// conductor-schema.md §9, CP-INV-007.
func TestMaintenance_LeaderNodeEmbeddedInOutput(t *testing.T) {
	const expectedLeaderNode = "cp-node-3"
	k8s := newFakeK8sWithLeader("platform-pod-0", expectedLeaderNode)
	outDir := t.TempDir()

	err := compileMaintenanceCore(
		platformv1alpha1.MaintenanceBundleOperationDrain,
		nil,
		"ccs-test",
		"seam-system",
		k8s,
		outDir,
	)
	if err != nil {
		t.Fatalf("compileMaintenanceCore: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "ccs-test-drain.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !strings.Contains(string(data), expectedLeaderNode) {
		t.Errorf("output missing leader node %q:\n%s", expectedLeaderNode, data)
	}
}

// WS2 — Maintenance malformed input validation tests.

// TestMaintenance_UnknownOperationFails verifies that validateMaintenanceOperation
// returns a descriptive error for an unrecognised operation type.
// This validates offline — no k8s client needed — because operation validation
// runs before the client is built. conductor-schema.md §9.
func TestMaintenance_UnknownOperationFails(t *testing.T) {
	_, err := validateMaintenanceOperation("reboot-cluster")
	if err == nil {
		t.Fatal("expected error for unknown operation 'reboot-cluster'; got nil")
	}
	// Error must mention the invalid value and the valid set.
	errStr := err.Error()
	if !strings.Contains(errStr, "reboot-cluster") {
		t.Errorf("error %q does not name the invalid operation", errStr)
	}
	for _, valid := range []string{"drain", "upgrade", "etcd-backup", "machineconfig-rotation"} {
		if !strings.Contains(errStr, valid) {
			t.Errorf("error %q does not list valid operation %q", errStr, valid)
		}
	}
}

// TestMaintenance_NoAPICallsMadeForDrain confirms the compile-time contract:
// for a drain operation, compileMaintenanceCore uses only the injected fake
// client and never touches a real cluster. The fake client has no HTTP backend
// so any real network call would panic or return an error. A successful run
// proves the function is fully offline when given a valid fake client.
func TestMaintenance_NoAPICallsMadeForDrain(t *testing.T) {
	k8s := newFakeK8sWithLeader("platform-pod-0", "cp-node-0")
	outDir := t.TempDir()

	err := compileMaintenanceCore(
		platformv1alpha1.MaintenanceBundleOperationDrain,
		nil,
		"ccs-test",
		"seam-system",
		k8s,
		outDir,
	)
	if err != nil {
		t.Fatalf("expected success with fake client; got: %v", err)
	}
}
