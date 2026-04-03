package runnerlib_test

import (
	"testing"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// allCapabilityConstants returns all 18 named capability constants.
// This slice is the authoritative list for this test file. If a capability is
// added or removed, this function must be updated. The test verifies count = 18.
func allCapabilityConstants() []string {
	return []string{
		// platform (15)
		runnerlib.CapabilityBootstrap,
		runnerlib.CapabilityTalosUpgrade,
		runnerlib.CapabilityKubeUpgrade,
		runnerlib.CapabilityStackUpgrade,
		runnerlib.CapabilityNodePatch,
		runnerlib.CapabilityNodeScaleUp,
		runnerlib.CapabilityNodeDecommission,
		runnerlib.CapabilityNodeReboot,
		runnerlib.CapabilityEtcdBackup,
		runnerlib.CapabilityEtcdMaintenance,
		runnerlib.CapabilityEtcdRestore,
		runnerlib.CapabilityPKIRotate,
		runnerlib.CapabilityCredentialRotate,
		runnerlib.CapabilityHardeningApply,
		runnerlib.CapabilityClusterReset,
		// wrapper (2)
		runnerlib.CapabilityPackCompile,
		runnerlib.CapabilityPackDeploy,
		// guardian (1)
		runnerlib.CapabilityRBACProvision,
	}
}

// TestAllCapabilityConstantsNonEmpty verifies that all 18 capability constants
// are non-empty strings.
func TestAllCapabilityConstantsNonEmpty(t *testing.T) {
	for _, c := range allCapabilityConstants() {
		if c == "" {
			t.Error("capability constant is empty string")
		}
	}
}

// TestAllCapabilityConstantsCount verifies exactly 18 named capabilities exist,
// matching conductor-schema.md Section 6.
func TestAllCapabilityConstantsCount(t *testing.T) {
	all := allCapabilityConstants()
	if len(all) != 18 {
		t.Errorf("capability constant count: got %d, want 18", len(all))
	}
}

// TestAllCapabilityConstantsUnique verifies that no two capability constants share
// the same string value. Duplicate capability names would cause silent routing
// errors in the capability registry.
func TestAllCapabilityConstantsUnique(t *testing.T) {
	seen := make(map[string]bool)
	for _, c := range allCapabilityConstants() {
		if seen[c] {
			t.Errorf("capability constant %q is duplicated", c)
		}
		seen[c] = true
	}
}

// TestNonObviousCapabilityConstantValues verifies the exact string values of the
// three non-obvious constants: pack-compile, pack-deploy, rbac-provision.
// These are the constants most likely to be misspelled. Operators depend on these
// exact strings matching what the runner's capability registry declares.
func TestNonObviousCapabilityConstantValues(t *testing.T) {
	if runnerlib.CapabilityPackCompile != "pack-compile" {
		t.Errorf("CapabilityPackCompile: got %q, want %q",
			runnerlib.CapabilityPackCompile, "pack-compile")
	}
	if runnerlib.CapabilityPackDeploy != "pack-deploy" {
		t.Errorf("CapabilityPackDeploy: got %q, want %q",
			runnerlib.CapabilityPackDeploy, "pack-deploy")
	}
	if runnerlib.CapabilityRBACProvision != "rbac-provision" {
		t.Errorf("CapabilityRBACProvision: got %q, want %q",
			runnerlib.CapabilityRBACProvision, "rbac-provision")
	}
}

// TestCapabilityConstantsMatchSchema verifies the exact string values of all
// 18 constants against conductor-schema.md Section 6. Any mismatch breaks the
// operator-runner Job dispatch protocol.
func TestCapabilityConstantsMatchSchema(t *testing.T) {
	expected := map[string]string{
		"CapabilityBootstrap":        "bootstrap",
		"CapabilityTalosUpgrade":     "talos-upgrade",
		"CapabilityKubeUpgrade":      "kube-upgrade",
		"CapabilityStackUpgrade":     "stack-upgrade",
		"CapabilityNodePatch":        "node-patch",
		"CapabilityNodeScaleUp":      "node-scale-up",
		"CapabilityNodeDecommission": "node-decommission",
		"CapabilityNodeReboot":       "node-reboot",
		"CapabilityEtcdBackup":       "etcd-backup",
		"CapabilityEtcdMaintenance":  "etcd-maintenance",
		"CapabilityEtcdRestore":      "etcd-restore",
		"CapabilityPKIRotate":        "pki-rotate",
		"CapabilityCredentialRotate": "credential-rotate",
		"CapabilityHardeningApply":   "hardening-apply",
		"CapabilityClusterReset":     "cluster-reset",
		"CapabilityPackCompile":      "pack-compile",
		"CapabilityPackDeploy":       "pack-deploy",
		"CapabilityRBACProvision":    "rbac-provision",
	}

	actual := map[string]string{
		"CapabilityBootstrap":        runnerlib.CapabilityBootstrap,
		"CapabilityTalosUpgrade":     runnerlib.CapabilityTalosUpgrade,
		"CapabilityKubeUpgrade":      runnerlib.CapabilityKubeUpgrade,
		"CapabilityStackUpgrade":     runnerlib.CapabilityStackUpgrade,
		"CapabilityNodePatch":        runnerlib.CapabilityNodePatch,
		"CapabilityNodeScaleUp":      runnerlib.CapabilityNodeScaleUp,
		"CapabilityNodeDecommission": runnerlib.CapabilityNodeDecommission,
		"CapabilityNodeReboot":       runnerlib.CapabilityNodeReboot,
		"CapabilityEtcdBackup":       runnerlib.CapabilityEtcdBackup,
		"CapabilityEtcdMaintenance":  runnerlib.CapabilityEtcdMaintenance,
		"CapabilityEtcdRestore":      runnerlib.CapabilityEtcdRestore,
		"CapabilityPKIRotate":        runnerlib.CapabilityPKIRotate,
		"CapabilityCredentialRotate": runnerlib.CapabilityCredentialRotate,
		"CapabilityHardeningApply":   runnerlib.CapabilityHardeningApply,
		"CapabilityClusterReset":     runnerlib.CapabilityClusterReset,
		"CapabilityPackCompile":      runnerlib.CapabilityPackCompile,
		"CapabilityPackDeploy":       runnerlib.CapabilityPackDeploy,
		"CapabilityRBACProvision":    runnerlib.CapabilityRBACProvision,
	}

	for name, want := range expected {
		got, ok := actual[name]
		if !ok {
			t.Errorf("constant %s not found in actual map", name)
			continue
		}
		if got != want {
			t.Errorf("%s: got %q, want %q", name, got, want)
		}
	}
}
