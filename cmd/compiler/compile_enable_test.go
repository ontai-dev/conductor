// compile_enable_test.go tests the compiler enable subcommand.
// Verifies that compileEnableBundle produces the phased directory structure and that
// each phase carries the required content per conductor-schema.md §9 Step 3 and §15.
// All tests are fully offline.
// conductor-schema.md §9, §15, guardian-schema.md §6.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// readPhaseFile reads a file within a named phase subdirectory.
func readPhaseFile(t *testing.T, outDir, phase, filename string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outDir, phase, filename))
	if err != nil {
		t.Fatalf("read %s/%s: %v", phase, filename, err)
	}
	return string(data)
}

// TestEnable_ProducesAllOutputFiles verifies that compileEnableBundle writes all five
// phase subdirectories, each containing a phase-meta.yaml. conductor-schema.md §9 Step 3.
func TestEnable_ProducesAllOutputFiles(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	phases := []struct {
		dir   string
		files []string
	}{
		{"01-guardian-bootstrap", []string{
			"phase-meta.yaml",
			"namespace-labels.yaml",
			"guardian-crds.yaml",
			"guardian-rbac.yaml",
			"guardian-rbacprofiles.yaml",
		}},
		{"02-guardian-deploy", []string{
			"phase-meta.yaml",
			"guardian-deployment.yaml",
		}},
		{"03-platform-wrapper", []string{
			"phase-meta.yaml",
			"platform-wrapper-crds.yaml",
			"platform-wrapper-rbac.yaml",
			"platform-wrapper-rbacprofiles.yaml",
			"platform-wrapper-deployments.yaml",
		}},
		{"04-conductor", []string{
			"phase-meta.yaml",
			"conductor-crds.yaml",
			"conductor-rbac.yaml",
			"conductor-rbacprofile.yaml",
			"conductor-deployment.yaml",
		}},
		{"05-post-bootstrap", []string{
			"phase-meta.yaml",
			"leaderelection.yaml",
		}},
	}

	for _, ph := range phases {
		for _, name := range ph.files {
			p := filepath.Join(outDir, ph.dir, name)
			if _, err := os.Stat(p); err != nil {
				t.Errorf("expected file %s/%s not found: %v", ph.dir, name, err)
			}
		}
	}
}

// TestEnable_ConductorDeploymentCarriesManagementRole verifies that the Conductor
// Deployment in 04-conductor/conductor-deployment.yaml carries CONDUCTOR_ROLE=management.
// This is the Role Declaration Contract stamped by compiler enable. §15.
func TestEnable_ConductorDeploymentCarriesManagementRole(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "v1.9.3-r1"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "04-conductor", "conductor-deployment.yaml")

	// Conductor Deployment must have CONDUCTOR_ROLE=management. §15.
	assertContainsStr(t, content, "CONDUCTOR_ROLE")
	assertContainsStr(t, content, "management")
}

// TestEnable_ConductorInOntSystem verifies that the Conductor Deployment is in
// ont-system and other operators are in seam-system. CONTEXT.md §4 Namespace Model.
func TestEnable_ConductorInOntSystem(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Conductor must be in ont-system.
	conductorDeploy := readPhaseFile(t, outDir, "04-conductor", "conductor-deployment.yaml")
	assertContainsStr(t, conductorDeploy, "namespace: ont-system")

	// Guardian and platform/wrapper operators must be in seam-system.
	guardianDeploy := readPhaseFile(t, outDir, "02-guardian-deploy", "guardian-deployment.yaml")
	assertContainsStr(t, guardianDeploy, "namespace: seam-system")

	pwDeploy := readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-deployments.yaml")
	assertContainsStr(t, pwDeploy, "namespace: seam-system")
}

// TestEnable_OperatorsYAMLContainsAllDeployments verifies that Deployments for all five
// Seam operators are present across the phase deployment files.
func TestEnable_OperatorsYAMLContainsAllDeployments(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect all deployment content across phases 2, 3, 4.
	content := readPhaseFile(t, outDir, "02-guardian-deploy", "guardian-deployment.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-deployments.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-deployment.yaml")

	assertContainsStr(t, content, "kind: Deployment")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "name: "+name) {
			t.Errorf("deployment files do not contain Deployment for %q", name)
		}
	}
}

// TestEnable_RBACYAMLContainsAllOperators verifies that SA, ClusterRole, and
// ClusterRoleBinding exist for each operator across the phase RBAC files.
func TestEnable_RBACYAMLContainsAllOperators(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect all RBAC content across phases 1, 3, 4.
	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbac.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbac.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-rbac.yaml")

	assertContainsStr(t, content, "kind: ServiceAccount")
	assertContainsStr(t, content, "kind: ClusterRole")
	assertContainsStr(t, content, "kind: ClusterRoleBinding")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, name+"-manager-role") {
			t.Errorf("RBAC files do not contain ClusterRole for %q", name)
		}
	}
}

// TestEnable_LeaderElectionYAMLContainsLeases verifies 05-post-bootstrap/leaderelection.yaml
// contains Lease resources for all operators.
func TestEnable_LeaderElectionYAMLContainsLeases(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "05-post-bootstrap", "leaderelection.yaml")

	assertContainsStr(t, content, "kind: Lease")
	// Conductor lease is in ont-system; others in seam-system.
	assertContainsStr(t, content, "conductor-management")
	assertContainsStr(t, content, "platform-leader")
	assertContainsStr(t, content, "guardian-leader")
}

// TestEnable_RBACProfilesYAMLContainsAllProfiles verifies that RBACProfile CRs for all
// five Seam operator service accounts are present across the phase RBACProfile files.
// guardian-schema.md §6 (Seam operator RBACProfiles).
func TestEnable_RBACProfilesYAMLContainsAllProfiles(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect RBACProfile content across phases 1, 3, 4.
	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-rbacprofile.yaml")

	assertContainsStr(t, content, "apiVersion: security.ontai.dev/v1alpha1")
	assertContainsStr(t, content, "kind: RBACProfile")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "rbac-"+name) {
			t.Errorf("RBACProfile files do not contain RBACProfile for %q", name)
		}
	}
}

// TestEnable_RBACProfilesCarryReviewAnnotation verifies that RBACProfile files
// include the human-review annotation. guardian-schema.md §6.
func TestEnable_RBACProfilesCarryReviewAnnotation(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-rbacprofile.yaml")

	assertContainsStr(t, content, "review-required")
}

// TestEnable_OutputIsDeterministic verifies that successive compileEnableBundle calls
// produce identical output for all phase files. conductor-design.md §1.2.
func TestEnable_OutputIsDeterministic(t *testing.T) {
	out1 := t.TempDir()
	out2 := t.TempDir()

	if err := compileEnableBundle(out1, "dev"); err != nil {
		t.Fatalf("first compileEnableBundle: %v", err)
	}
	if err := compileEnableBundle(out2, "dev"); err != nil {
		t.Fatalf("second compileEnableBundle: %v", err)
	}

	checks := []struct{ phase, file string }{
		{"01-guardian-bootstrap", "phase-meta.yaml"},
		{"01-guardian-bootstrap", "namespace-labels.yaml"},
		{"01-guardian-bootstrap", "guardian-crds.yaml"},
		{"01-guardian-bootstrap", "guardian-rbac.yaml"},
		{"01-guardian-bootstrap", "guardian-rbacprofiles.yaml"},
		{"02-guardian-deploy", "phase-meta.yaml"},
		{"02-guardian-deploy", "guardian-deployment.yaml"},
		{"03-platform-wrapper", "phase-meta.yaml"},
		{"03-platform-wrapper", "platform-wrapper-crds.yaml"},
		{"03-platform-wrapper", "platform-wrapper-rbac.yaml"},
		{"03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml"},
		{"03-platform-wrapper", "platform-wrapper-deployments.yaml"},
		{"04-conductor", "phase-meta.yaml"},
		{"04-conductor", "conductor-crds.yaml"},
		{"04-conductor", "conductor-rbac.yaml"},
		{"04-conductor", "conductor-rbacprofile.yaml"},
		{"04-conductor", "conductor-deployment.yaml"},
		{"05-post-bootstrap", "phase-meta.yaml"},
		{"05-post-bootstrap", "leaderelection.yaml"},
	}

	for _, c := range checks {
		d1, err1 := os.ReadFile(filepath.Join(out1, c.phase, c.file))
		d2, err2 := os.ReadFile(filepath.Join(out2, c.phase, c.file))
		if err1 != nil || err2 != nil {
			t.Errorf("read %s/%s: err1=%v err2=%v", c.phase, c.file, err1, err2)
			continue
		}
		if string(d1) != string(d2) {
			t.Errorf("%s/%s is not deterministic: successive calls produced different output", c.phase, c.file)
		}
	}
}

// TestEnable_VersionPropagatesIntoImages verifies that the --version flag value
// appears in operator image references across deployment files.
func TestEnable_VersionPropagatesIntoImages(t *testing.T) {
	outDir := t.TempDir()
	const version = "v1.9.3-r5"
	if err := compileEnableBundle(outDir, version); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Version must appear in all three deployment phase files.
	for _, path := range []struct{ phase, file string }{
		{"02-guardian-deploy", "guardian-deployment.yaml"},
		{"03-platform-wrapper", "platform-wrapper-deployments.yaml"},
		{"04-conductor", "conductor-deployment.yaml"},
	} {
		content := readPhaseFile(t, outDir, path.phase, path.file)
		assertContainsStr(t, content, version)
	}
}

// TestEnable_CRDsYAMLIncludesAllOperatorCRDs verifies that all operator API groups
// are present across the phase CRD files. conductor-schema.md §9 Step 3.
func TestEnable_CRDsYAMLIncludesAllOperatorCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect all CRD content across phases 1, 3, 4.
	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-crds.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-crds.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-crds.yaml")

	for _, group := range []string{
		"platform.ontai.dev",
		"security.ontai.dev",
		"infra.ontai.dev",
		"infrastructure.ontai.dev",
		"runner.ontai.dev",
	} {
		if !strings.Contains(content, group) {
			t.Errorf("CRD files missing API group %q", group)
		}
	}
}

// --- WS2: namespace-labels.yaml tests ---

// TestEnable_NamespaceLabels_BothNamespacesPresent verifies that namespace-labels.yaml
// contains patches for both kube-system and seam-system.
// guardian 25c9e93 WS3 CheckBootstrapLabels contract.
func TestEnable_NamespaceLabels_BothNamespacesPresent(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "namespace-labels.yaml")

	assertContainsStr(t, content, "kube-system")
	assertContainsStr(t, content, "seam-system")
}

// TestEnable_NamespaceLabels_CorrectKindAndLabel verifies that namespace-labels.yaml
// carries kind: Namespace and the correct seam.ontai.dev/webhook-mode=exempt label.
func TestEnable_NamespaceLabels_CorrectKindAndLabel(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "namespace-labels.yaml")

	assertContainsStr(t, content, "kind: Namespace")
	assertContainsStr(t, content, "seam.ontai.dev/webhook-mode")
	assertContainsStr(t, content, "exempt")
}

// TestEnable_NamespaceLabels_IsSSAPatchOnly verifies that namespace-labels.yaml does NOT
// contain spec or status fields — it must be a server-side apply metadata-only patch,
// not a full Namespace manifest. INV-020, CS-INV-004.
func TestEnable_NamespaceLabels_IsSSAPatchOnly(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "namespace-labels.yaml")

	if strings.Contains(content, "spec:") {
		t.Error("namespace-labels.yaml contains spec: — must be SSA metadata-only patch")
	}
	if strings.Contains(content, "status:") {
		t.Error("namespace-labels.yaml contains status: — must be SSA metadata-only patch")
	}
}

// TestEnable_NamespaceLabels_PhaseMeta verifies that 01-guardian-bootstrap/phase-meta.yaml
// declares the correct phase name, order=1, and lists namespace-labels.yaml first
// in applyOrder.
func TestEnable_NamespaceLabels_PhaseMeta(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "phase-meta.yaml")

	assertContainsStr(t, content, "phase: guardian-bootstrap")
	assertContainsStr(t, content, "order: 1")
	// namespace-labels.yaml must be the first entry in applyOrder.
	namespaceIdx := strings.Index(content, "namespace-labels.yaml")
	guardianCRDsIdx := strings.Index(content, "guardian-crds.yaml")
	if namespaceIdx < 0 {
		t.Error("phase-meta.yaml does not list namespace-labels.yaml in applyOrder")
	}
	if guardianCRDsIdx < 0 {
		t.Error("phase-meta.yaml does not list guardian-crds.yaml in applyOrder")
	}
	if namespaceIdx > 0 && guardianCRDsIdx > 0 && namespaceIdx > guardianCRDsIdx {
		t.Error("namespace-labels.yaml must appear before guardian-crds.yaml in applyOrder")
	}
}
