// compile_enable_test.go tests the compiler enable subcommand.
// Verifies that compileEnableBundle produces all five manifest files and that
// each carries the required content per conductor-schema.md §9 Step 3 and §15.
// All tests are fully offline.
// conductor-schema.md §9, §15, guardian-schema.md §6.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestEnable_ProducesAllOutputFiles verifies that compileEnableBundle writes
// all five required manifest files. conductor-schema.md §9 Step 3.
func TestEnable_ProducesAllOutputFiles(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	expectedFiles := []string{
		"crds.yaml",
		"operators.yaml",
		"rbac.yaml",
		"leaderelection.yaml",
		"rbacprofiles.yaml",
	}
	for _, name := range expectedFiles {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Errorf("expected output file %q not found: %v", name, err)
		}
	}
}

// TestEnable_ConductorDeploymentCarriesManagementRole verifies that the Conductor
// Deployment in operators.yaml carries CONDUCTOR_ROLE=management.
// This is the Role Declaration Contract stamped by compiler enable. §15.
func TestEnable_ConductorDeploymentCarriesManagementRole(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "v1.9.3-r1"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "operators.yaml"))
	if err != nil {
		t.Fatalf("read operators.yaml: %v", err)
	}
	content := string(data)

	// Conductor Deployment must have CONDUCTOR_ROLE=management. §15.
	assertContainsStr(t, content, "CONDUCTOR_ROLE")
	assertContainsStr(t, content, "management")
}

// TestEnable_ConductorInOntSystem verifies that the Conductor Deployment is in
// ont-system, not seam-system. CONTEXT.md §4 Namespace Model.
func TestEnable_ConductorInOntSystem(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "operators.yaml"))
	content := string(data)

	assertContainsStr(t, content, "namespace: ont-system")
	// All other operators must be in seam-system.
	assertContainsStr(t, content, "namespace: seam-system")
}

// TestEnable_OperatorsYAMLContainsAllDeployments verifies operators.yaml includes
// Deployments for all five Seam operators.
func TestEnable_OperatorsYAMLContainsAllDeployments(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "operators.yaml"))
	content := string(data)

	assertContainsStr(t, content, "kind: Deployment")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "name: "+name) {
			t.Errorf("operators.yaml does not contain Deployment for %q", name)
		}
	}
}

// TestEnable_RBACYAMLContainsAllOperators verifies rbac.yaml includes SA,
// ClusterRole, and ClusterRoleBinding for each operator.
func TestEnable_RBACYAMLContainsAllOperators(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "rbac.yaml"))
	content := string(data)

	assertContainsStr(t, content, "kind: ServiceAccount")
	assertContainsStr(t, content, "kind: ClusterRole")
	assertContainsStr(t, content, "kind: ClusterRoleBinding")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, name+"-manager-role") {
			t.Errorf("rbac.yaml does not contain ClusterRole for %q", name)
		}
	}
}

// TestEnable_LeaderElectionYAMLContainsLeases verifies leaderelection.yaml
// contains Lease resources for all operators.
func TestEnable_LeaderElectionYAMLContainsLeases(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "leaderelection.yaml"))
	content := string(data)

	assertContainsStr(t, content, "kind: Lease")
	// Conductor lease is in ont-system; others in seam-system.
	assertContainsStr(t, content, "conductor-management")
	assertContainsStr(t, content, "platform-leader")
	assertContainsStr(t, content, "guardian-leader")
}

// TestEnable_RBACProfilesYAMLContainsAllProfiles verifies rbacprofiles.yaml
// includes RBACProfile CRs for all five Seam operator service accounts.
// guardian-schema.md §6 (Seam operator RBACProfiles).
func TestEnable_RBACProfilesYAMLContainsAllProfiles(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "rbacprofiles.yaml"))
	content := string(data)

	assertContainsStr(t, content, "apiVersion: security.ontai.dev/v1alpha1")
	assertContainsStr(t, content, "kind: RBACProfile")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "rbac-"+name) {
			t.Errorf("rbacprofiles.yaml does not contain RBACProfile for %q", name)
		}
	}
}

// TestEnable_RBACProfilesCarryReviewAnnotation verifies that rbacprofiles.yaml
// includes the human-review annotation. guardian-schema.md §6.
func TestEnable_RBACProfilesCarryReviewAnnotation(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "rbacprofiles.yaml"))
	assertContainsStr(t, string(data), "review-required")
}

// TestEnable_OutputIsDeterministic verifies that successive compileEnableBundle
// calls produce identical output for all five manifest files.
// conductor-design.md §1.2 Deterministic Execution.
func TestEnable_OutputIsDeterministic(t *testing.T) {
	out1 := t.TempDir()
	out2 := t.TempDir()

	if err := compileEnableBundle(out1, "dev"); err != nil {
		t.Fatalf("first compileEnableBundle: %v", err)
	}
	if err := compileEnableBundle(out2, "dev"); err != nil {
		t.Fatalf("second compileEnableBundle: %v", err)
	}

	for _, name := range []string{"crds.yaml", "operators.yaml", "rbac.yaml",
		"leaderelection.yaml", "rbacprofiles.yaml"} {
		d1, _ := os.ReadFile(filepath.Join(out1, name))
		d2, _ := os.ReadFile(filepath.Join(out2, name))
		if string(d1) != string(d2) {
			t.Errorf("%s is not deterministic: successive calls produced different output", name)
		}
	}
}

// TestEnable_VersionPropagatesIntoImages verifies that the --version flag value
// appears in operator image references in operators.yaml.
func TestEnable_VersionPropagatesIntoImages(t *testing.T) {
	outDir := t.TempDir()
	const version = "v1.9.3-r5"
	if err := compileEnableBundle(outDir, version); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "operators.yaml"))
	assertContainsStr(t, string(data), version)
}

// TestEnable_CRDsYAMLIncludesAllOperatorCRDs verifies that the crds.yaml produced
// by enable is the same complete CRD bundle as compiler launch.
func TestEnable_CRDsYAMLIncludesAllOperatorCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev"); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	for _, group := range []string{
		"platform.ontai.dev",
		"security.ontai.dev",
		"infra.ontai.dev",
		"infrastructure.ontai.dev",
		"runner.ontai.dev",
	} {
		if !strings.Contains(content, group) {
			t.Errorf("crds.yaml missing API group %q", group)
		}
	}
}
