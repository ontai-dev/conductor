// compile_launch_test.go tests the compiler launch subcommand.
// Verifies that compileLaunchBundle produces a crds.yaml bundle containing
// CRD YAML from all Seam operators: platform, guardian, wrapper, seam-core,
// conductor. All tests are fully offline.
// conductor-schema.md §9 Step 2.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLaunch_ProducesCRDsYAML verifies that compileLaunchBundle writes
// crds.yaml to the output directory.
func TestLaunch_ProducesCRDsYAML(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "crds.yaml")); err != nil {
		t.Errorf("expected crds.yaml not found: %v", err)
	}
}

// TestLaunch_BundleContainsPlatformCRDs verifies that platform.ontai.dev CRDs
// are present in the bundle. platform-schema.md §5.
func TestLaunch_BundleContainsPlatformCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	// TalosCluster is the root platform CRD.
	assertContainsStr(t, content, "platform.ontai.dev")
	assertContainsStr(t, content, "talosclusters")
}

// TestLaunch_BundleContainsGuardianCRDs verifies that security.ontai.dev CRDs
// are present in the bundle. guardian-schema.md §7.
func TestLaunch_BundleContainsGuardianCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	assertContainsStr(t, content, "security.ontai.dev")
	assertContainsStr(t, content, "rbacprofiles")
}

// TestLaunch_BundleContainsWrapperCRDs verifies that infra.ontai.dev CRDs
// are present in the bundle. wrapper-schema.md.
func TestLaunch_BundleContainsWrapperCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	assertContainsStr(t, content, "infra.ontai.dev")
	assertContainsStr(t, content, "clusterpacks")
}

// TestLaunch_BundleContainsSeamCoreCRDs verifies that infrastructure.ontai.dev
// CRDs (InfrastructureLineageIndex) are present in the bundle.
func TestLaunch_BundleContainsSeamCoreCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	assertContainsStr(t, content, "infrastructure.ontai.dev")
	assertContainsStr(t, content, "infrastructurelineageindices")
}

// TestLaunch_BundleContainsRunnerConfigCRD verifies that the RunnerConfig CRD
// from the conductor shared lib is present in the bundle. conductor-schema.md §5.
func TestLaunch_BundleContainsRunnerConfigCRD(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	assertContainsStr(t, content, "runner.ontai.dev")
	assertContainsStr(t, content, "runnerconfigs")
}

// TestLaunch_BundleIsDeterministic verifies that successive compileLaunchBundle
// calls produce identical output. conductor-design.md §1.2.
func TestLaunch_BundleIsDeterministic(t *testing.T) {
	out1 := t.TempDir()
	out2 := t.TempDir()

	if err := compileLaunchBundle(out1); err != nil {
		t.Fatalf("first compileLaunchBundle: %v", err)
	}
	if err := compileLaunchBundle(out2); err != nil {
		t.Fatalf("second compileLaunchBundle: %v", err)
	}

	data1, _ := os.ReadFile(filepath.Join(out1, "crds.yaml"))
	data2, _ := os.ReadFile(filepath.Join(out2, "crds.yaml"))

	if string(data1) != string(data2) {
		t.Error("compileLaunchBundle is not deterministic: successive calls produced different output")
	}
}

// TestLaunch_BundleContainsCustomResourceDefinitionKind verifies that all entries
// in the bundle declare kind: CustomResourceDefinition.
func TestLaunch_BundleContainsCustomResourceDefinitionKind(t *testing.T) {
	outDir := t.TempDir()
	if err := compileLaunchBundle(outDir); err != nil {
		t.Fatalf("compileLaunchBundle error: %v", err)
	}
	data, _ := os.ReadFile(filepath.Join(outDir, "crds.yaml"))
	content := string(data)

	assertContainsStr(t, content, "kind: CustomResourceDefinition")
	// All entries should be CRDs — none should be non-CRD resources.
	if strings.Contains(content, "kind: Deployment") {
		t.Error("crds.yaml unexpectedly contains Deployment resources")
	}
}
