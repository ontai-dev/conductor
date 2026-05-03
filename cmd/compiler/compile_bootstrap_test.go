// compile_bootstrap_test.go tests the compiler bootstrap subcommand.
// Covers input validation, output structure (Secrets, TalosCluster CR,
// bootstrap-sequence.yaml), and naming convention per platform-schema.md §9.
// All tests are fully offline — no cluster connectivity.
// conductor-schema.md §9 Step 1.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// bootstrapInputYAML is a minimal valid tenant cluster bootstrap input used
// for compiler output structure tests. Uses a real (but minimal) Talos version
// string for version contract parsing.
const bootstrapInputYAML = `
name: ccs-mgmt
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: node1
      ip: "10.20.0.11"
      role: init
    - hostname: node2
      ip: "10.20.0.12"
      role: controlplane
    - hostname: node3
      ip: "10.20.0.13"
      role: worker
`

// TestBootstrap_ProducesExpectedOutputFiles verifies that compileBootstrap writes
// exactly the expected output files: one Secret per node, one TalosCluster CR,
// and bootstrap-sequence.yaml.
func TestBootstrap_ProducesExpectedOutputFiles(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, bootstrapInputYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	// Expect 3 node Secrets + 1 TalosCluster + 1 bootstrap-sequence.
	expectedFiles := []string{
		"seam-mc-ccs-mgmt-node1.yaml",
		"seam-mc-ccs-mgmt-node2.yaml",
		"seam-mc-ccs-mgmt-node3.yaml",
		"ccs-mgmt.yaml",
		"bootstrap-sequence.yaml",
	}
	for _, name := range expectedFiles {
		path := filepath.Join(outDir, name)
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected output file %q not found: %v", name, err)
		}
	}
}

// TestBootstrap_SecretHasCorrectStructure verifies that the generated machineconfig
// Secret for the init node has the required Kubernetes Secret fields.
// platform-schema.md §9: naming convention seam-mc-{cluster}-{hostname}.
func TestBootstrap_SecretHasCorrectStructure(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, bootstrapInputYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-ccs-mgmt-node1.yaml"))
	if err != nil {
		t.Fatalf("read Secret YAML: %v", err)
	}
	content := string(data)

	assertContainsStr(t, content, "apiVersion: v1")
	assertContainsStr(t, content, "kind: Secret")
	assertContainsStr(t, content, "name: seam-mc-ccs-mgmt-node1")
	assertContainsStr(t, content, "namespace: seam-tenant-ccs-mgmt")
	assertContainsStr(t, content, "machineconfig.yaml:")
	assertContainsStr(t, content, "ontai.dev/cluster: ccs-mgmt")
}

// TestBootstrap_TalosClusterHasCorrectSpec verifies that the TalosCluster CR
// produced by compileBootstrap has mode=bootstrap and capi.enabled=false
// (management cluster direct path). platform-schema.md §5.
func TestBootstrap_TalosClusterHasCorrectSpec(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, bootstrapInputYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "ccs-mgmt.yaml"))
	if err != nil {
		t.Fatalf("read TalosCluster YAML: %v", err)
	}
	content := string(data)

	assertContainsStr(t, content, "apiVersion: infrastructure.ontai.dev/v1alpha1")
	assertContainsStr(t, content, "kind: InfrastructureTalosCluster")
	assertContainsStr(t, content, "name: ccs-mgmt")
	assertContainsStr(t, content, "mode: bootstrap")
	// capi.enabled=false means nil CAPIConfig pointer -- capi block is absent (C-34).
	if strings.Contains(content, "capi:") {
		t.Errorf("management cluster CR must not contain capi block; got:\n%s", content)
	}
}

// TestBootstrap_SequenceDocumentsApplyOrder verifies that bootstrap-sequence.yaml
// contains both steps in the correct order.
func TestBootstrap_SequenceDocumentsApplyOrder(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, bootstrapInputYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "bootstrap-sequence.yaml"))
	if err != nil {
		t.Fatalf("read bootstrap-sequence.yaml: %v", err)
	}
	content := string(data)

	// C-36: bootstrap-sequence.yaml is now a ConfigMap so kubectl apply works.
	assertContainsStr(t, content, "kind: ConfigMap")
	assertContainsStr(t, content, "seam-bootstrap-sequence-ccs-mgmt")
	// The BootstrapSequence content is embedded in ConfigMap.data["sequence.yaml"].
	assertContainsStr(t, content, "clusterName: ccs-mgmt")
	assertContainsStr(t, content, "seam-mc-ccs-mgmt-node1.yaml")
	assertContainsStr(t, content, "ccs-mgmt.yaml")

	// Verify step ordering by parsing the ConfigMap, extracting sequence.yaml, and
	// unmarshaling the inner BootstrapSequence.
	var cm map[string]interface{}
	if err := yaml.Unmarshal(data, &cm); err != nil {
		t.Fatalf("parse bootstrap-sequence.yaml as ConfigMap: %v", err)
	}
	dataField, ok := cm["data"].(map[string]interface{})
	if !ok {
		t.Fatalf("ConfigMap has no data field")
	}
	seqYAML, ok := dataField["sequence.yaml"].(string)
	if !ok {
		t.Fatalf("ConfigMap data has no sequence.yaml key")
	}
	var seq BootstrapSequence
	if err := yaml.Unmarshal([]byte(seqYAML), &seq); err != nil {
		t.Fatalf("parse inner sequence.yaml: %v", err)
	}
	if len(seq.Steps) != 2 {
		t.Fatalf("expected 2 steps, got %d", len(seq.Steps))
	}
	if seq.Steps[0].Step != 1 {
		t.Errorf("step 0 number = %d, want 1", seq.Steps[0].Step)
	}
	if seq.Steps[1].Step != 2 {
		t.Errorf("step 1 number = %d, want 2", seq.Steps[1].Step)
	}
}

// TestBootstrap_SecretNamingConvention verifies all secrets follow the
// seam-mc-{cluster}-{hostname} naming convention. platform-schema.md §9.
func TestBootstrap_SecretNamingConvention(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, bootstrapInputYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	nodes := []string{"node1", "node2", "node3"}
	for _, hostname := range nodes {
		secretName := "seam-mc-ccs-mgmt-" + hostname
		path := filepath.Join(outDir, secretName+".yaml")
		data, err := os.ReadFile(path)
		if err != nil {
			t.Errorf("secret for %s not found: %v", hostname, err)
			continue
		}
		if !strings.Contains(string(data), secretName) {
			t.Errorf("secret file %s.yaml does not contain name %q", secretName, secretName)
		}
	}
}

// TestBootstrap_MissingBootstrapSectionFails verifies that compileBootstrap
// fails fast when the bootstrap section is absent from the input.
func TestBootstrap_MissingBootstrapSectionFails(t *testing.T) {
	inputYAML := `name: ccs-mgmt
mode: bootstrap
role: tenant
capi:
  enabled: false
`
	inputPath := writeInputFile(t, inputYAML)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Error("expected error for missing bootstrap section, got nil")
	}
}

// TestBootstrap_MissingControlPlaneEndpointFails verifies that missing
// controlPlaneEndpoint causes a validation error.
func TestBootstrap_MissingControlPlaneEndpointFails(t *testing.T) {
	inputYAML := `name: ccs-mgmt
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  nodes:
    - hostname: node1
      ip: "10.20.0.11"
      role: init
`
	inputPath := writeInputFile(t, inputYAML)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Error("expected error for missing controlPlaneEndpoint, got nil")
	}
}

// TestBootstrap_MultipleInitNodesFails verifies that having more than one init
// node causes a validation error.
func TestBootstrap_MultipleInitNodesFails(t *testing.T) {
	inputYAML := `name: ccs-mgmt
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  nodes:
    - hostname: node1
      ip: "10.20.0.11"
      role: init
    - hostname: node2
      ip: "10.20.0.12"
      role: init
`
	inputPath := writeInputFile(t, inputYAML)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Error("expected error for multiple init nodes, got nil")
	}
	if err != nil && !strings.Contains(err.Error(), "init") {
		t.Errorf("error %q should mention 'init'", err.Error())
	}
}

// TestBootstrap_InvalidRoleFails verifies that an unknown node role fails validation.
func TestBootstrap_InvalidRoleFails(t *testing.T) {
	inputYAML := `name: ccs-mgmt
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  nodes:
    - hostname: node1
      ip: "10.20.0.11"
      role: init
    - hostname: node2
      ip: "10.20.0.12"
      role: not-a-real-role
`
	inputPath := writeInputFile(t, inputYAML)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Error("expected error for invalid node role, got nil")
	}
}

// TestBootstrap_DefaultInstallerImageApplied verifies that when no installerImage
// is provided, the default is derived from the talosVersion.
func TestBootstrap_DefaultInstallerImageApplied(t *testing.T) {
	inputYAML := `name: ccs-mgmt
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  nodes:
    - hostname: node1
      ip: "10.20.0.11"
      role: init
`
	inputPath := writeInputFile(t, inputYAML)
	outDir := t.TempDir()
	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	// The Secret YAML should contain the default installer image reference.
	data, _ := os.ReadFile(filepath.Join(outDir, "seam-mc-ccs-mgmt-node1.yaml"))
	content := string(data)
	assertContainsStr(t, content, "machineconfig.yaml:")
	// machineconfig.yaml should contain the default installer image.
	assertContainsStr(t, content, "ghcr.io/siderolabs/installer:v1.7.0")
}

// WS2 — Bootstrap malformed input validation tests.

// TestBootstrap_MissingNameFieldYieldsDescriptiveError verifies that a
// ClusterInput without a name field produces an error that mentions "name".
// conductor-schema.md §9: Compiler fails fast on missing required fields.
func TestBootstrap_MissingNameFieldYieldsDescriptiveError(t *testing.T) {
	const input = `
mode: bootstrap
role: tenant
bootstrap:
  controlPlaneEndpoint: 10.20.0.10
  talosVersion: v1.7.0
  nodes:
    - hostname: cp-0
      ip: 10.20.0.11
      role: init
`
	inputPath := writeInputFile(t, input)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Fatal("expected error for missing name field; got nil")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Errorf("error %q does not mention 'name'", err.Error())
	}
}

// TestBootstrap_CAPIDisabled_NoCapiBlockInCR verifies that a TalosCluster CR
// compiled with capi.enabled=false contains no capi field in YAML output.
// Regression test for C-34: CAPIConfig was a non-pointer struct so omitempty
// never suppressed the block; now it is *CAPIConfig (nil when disabled).
func TestBootstrap_CAPIDisabled_NoCapiBlockInCR(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, bootstrapInputYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	crPath := filepath.Join(outDir, "ccs-mgmt.yaml")
	data, err := os.ReadFile(crPath)
	if err != nil {
		t.Fatalf("read TalosCluster CR: %v", err)
	}
	cr := string(data)
	if strings.Contains(cr, "capi:") {
		t.Errorf("TalosCluster CR with capi.enabled=false must not contain a capi block; got:\n%s", cr)
	}
	if strings.Contains(cr, "controlPlane:") {
		t.Errorf("TalosCluster CR with capi.enabled=false must not contain controlPlane; got:\n%s", cr)
	}
}

// writeInputFile writes YAML content to a temp file and returns its path.
func writeInputFile(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "input-*.yaml")
	if err != nil {
		t.Fatalf("create temp input file: %v", err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("write input file: %v", err)
	}
	f.Close()
	return f.Name()
}
