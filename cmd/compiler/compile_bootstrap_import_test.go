// compile_bootstrap_import_test.go covers the two importExistingCluster CA
// extraction paths: local machine config files (machineConfigPaths) and the
// Kubernetes API fallback (machineConfigPaths absent). conductor-schema.md §9 Step 1.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// generateMachineConfigFile produces a valid Talos init-node machine config YAML
// file for use in import-path tests. It runs compileBootstrap with fresh PKI to
// generate a seam-mc Secret, extracts the machineconfig.yaml field, writes it to
// a temp file, and returns the path.
func generateMachineConfigFile(t *testing.T, clusterName, hostname string) string {
	t.Helper()

	input := fmt.Sprintf(`
name: %s
namespace: seam-system
mode: bootstrap
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: %s
      ip: "10.0.0.1"
      role: init
`, clusterName, hostname)

	inputPath := writeInputFile(t, input)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("generateMachineConfigFile: compileBootstrap failed: %v", err)
	}

	// Read the Secret YAML produced for the init node.
	secretPath := filepath.Join(outDir, fmt.Sprintf("seam-mc-%s-%s.yaml", clusterName, hostname))
	secretData, err := os.ReadFile(secretPath)
	if err != nil {
		t.Fatalf("generateMachineConfigFile: read secret YAML: %v", err)
	}

	// Extract machineconfig.yaml from the Secret's stringData field.
	var secretObj struct {
		StringData map[string]string `yaml:"stringData"`
	}
	if err := yaml.Unmarshal(secretData, &secretObj); err != nil {
		t.Fatalf("generateMachineConfigFile: parse secret YAML: %v", err)
	}
	mcYAML, ok := secretObj.StringData["machineconfig.yaml"]
	if !ok {
		t.Fatal("generateMachineConfigFile: secret missing machineconfig.yaml field")
	}

	// Write the raw machine config YAML to a dedicated temp file.
	f, err := os.CreateTemp(t.TempDir(), "mc-*.yaml")
	if err != nil {
		t.Fatalf("generateMachineConfigFile: create temp file: %v", err)
	}
	if _, err := f.WriteString(mcYAML); err != nil {
		t.Fatalf("generateMachineConfigFile: write machine config: %v", err)
	}
	f.Close()
	return f.Name()
}

// ── Local file path (machineConfigPaths non-empty) ────────────────────────────

// TestBootstrap_ImportExistingCluster_LocalFilePath verifies that when
// importExistingCluster=true and machineConfigPaths is non-empty, Compiler
// reads CA material from the local machine config file and successfully generates
// all output artifacts (machine config Secrets, TalosCluster CR, bootstrap-sequence).
// This path is used for clusters bootstrapped before Seam.
func TestBootstrap_ImportExistingCluster_LocalFilePath(t *testing.T) {
	// Generate a real init-node machine config file from a fresh PKI bundle.
	mcPath := generateMachineConfigFile(t, "import-cluster", "cp1")

	input := fmt.Sprintf(`
name: import-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  cp1: %s
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
    - hostname: wk1
      ip: "10.0.0.2"
      role: worker
`, mcPath)

	inputPath := writeInputFile(t, input)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap (local file path) error: %v", err)
	}

	// All expected output files must be present.
	for _, name := range []string{
		"seam-tenant-namespace.yaml",
		"seam-mc-import-cluster-cp1.yaml",
		"seam-mc-import-cluster-wk1.yaml",
		"import-cluster.yaml",
		"bootstrap-sequence.yaml",
	} {
		if _, err := os.Stat(filepath.Join(outDir, name)); err != nil {
			t.Errorf("expected output file %q not found: %v", name, err)
		}
	}
}

// TestBootstrap_ImportExistingCluster_LocalFileMissingReturnsError verifies that
// when machineConfigPaths is non-empty but the referenced file does not exist,
// Compiler returns an error rather than panicking or silently producing output.
func TestBootstrap_ImportExistingCluster_LocalFileMissingReturnsError(t *testing.T) {
	input := `
name: import-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  cp1: /nonexistent/machineconfig.yaml
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`
	inputPath := writeInputFile(t, input)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Fatal("expected error for missing machine config file; got nil")
	}
}

// TestBootstrap_ImportExistingCluster_InitNodeAbsentFromMapReturnsError verifies
// that when machineConfigPaths is non-empty but the init node hostname is absent
// from the map, Compiler returns an error. The init node entry is required for
// CA extraction; omitting it is a configuration error.
func TestBootstrap_ImportExistingCluster_InitNodeAbsentFromMapReturnsError(t *testing.T) {
	input := `
name: import-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  worker1: /some/path/worker.yaml
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`
	inputPath := writeInputFile(t, input)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Fatal("expected error when init node hostname absent from machineConfigPaths; got nil")
	}
	if !containsStr(err.Error(), "cp1") {
		t.Errorf("error message should mention the missing hostname %q; got: %v", "cp1", err)
	}
}

// ── seam-tenant namespace manifest (WS4) ─────────────────────────────────────

// TestBootstrap_ImportMode_EmitsSeamTenantNamespaceManifest verifies that
// compileBootstrap with importExistingCluster=true produces seam-tenant-namespace.yaml
// with the correct name (seam-tenant-{clusterName}) and required labels.
// Governor ruling 2026-04-21: mode=import compiler output must include the namespace.
func TestBootstrap_ImportMode_EmitsSeamTenantNamespaceManifest(t *testing.T) {
	mcPath := generateMachineConfigFile(t, "my-cluster", "cp1")
	input := fmt.Sprintf(`
name: my-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  cp1: %s
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`, mcPath)

	inputPath := writeInputFile(t, input)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	nsPath := filepath.Join(outDir, "seam-tenant-namespace.yaml")
	nsData, err := os.ReadFile(nsPath)
	if err != nil {
		t.Fatalf("seam-tenant-namespace.yaml not emitted: %v", err)
	}
	content := string(nsData)
	assertContainsStr(t, content, "name: seam-tenant-my-cluster")
	assertContainsStr(t, content, "ontai.dev/tenant: \"true\"")
	assertContainsStr(t, content, "ontai.dev/cluster: my-cluster")
}

// TestBootstrap_BootstrapMode_DoesNotEmitSeamTenantNamespaceManifest verifies that
// compileBootstrap in mode=bootstrap (importExistingCluster=false) does NOT emit
// seam-tenant-namespace.yaml. Platform creates the namespace for bootstrap/CAPI clusters.
// Governor ruling 2026-04-21.
func TestBootstrap_BootstrapMode_DoesNotEmitSeamTenantNamespaceManifest(t *testing.T) {
	input := `
name: fresh-cluster
namespace: seam-system
mode: bootstrap
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`
	inputPath := writeInputFile(t, input)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	nsPath := filepath.Join(outDir, "seam-tenant-namespace.yaml")
	if _, err := os.Stat(nsPath); err == nil {
		t.Error("seam-tenant-namespace.yaml must not be emitted for mode=bootstrap")
	}
}

// TestBootstrap_ImportMode_NamespaceNameIsSeamTenantNotTenant verifies that the
// emitted namespace name is seam-tenant-{clusterName}, not tenant-{clusterName}.
// tenant-{x} is permanently abolished. Governor ruling 2026-04-21.
func TestBootstrap_ImportMode_NamespaceNameIsSeamTenantNotTenant(t *testing.T) {
	mcPath := generateMachineConfigFile(t, "target-cluster", "node1")
	input := fmt.Sprintf(`
name: target-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  node1: %s
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: node1
      ip: "10.0.0.1"
      role: init
`, mcPath)

	inputPath := writeInputFile(t, input)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	nsData, err := os.ReadFile(filepath.Join(outDir, "seam-tenant-namespace.yaml"))
	if err != nil {
		t.Fatalf("seam-tenant-namespace.yaml not found: %v", err)
	}
	content := string(nsData)
	if strings.Contains(content, "name: tenant-target-cluster") {
		t.Error("namespace name must be seam-tenant-target-cluster, not tenant-target-cluster")
	}
	assertContainsStr(t, content, "name: seam-tenant-target-cluster")
}

// ── Kubernetes API fallback (machineConfigPaths absent) ───────────────────────

// TestBootstrap_ImportExistingCluster_KubeconfigFallback verifies that when
// importExistingCluster=true and machineConfigPaths is absent, Compiler falls
// back to the Kubernetes API path and returns an error when the kubeconfig
// is unreachable. This is the existing Seam-cluster import path.
func TestBootstrap_ImportExistingCluster_KubeconfigFallback(t *testing.T) {
	input := `
name: import-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
importExistingCluster: true
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`
	inputPath := writeInputFile(t, input)
	// Pass a non-existent kubeconfig — the API path must fail with an error.
	err := compileBootstrap(inputPath, t.TempDir(), "/nonexistent/kubeconfig.yaml", "")
	if err == nil {
		t.Fatal("expected error for missing kubeconfig in API fallback path; got nil")
	}
}
