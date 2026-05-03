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
role: tenant
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
role: management
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
role: management
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
role: management
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

// ── seam-tenant namespace manifest ───────────────────────────────────────────

// TestBootstrap_ImportMode_EmitsSeamTenantNamespaceManifest verifies that
// compileBootstrap with importExistingCluster=true produces seam-tenant-namespace.yaml
// with the correct name (seam-tenant-{clusterName}) and required labels.
// The manifest is required so the admin can apply it before the Secrets (which
// live in seam-tenant-{cluster}) in a single kubectl apply -f compiled/bootstrap/.
// Governor ruling 2026-04-21: mode=import compiler output must include the namespace.
func TestBootstrap_ImportMode_EmitsSeamTenantNamespaceManifest(t *testing.T) {
	mcPath := generateMachineConfigFile(t, "my-cluster", "cp1")
	input := fmt.Sprintf(`
name: my-cluster
namespace: seam-system
mode: import
role: management
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
role: tenant
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
role: management
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
role: management
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

// ── T-01: mode/role discriminator matrix ─────────────────────────────────────

// TestReadClusterInput_ImportModeAbsentRoleDefaultsToManagement verifies that
// readClusterInput accepts mode=import with an absent role field, treating it
// as defaulting to management (consistent with the mgmt-import.yaml fixture).
func TestReadClusterInput_ImportModeAbsentRoleDefaultsToManagement(t *testing.T) {
	input := `
name: my-cluster
namespace: seam-system
mode: import
capi:
  enabled: false
`
	inputPath := writeInputFile(t, input)
	got, err := readClusterInput(inputPath)
	if err != nil {
		t.Fatalf("expected no error for mode=import with absent role; got: %v", err)
	}
	if got.Role != "" {
		t.Errorf("ClusterInput.Role = %q; want empty (clusterRole defaults to management at compile time)", got.Role)
	}
}

// TestReadClusterInput_ImportModeInvalidRoleReturnsError verifies that
// readClusterInput rejects mode=import with a role value other than
// "management" or "tenant".
func TestReadClusterInput_ImportModeInvalidRoleReturnsError(t *testing.T) {
	input := `
name: my-cluster
namespace: seam-system
mode: import
role: worker
capi:
  enabled: false
`
	inputPath := writeInputFile(t, input)
	_, err := readClusterInput(inputPath)
	if err == nil {
		t.Fatal("expected error for mode=import with invalid role; got nil")
	}
	if !containsStr(err.Error(), "worker") {
		t.Errorf("error should mention the invalid role value; got: %v", err)
	}
}

// TestReadClusterInput_ImportModeManagementRoleAccepted verifies that
// readClusterInput accepts mode=import with role=management.
func TestReadClusterInput_ImportModeManagementRoleAccepted(t *testing.T) {
	input := `
name: my-cluster
namespace: seam-system
mode: import
role: management
capi:
  enabled: false
`
	inputPath := writeInputFile(t, input)
	in, err := readClusterInput(inputPath)
	if err != nil {
		t.Fatalf("unexpected error for mode=import role=management: %v", err)
	}
	if in.Role != "management" {
		t.Errorf("expected role=management; got %q", in.Role)
	}
}

// TestReadClusterInput_ImportModeTenantRoleAccepted verifies that
// readClusterInput accepts mode=import with role=tenant.
func TestReadClusterInput_ImportModeTenantRoleAccepted(t *testing.T) {
	input := `
name: my-cluster
namespace: seam-system
mode: import
role: tenant
capi:
  enabled: false
`
	inputPath := writeInputFile(t, input)
	in, err := readClusterInput(inputPath)
	if err != nil {
		t.Fatalf("unexpected error for mode=import role=tenant: %v", err)
	}
	if in.Role != "tenant" {
		t.Errorf("expected role=tenant; got %q", in.Role)
	}
}

// TestBootstrap_BootstrapTenantCAPIFalse_RoleTenantEmitted verifies that compileBootstrap
// with mode=bootstrap and role=tenant emits role: tenant in the TalosCluster CR.
// mode=bootstrap is only permitted for tenant clusters (management clusters must use
// mode=import per the management bootstrap guard in readClusterInput).
func TestBootstrap_BootstrapTenantCAPIFalse_RoleTenantEmitted(t *testing.T) {
	input := `
name: tenant-cluster
namespace: seam-system
mode: bootstrap
role: tenant
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
	crData, err := os.ReadFile(filepath.Join(outDir, "tenant-cluster.yaml"))
	if err != nil {
		t.Fatalf("TalosCluster CR not found: %v", err)
	}
	assertContainsStr(t, string(crData), "role: tenant")
}

// TestBootstrap_BootstrapCAPITrue_RoleTenantEmitted verifies that compileBootstrap
// with mode=bootstrap, role=tenant, and capi.enabled=true emits role: tenant in
// the TalosCluster CR. The role=tenant input is required because mode=bootstrap
// is blocked for role=management (management clusters must use mode=import).
func TestBootstrap_BootstrapCAPITrue_RoleTenantEmitted(t *testing.T) {
	input := `
name: tenant-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: true
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  controlPlaneReplicas: 3
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.20:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.2"
      role: init
`
	inputPath := writeInputFile(t, input)
	outDir := t.TempDir()
	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}
	crData, err := os.ReadFile(filepath.Join(outDir, "tenant-cluster.yaml"))
	if err != nil {
		t.Fatalf("TalosCluster CR not found: %v", err)
	}
	assertContainsStr(t, string(crData), "role: tenant")
}

// TestBootstrap_ImportManagement_RoleEmitted verifies that compileBootstrap
// with mode=import and role=management emits role: management in the CR.
func TestBootstrap_ImportManagement_RoleEmitted(t *testing.T) {
	mcPath := generateMachineConfigFile(t, "mgmt", "cp1")
	input := fmt.Sprintf(`
name: mgmt
namespace: seam-system
mode: import
role: management
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
	crData, err := os.ReadFile(filepath.Join(outDir, "mgmt.yaml"))
	if err != nil {
		t.Fatalf("TalosCluster CR not found: %v", err)
	}
	assertContainsStr(t, string(crData), "role: management")
}

// TestBootstrap_ImportTenant_RoleEmitted verifies that compileBootstrap
// with mode=import and role=tenant emits role: tenant in the CR.
func TestBootstrap_ImportTenant_RoleEmitted(t *testing.T) {
	mcPath := generateMachineConfigFile(t, "tnt", "cp1")
	input := fmt.Sprintf(`
name: tnt
namespace: seam-system
mode: import
role: tenant
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
	crData, err := os.ReadFile(filepath.Join(outDir, "tnt.yaml"))
	if err != nil {
		t.Fatalf("TalosCluster CR not found: %v", err)
	}
	assertContainsStr(t, string(crData), "role: tenant")
}

// ── T-MGMT-BOOTSTRAP-GUARD: management cluster bootstrap guard ───────────────

// TestReadClusterInput_BootstrapManagementRoleRejected verifies that
// readClusterInput rejects mode=bootstrap with role=management, since the
// management cluster cannot be bootstrapped via the compiler.
// Management clusters must use mode=import. The compiler guard prevents
// accidentally generating fresh PKI for a cluster that must be imported.
func TestReadClusterInput_BootstrapManagementRoleRejected(t *testing.T) {
	input := `
name: ccs-mgmt
namespace: seam-system
mode: bootstrap
role: management
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.9.3"
  kubernetesVersion: "1.32.3"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.20.0.11"
      role: init
`
	inputPath := writeInputFile(t, input)
	_, err := readClusterInput(inputPath)
	if err == nil {
		t.Fatal("expected error for mode=bootstrap role=management; got nil")
	}
	if !containsStr(err.Error(), "mode=import") {
		t.Errorf("error should mention mode=import as the correct alternative; got: %v", err)
	}
}

// TestReadClusterInput_BootstrapEmptyRoleRejected verifies that
// readClusterInput rejects mode=bootstrap with an absent role field.
// An absent role defaults to management, which cannot be bootstrapped.
func TestReadClusterInput_BootstrapEmptyRoleRejected(t *testing.T) {
	input := `
name: ccs-mgmt
namespace: seam-system
mode: bootstrap
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.20.0.10:6443"
  talosVersion: "v1.9.3"
  kubernetesVersion: "1.32.3"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.20.0.11"
      role: init
`
	inputPath := writeInputFile(t, input)
	_, err := readClusterInput(inputPath)
	if err == nil {
		t.Fatal("expected error for mode=bootstrap with absent role (defaults to management); got nil")
	}
	if !containsStr(err.Error(), "mode=import") {
		t.Errorf("error should mention mode=import as the correct alternative; got: %v", err)
	}
}

// TestReadClusterInput_BootstrapTenantRoleAccepted verifies that
// readClusterInput accepts mode=bootstrap with role=tenant.
// Tenant clusters may be bootstrapped via the compiler.
func TestReadClusterInput_BootstrapTenantRoleAccepted(t *testing.T) {
	input := `
name: tenant-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.9.3"
  kubernetesVersion: "1.32.3"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`
	inputPath := writeInputFile(t, input)
	in, err := readClusterInput(inputPath)
	if err != nil {
		t.Fatalf("unexpected error for mode=bootstrap role=tenant: %v", err)
	}
	if in.Role != "tenant" {
		t.Errorf("expected role=tenant; got %q", in.Role)
	}
}
