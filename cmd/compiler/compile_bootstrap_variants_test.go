// compile_bootstrap_variants_test.go verifies compiler output conformance for all
// three TalosCluster variants: management import, tenant import, and tenant CAPI.
// Fixtures are in conductor/test/fixtures/compiler/.
// All tests are fully offline -- no cluster connectivity. conductor-schema.md §9.
package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// fixturesDir returns the path to conductor/test/fixtures/compiler/ relative to
// the test binary working directory (cmd/compiler/).
func fixturesDir() string {
	return filepath.Join("..", "..", "test", "fixtures", "compiler")
}

// readFixture reads a named fixture file from the compiler fixtures directory.
func readFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(fixturesDir(), name))
	if err != nil {
		t.Fatalf("read fixture %q: %v", name, err)
	}
	return string(data)
}

// writeFixtureWithMachineConfigPaths produces a modified fixture YAML string with
// machineConfigPaths injected for the named init hostname. Used for import-path
// tests that need offline PKI extraction via a local machine config file.
func writeFixtureWithMachineConfigPaths(t *testing.T, baseYAML, initHostname, mcPath string) string {
	t.Helper()
	return baseYAML + fmt.Sprintf("\nmachineConfigPaths:\n  %s: %s\n", initHostname, mcPath)
}

// assertContainsTrimmed checks that want is present in got, trimming whitespace
// to produce a clean error message.
func assertContainsTrimmed(t *testing.T, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("expected output to contain %q\nactual:\n%s", want, got)
	}
}

// assertAbsent checks that needle is NOT present in got.
func assertAbsent(t *testing.T, got, needle string) {
	t.Helper()
	if strings.Contains(got, needle) {
		t.Errorf("expected output NOT to contain %q\nactual:\n%s", needle, got)
	}
}

// readTalosClusterCR reads and returns the content of {clusterName}.yaml from outDir.
func readTalosClusterCR(t *testing.T, outDir, clusterName string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outDir, clusterName+".yaml"))
	if err != nil {
		t.Fatalf("read TalosCluster CR %s.yaml: %v", clusterName, err)
	}
	return string(data)
}

// ── Test 1: management cluster import (capi.enabled=false) ───────────────────

// TestBootstrapVariants_MgmtImport_TalosClusterConformance verifies that the
// management cluster import fixture produces a TalosCluster CR with the correct
// fields and that the capi block is absent (C-34 regression guard).
// Assertions: apiVersion, kind, mode=import, capi block absent, clusterEndpoint,
// role=management, ontai.dev/owns-runnerconfig annotation.
func TestBootstrapVariants_MgmtImport_TalosClusterConformance(t *testing.T) {
	// Generate a real init-node machine config for offline PKI extraction.
	mcPath := generateMachineConfigFile(t, "ccs-mgmt-import-test", "ccs-mgmt-cp1")

	baseFixture := readFixture(t, "mgmt-import.yaml")
	inputYAML := writeFixtureWithMachineConfigPaths(t, baseFixture, "ccs-mgmt-cp1", mcPath)
	inputPath := writeInputFile(t, inputYAML)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap: %v", err)
	}

	cr := readTalosClusterCR(t, outDir, "ccs-mgmt")

	// Type metadata (infrastructure.ontai.dev -- Decision G, seam-core owns TalosCluster).
	assertContainsTrimmed(t, cr, "apiVersion: infrastructure.ontai.dev/v1alpha1")
	assertContainsTrimmed(t, cr, "kind: InfrastructureTalosCluster")

	// Mode must be import (importExistingCluster=true in fixture).
	assertContainsTrimmed(t, cr, "mode: import")

	// capi block must be absent -- C-34: nil *CAPIConfig suppresses the block.
	assertAbsent(t, cr, "capi:")
	assertAbsent(t, cr, "controlPlane:")

	// Cluster endpoint from bootstrap.controlPlaneEndpoint stripped of scheme.
	assertContainsTrimmed(t, cr, "clusterEndpoint: 10.20.0.10:6443")

	// Role defaults to management when fixture has no role field.
	assertContainsTrimmed(t, cr, "role: management")

	// owns-runnerconfig annotation must be present.
	assertContainsTrimmed(t, cr, "ontai.dev/owns-runnerconfig: \"true\"")

	// Namespace must be seam-system (from fixture).
	assertContainsTrimmed(t, cr, "namespace: seam-system")
}

// ── Test 2: tenant cluster import (capi.enabled=false) ───────────────────────

// TestBootstrapVariants_TenantImport_TalosClusterConformance verifies that the
// tenant import fixture produces a TalosCluster CR in the correct namespace with
// role=tenant and no capi block.
func TestBootstrapVariants_TenantImport_TalosClusterConformance(t *testing.T) {
	// Generate a real init-node machine config for offline PKI extraction.
	mcPath := generateMachineConfigFile(t, "ccs-dev-import-test", "ccs-dev-cp1")

	baseFixture := readFixture(t, "tenant-import.yaml")
	inputYAML := writeFixtureWithMachineConfigPaths(t, baseFixture, "ccs-dev-cp1", mcPath)
	inputPath := writeInputFile(t, inputYAML)
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap: %v", err)
	}

	cr := readTalosClusterCR(t, outDir, "ccs-dev")

	// Mode must be import.
	assertContainsTrimmed(t, cr, "mode: import")

	// capi block must be absent -- C-34 regression guard.
	assertAbsent(t, cr, "capi:")
	assertAbsent(t, cr, "controlPlane:")

	// Role must be tenant -- tenant-import.yaml sets role: tenant explicitly.
	assertContainsTrimmed(t, cr, "role: tenant")

	// Namespace confirms tenant placement.
	assertContainsTrimmed(t, cr, "namespace: seam-tenant-ccs-dev")

	// Cluster endpoint from bootstrap.controlPlaneEndpoint.
	assertContainsTrimmed(t, cr, "clusterEndpoint: 10.20.1.10:6443")
}

// ── Test 3: tenant cluster CAPI bootstrap (capi.enabled=true) ────────────────

// TestBootstrapVariants_TenantCAPI_TalosClusterConformance verifies that the
// CAPI tenant cluster fixture produces a TalosCluster CR with the full CAPI block
// populated: controlPlane.replicas, workers, ciliumPackRef.
// This is the primary regression guard for the CAPIInput struct extension.
func TestBootstrapVariants_TenantCAPI_TalosClusterConformance(t *testing.T) {
	// tenant-capi.yaml uses fresh PKI (importExistingCluster=false) -- offline-capable.
	inputPath := writeInputFile(t, readFixture(t, "tenant-capi.yaml"))
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap: %v", err)
	}

	cr := readTalosClusterCR(t, outDir, "ccs-app")

	// Mode must be bootstrap (importExistingCluster not set in fixture).
	assertContainsTrimmed(t, cr, "mode: bootstrap")

	// CAPI block must be present with enabled=true.
	assertContainsTrimmed(t, cr, "enabled: true")

	// controlPlane block must be present and non-empty (replicas=3).
	assertContainsTrimmed(t, cr, "controlPlane:")
	assertContainsTrimmed(t, cr, "replicas: 3")

	// workers section must have one pool named default with replicas=2.
	assertContainsTrimmed(t, cr, "workers:")
	assertContainsTrimmed(t, cr, "name: default")
	assertContainsTrimmed(t, cr, "replicas: 2")

	// ciliumPackRef must reference the cluster-specific Cilium pack.
	assertContainsTrimmed(t, cr, "ciliumPackRef:")
	assertContainsTrimmed(t, cr, "name: cilium-ccs-app")
	assertContainsTrimmed(t, cr, "version: v1.16.0")

	// Role must be tenant.
	assertContainsTrimmed(t, cr, "role: tenant")

	// Namespace from fixture.
	assertContainsTrimmed(t, cr, "namespace: seam-tenant-ccs-app")

	// Cluster endpoint carried from the bootstrap section.
	assertContainsTrimmed(t, cr, "clusterEndpoint: 10.20.2.10:6443")
}

// TestBootstrapVariants_TenantCAPI_CAPIRoundTrip verifies that the CAPI block in
// the produced TalosCluster CR can be round-tripped through YAML unmarshal and
// that controlPlane.replicas, workers, and ciliumPackRef are all recoverable.
// Guards against silent YAML field name drift between CAPIConfig Go types and
// JSON/YAML tags.
func TestBootstrapVariants_TenantCAPI_CAPIRoundTrip(t *testing.T) {
	inputPath := writeInputFile(t, readFixture(t, "tenant-capi.yaml"))
	outDir := t.TempDir()

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "ccs-app.yaml"))
	if err != nil {
		t.Fatalf("read CR: %v", err)
	}

	var cr map[string]interface{}
	if err := yaml.Unmarshal(data, &cr); err != nil {
		t.Fatalf("unmarshal CR: %v", err)
	}

	spec, ok := cr["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec field missing or wrong type")
	}
	capi, ok := spec["capi"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec.capi field missing or wrong type")
	}

	// controlPlane must be present and non-empty.
	cp, ok := capi["controlPlane"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec.capi.controlPlane missing or wrong type when capi.enabled=true")
	}
	if replicas, _ := cp["replicas"].(float64); int(replicas) != 3 {
		t.Errorf("spec.capi.controlPlane.replicas = %v, want 3", cp["replicas"])
	}

	// workers must have exactly one entry.
	workers, ok := capi["workers"].([]interface{})
	if !ok || len(workers) != 1 {
		t.Fatalf("spec.capi.workers: want 1 entry, got %v", capi["workers"])
	}
	pool, ok := workers[0].(map[string]interface{})
	if !ok {
		t.Fatalf("workers[0] not a map")
	}
	if name, _ := pool["name"].(string); name != "default" {
		t.Errorf("workers[0].name = %q, want %q", name, "default")
	}
	if replicas, _ := pool["replicas"].(float64); int(replicas) != 2 {
		t.Errorf("workers[0].replicas = %v, want 2", pool["replicas"])
	}

	// ciliumPackRef must be present.
	packRef, ok := capi["ciliumPackRef"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec.capi.ciliumPackRef missing or wrong type")
	}
	if name, _ := packRef["name"].(string); name != "cilium-ccs-app" {
		t.Errorf("ciliumPackRef.name = %q, want %q", name, "cilium-ccs-app")
	}
}
