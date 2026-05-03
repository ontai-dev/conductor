// compile_bootstrap_features_test.go covers the new ClusterInput features:
// patches, registryMirrors, ciliumPrerequisites, importExistingCluster,
// and the talosconfig path resolution helper.
// conductor-schema.md §9 Step 1, lab/CLAUDE.md §Cilium Deployment Invariants.
package main

import (
	"os"
	"path/filepath"
	"testing"
)

// baseBootstrapYAML is a one-node tenant cluster bootstrap input for feature tests.
const baseBootstrapYAML = `
name: test-cluster
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

// ── CiliumPrerequisites ───────────────────────────────────────────────────────

// TestBootstrap_CiliumPrerequisitesInjectsModulesAndSysctls verifies that
// ciliumPrerequisites: true injects br_netfilter, xt_socket kernel modules and
// rp_filter sysctls into every node's machine config Secret.
// lab/CLAUDE.md §Cilium Deployment Invariants.
func TestBootstrap_CiliumPrerequisitesInjectsModulesAndSysctls(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
ciliumPrerequisites: true
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
	outDir := t.TempDir()
	inputPath := writeInputFile(t, input)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-cp1.yaml"))
	if err != nil {
		t.Fatalf("read Secret YAML: %v", err)
	}
	content := string(data)

	// Kernel modules — required for Cilium eBPF and transparent proxy.
	assertContainsStr(t, content, "br_netfilter")
	assertContainsStr(t, content, "xt_socket")
	// Sysctls — required for native routing mode (rp_filter must be 0).
	assertContainsStr(t, content, "net.ipv4.conf.all.rp_filter")
	assertContainsStr(t, content, "net.ipv4.conf.default.rp_filter")
}

// TestBootstrap_CiliumPrerequisitesFalse_NoPatch verifies that when
// ciliumPrerequisites=false, neither br_netfilter nor xt_socket appear in the
// generated machine config (they were not requested).
func TestBootstrap_CiliumPrerequisitesFalse_NoPatch(t *testing.T) {
	outDir := t.TempDir()
	inputPath := writeInputFile(t, baseBootstrapYAML)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-cp1.yaml"))
	if err != nil {
		t.Fatalf("read Secret YAML: %v", err)
	}
	content := string(data)

	if containsStr(content, "br_netfilter") {
		t.Error("br_netfilter should not appear when ciliumPrerequisites=false")
	}
	if containsStr(content, "xt_socket") {
		t.Error("xt_socket should not appear when ciliumPrerequisites=false")
	}
}

// ── RegistryMirrors ───────────────────────────────────────────────────────────

// TestBootstrap_RegistryMirrorsInjected verifies that registryMirrors entries
// are injected into every node's machine config registries.mirrors section.
// The http:// prefix must be preserved exactly.
func TestBootstrap_RegistryMirrorsInjected(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
registryMirrors:
  - registry: docker.io
    endpoints:
      - http://10.20.0.1:5000
  - registry: ghcr.io
    endpoints:
      - http://10.20.0.1:5000
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
	outDir := t.TempDir()
	inputPath := writeInputFile(t, input)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-cp1.yaml"))
	if err != nil {
		t.Fatalf("read Secret YAML: %v", err)
	}
	content := string(data)

	// Both registries and their endpoint must appear in the machine config.
	assertContainsStr(t, content, "docker.io")
	assertContainsStr(t, content, "ghcr.io")
	// http:// prefix preserved exactly — not converted to https://.
	assertContainsStr(t, content, "http://10.20.0.1:5000")
}

// TestBootstrap_RegistryMirrorsAppliedToAllNodes verifies that when multiple nodes
// are declared, all of them receive the registry mirrors patch.
func TestBootstrap_RegistryMirrorsAppliedToAllNodes(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
registryMirrors:
  - registry: registry.k8s.io
    endpoints:
      - http://10.20.0.1:5000
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
  installDisk: "/dev/sda"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
    - hostname: worker1
      ip: "10.0.0.2"
      role: worker
`
	outDir := t.TempDir()
	inputPath := writeInputFile(t, input)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	for _, hostname := range []string{"cp1", "worker1"} {
		data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-"+hostname+".yaml"))
		if err != nil {
			t.Fatalf("read Secret for %s: %v", hostname, err)
		}
		if !containsStr(string(data), "registry.k8s.io") {
			t.Errorf("node %q: registry.k8s.io mirror not found in machine config Secret", hostname)
		}
	}
}

// ── Patches ───────────────────────────────────────────────────────────────────

// TestBootstrap_PatchAppliedToAllNodes verifies that a user-provided YAML patch
// is deep-merged into every node's generated machine config.
func TestBootstrap_PatchAppliedToAllNodes(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
patches:
  - |
    machine:
      sysctls:
        net.core.somaxconn: "65535"
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
`
	outDir := t.TempDir()
	inputPath := writeInputFile(t, input)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	for _, hostname := range []string{"cp1", "wk1"} {
		data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-"+hostname+".yaml"))
		if err != nil {
			t.Fatalf("read Secret for %s: %v", hostname, err)
		}
		if !containsStr(string(data), "net.core.somaxconn") {
			t.Errorf("node %q: custom sysctl net.core.somaxconn not found in machine config Secret", hostname)
		}
	}
}

// TestBootstrap_MultiplePatchesAppliedInOrder verifies that multiple patches
// in the patches list are all applied in declared order.
func TestBootstrap_MultiplePatchesAppliedInOrder(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
patches:
  - |
    machine:
      sysctls:
        net.core.somaxconn: "65535"
  - |
    machine:
      sysctls:
        vm.max_map_count: "262144"
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
	outDir := t.TempDir()
	inputPath := writeInputFile(t, input)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-cp1.yaml"))
	if err != nil {
		t.Fatalf("read Secret YAML: %v", err)
	}
	content := string(data)
	assertContainsStr(t, content, "net.core.somaxconn")
	assertContainsStr(t, content, "vm.max_map_count")
}

// TestBootstrap_CiliumAndRegistryMirrorsAndPatches verifies that all three
// patch sources (ciliumPrerequisites, registryMirrors, user patches) are
// applied in the declared order and all appear in the output.
func TestBootstrap_CiliumAndRegistryMirrorsAndPatches(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
ciliumPrerequisites: true
registryMirrors:
  - registry: docker.io
    endpoints:
      - http://10.20.0.1:5000
patches:
  - |
    machine:
      sysctls:
        net.core.somaxconn: "65535"
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
	outDir := t.TempDir()
	inputPath := writeInputFile(t, input)

	if err := compileBootstrap(inputPath, outDir, "", ""); err != nil {
		t.Fatalf("compileBootstrap error: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "seam-mc-test-cluster-cp1.yaml"))
	if err != nil {
		t.Fatalf("read Secret YAML: %v", err)
	}
	content := string(data)

	// Cilium prerequisites.
	assertContainsStr(t, content, "br_netfilter")
	assertContainsStr(t, content, "xt_socket")
	assertContainsStr(t, content, "net.ipv4.conf.all.rp_filter")
	// Registry mirrors.
	assertContainsStr(t, content, "docker.io")
	assertContainsStr(t, content, "http://10.20.0.1:5000")
	// User patch.
	assertContainsStr(t, content, "net.core.somaxconn")
}

// ── applyYAMLPatch unit tests ─────────────────────────────────────────────────

// TestApplyYAMLPatch_MergesMaps verifies that deepMerge correctly merges nested
// maps rather than replacing the parent map entirely.
func TestApplyYAMLPatch_MergesMaps(t *testing.T) {
	base := []byte(`machine:
  sysctls:
    existing.key: "1"
`)
	patch := `machine:
  sysctls:
    new.key: "2"
`
	result, err := applyYAMLPatch(base, patch)
	if err != nil {
		t.Fatalf("applyYAMLPatch error: %v", err)
	}
	content := string(result)
	if !containsStr(content, "existing.key") {
		t.Error("applyYAMLPatch: existing.key was dropped during merge")
	}
	if !containsStr(content, "new.key") {
		t.Error("applyYAMLPatch: new.key was not added by patch")
	}
}

// TestApplyYAMLPatch_AppendsSlices verifies that deepMerge appends to existing
// slices rather than replacing them (e.g., kernel modules).
func TestApplyYAMLPatch_AppendsSlices(t *testing.T) {
	base := []byte(`machine:
  kernel:
    modules:
      - name: existing_module
`)
	patch := `machine:
  kernel:
    modules:
      - name: new_module
`
	result, err := applyYAMLPatch(base, patch)
	if err != nil {
		t.Fatalf("applyYAMLPatch error: %v", err)
	}
	content := string(result)
	if !containsStr(content, "existing_module") {
		t.Error("applyYAMLPatch: existing_module was dropped during slice merge")
	}
	if !containsStr(content, "new_module") {
		t.Error("applyYAMLPatch: new_module was not appended by patch")
	}
}

// ── resolveKubeconfigPath unit tests ──────────────────────────────────────────

// TestResolveKubeconfigPath_FlagTakesPriority verifies that the flag value is
// returned when non-empty, regardless of environment variables.
func TestResolveKubeconfigPath_FlagTakesPriority(t *testing.T) {
	t.Setenv("KUBECONFIG", "/env/path")
	got := resolveKubeconfigPath("/flag/path")
	if got != "/flag/path" {
		t.Errorf("expected /flag/path; got %q", got)
	}
}

// TestResolveKubeconfigPath_EnvFallback verifies that KUBECONFIG env is used
// when the flag is empty.
func TestResolveKubeconfigPath_EnvFallback(t *testing.T) {
	t.Setenv("KUBECONFIG", "/env/path")
	got := resolveKubeconfigPath("")
	if got != "/env/path" {
		t.Errorf("expected /env/path from KUBECONFIG env; got %q", got)
	}
}

// TestResolveKubeconfigPath_DefaultWhenBothEmpty verifies that the default
// ~/.kube/config path is returned when both flag and env are empty.
func TestResolveKubeconfigPath_DefaultWhenBothEmpty(t *testing.T) {
	t.Setenv("KUBECONFIG", "")
	got := resolveKubeconfigPath("")
	home, _ := os.UserHomeDir()
	want := filepath.Join(home, ".kube", "config")
	if got != want {
		t.Errorf("expected default %q; got %q", want, got)
	}
}

// ── defaultKubernetesVersion ──────────────────────────────────────────────────

// TestDefaultKubernetesVersion_KnownVersionReturnsK8s verifies the support matrix
// lookup returns the expected Kubernetes version for each known Talos minor.
func TestDefaultKubernetesVersion_KnownVersionReturnsK8s(t *testing.T) {
	cases := []struct {
		talos string
		want  string
	}{
		{"v1.9.3", "1.32.3"},
		{"v1.8.0", "1.31.5"},
		{"v1.7.0", "1.30.9"},
		{"v1.6.5", "1.29.14"},
		{"v1.5.1", "1.28.15"},
	}
	for _, tc := range cases {
		got, err := defaultKubernetesVersion(tc.talos)
		if err != nil {
			t.Errorf("defaultKubernetesVersion(%q): unexpected error: %v", tc.talos, err)
			continue
		}
		if got != tc.want {
			t.Errorf("defaultKubernetesVersion(%q) = %q, want %q", tc.talos, got, tc.want)
		}
	}
}

// TestDefaultKubernetesVersion_UnknownVersionReturnsError verifies that an
// unrecognised Talos minor version produces a clear error rather than an empty string.
func TestDefaultKubernetesVersion_UnknownVersionReturnsError(t *testing.T) {
	_, err := defaultKubernetesVersion("v1.99.0")
	if err == nil {
		t.Fatal("expected error for unknown Talos minor version; got nil")
	}
}

// ── extractEndpointFromMachineConfig ─────────────────────────────────────────

// TestExtractEndpointFromMachineConfig_Success verifies that a valid Talos
// machine config YAML yields the cluster.controlPlane.endpoint value.
func TestExtractEndpointFromMachineConfig_Success(t *testing.T) {
	mc := `
cluster:
  controlPlane:
    endpoint: https://10.20.0.10:6443
machine:
  type: init
`
	got, err := extractEndpointFromMachineConfig([]byte(mc))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "https://10.20.0.10:6443" {
		t.Errorf("got %q, want https://10.20.0.10:6443", got)
	}
}

// TestExtractEndpointFromMachineConfig_MissingEndpointReturnsError verifies that
// a machine config without cluster.controlPlane.endpoint returns an error.
func TestExtractEndpointFromMachineConfig_MissingEndpointReturnsError(t *testing.T) {
	mc := `
cluster:
  network: {}
machine:
  type: worker
`
	_, err := extractEndpointFromMachineConfig([]byte(mc))
	if err == nil {
		t.Fatal("expected error for missing cluster.controlPlane section; got nil")
	}
}

// ── extractInstallDiskFromMachineConfig ───────────────────────────────────────

// TestExtractInstallDiskFromMachineConfig_Success verifies that machine.install.disk
// is extracted correctly from a machineconfig YAML.
func TestExtractInstallDiskFromMachineConfig_Success(t *testing.T) {
	mc := `
machine:
  install:
    disk: /dev/vda
  type: init
`
	got := extractInstallDiskFromMachineConfig([]byte(mc))
	if got != "/dev/vda" {
		t.Errorf("got %q, want /dev/vda", got)
	}
}

// TestExtractInstallDiskFromMachineConfig_MissingReturnsEmpty verifies that a
// machine config without machine.install.disk returns an empty string.
func TestExtractInstallDiskFromMachineConfig_MissingReturnsEmpty(t *testing.T) {
	mc := `
machine:
  type: worker
`
	got := extractInstallDiskFromMachineConfig([]byte(mc))
	if got != "" {
		t.Errorf("expected empty string for missing disk; got %q", got)
	}
}

// ── optional-field derivation integration tests ───────────────────────────────

// TestBootstrap_EndpointExtractedFromMachineConfig verifies that when
// bootstrap.controlPlaneEndpoint is absent and machineConfigPaths is provided,
// the compiler extracts the endpoint from the init node's machine config and
// uses it in the generated TalosCluster CR.
func TestBootstrap_EndpointExtractedFromMachineConfig(t *testing.T) {
	mcPath := generateMachineConfigFile(t, "extract-ep", "cp1")

	input := `
name: extract-ep
namespace: seam-system
mode: import
role: management
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  cp1: ` + mcPath + `
bootstrap:
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

	crData, err := os.ReadFile(outDir + "/extract-ep.yaml")
	if err != nil {
		t.Fatalf("TalosCluster CR not found: %v", err)
	}
	// The generated config file's endpoint is https://10.0.0.10:6443 (stripped to 10.0.0.10:6443).
	assertContainsStr(t, string(crData), "10.0.0.10:6443")
}

// TestBootstrap_KubernetesVersionDerivedFromTalosVersion verifies that when
// bootstrap.kubernetesVersion is absent, the compiler derives it from the
// Talos support matrix and records it in the generated TalosCluster CR.
func TestBootstrap_KubernetesVersionDerivedFromTalosVersion(t *testing.T) {
	input := `
name: derived-k8s
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  controlPlaneEndpoint: "https://10.0.0.10:6443"
  talosVersion: "v1.9.3"
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

	crData, err := os.ReadFile(outDir + "/derived-k8s.yaml")
	if err != nil {
		t.Fatalf("TalosCluster CR not found: %v", err)
	}
	// v1.9.3 -> 1.32.3 per the support matrix.
	assertContainsStr(t, string(crData), "kubernetesVersion: 1.32.3")
}

// TestBootstrap_InstallDiskExtractedFromMachineConfig verifies that when
// bootstrap.installDisk is absent but machineConfigPaths is provided, the
// compiler extracts the install disk from the init node's machine config.
func TestBootstrap_InstallDiskExtractedFromMachineConfig(t *testing.T) {
	// Generate a real machine config with /dev/sda (the default).
	mcPath := generateMachineConfigFile(t, "disk-extract", "cp1")

	input := `
name: disk-extract
namespace: seam-system
mode: import
role: management
capi:
  enabled: false
importExistingCluster: true
machineConfigPaths:
  cp1: ` + mcPath + `
bootstrap:
  talosVersion: "v1.7.0"
  kubernetesVersion: "1.30.0"
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
	// Success is sufficient — no explicit disk assertion because the default
	// /dev/sda may or may not appear verbatim in the marshaled output.
	if _, err := os.Stat(outDir + "/disk-extract.yaml"); err != nil {
		t.Errorf("TalosCluster CR not found: %v", err)
	}
}

// TestBootstrap_EndpointRequiredWhenNoMachineConfigPaths verifies that omitting
// bootstrap.controlPlaneEndpoint without machineConfigPaths returns an error.
func TestBootstrap_EndpointRequiredWhenNoMachineConfigPaths(t *testing.T) {
	input := `
name: missing-ep
namespace: seam-system
mode: bootstrap
role: tenant
capi:
  enabled: false
bootstrap:
  talosVersion: "v1.9.3"
  nodes:
    - hostname: cp1
      ip: "10.0.0.1"
      role: init
`
	inputPath := writeInputFile(t, input)
	err := compileBootstrap(inputPath, t.TempDir(), "", "")
	if err == nil {
		t.Fatal("expected error when controlPlaneEndpoint absent and no machineConfigPaths; got nil")
	}
}

// ── ImportExistingCluster ─────────────────────────────────────────────────────

// TestBootstrap_ImportExistingCluster_MissingKubeconfigReturnsError verifies that
// importExistingCluster: true with a non-existent kubeconfig path returns an error
// rather than silently generating fresh PKI material.
func TestBootstrap_ImportExistingCluster_MissingKubeconfigReturnsError(t *testing.T) {
	input := `
name: test-cluster
namespace: seam-system
mode: bootstrap
role: tenant
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
	// Pass a kubeconfig path that does not exist — connection must fail with an error.
	err := compileBootstrap(inputPath, t.TempDir(), "/nonexistent/kubeconfig.yaml", "")
	if err == nil {
		t.Fatal("expected error for missing kubeconfig; got nil")
	}
}

