package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// WS1 — PackBuild compile-only contract unit tests.
// compilePackBuild is fully offline: it reads a PackBuildInput YAML file and
// writes a ClusterPack CR YAML. No cluster access. No API calls.

// writePackBuildInput writes a PackBuildInput YAML file to a temp directory
// and returns the file path.
func writePackBuildInput(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "packbuild.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatalf("write packbuild input: %v", err)
	}
	return path
}

const validPackBuildInput = `
name: my-pack
version: v1.2.3
registryUrl: registry.example.com/packs/my-pack
digest: sha256:abc123def456abc123def456abc123def456abc123def456abc123def456abc123
checksum: deadbeef
sourceBuildRef: build-0042
`

// TestPackBuild_ProducesClusterPackYAML verifies that a valid PackBuildInput
// produces exactly one output file named after the pack.
// No API calls: compilePackBuild is a pure file-in / file-out function.
func TestPackBuild_ProducesClusterPackYAML(t *testing.T) {
	inputPath := writePackBuildInput(t, validPackBuildInput)
	outDir := t.TempDir()

	if err := compilePackBuild(inputPath, outDir); err != nil {
		t.Fatalf("compilePackBuild: %v", err)
	}

	outPath := filepath.Join(outDir, "my-pack.yaml")
	if _, err := os.Stat(outPath); os.IsNotExist(err) {
		t.Fatalf("expected output file %q; not found", outPath)
	}
}

// TestPackBuild_ClusterPackHasCorrectAPIVersionAndKind verifies the emitted
// ClusterPack CR carries apiVersion=infrastructure.ontai.dev/v1alpha1 and kind=InfrastructureClusterPack.
// conductor-schema.md §9.
func TestPackBuild_ClusterPackHasCorrectAPIVersionAndKind(t *testing.T) {
	inputPath := writePackBuildInput(t, validPackBuildInput)
	outDir := t.TempDir()

	if err := compilePackBuild(inputPath, outDir); err != nil {
		t.Fatalf("compilePackBuild: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "my-pack.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	var cr map[string]interface{}
	if err := yaml.Unmarshal(data, &cr); err != nil {
		t.Fatalf("parse output YAML: %v", err)
	}

	if cr["apiVersion"] != "infrastructure.ontai.dev/v1alpha1" {
		t.Errorf("apiVersion: got %q; want %q", cr["apiVersion"], "infrastructure.ontai.dev/v1alpha1")
	}
	if cr["kind"] != "InfrastructureClusterPack" {
		t.Errorf("kind: got %q; want %q", cr["kind"], "InfrastructureClusterPack")
	}
}

// TestPackBuild_ClusterPackHasCorrectSpecFields verifies the ClusterPack spec
// carries version, registryUrl/digest, checksum, and sourceBuildRef from the input.
func TestPackBuild_ClusterPackHasCorrectSpecFields(t *testing.T) {
	inputPath := writePackBuildInput(t, validPackBuildInput)
	outDir := t.TempDir()

	if err := compilePackBuild(inputPath, outDir); err != nil {
		t.Fatalf("compilePackBuild: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "my-pack.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	content := string(data)
	for _, want := range []string{
		"v1.2.3",
		"registry.example.com/packs/my-pack",
		"sha256:abc123def456",
		"deadbeef",
		"build-0042",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("output YAML missing %q", want)
		}
	}
}

// TestPackBuild_DefaultNamespaceIsSeamSystem verifies that when the input omits
// namespace, the output CR is placed in seam-system.
func TestPackBuild_DefaultNamespaceIsSeamSystem(t *testing.T) {
	const noNamespaceInput = `
name: ns-test-pack
version: v0.1.0
registryUrl: registry.example.com/packs/ns-test
digest: sha256:0000000000000000000000000000000000000000000000000000000000000000
`
	inputPath := writePackBuildInput(t, noNamespaceInput)
	outDir := t.TempDir()

	if err := compilePackBuild(inputPath, outDir); err != nil {
		t.Fatalf("compilePackBuild: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "ns-test-pack.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	if !strings.Contains(string(data), "seam-system") {
		t.Errorf("expected namespace seam-system in output; not found:\n%s", data)
	}
}

// WS2 — PackBuild malformed input validation tests.

// TestPackBuild_MissingNameFails verifies that a PackBuildInput without a name
// field returns a descriptive error containing "name".
func TestPackBuild_MissingNameFails(t *testing.T) {
	const input = `
version: v1.0.0
registryUrl: registry.example.com/packs/foo
digest: sha256:abc
`
	inputPath := writePackBuildInput(t, input)
	err := compilePackBuild(inputPath, t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing name; got nil")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Errorf("error %q does not mention 'name'", err.Error())
	}
}

// TestPackBuild_MissingVersionFails verifies that a PackBuildInput without version
// returns a descriptive error containing "version".
func TestPackBuild_MissingVersionFails(t *testing.T) {
	const input = `
name: my-pack
registryUrl: registry.example.com/packs/foo
digest: sha256:abc
`
	inputPath := writePackBuildInput(t, input)
	err := compilePackBuild(inputPath, t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing version; got nil")
	}
	if !strings.Contains(err.Error(), "version") {
		t.Errorf("error %q does not mention 'version'", err.Error())
	}
}

// TestPackBuild_MissingRegistryURLFails verifies that a PackBuildInput without
// registryUrl returns a descriptive error containing "registryUrl".
func TestPackBuild_MissingRegistryURLFails(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
digest: sha256:abc
`
	inputPath := writePackBuildInput(t, input)
	err := compilePackBuild(inputPath, t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing registryUrl; got nil")
	}
	if !strings.Contains(err.Error(), "registryUrl") {
		t.Errorf("error %q does not mention 'registryUrl'", err.Error())
	}
}

// TestPackBuild_MissingDigestFails verifies that a PackBuildInput without digest
// returns a descriptive error containing "digest".
func TestPackBuild_MissingDigestFails(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/foo
`
	inputPath := writePackBuildInput(t, input)
	err := compilePackBuild(inputPath, t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing digest; got nil")
	}
	if !strings.Contains(err.Error(), "digest") {
		t.Errorf("error %q does not mention 'digest'", err.Error())
	}
}

// WS-RAW — RawSource packbuild unit tests.

// TestRawCompilePackBuild_SplitsAndEmitsClusterPack verifies that rawCompilePackBuild
// reads YAML from a directory, splits into layers, pushes to OCI, and emits a
// ClusterPack CR with rbacDigest and workloadDigest populated.
func TestRawCompilePackBuild_SplitsAndEmitsClusterPack(t *testing.T) {
	ociSrv := mockOCIRegistry(t)
	defer ociSrv.Close()
	ociHost := strings.TrimPrefix(ociSrv.URL, "http://")

	srcDir := t.TempDir()
	const sampleYAML = `---
apiVersion: v1
kind: Namespace
metadata:
  name: test-system
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: test-sa
  namespace: test-system
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: test-app
  namespace: test-system
spec:
  replicas: 1
  selector:
    matchLabels:
      app: test-app
  template:
    metadata:
      labels:
        app: test-app
    spec:
      containers:
      - name: test
        image: 10.20.0.1:5000/test:dev
`
	if err := os.WriteFile(filepath.Join(srcDir, "manifests.yaml"), []byte(sampleYAML), 0644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	outDir := t.TempDir()
	in := PackBuildInput{
		Name:           "test-raw-pack",
		Version:        "v0.1.0-r1",
		RegistryURL:    ociHost + "/packs/test-raw-pack",
		Namespace:      "seam-tenant-ccs-mgmt",
		TargetClusters: []string{"ccs-mgmt"},
		BasePackName:   "test-raw-pack",
		RawSource:      &RawSource{Path: srcDir},
	}

	if err := rawCompilePackBuild(context.Background(), in, "", outDir); err != nil {
		t.Fatalf("rawCompilePackBuild: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "test-raw-pack.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	content := string(data)
	for _, want := range []string{
		"infrastructure.ontai.dev/v1alpha1",
		"InfrastructureClusterPack",
		"rbacDigest",
		"workloadDigest",
		"v0.1.0-r1",
		"seam-tenant-ccs-mgmt",
	} {
		if !strings.Contains(content, want) {
			t.Errorf("output YAML missing %q", want)
		}
	}
}

// TestRawCompilePackBuild_EmptyDirectoryFails verifies that a directory with no
// YAML files returns a descriptive error.
func TestRawCompilePackBuild_EmptyDirectoryFails(t *testing.T) {
	ociSrv := mockOCIRegistry(t)
	defer ociSrv.Close()
	ociHost := strings.TrimPrefix(ociSrv.URL, "http://")

	in := PackBuildInput{
		Name:        "empty-pack",
		Version:     "v0.1.0-r1",
		RegistryURL: ociHost + "/packs/empty-pack",
		Namespace:   "seam-system",
		RawSource:   &RawSource{Path: t.TempDir()},
	}

	err := rawCompilePackBuild(context.Background(), in, "", t.TempDir())
	if err == nil {
		t.Fatal("expected error for empty directory; got nil")
	}
	if !strings.Contains(err.Error(), "no YAML files") {
		t.Errorf("error %q does not mention 'no YAML files'", err.Error())
	}
}

// TestRawCompilePackBuild_MissingPathFails verifies that an empty rawSource.path
// returns a descriptive error.
func TestRawCompilePackBuild_MissingPathFails(t *testing.T) {
	in := PackBuildInput{
		Name:        "path-missing-pack",
		Version:     "v0.1.0-r1",
		RegistryURL: "localhost:5000/packs/path-missing",
		Namespace:   "seam-system",
		RawSource:   &RawSource{Path: ""},
	}

	err := rawCompilePackBuild(context.Background(), in, "", t.TempDir())
	if err == nil {
		t.Fatal("expected error for missing path; got nil")
	}
	if !strings.Contains(err.Error(), "path") {
		t.Errorf("error %q does not mention 'path'", err.Error())
	}
}

// ── category validation (T-05, T-11) ─────────────────────────────────────────

// TestCategory_InvalidValueFails verifies that an unknown category string is
// rejected at input validation time.
func TestCategory_InvalidValueFails(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/foo
digest: sha256:abc
category: banana
`
	err := compilePackBuild(writePackBuildInput(t, input), t.TempDir())
	if err == nil {
		t.Fatal("expected error for invalid category; got nil")
	}
	if !strings.Contains(err.Error(), "category") {
		t.Errorf("error %q does not mention 'category'", err.Error())
	}
}

// TestCategory_HelmRequiresHelmSource verifies that category=helm is rejected
// when helmSource is absent.
func TestCategory_HelmRequiresHelmSource(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/foo
digest: sha256:abc
category: helm
`
	err := compilePackBuild(writePackBuildInput(t, input), t.TempDir())
	if err == nil {
		t.Fatal("expected error for category=helm without helmSource; got nil")
	}
	if !strings.Contains(err.Error(), "helmSource") {
		t.Errorf("error %q does not mention 'helmSource'", err.Error())
	}
}

// TestCategory_RawRequiresRawSource verifies that category=raw is rejected when
// rawSource is absent.
func TestCategory_RawRequiresRawSource(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/foo
digest: sha256:abc
category: raw
`
	err := compilePackBuild(writePackBuildInput(t, input), t.TempDir())
	if err == nil {
		t.Fatal("expected error for category=raw without rawSource; got nil")
	}
	if !strings.Contains(err.Error(), "rawSource") {
		t.Errorf("error %q does not mention 'rawSource'", err.Error())
	}
}

// TestCategory_HelmWithRawSourceFails verifies cross-contamination rejection:
// helmSource present while category=raw.
func TestCategory_HelmWithRawSourceFails(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/foo
category: raw
rawSource:
  path: /tmp
helmSource:
  url: http://charts.example.com/nginx-ingress-1.0.0.tgz
  chart: nginx-ingress
  version: 1.0.0
`
	err := compilePackBuild(writePackBuildInput(t, input), t.TempDir())
	if err == nil {
		t.Fatal("expected error for helmSource set with category=raw; got nil")
	}
	if !strings.Contains(err.Error(), "helmSource") {
		t.Errorf("error %q does not mention 'helmSource'", err.Error())
	}
}

// TestCategory_KustomizeNotImplemented verifies that category=kustomize returns
// a not-implemented error (T-12 is deferred).
func TestCategory_KustomizeNotImplemented(t *testing.T) {
	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/foo
category: kustomize
`
	err := compilePackBuild(writePackBuildInput(t, input), t.TempDir())
	if err == nil {
		t.Fatal("expected error for category=kustomize (not implemented); got nil")
	}
}

// TestCategory_RawDispatchesViaCategoryField verifies that category=raw
// dispatches to rawCompilePackBuild (T-13 category-driven dispatch).
func TestCategory_RawDispatchesViaCategoryField(t *testing.T) {
	ociSrv := mockOCIRegistry(t)
	defer ociSrv.Close()
	ociHost := strings.TrimPrefix(ociSrv.URL, "http://")

	srcDir := t.TempDir()
	const cmYAML = `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: category-test-cm
  namespace: test-system
data:
  key: value
`
	if err := os.WriteFile(filepath.Join(srcDir, "cm.yaml"), []byte(cmYAML), 0644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	inputContent := fmt.Sprintf(`
name: category-raw-pack
version: v0.1.0-r1
namespace: seam-system
registryUrl: %s/packs/category-raw-pack
category: raw
rawSource:
  path: %s
`, ociHost, srcDir)
	outDir := t.TempDir()
	if err := compilePackBuild(writePackBuildInput(t, inputContent), outDir); err != nil {
		t.Fatalf("compilePackBuild with category=raw: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "category-raw-pack.yaml")); os.IsNotExist(err) {
		t.Fatal("expected output ClusterPack CR with category=raw dispatch; not found")
	}
}

// TestPackBuild_RawSourceDispatchesToRawCompile verifies that compilePackBuild
// dispatches to rawCompilePackBuild when rawSource is set in the input file.
func TestPackBuild_RawSourceDispatchesToRawCompile(t *testing.T) {
	ociSrv := mockOCIRegistry(t)
	defer ociSrv.Close()
	ociHost := strings.TrimPrefix(ociSrv.URL, "http://")

	srcDir := t.TempDir()
	const cmYAML = `---
apiVersion: v1
kind: ConfigMap
metadata:
  name: test-cm
  namespace: test-system
data:
  key: value
`
	if err := os.WriteFile(filepath.Join(srcDir, "cm.yaml"), []byte(cmYAML), 0644); err != nil {
		t.Fatalf("write manifest: %v", err)
	}

	inputContent := fmt.Sprintf(`
name: dispatch-raw-pack
version: v0.1.0-r1
namespace: seam-system
registryUrl: %s/packs/dispatch-raw-pack
rawSource:
  path: %s
`, ociHost, srcDir)
	inputPath := writePackBuildInput(t, inputContent)

	outDir := t.TempDir()
	if err := compilePackBuild(inputPath, outDir); err != nil {
		t.Fatalf("compilePackBuild with rawSource: %v", err)
	}
	if _, err := os.Stat(filepath.Join(outDir, "dispatch-raw-pack.yaml")); os.IsNotExist(err) {
		t.Fatal("expected output ClusterPack CR; not found")
	}
}
