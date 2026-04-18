package main

import (
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
// ClusterPack CR carries apiVersion=infra.ontai.dev/v1alpha1 and kind=ClusterPack.
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

	if cr["apiVersion"] != "infra.ontai.dev/v1alpha1" {
		t.Errorf("apiVersion: got %q; want %q", cr["apiVersion"], "infra.ontai.dev/v1alpha1")
	}
	if cr["kind"] != "ClusterPack" {
		t.Errorf("kind: got %q; want %q", cr["kind"], "ClusterPack")
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
