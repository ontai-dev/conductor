// compile_component_test.go tests the compiler component subcommand logic.
// Covers catalog mode rendering, unknown component error, and descriptor mode.
// All tests are fully offline — no cluster connectivity. conductor-schema.md §16.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// --- parseClusters tests ---

func TestParseClusters_CommaSeparated(t *testing.T) {
	got := parseClusters("ccs-dev,ccs-test, management")
	want := []string{"ccs-dev", "ccs-test", "management"}
	if len(got) != len(want) {
		t.Fatalf("parseClusters: got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("parseClusters[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestParseClusters_Empty(t *testing.T) {
	got := parseClusters("")
	if got != nil {
		t.Errorf("parseClusters(\"\") = %v, want nil", got)
	}
}

func TestParseClusters_SingleCluster(t *testing.T) {
	got := parseClusters("management")
	if len(got) != 1 || got[0] != "management" {
		t.Errorf("parseClusters(\"management\") = %v, want [management]", got)
	}
}

// --- runCatalogMode tests ---

// TestCatalogMode_UnknownComponentFails verifies that runCatalogMode fails fast
// with a clear error when an unknown component name is given.
// conductor-schema.md §16 Catalog Mode.
func TestCatalogMode_UnknownComponentFails(t *testing.T) {
	err := runCatalogMode(
		[]string{"not-a-real-component"},
		"seam-tenant-management",
		[]string{"management"},
		"platform-rbac-policy",
		"",
	)
	if err == nil {
		t.Fatal("expected error for unknown component, got nil")
	}
	if !strings.Contains(err.Error(), "not-a-real-component") {
		t.Errorf("error %q does not mention the unknown component name", err.Error())
	}
	if !strings.Contains(err.Error(), "available") {
		t.Errorf("error %q does not list available components", err.Error())
	}
}

// TestCatalogMode_MissingNamespaceFails verifies that runCatalogMode rejects
// an empty namespace.
func TestCatalogMode_MissingNamespaceFails(t *testing.T) {
	err := runCatalogMode(
		[]string{"cilium"},
		"",
		[]string{"management"},
		"platform-rbac-policy",
		"",
	)
	if err == nil {
		t.Fatal("expected error for empty namespace, got nil")
	}
}

// TestCatalogMode_MissingTargetClustersFails verifies that runCatalogMode rejects
// empty target clusters.
func TestCatalogMode_MissingTargetClustersFails(t *testing.T) {
	err := runCatalogMode(
		[]string{"cilium"},
		"seam-tenant-management",
		nil,
		"platform-rbac-policy",
		"",
	)
	if err == nil {
		t.Fatal("expected error for empty target clusters, got nil")
	}
}

// TestCatalogMode_MissingRBACPolicyRefFails verifies that runCatalogMode rejects
// an empty rbacPolicyRef.
func TestCatalogMode_MissingRBACPolicyRefFails(t *testing.T) {
	err := runCatalogMode(
		[]string{"cilium"},
		"seam-tenant-management",
		[]string{"management"},
		"",
		"",
	)
	if err == nil {
		t.Fatal("expected error for empty rbacPolicyRef, got nil")
	}
}

// TestCatalogMode_SingleComponentToFile verifies that runCatalogMode writes a
// single component's RBACProfile YAML to a file path.
func TestCatalogMode_SingleComponentToFile(t *testing.T) {
	dir := t.TempDir()
	outPath := filepath.Join(dir, "rbac-cilium.yaml")

	err := runCatalogMode(
		[]string{"cilium"},
		"seam-tenant-management",
		[]string{"management"},
		"platform-rbac-policy",
		outPath,
	)
	if err != nil {
		t.Fatalf("runCatalogMode error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	yaml := string(data)

	assertContainsStr(t, yaml, "apiVersion: security.ontai.dev/v1alpha1")
	assertContainsStr(t, yaml, "kind: RBACProfile")
	assertContainsStr(t, yaml, "name: rbac-cilium")
	assertContainsStr(t, yaml, "namespace: seam-tenant-management")
	assertContainsStr(t, yaml, "rbacPolicyRef: platform-rbac-policy")
	assertContainsStr(t, yaml, "- management")
}

// TestCatalogMode_MultipleComponentsToDirectory verifies that runCatalogMode
// writes one file per component when multiple components are requested.
func TestCatalogMode_MultipleComponentsToDirectory(t *testing.T) {
	dir := t.TempDir()

	err := runCatalogMode(
		[]string{"cilium", "kueue"},
		"seam-tenant-management",
		[]string{"ccs-dev"},
		"platform-rbac-policy",
		dir,
	)
	if err != nil {
		t.Fatalf("runCatalogMode error: %v", err)
	}

	// Both files must exist in the output directory.
	ciliumPath := filepath.Join(dir, "rbac-cilium.yaml")
	kueuePath := filepath.Join(dir, "rbac-kueue.yaml")

	for _, path := range []string{ciliumPath, kueuePath} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("expected output file %q, got error: %v", path, err)
		}
	}

	// Spot-check kueue output.
	data, err := os.ReadFile(kueuePath)
	if err != nil {
		t.Fatalf("read kueue output: %v", err)
	}
	assertContainsStr(t, string(data), "principalRef: kueue-controller-manager")
}

// TestCatalogMode_FailFastOnFirstUnknown verifies that runCatalogMode fails fast
// before rendering anything when a mix of known and unknown names is given.
// conductor-schema.md §16 Catalog Mode: fail fast.
func TestCatalogMode_FailFastOnFirstUnknown(t *testing.T) {
	dir := t.TempDir()

	err := runCatalogMode(
		[]string{"cilium", "not-real", "kueue"},
		"seam-tenant-management",
		[]string{"management"},
		"platform-rbac-policy",
		dir,
	)
	if err == nil {
		t.Fatal("expected error for unknown component in list, got nil")
	}
	// No files should have been written (fail before any render).
	entries, _ := os.ReadDir(dir)
	if len(entries) != 0 {
		t.Errorf("expected no output files on fail-fast, got %d", len(entries))
	}
}

// --- runDescriptorMode tests ---

// TestDescriptorMode_ProducesScaffoldToFile verifies that runDescriptorMode
// writes a human-review-annotated RBACProfile scaffold to a file.
// conductor-schema.md §16 Custom Mode.
func TestDescriptorMode_ProducesScaffoldToFile(t *testing.T) {
	descriptorYAML := `name: my-custom-operator
namespace: seam-tenant-management
principalRef: my-operator-sa
rbacPolicyRef: platform-rbac-policy
`
	dir := t.TempDir()
	descPath := filepath.Join(dir, "descriptor.yaml")
	if err := os.WriteFile(descPath, []byte(descriptorYAML), 0644); err != nil {
		t.Fatalf("write descriptor: %v", err)
	}

	outPath := filepath.Join(dir, "output.yaml")
	err := runDescriptorMode(descPath, outPath)
	if err != nil {
		t.Fatalf("runDescriptorMode error: %v", err)
	}

	data, err := os.ReadFile(outPath)
	if err != nil {
		t.Fatalf("read output: %v", err)
	}
	scaffold := string(data)

	assertContainsStr(t, scaffold, "HUMAN REVIEW REQUIRED")
	assertContainsStr(t, scaffold, "apiVersion: security.ontai.dev/v1alpha1")
	assertContainsStr(t, scaffold, "kind: RBACProfile")
	assertContainsStr(t, scaffold, "name: rbac-my-custom-operator")
	assertContainsStr(t, scaffold, "namespace: seam-tenant-management")
	assertContainsStr(t, scaffold, "principalRef: my-operator-sa")
	assertContainsStr(t, scaffold, "rbacPolicyRef: platform-rbac-policy")
	assertContainsStr(t, scaffold, "spec.lineage is controller-managed")
	assertContainsStr(t, scaffold, "TODO")
}

// TestDescriptorMode_MissingNameFails verifies that runDescriptorMode rejects a
// descriptor with no name.
func TestDescriptorMode_MissingNameFails(t *testing.T) {
	descriptorYAML := `namespace: seam-tenant-management
`
	dir := t.TempDir()
	descPath := filepath.Join(dir, "descriptor.yaml")
	if err := os.WriteFile(descPath, []byte(descriptorYAML), 0644); err != nil {
		t.Fatalf("write descriptor: %v", err)
	}

	err := runDescriptorMode(descPath, "")
	if err == nil {
		t.Error("expected error for descriptor with no name, got nil")
	}
}

// TestDescriptorMode_MissingNamespaceFails verifies that runDescriptorMode rejects
// a descriptor with no namespace.
func TestDescriptorMode_MissingNamespaceFails(t *testing.T) {
	descriptorYAML := `name: my-operator
`
	dir := t.TempDir()
	descPath := filepath.Join(dir, "descriptor.yaml")
	if err := os.WriteFile(descPath, []byte(descriptorYAML), 0644); err != nil {
		t.Fatalf("write descriptor: %v", err)
	}

	err := runDescriptorMode(descPath, "")
	if err == nil {
		t.Error("expected error for descriptor with no namespace, got nil")
	}
}

// TestDescriptorMode_NonexistentDescriptorFails verifies that runDescriptorMode
// returns an error when the descriptor path does not exist.
func TestDescriptorMode_NonexistentDescriptorFails(t *testing.T) {
	err := runDescriptorMode("/tmp/does-not-exist-descriptor.yaml", "")
	if err == nil {
		t.Error("expected error for missing descriptor path, got nil")
	}
}

// TestDescriptorMode_DefaultsApplied verifies that optional descriptor fields
// receive sensible defaults (principalRef defaults to name, rbacPolicyRef
// defaults to "default-rbac-policy").
func TestDescriptorMode_DefaultsApplied(t *testing.T) {
	descriptorYAML := `name: bare-operator
namespace: seam-tenant-management
`
	dir := t.TempDir()
	descPath := filepath.Join(dir, "descriptor.yaml")
	if err := os.WriteFile(descPath, []byte(descriptorYAML), 0644); err != nil {
		t.Fatalf("write descriptor: %v", err)
	}

	outPath := filepath.Join(dir, "output.yaml")
	err := runDescriptorMode(descPath, outPath)
	if err != nil {
		t.Fatalf("runDescriptorMode error: %v", err)
	}

	data, _ := os.ReadFile(outPath)
	scaffold := string(data)

	// principalRef defaults to the component name.
	assertContainsStr(t, scaffold, "principalRef: bare-operator")
	// rbacPolicyRef defaults to "default-rbac-policy".
	assertContainsStr(t, scaffold, "rbacPolicyRef: default-rbac-policy")
}

// --- principalRefForEntry tests ---

// TestPrincipalRefForEntry_KnownEntries verifies the SA name mapping for all
// catalog entries used in --discover mode.
func TestPrincipalRefForEntry_KnownEntries(t *testing.T) {
	cases := []struct {
		entryName string
		wantSA    string
	}{
		{"cilium", "cilium"},
		{"cnpg", "cnpg-manager"},
		{"kueue", "kueue-controller-manager"},
		{"cert-manager", "cert-manager"},
		{"local-path-provisioner", "local-path-provisioner-service-account"},
	}
	for _, tc := range cases {
		t.Run(tc.entryName, func(t *testing.T) {
			got := principalRefForEntry(tc.entryName)
			if got != tc.wantSA {
				t.Errorf("principalRefForEntry(%q) = %q, want %q", tc.entryName, got, tc.wantSA)
			}
		})
	}
}

// assertContainsStr is a test helper that fails if substr is not in s.
func assertContainsStr(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("output does not contain %q\nactual output:\n%s", substr, s)
	}
}

// WS2 — Component malformed input validation tests.

// TestComponent_BothComponentAndDescriptorFails verifies that specifying both
// --component and --descriptor returns an error. The two flags are mutually
// exclusive: catalog mode and descriptor mode cannot be combined.
// conductor-schema.md §16.
func TestComponent_BothComponentAndDescriptorFails(t *testing.T) {
	err := validateComponentFlagConflict([]string{"cilium"}, "/some/descriptor.yaml")
	if err == nil {
		t.Fatal("expected error for combined --component + --descriptor; got nil")
	}
	if !strings.Contains(err.Error(), "mutually exclusive") {
		t.Errorf("error %q does not mention 'mutually exclusive'", err.Error())
	}
}

// TestComponent_ComponentAloneNoConflict verifies that --component alone
// passes the mutual exclusion check.
func TestComponent_ComponentAloneNoConflict(t *testing.T) {
	if err := validateComponentFlagConflict([]string{"cilium"}, ""); err != nil {
		t.Errorf("expected nil for --component alone; got: %v", err)
	}
}

// TestComponent_DescriptorAloneNoConflict verifies that --descriptor alone
// passes the mutual exclusion check.
func TestComponent_DescriptorAloneNoConflict(t *testing.T) {
	if err := validateComponentFlagConflict(nil, "/some/descriptor.yaml"); err != nil {
		t.Errorf("expected nil for --descriptor alone; got: %v", err)
	}
}

// WS2 — Unwritable output path tests.

// TestAnySubcommand_UnwritableOutputPathFails verifies that a subcommand fails
// with a filesystem error (not a panic) when the output path is unwritable.
// Uses compilePackBuild with a valid input but an output path where the parent
// is itself a regular file (ENOTDIR) — reliable across root and non-root.
func TestAnySubcommand_UnwritableOutputPathFails(t *testing.T) {
	// Create a regular file that will serve as the "parent directory".
	dir := t.TempDir()
	blockingFile := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(blockingFile, []byte("blocking"), 0644); err != nil {
		t.Fatalf("create blocking file: %v", err)
	}

	// Output path inside the blocking file (impossible on all POSIX systems).
	unwritableOutput := filepath.Join(blockingFile, "subdir")

	const input = `
name: my-pack
version: v1.0.0
registryUrl: registry.example.com/packs/my-pack
digest: sha256:abc123
`
	inputPath := writePackBuildInput(t, input)
	err := compilePackBuild(inputPath, unwritableOutput)
	if err == nil {
		t.Fatal("expected error for unwritable output path; got nil")
	}
	// Must be a filesystem error, not a panic. Error is non-nil — that's the contract.
}
