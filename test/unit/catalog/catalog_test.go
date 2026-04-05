// Package catalog_test contains unit tests for the internal/catalog package.
// Tests cover catalog load, entry lookup, RBACProfile YAML rendering, and
// descriptor scaffold generation. All tests are fully offline — no cluster
// connectivity. conductor-schema.md §16.
package catalog_test

import (
	"strings"
	"testing"

	"github.com/ontai-dev/conductor/internal/catalog"
)

// --- Catalog load and lookup tests ---

// TestCatalogAll_ReturnsExpectedEntries verifies that All() returns the five
// canonical entries defined in conductor-schema.md §16.
func TestCatalogAll_ReturnsExpectedEntries(t *testing.T) {
	entries := catalog.All()

	expectedNames := map[string]bool{
		"cilium":                 true,
		"cnpg":                   true,
		"kueue":                  true,
		"cert-manager":           true,
		"local-path-provisioner": true,
	}
	if len(entries) != len(expectedNames) {
		t.Fatalf("expected %d catalog entries, got %d", len(expectedNames), len(entries))
	}
	for _, e := range entries {
		if !expectedNames[e.Name] {
			t.Errorf("unexpected catalog entry %q", e.Name)
		}
		if e.Version == "" {
			t.Errorf("entry %q has empty Version", e.Name)
		}
		if e.Description == "" {
			t.Errorf("entry %q has empty Description", e.Name)
		}
		if e.RBACProfileName == "" {
			t.Errorf("entry %q has empty RBACProfileName", e.Name)
		}
	}
}

// TestCatalogLookup_KnownComponents verifies that Lookup finds all known components.
func TestCatalogLookup_KnownComponents(t *testing.T) {
	cases := []struct {
		name            string
		wantProfileName string
	}{
		{"cilium", "rbac-cilium"},
		{"cnpg", "rbac-cnpg"},
		{"kueue", "rbac-kueue"},
		{"cert-manager", "rbac-cert-manager"},
		{"local-path-provisioner", "rbac-local-path-provisioner"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			entry, ok := catalog.Lookup(tc.name)
			if !ok {
				t.Fatalf("Lookup(%q) returned false, want true", tc.name)
			}
			if entry.Name != tc.name {
				t.Errorf("entry.Name = %q, want %q", entry.Name, tc.name)
			}
			if entry.RBACProfileName != tc.wantProfileName {
				t.Errorf("entry.RBACProfileName = %q, want %q",
					entry.RBACProfileName, tc.wantProfileName)
			}
		})
	}
}

// TestCatalogLookup_UnknownComponent verifies that Lookup returns false for
// unknown component names.
func TestCatalogLookup_UnknownComponent(t *testing.T) {
	_, ok := catalog.Lookup("not-a-real-component")
	if ok {
		t.Error("Lookup(unknown) returned true, want false")
	}
	_, ok = catalog.Lookup("")
	if ok {
		t.Error("Lookup(\"\") returned true, want false")
	}
}

// TestCatalogAvailableNames_IsSorted verifies that AvailableNames returns a
// sorted list of all catalog entry names.
func TestCatalogAvailableNames_IsSorted(t *testing.T) {
	names := catalog.AvailableNames()
	if len(names) == 0 {
		t.Fatal("AvailableNames returned empty list")
	}
	for i := 1; i < len(names); i++ {
		if names[i] < names[i-1] {
			t.Errorf("AvailableNames not sorted at index %d: %q < %q",
				i, names[i], names[i-1])
		}
	}
}

// --- RBACProfile YAML rendering tests ---

var standardParams = catalog.RenderParams{
	Namespace:      "seam-tenant-management",
	TargetClusters: []string{"management"},
	RBACPolicyRef:  "platform-rbac-policy",
}

// TestCiliumRender_ProducesValidRBACProfileYAML verifies that the cilium catalog
// entry renders a valid RBACProfile YAML with all required fields populated.
func TestCiliumRender_ProducesValidRBACProfileYAML(t *testing.T) {
	entry, ok := catalog.Lookup("cilium")
	if !ok {
		t.Fatal("cilium not found in catalog")
	}

	out, err := entry.Render(standardParams)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	yaml := string(out)

	// Verify RBACProfile structural fields.
	assertContains(t, yaml, "apiVersion: security.ontai.dev/v1alpha1")
	assertContains(t, yaml, "kind: RBACProfile")
	assertContains(t, yaml, "name: rbac-cilium")
	assertContains(t, yaml, "namespace: seam-tenant-management")
	assertContains(t, yaml, "principalRef: cilium")
	assertContains(t, yaml, "rbacPolicyRef: platform-rbac-policy")
	assertContains(t, yaml, "- management")

	// Verify catalog annotations present.
	assertContains(t, yaml, "ontai.dev/catalog-component: \"cilium\"")
	assertContains(t, yaml, "ontai.dev/catalog-version:")
}

// TestCNPGRender_ProducesValidRBACProfileYAML verifies that the cnpg catalog
// entry renders correctly with multiple target clusters.
func TestCNPGRender_ProducesValidRBACProfileYAML(t *testing.T) {
	entry, ok := catalog.Lookup("cnpg")
	if !ok {
		t.Fatal("cnpg not found in catalog")
	}

	params := catalog.RenderParams{
		Namespace:      "seam-tenant-ccs-dev",
		TargetClusters: []string{"ccs-dev", "ccs-test"},
		RBACPolicyRef:  "cluster-rbac-policy",
	}
	out, err := entry.Render(params)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	yaml := string(out)

	assertContains(t, yaml, "apiVersion: security.ontai.dev/v1alpha1")
	assertContains(t, yaml, "kind: RBACProfile")
	assertContains(t, yaml, "name: rbac-cnpg")
	assertContains(t, yaml, "namespace: seam-tenant-ccs-dev")
	assertContains(t, yaml, "principalRef: cnpg-manager")
	assertContains(t, yaml, "rbacPolicyRef: cluster-rbac-policy")
	assertContains(t, yaml, "- ccs-dev")
	assertContains(t, yaml, "- ccs-test")
	assertContains(t, yaml, "ontai.dev/catalog-component: \"cnpg\"")
}

// TestKueueRender_ProducesValidRBACProfileYAML verifies the kueue entry.
func TestKueueRender_ProducesValidRBACProfileYAML(t *testing.T) {
	entry, ok := catalog.Lookup("kueue")
	if !ok {
		t.Fatal("kueue not found in catalog")
	}

	out, err := entry.Render(standardParams)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	yaml := string(out)
	assertContains(t, yaml, "name: rbac-kueue")
	assertContains(t, yaml, "principalRef: kueue-controller-manager")
}

// TestCertManagerRender_ProducesValidRBACProfileYAML verifies the cert-manager entry.
func TestCertManagerRender_ProducesValidRBACProfileYAML(t *testing.T) {
	entry, ok := catalog.Lookup("cert-manager")
	if !ok {
		t.Fatal("cert-manager not found in catalog")
	}

	out, err := entry.Render(standardParams)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	yaml := string(out)
	assertContains(t, yaml, "name: rbac-cert-manager")
	assertContains(t, yaml, "principalRef: cert-manager")
}

// TestLocalPathProvisionerRender_ProducesValidRBACProfileYAML verifies the
// local-path-provisioner entry.
func TestLocalPathProvisionerRender_ProducesValidRBACProfileYAML(t *testing.T) {
	entry, ok := catalog.Lookup("local-path-provisioner")
	if !ok {
		t.Fatal("local-path-provisioner not found in catalog")
	}

	out, err := entry.Render(standardParams)
	if err != nil {
		t.Fatalf("Render error: %v", err)
	}
	yaml := string(out)
	assertContains(t, yaml, "name: rbac-local-path-provisioner")
	assertContains(t, yaml, "principalRef: local-path-provisioner-service-account")
}

// TestRender_DeterministicOutput verifies that the same inputs produce identical
// outputs on successive calls. conductor-design.md §1.2 Deterministic Execution.
func TestRender_DeterministicOutput(t *testing.T) {
	entry, _ := catalog.Lookup("cilium")

	out1, err := entry.Render(standardParams)
	if err != nil {
		t.Fatalf("first Render error: %v", err)
	}
	out2, err := entry.Render(standardParams)
	if err != nil {
		t.Fatalf("second Render error: %v", err)
	}
	if string(out1) != string(out2) {
		t.Error("Render is not deterministic: successive calls with same params produced different output")
	}
}

// TestRender_MissingNamespaceFails verifies that Render fails fast when
// RenderParams.Namespace is empty.
func TestRender_MissingNamespaceFails(t *testing.T) {
	entry, _ := catalog.Lookup("cilium")
	_, err := entry.Render(catalog.RenderParams{
		Namespace:      "",
		TargetClusters: []string{"management"},
		RBACPolicyRef:  "policy",
	})
	if err == nil {
		t.Error("expected error for empty Namespace, got nil")
	}
}

// TestRender_MissingTargetClustersFails verifies that Render fails fast when
// RenderParams.TargetClusters is empty.
func TestRender_MissingTargetClustersFails(t *testing.T) {
	entry, _ := catalog.Lookup("cilium")
	_, err := entry.Render(catalog.RenderParams{
		Namespace:      "seam-tenant-management",
		TargetClusters: nil,
		RBACPolicyRef:  "policy",
	})
	if err == nil {
		t.Error("expected error for empty TargetClusters, got nil")
	}
}

// TestRender_MissingRBACPolicyRefFails verifies that Render fails fast when
// RenderParams.RBACPolicyRef is empty.
func TestRender_MissingRBACPolicyRefFails(t *testing.T) {
	entry, _ := catalog.Lookup("cilium")
	_, err := entry.Render(catalog.RenderParams{
		Namespace:      "seam-tenant-management",
		TargetClusters: []string{"management"},
		RBACPolicyRef:  "",
	})
	if err == nil {
		t.Error("expected error for empty RBACPolicyRef, got nil")
	}
}

// --- DescriptorScaffold tests ---

// TestDescriptorScaffold_ProducesReviewAnnotatedYAML verifies that
// DescriptorScaffold produces a YAML scaffold with prominent human-review
// comment blocks. conductor-schema.md §16 Custom Mode.
func TestDescriptorScaffold_ProducesReviewAnnotatedYAML(t *testing.T) {
	desc := catalog.ComponentDescriptor{
		Name:          "my-custom-operator",
		Namespace:     "seam-tenant-management",
		PrincipalRef:  "my-operator-sa",
		RBACPolicyRef: "platform-rbac-policy",
	}

	out, err := catalog.DescriptorScaffold(desc)
	if err != nil {
		t.Fatalf("DescriptorScaffold error: %v", err)
	}
	scaffold := string(out)

	// Must have human-review header.
	assertContains(t, scaffold, "HUMAN REVIEW REQUIRED")
	assertContains(t, scaffold, "apiVersion: security.ontai.dev/v1alpha1")
	assertContains(t, scaffold, "kind: RBACProfile")
	assertContains(t, scaffold, "name: rbac-my-custom-operator")
	assertContains(t, scaffold, "namespace: seam-tenant-management")
	assertContains(t, scaffold, "principalRef: my-operator-sa")
	assertContains(t, scaffold, "rbacPolicyRef: platform-rbac-policy")
	// Must include the controller-managed lineage note.
	assertContains(t, scaffold, "spec.lineage is controller-managed")
	// Must include TODO placeholders.
	assertContains(t, scaffold, "TODO")
}

// TestDescriptorScaffold_EmptyNameFails verifies that DescriptorScaffold rejects
// an empty descriptor name.
func TestDescriptorScaffold_EmptyNameFails(t *testing.T) {
	_, err := catalog.DescriptorScaffold(catalog.ComponentDescriptor{
		Name:      "",
		Namespace: "seam-tenant-management",
	})
	if err == nil {
		t.Error("expected error for empty Name, got nil")
	}
}

// TestDescriptorScaffold_EmptyNamespaceFails verifies that DescriptorScaffold
// rejects an empty descriptor namespace.
func TestDescriptorScaffold_EmptyNamespaceFails(t *testing.T) {
	_, err := catalog.DescriptorScaffold(catalog.ComponentDescriptor{
		Name:      "my-operator",
		Namespace: "",
	})
	if err == nil {
		t.Error("expected error for empty Namespace, got nil")
	}
}

// assertContains is a test helper that fails if substr is not in s.
func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("output does not contain %q\nactual output:\n%s", substr, s)
	}
}
