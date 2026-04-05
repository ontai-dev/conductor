// Package catalog provides the versioned embedded catalog of canonical RBACProfile
// definitions for known ecosystem components operating in Guardian-governed clusters.
//
// The catalog is a compile-time embedded artifact — no runtime network calls, no
// external fetch. All entries render deterministically from the same inputs.
//
// New catalog entries require a Pull Request to the conductor repository and a
// Platform Governor review before merge. conductor-schema.md §16.
package catalog

import (
	"bytes"
	"embed"
	"fmt"
	"sort"
	"strings"
	"text/template"
)

// catalogFS holds the embedded entry YAML templates.
// All files under entries/ are embedded at compile time.
//
//go:embed entries
var catalogFS embed.FS

// CatalogEntry is a versioned RBACProfile definition for a known ecosystem component.
// Each entry is authored for a specific component version and is immutable once merged.
// New entries are added via PR — no runtime modification is permitted.
type CatalogEntry struct {
	// Name is the canonical catalog key (e.g., "cilium", "cnpg").
	// Used as the --component flag value in compiler component.
	Name string

	// Version is the component version this catalog entry was authored for.
	// Informational — Compiler does not enforce version compatibility.
	Version string

	// Description is a human-readable description of the component.
	Description string

	// RBACProfileName is the metadata.name of the emitted RBACProfile CR.
	// Must match the catalog table in conductor-schema.md §16.
	RBACProfileName string
}

// RenderParams carries the template substitution values for a catalog entry render.
// All fields must be provided — empty values produce syntactically valid but
// semantically incomplete RBACProfile YAML that Guardian will reject at admission.
type RenderParams struct {
	// Namespace is the target namespace for the RBACProfile CR.
	// Should be the tenant namespace for the cluster (seam-tenant-{cluster-name}).
	Namespace string

	// TargetClusters is the list of cluster names this RBACProfile grants access to.
	// Maps to spec.targetClusters. Must not be empty.
	TargetClusters []string

	// RBACPolicyRef is the name of the governing RBACPolicy in the same namespace.
	// Maps to spec.rbacPolicyRef. Must not be empty.
	RBACPolicyRef string
}

// entries is the immutable catalog. Order matches conductor-schema.md §16 table.
// New entries require a PR and Platform Governor review — never add at runtime.
var entries = []CatalogEntry{
	{
		Name:            "cilium",
		Version:         "v1.16",
		Description:     "Cilium CNI agent and operator",
		RBACProfileName: "rbac-cilium",
	},
	{
		Name:            "cnpg",
		Version:         "v1.24",
		Description:     "CloudNativePG operator",
		RBACProfileName: "rbac-cnpg",
	},
	{
		Name:            "kueue",
		Version:         "v0.9",
		Description:     "Kueue batch scheduler",
		RBACProfileName: "rbac-kueue",
	},
	{
		Name:            "cert-manager",
		Version:         "v1.16",
		Description:     "cert-manager controller and webhook",
		RBACProfileName: "rbac-cert-manager",
	},
	{
		Name:            "local-path-provisioner",
		Version:         "v0.0.30",
		Description:     "Rancher local-path-provisioner",
		RBACProfileName: "rbac-local-path-provisioner",
	},
}

// All returns all catalog entries in canonical order.
func All() []CatalogEntry {
	return entries
}

// Lookup finds a catalog entry by name. Returns (entry, true) when found,
// (zero value, false) when not found. Name matching is case-sensitive.
func Lookup(name string) (CatalogEntry, bool) {
	for _, e := range entries {
		if e.Name == name {
			return e, true
		}
	}
	return CatalogEntry{}, false
}

// AvailableNames returns a sorted list of all catalog entry names.
// Used in error messages for unknown component lookups.
func AvailableNames() []string {
	names := make([]string, len(entries))
	for i, e := range entries {
		names[i] = e.Name
	}
	sort.Strings(names)
	return names
}

// Render renders the catalog entry's YAML template with the given parameters.
// Returns the rendered RBACProfile YAML bytes. Error implies a bug in the
// catalog template or a programming error — not a user error.
func (e CatalogEntry) Render(params RenderParams) ([]byte, error) {
	templateContent, err := catalogFS.ReadFile("entries/" + e.Name + ".yaml")
	if err != nil {
		return nil, fmt.Errorf("catalog: read template for %q: %w", e.Name, err)
	}

	// Validate params before rendering — surface missing values explicitly.
	if params.Namespace == "" {
		return nil, fmt.Errorf("catalog: RenderParams.Namespace must not be empty for %q", e.Name)
	}
	if len(params.TargetClusters) == 0 {
		return nil, fmt.Errorf("catalog: RenderParams.TargetClusters must not be empty for %q", e.Name)
	}
	if params.RBACPolicyRef == "" {
		return nil, fmt.Errorf("catalog: RenderParams.RBACPolicyRef must not be empty for %q", e.Name)
	}

	// templateData bundles entry metadata with render params for the template engine.
	type templateData struct {
		RenderParams
		EntryName       string
		EntryVersion    string
		EntryDescription string
		RBACProfileName  string
	}

	tmpl, err := template.New(e.Name).Parse(string(templateContent))
	if err != nil {
		return nil, fmt.Errorf("catalog: parse template for %q: %w", e.Name, err)
	}

	var buf bytes.Buffer
	data := templateData{
		RenderParams:     params,
		EntryName:        e.Name,
		EntryVersion:     e.Version,
		EntryDescription: e.Description,
		RBACProfileName:  e.RBACProfileName,
	}
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("catalog: render template for %q: %w", e.Name, err)
	}
	return buf.Bytes(), nil
}

// DescriptorScaffold produces an RBACProfile scaffold YAML for a custom (non-catalog)
// component descriptor. The scaffold carries prominent human-review comment blocks
// and stubs all required fields. conductor-schema.md §16 Custom Mode.
func DescriptorScaffold(descriptor ComponentDescriptor) ([]byte, error) {
	if descriptor.Name == "" {
		return nil, fmt.Errorf("catalog: descriptor Name must not be empty")
	}
	if descriptor.Namespace == "" {
		return nil, fmt.Errorf("catalog: descriptor Namespace must not be empty")
	}

	profileName := "rbac-" + strings.ToLower(strings.ReplaceAll(descriptor.Name, " ", "-"))

	scaffold := fmt.Sprintf(`# ============================================================================
# HUMAN REVIEW REQUIRED — RBACProfile scaffold generated by compiler component
# ============================================================================
# This file was generated from a custom component descriptor and has NOT been
# reviewed or merged into the conductor catalog. Before applying this file:
#
#   1. Review ALL permissionDeclarations against the component's actual RBAC needs.
#   2. Create the referenced PermissionSet CRs in security-system before applying.
#   3. Verify targetClusters and rbacPolicyRef match your cluster topology.
#   4. Have the Platform Governor review this file before GitOps commit.
#
# To add this component to the official catalog, open a PR to the conductor
# repository. New catalog entries require Platform Governor review.
# conductor-schema.md §16 Custom Mode.
# ============================================================================
apiVersion: security.ontai.dev/v1alpha1
kind: RBACProfile
metadata:
  # REVIEW: Adjust the name to match your component naming convention.
  name: %s
  namespace: %s
spec:
  # REVIEW: Set principalRef to the ServiceAccount name this component uses.
  principalRef: %s
  # REVIEW: List the cluster names this component should be permitted to operate on.
  targetClusters:
  - # TODO: add cluster names
  # REVIEW: Reference the governing RBACPolicy that constrains this profile.
  rbacPolicyRef: %s
  # REVIEW: Declare the PermissionSets this component requires. Create each
  # referenced PermissionSet in security-system before applying this profile.
  permissionDeclarations:
  - # TODO: permissionSetRef: <permission-set-name>
    # TODO: scope: cluster  # or: namespaced
  # spec.lineage is controller-managed — do not author this field manually.
  # The InfrastructureLineageController sets it after admission. CLAUDE.md §14.
`,
		profileName,
		descriptor.Namespace,
		descriptor.PrincipalRef,
		descriptor.RBACPolicyRef,
	)

	return []byte(scaffold), nil
}

// ComponentDescriptor is the human-authored descriptor for a custom (non-catalog)
// component. Provided via --descriptor flag to compiler component.
type ComponentDescriptor struct {
	// Name is the component name (e.g., "my-operator").
	Name string `yaml:"name"`

	// Namespace is the target namespace for the generated RBACProfile.
	Namespace string `yaml:"namespace"`

	// PrincipalRef is the ServiceAccount name the component uses.
	// Defaults to the component name when empty.
	PrincipalRef string `yaml:"principalRef,omitempty"`

	// RBACPolicyRef is the governing RBACPolicy name.
	// Defaults to "default-rbac-policy" when empty.
	RBACPolicyRef string `yaml:"rbacPolicyRef,omitempty"`
}
