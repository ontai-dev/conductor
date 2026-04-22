// Package packbuild implements compile-time helpers for ClusterPack artifact
// assembly. The split functions separate RBAC resources, cluster-scoped non-RBAC
// resources, and workload resources for the three-bucket OCI artifact contract.
// wrapper-schema.md §4.
package packbuild

import (
	"fmt"
	"strings"

	sigsyaml "sigs.k8s.io/yaml"
)

// rbacKinds is the set of Kubernetes resource kinds treated as RBAC resources.
// Routed through guardian /rbac-intake before anything else.
var rbacKinds = map[string]bool{
	"ServiceAccount":     true,
	"Role":               true,
	"ClusterRole":        true,
	"RoleBinding":        true,
	"ClusterRoleBinding": true,
}

// clusterScopedKinds is the set of cluster-scoped non-RBAC resource kinds that
// require a ClusterRole to apply and must be separated from namespace-scoped
// workload resources. Applied after guardian intake, before workload.
// wrapper-schema.md §4, Governor ruling 2026-04-22.
var clusterScopedKinds = map[string]bool{
	"MutatingWebhookConfiguration":   true,
	"ValidatingWebhookConfiguration": true,
	"CustomResourceDefinition":        true,
	"APIService":                      true,
	"PriorityClass":                   true,
	"StorageClass":                    true,
	"IngressClass":                    true,
	"ClusterIssuer":                   true,
}

// Manifest holds a single Kubernetes manifest as raw YAML text plus the parsed
// Kind and Namespace fields.
type Manifest struct {
	// Kind is the Kubernetes resource kind extracted from the manifest.
	Kind string
	// Namespace is the metadata.namespace extracted from the manifest.
	// Empty for cluster-scoped resources.
	Namespace string
	// Name is the metadata.name extracted from the manifest.
	Name string
	// YAML is the full raw YAML text of the manifest document.
	YAML string
}

// ParseManifests splits a multi-document YAML string (separated by "---") into
// individual Manifest records. Blank or whitespace-only documents are skipped.
// Returns an error if any document cannot be parsed as a YAML object.
func ParseManifests(multiYAML string) ([]Manifest, error) {
	docs := strings.Split(multiYAML, "\n---")
	var result []Manifest
	for _, doc := range docs {
		trimmed := strings.TrimSpace(doc)
		if trimmed == "" || trimmed == "---" {
			continue
		}
		var obj map[string]interface{}
		if err := sigsyaml.Unmarshal([]byte(trimmed), &obj); err != nil {
			return nil, fmt.Errorf("parse manifest: %w", err)
		}
		kind, _ := obj["kind"].(string)
		meta, _ := obj["metadata"].(map[string]interface{})
		name, _ := meta["name"].(string)
		ns, _ := meta["namespace"].(string)
		result = append(result, Manifest{Kind: kind, Namespace: ns, Name: name, YAML: trimmed})
	}
	return result, nil
}

// SplitManifests partitions manifests into three slices:
//   - rbac: ServiceAccount, Role, ClusterRole, RoleBinding, ClusterRoleBinding
//   - clusterScoped: cluster-scoped non-RBAC kinds (webhooks, CRDs, etc.)
//   - workload: everything else
//
// Namespace injection: for each namespace referenced by RBAC manifests that
// does not already have an explicit Namespace manifest anywhere in the full set,
// a synthetic Namespace manifest is prepended to the workload slice.
// wrapper-schema.md §4, Governor ruling 2026-04-22 three-bucket split.
func SplitManifests(manifests []Manifest) (rbac, clusterScoped, workload []Manifest) {
	// Collect all explicitly declared Namespace names.
	explicitNamespaces := make(map[string]struct{})
	for _, m := range manifests {
		if m.Kind == "Namespace" && m.Name != "" {
			explicitNamespaces[m.Name] = struct{}{}
		}
	}

	for _, m := range manifests {
		switch {
		case rbacKinds[m.Kind]:
			rbac = append(rbac, m)
		case clusterScopedKinds[m.Kind]:
			clusterScoped = append(clusterScoped, m)
		default:
			workload = append(workload, m)
		}
	}

	// Inject synthetic Namespace manifests into workload for namespaces
	// referenced by RBAC resources that are not already declared.
	var injected []Manifest
	seen := make(map[string]struct{})
	for _, m := range rbac {
		if m.Namespace == "" {
			continue
		}
		if _, explicit := explicitNamespaces[m.Namespace]; explicit {
			continue
		}
		if _, already := seen[m.Namespace]; already {
			continue
		}
		seen[m.Namespace] = struct{}{}
		injected = append(injected, Manifest{
			Kind:      "Namespace",
			Namespace: "",
			Name:      m.Namespace,
			YAML:      fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", m.Namespace),
		})
	}
	workload = append(injected, workload...)
	return
}

// SplitRBACAndWorkload is the legacy two-bucket split. Preserved for callers
// that do not yet need the cluster-scoped bucket. Delegates to SplitManifests
// and merges clusterScoped into workload so callers are unaffected.
func SplitRBACAndWorkload(manifests []Manifest) (rbac, workload []Manifest) {
	r, cs, w := SplitManifests(manifests)
	return r, append(cs, w...)
}
