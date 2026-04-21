// Package packbuild implements compile-time helpers for ClusterPack artifact
// assembly. The split functions separate RBAC resources from workload resources
// so the two-layer OCI artifact contract can be enforced.
// wrapper-schema.md §4.
package packbuild

import (
	"fmt"
	"strings"

	sigsyaml "sigs.k8s.io/yaml"
)

// rbacKinds is the set of Kubernetes resource kinds treated as RBAC resources.
// These are routed through guardian /rbac-intake before workload resources are
// applied. wrapper-schema.md §4.
var rbacKinds = map[string]bool{
	"ServiceAccount":     true,
	"Role":               true,
	"ClusterRole":        true,
	"RoleBinding":        true,
	"ClusterRoleBinding": true,
}

// Manifest holds a single Kubernetes manifest as raw YAML text plus the parsed
// Kind and Namespace fields. Used as input and output of SplitRBACAndWorkload.
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

// SplitRBACAndWorkload partitions manifests into two slices:
//   - rbac: ServiceAccount, Role, ClusterRole, RoleBinding, ClusterRoleBinding
//   - workload: everything else
//
// Namespace injection: for each namespace referenced by RBAC manifests that
// does not already have an explicit Namespace manifest anywhere in the full set,
// a synthetic Namespace manifest is prepended to the workload slice. This
// ensures the target cluster has the namespace before the pack-deploy capability
// applies RBAC resources into it. Namespace manifests are always in the workload
// layer (never RBAC) because guardian may not be present on all tenant clusters.
// wrapper-schema.md §4, Governor-approved session/13.
func SplitRBACAndWorkload(manifests []Manifest) (rbac, workload []Manifest) {
	// Collect all explicitly declared Namespace names.
	explicitNamespaces := make(map[string]struct{})
	for _, m := range manifests {
		if m.Kind == "Namespace" && m.Name != "" {
			explicitNamespaces[m.Name] = struct{}{}
		}
	}

	for _, m := range manifests {
		if rbacKinds[m.Kind] {
			rbac = append(rbac, m)
		} else {
			workload = append(workload, m)
		}
	}

	// Inject a synthetic Namespace manifest into the workload slice for each
	// namespace referenced by RBAC resources that is not already declared.
	// These are prepended so they are applied first when the workload layer is
	// processed before guardian rbac-intake. wrapper-schema.md §4.
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
			YAML: fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", m.Namespace),
		})
	}
	workload = append(injected, workload...)
	return
}
