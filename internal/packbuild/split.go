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
// Kind field. Used as input and output of SplitRBACAndWorkload.
type Manifest struct {
	// Kind is the Kubernetes resource kind extracted from the manifest.
	Kind string
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
		result = append(result, Manifest{Kind: kind, YAML: trimmed})
	}
	return result, nil
}

// SplitRBACAndWorkload partitions manifests into two slices:
// rbac (ServiceAccount, Role, ClusterRole, RoleBinding, ClusterRoleBinding)
// and workload (everything else). Pure function: no side effects, no OCI calls,
// no modification of the input slice. wrapper-schema.md §4.
func SplitRBACAndWorkload(manifests []Manifest) (rbac, workload []Manifest) {
	for _, m := range manifests {
		if rbacKinds[m.Kind] {
			rbac = append(rbac, m)
		} else {
			workload = append(workload, m)
		}
	}
	return
}
