// compile_packbuild_split.go re-exports the RBAC/workload manifest splitting
// helpers from internal/packbuild for use within the compiler main package.
// wrapper-schema.md §4.
package main

import "github.com/ontai-dev/conductor/internal/packbuild"

// PackManifest aliases packbuild.Manifest for use within the compiler main package.
type PackManifest = packbuild.Manifest

// ParsePackManifests parses a multi-document YAML string into PackManifest records.
// Delegates to packbuild.ParseManifests.
func ParsePackManifests(multiYAML string) ([]PackManifest, error) {
	return packbuild.ParseManifests(multiYAML)
}

// SplitRBACAndWorkload partitions a PackManifest slice into RBAC and workload slices.
// Delegates to packbuild.SplitRBACAndWorkload. Pure function with no side effects.
func SplitRBACAndWorkload(manifests []PackManifest) (rbac, workload []PackManifest) {
	return packbuild.SplitRBACAndWorkload(manifests)
}
