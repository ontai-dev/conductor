// compile_packbuild_split.go re-exports the manifest splitting helpers from
// internal/packbuild for use within the compiler main package.
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

// SplitManifests partitions a PackManifest slice into RBAC, cluster-scoped, and
// workload slices. Delegates to packbuild.SplitManifests.
func SplitManifests(manifests []PackManifest) (rbac, clusterScoped, workload []PackManifest) {
	return packbuild.SplitManifests(manifests)
}

// SplitRBACAndWorkload is the legacy two-bucket split. Preserved for any
// callers that do not need the cluster-scoped bucket. Delegates to
// packbuild.SplitRBACAndWorkload.
func SplitRBACAndWorkload(manifests []PackManifest) (rbac, workload []PackManifest) {
	return packbuild.SplitRBACAndWorkload(manifests)
}
