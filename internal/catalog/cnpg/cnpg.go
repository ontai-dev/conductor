// Package cnpg provides the embedded CloudNativePG operator manifest for use
// by the compiler enable phase 0 (infrastructure-dependencies).
//
// The manifest is pinned to Version and embedded at compile time — no runtime
// network fetch occurs. Updating the CNPG version requires authoring a new
// operator.yaml and incrementing Version, submitted via PR with Platform Governor
// review per conductor-schema.md §16.
//
// guardian-schema.md §16 CNPG Deployment Contract.
// conductor-schema.md §9 phase 0.
package cnpg

import _ "embed"

// Version is the pinned CloudNativePG operator version embedded in this package.
const Version = "v1.24.0"

// OperatorManifest is the complete CNPG operator manifest (Namespace, CRDs,
// ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment) embedded at
// compile time from operator.yaml. It is applied verbatim by the enable pipeline
// as phase 0 before any Seam operator is deployed.
//
//go:embed operator.yaml
var OperatorManifest []byte
