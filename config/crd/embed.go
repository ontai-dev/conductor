// Package crd previously embedded conductor's own CRD YAML files.
// After T-2B-9 migration, all conductor CRDs (InfrastructureRunnerConfig,
// InfrastructurePackReceipt) are declared in seam-core (infrastructure.ontai.dev).
// The compiler bundles them from seam-core/config/crd directly.
// This package is retained for structural consistency only.
package crd

import "embed"

// FS is an empty embedded filesystem. Conductor's CRDs are now in seam-core.
var FS embed.FS
