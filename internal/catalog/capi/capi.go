// Package capi provides embedded CAPI provider manifests for use by the
// compiler enable phase 00b (capi-prerequisites).
//
// All manifests are pinned to specific upstream versions and embedded at
// compile time — no runtime network fetch occurs. Updating a provider version
// requires authoring a new YAML file and incrementing the corresponding
// Version constant, submitted via PR with Platform Governor review.
//
// platform-schema.md §3 CAPI composition model.
// conductor-schema.md §9 phase 00b.
package capi

import _ "embed"

// CAPIVersion is the pinned cluster-api core version embedded in this package.
const CAPIVersion = "v1.9.4"

// TalosBootstrapVersion is the pinned Talos CAPI bootstrap provider version.
const TalosBootstrapVersion = "v0.6.8"

// TalosControlPlaneVersion is the pinned Talos CAPI control plane provider version.
const TalosControlPlaneVersion = "v0.5.8"

// CoreManifest is the CAPI core operator manifest (Namespace, CRDs, RBAC, Deployment)
// embedded at compile time from capi-core.yaml. It is applied as the first file
// in the 00b-capi-prerequisites phase.
//
//go:embed capi-core.yaml
var CoreManifest []byte

// TalosBootstrapManifest is the Talos CAPI bootstrap provider manifest
// (CRDs, RBAC, Deployment) embedded at compile time from capi-talos-bootstrap.yaml.
//
//go:embed capi-talos-bootstrap.yaml
var TalosBootstrapManifest []byte

// TalosControlPlaneManifest is the Talos CAPI control plane provider manifest
// (CRDs, RBAC, Deployment) embedded at compile time from capi-talos-controlplane.yaml.
//
//go:embed capi-talos-controlplane.yaml
var TalosControlPlaneManifest []byte

// SeamInfrastructureCRDs is the SeamInfrastructureCluster and SeamInfrastructureMachine
// CRD definitions from the platform repository, embedded at compile time from
// seam-infrastructure-crds.yaml. platform-schema.md §4.
//
//go:embed seam-infrastructure-crds.yaml
var SeamInfrastructureCRDs []byte
