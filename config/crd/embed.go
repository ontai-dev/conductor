// Package crd exposes the conductor shared library's CRD YAML files
// as an embedded filesystem. Contains the RunnerConfig CRD for the
// runner.ontai.dev API group. Imported by the Compiler binary to build
// the CRD manifest bundle for compiler launch. conductor-schema.md §9.
package crd

import "embed"

// FS contains all CRD YAML files from this directory.
//
//go:embed *.yaml
var FS embed.FS
