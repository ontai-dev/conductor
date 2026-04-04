// Binary compiler is the Compiler binary entry point.
//
// The Compiler runs in compile mode only. It is a short-lived tool invoked by
// humans or by the bootstrap pipeline on the operator workstation or in a
// compile-phase pipeline. It is never deployed to any cluster. INV-022.
//
// Subcommands:
//
//	compiler compile cluster --input <path> --output <path>
//	compiler compile pack    --input <path> --output <path>
//
// conductor-schema.md §9, conductor-design.md §3.
package main

import (
	"flag"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "compile":
		runCompile(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "compiler: unknown subcommand %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

// runCompile dispatches to the cluster or pack compile pipeline.
// conductor-schema.md §9, conductor-design.md §4.1.
func runCompile(args []string) {
	if len(args) < 1 {
		fmt.Fprintln(os.Stderr, "compiler compile: requires target: cluster or pack")
		os.Exit(1)
	}

	fs := flag.NewFlagSet("compile", flag.ExitOnError)
	input := fs.String("input", "", "Path to input spec file or directory (required)")
	output := fs.String("output", "", "Path to output directory (required)")

	target := args[0]
	if err := fs.Parse(args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "compiler compile: flag error: %v\n", err)
		os.Exit(1)
	}

	if *input == "" {
		fmt.Fprintln(os.Stderr, "compiler compile: --input is required")
		os.Exit(1)
	}
	if *output == "" {
		fmt.Fprintln(os.Stderr, "compiler compile: --output is required")
		os.Exit(1)
	}

	switch target {
	case "cluster":
		if err := compileCluster(*input, *output); err != nil {
			fmt.Fprintf(os.Stderr, "compiler compile cluster: %v\n", err)
			os.Exit(1)
		}
	case "pack":
		if err := compilePack(*input, *output); err != nil {
			fmt.Fprintf(os.Stderr, "compiler compile pack: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "compiler compile: unknown target %q; want cluster or pack\n", target)
		os.Exit(1)
	}
}

// compileCluster runs the cluster compilation pipeline.
// Validates TalosCluster spec, generates and SOPS-encrypts secrets, writes
// encrypted files to the output path. conductor-schema.md §9.
//
// Pipeline (conductor-schema.md §9):
//  1. Read TalosCluster spec and human-provided machineconfig files.
//  2. Validate TalosCluster spec against platform-schema.md rules.
//  3. Validate machineconfig structure against the declared Talos version.
//  4. SOPS-encrypt talos-secret, machineconfigs, talosconfig using admin's age key.
//  5. Write encrypted files to the output path (clusters/{cluster-name}/ in git).
//  6. Produce validation report.
//
// NOT YET IMPLEMENTED — blocked on SOPS age-encryption client package
// (internal/clients/sops). Schedule a dedicated Conductor Engineer session
// to add the SOPS handler and implement this pipeline end-to-end.
func compileCluster(input, output string) error {
	return fmt.Errorf(
		"compile cluster: not yet implemented\n"+
			"  input: %s\n"+
			"  output: %s\n"+
			"  blocked: SOPS age-encryption client (internal/clients/sops) not yet implemented\n"+
			"  action: schedule a dedicated Conductor Engineer session — conductor-schema.md §9",
		input, output,
	)
}

// compilePack runs the pack compilation pipeline.
// Renders Helm charts, resolves Kustomize overlays, normalizes manifests,
// pins image digests, and pushes a ClusterPack OCI artifact.
// conductor-schema.md §9, CapabilityPackCompile.
//
// Pipeline (conductor-schema.md §9):
//  1. Read PackBuild spec.
//  2. Pull Helm chart via helm goclient. Render with declared values.
//  3. Resolve Kustomize overlay via kustomize goclient.
//  4. Normalize all inputs to flat Kubernetes manifests.
//  5. Validate all resources against target Kubernetes version schemas.
//  6. Build execution order from declared dependencies. Fail if acyclic check fails.
//  7. Generate minimum necessary RBAC.
//  8. Pin all image references to digest.
//  9. Compute content-addressed checksum.
// 10. Generate provenance record with build identity, timestamp, source digests.
// 11. Push ClusterPack artifact to OCI registry.
// 12. Write OperationResult with registered ClusterPack version and digest.
// 13. Exit.
//
// NOT YET IMPLEMENTED — blocked on compile-mode client packages:
//   internal/clients/helm (Helm chart rendering)
//   internal/clients/kustomize (Kustomize overlay resolution)
// Schedule a dedicated Conductor Engineer session to add these packages and
// implement this pipeline end-to-end. OCI push is available via internal/capability
// adapters — that step can reuse the existing adapter once the above are in place.
func compilePack(input, output string) error {
	return fmt.Errorf(
		"compile pack: not yet implemented\n"+
			"  input: %s\n"+
			"  output: %s\n"+
			"  blocked: Helm goclient (internal/clients/helm) and Kustomize goclient (internal/clients/kustomize) not yet implemented\n"+
			"  action: schedule a dedicated Conductor Engineer session — conductor-schema.md §9",
		input, output,
	)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: compiler <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  compile cluster --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  compile pack    --input <path> --output <path>")
}
