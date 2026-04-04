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
// TODO: implement full pipeline in a dedicated Conductor Engineer session
// covering Helm rendering, SOPS encryption, and talosconfig generation.
func compileCluster(input, output string) error {
	// Stub: validate paths are non-empty (already checked by caller).
	// Full pipeline: load TalosCluster spec → validate → generate secrets →
	// SOPS-encrypt → write to output. conductor-schema.md §9 Phase 1-5.
	fmt.Printf("compiler: compile cluster: input=%s output=%s [pipeline stub]\n", input, output)
	return nil
}

// compilePack runs the pack compilation pipeline.
// Renders Helm charts, resolves Kustomize overlays, normalizes manifests,
// pins image digests, and pushes a ClusterPack OCI artifact.
// conductor-schema.md §9, CapabilityPackCompile.
//
// TODO: implement full pipeline in a dedicated Conductor Engineer session
// covering Helm rendering, Kustomize resolution, and OCI push.
func compilePack(input, output string) error {
	// Stub: validate paths are non-empty (already checked by caller).
	// Full pipeline: load PackBuild spec → Helm render → Kustomize resolve →
	// normalize → validate → artifact construction → OCI push.
	// conductor-schema.md §9 Phase 1-5.
	fmt.Printf("compiler: compile pack: input=%s output=%s [pipeline stub]\n", input, output)
	return nil
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: compiler <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  compile cluster --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  compile pack    --input <path> --output <path>")
}
