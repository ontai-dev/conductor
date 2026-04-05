// Binary compiler is the Compiler binary entry point.
//
// The Compiler is a CR compiler — it reads human-authored spec files,
// validates them against platform schema rules, and produces Kubernetes CR
// YAML ready to apply to the management cluster. It is a short-lived tool
// invoked by humans or the bootstrap pipeline on the operator workstation
// or in a compile-phase pipeline. It is never deployed to any cluster. INV-022.
//
// Subcommands:
//
//	compiler bootstrap --input <path> --output <path>
//	compiler launch    --output <path>
//	compiler enable    --output <path> [--version <tag>]
//	compiler packbuild --input <path> --output <path>
//	compiler domain
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
	case "bootstrap":
		runSubcommand("bootstrap", os.Args[2:], compileBootstrap)
	case "launch":
		runLaunchSubcommand(os.Args[2:])
	case "enable":
		runEnableSubcommand(os.Args[2:])
	case "packbuild":
		runSubcommand("packbuild", os.Args[2:], compilePackBuild)
	case "component":
		runComponentSubcommand(os.Args[2:])
	case "maintenance":
		runMaintenanceSubcommand(os.Args[2:])
	case "domain":
		fmt.Fprintln(os.Stderr, "compiler domain: reserved — domain CR compilation is not yet implemented")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "compiler: unknown subcommand %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

// runSubcommand parses --input and --output flags then calls fn.
// All four active subcommands share this flag shape.
func runSubcommand(name string, args []string, fn func(input, output string) error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	input := fs.String("input", "", "Path to input spec file (required)")
	output := fs.String("output", "", "Path to output directory (required)")

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "compiler %s: flag error: %v\n", name, err)
		os.Exit(1)
	}
	if *input == "" {
		fmt.Fprintf(os.Stderr, "compiler %s: --input is required\n", name)
		os.Exit(1)
	}
	if *output == "" {
		fmt.Fprintf(os.Stderr, "compiler %s: --output is required\n", name)
		os.Exit(1)
	}

	if err := fn(*input, *output); err != nil {
		fmt.Fprintf(os.Stderr, "compiler %s: %v\n", name, err)
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: compiler <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  bootstrap --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  launch    --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  enable    --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  packbuild --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  component  --component <name> [--component <name>...] --namespace <ns> --target-clusters <list> --rbac-policy-ref <name> [--output <path>]")
	fmt.Fprintln(os.Stderr, "  component  --descriptor <path> [--output <path>]")
	fmt.Fprintln(os.Stderr, "  component  --discover [--namespace <ns>] [--kubeconfig <path>] [--output <path>]")
	fmt.Fprintln(os.Stderr, "  maintenance --operation <type> --cluster <name> --output <path>")
	fmt.Fprintln(os.Stderr, "  domain")
}
