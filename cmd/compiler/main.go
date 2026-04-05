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
//	compiler launch    --input <path> --output <path>
//	compiler enable    --input <path> --output <path>
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
		runSubcommand("launch", os.Args[2:], compileLaunch)
	case "enable":
		runSubcommand("enable", os.Args[2:], compileEnable)
	case "packbuild":
		runSubcommand("packbuild", os.Args[2:], compilePackBuild)
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

// compileBootstrap compiles a TalosCluster CR for the bootstrap phase.
// Reads the human-provided cluster spec, validates it against bootstrap
// phase rules, and writes a TalosCluster CR YAML to the output directory.
// The bootstrap phase provisions tenant cluster agents and is declared in
// RunnerConfig.phases. conductor-schema.md §5, platform-schema.md.
//
// Pipeline:
//  1. Read cluster spec from input path.
//  2. Validate spec against platform-schema.md bootstrap phase rules.
//  3. Emit TalosCluster CR YAML to output directory.
//
// NOT YET IMPLEMENTED — requires TalosCluster CR type integration from the
// platform library. Schedule a Platform + Conductor joint Engineer session.
func compileBootstrap(input, output string) error {
	return fmt.Errorf(
		"not yet implemented\n"+
			"  input: %s\n"+
			"  output: %s\n"+
			"  blocked: TalosCluster CR type not yet integrated from platform library\n"+
			"  action: schedule a Platform + Conductor joint Engineer session",
		input, output,
	)
}

// compileLaunch compiles a TalosCluster CR for the launch phase.
// Reads the human-provided cluster spec, validates it against launch phase
// rules, and writes a TalosCluster CR YAML to the output directory. The
// launch phase is declared in RunnerConfig.phases for both management and
// tenant clusters. conductor-schema.md §5, platform-schema.md.
//
// Pipeline:
//  1. Read cluster spec from input path.
//  2. Validate spec against platform-schema.md launch phase rules.
//  3. Emit TalosCluster CR YAML to output directory.
//
// NOT YET IMPLEMENTED — requires TalosCluster CR type integration from the
// platform library. Schedule a Platform + Conductor joint Engineer session.
func compileLaunch(input, output string) error {
	return fmt.Errorf(
		"not yet implemented\n"+
			"  input: %s\n"+
			"  output: %s\n"+
			"  blocked: TalosCluster CR type not yet integrated from platform library\n"+
			"  action: schedule a Platform + Conductor joint Engineer session",
		input, output,
	)
}

// compileEnable compiles a TalosCluster CR for the enable phase.
// Reads the human-provided management cluster spec, validates it against
// enable phase rules, and writes a TalosCluster CR YAML to the output
// directory. The enable phase installs operators on the management cluster
// and is declared in RunnerConfig.phases for management clusters only.
// conductor-schema.md §5, platform-schema.md.
//
// Pipeline:
//  1. Read cluster spec from input path.
//  2. Validate spec against platform-schema.md enable phase rules.
//  3. Emit TalosCluster CR YAML to output directory.
//
// NOT YET IMPLEMENTED — requires TalosCluster CR type integration from the
// platform library. Schedule a Platform + Conductor joint Engineer session.
func compileEnable(input, output string) error {
	return fmt.Errorf(
		"not yet implemented\n"+
			"  input: %s\n"+
			"  output: %s\n"+
			"  blocked: TalosCluster CR type not yet integrated from platform library\n"+
			"  action: schedule a Platform + Conductor joint Engineer session",
		input, output,
	)
}

// compilePackBuild compiles a ClusterPack CR from a PackBuild spec.
// Reads the human-provided PackBuild spec, validates it against wrapper
// schema rules, and writes a ClusterPack CR YAML to the output directory.
// conductor-schema.md §6 (pack-compile), wrapper-schema.md.
//
// Pipeline:
//  1. Read PackBuild spec from input path.
//  2. Validate spec against wrapper-schema.md PackBuild rules.
//  3. Emit ClusterPack CR YAML to output directory.
//
// NOT YET IMPLEMENTED — requires ClusterPack CR type integration from the
// wrapper library. Schedule a Wrapper + Conductor joint Engineer session.
func compilePackBuild(input, output string) error {
	return fmt.Errorf(
		"not yet implemented\n"+
			"  input: %s\n"+
			"  output: %s\n"+
			"  blocked: ClusterPack CR type not yet integrated from wrapper library\n"+
			"  action: schedule a Wrapper + Conductor joint Engineer session",
		input, output,
	)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: compiler <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  bootstrap --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  launch    --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  enable    --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  packbuild --input <path> --output <path>")
	fmt.Fprintln(os.Stderr, "  domain")
}
