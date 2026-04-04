// Binary conductor is the Conductor binary entry point.
//
// The Conductor binary supports execute and agent modes only. It is deployed as
// a long-lived Deployment in ont-system (agent mode) and as short-lived Kueue
// Jobs (execute mode). It is distroless and never runs compile mode. INV-022,
// INV-023.
//
// Subcommands:
//
//	conductor execute   — reads CAPABILITY, CLUSTER_REF, OPERATION_RESULT_CM from env
//	conductor agent     -- --cluster-ref <name>
//
// Compile mode is explicitly refused with InvariantViolation exit. INV-023.
// conductor-schema.md §10, conductor-design.md §3.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	switch os.Args[1] {
	case "compile":
		// INV-023: Conductor binary does not support compile mode.
		// This check is the first thing that runs — before any other initialization.
		kernel.GuardNotCompileMode(config.ModeCompile)

	case "execute":
		runExecute()

	case "agent":
		runAgent(os.Args[2:])

	default:
		fmt.Fprintf(os.Stderr, "conductor: unknown subcommand %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

// runExecute implements the execute-mode pipeline.
// Reads CAPABILITY, CLUSTER_REF, and OPERATION_RESULT_CM from the environment,
// resolves the named capability from the registry, executes it, and exits.
// conductor-design.md §4.2.
func runExecute() {
	ctx, err := config.BuildExecuteContext()
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: %v\n", err)
		os.Exit(1)
	}

	reg := capability.NewRegistry()
	registerAllCapabilities(reg)

	if err := kernel.RunExecute(ctx, reg); err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: %v\n", err)
		os.Exit(1)
	}
}

// runAgent implements the agent-mode pipeline.
// Starts leader election and all long-running control loops.
// conductor-design.md §4.3.
func runAgent(args []string) {
	fs := flag.NewFlagSet("agent", flag.ExitOnError)
	clusterRef := fs.String("cluster-ref", "", "Cluster name this agent instance governs (required)")
	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: flag error: %v\n", err)
		os.Exit(1)
	}

	ctx, err := config.BuildAgentContext(*clusterRef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: %v\n", err)
		os.Exit(1)
	}

	if err := kernel.RunAgent(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: %v\n", err)
		os.Exit(1)
	}
}

// registerAllCapabilities populates the capability registry with all named
// capabilities supported by this Conductor image. All registrations are static
// at build time — no runtime plugin loading. CR-INV-004, conductor-design.md §2.3.
func registerAllCapabilities(reg *capability.Registry) {
	capability.RegisterAll(reg)
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: conductor <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  execute              (reads CAPABILITY, CLUSTER_REF, OPERATION_RESULT_CM from env)")
	fmt.Fprintln(os.Stderr, "  agent --cluster-ref <name>")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Note: compile mode is not supported by the Conductor binary. Use the Compiler binary.")
}
