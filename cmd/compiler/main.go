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
		runBootstrapSubcommand(os.Args[2:])
	case "launch":
		runLaunchSubcommand(os.Args[2:])
	case "enable":
		runEnableSubcommand(os.Args[2:])
	case "packbuild":
		runSubcommand("packbuild", os.Args[2:], packbuildHelp, compilePackBuild)
	case "component":
		runComponentSubcommand(os.Args[2:])
	case "maintenance":
		runMaintenanceSubcommand(os.Args[2:])
	case "domain":
		fmt.Fprintln(os.Stderr, "this subcommand is reserved for future Sovereign Domain surface and is not yet implemented")
		os.Exit(1)
	default:
		fmt.Fprintf(os.Stderr, "compiler: unknown subcommand %q\n", os.Args[1])
		printUsage()
		os.Exit(1)
	}
}

// bootstrapHelp is the authored per-subcommand help for 'compiler bootstrap'.
const bootstrapHelp = `Usage: compiler bootstrap --input <path> --output <path> [--kubeconfig <path>]

Compile a cluster declaration YAML into Talos machine config Secrets and bootstrap CRs.

Input contract:
  --input      Path to a cluster declaration YAML file (ClusterInput schema).
               Declares cluster name, mode, node list, and optional patches.

  --kubeconfig Path to a kubeconfig file (flag → $KUBECONFIG → ~/.kube/config).
               Required only when importExistingCluster: true in the input file.
               When importExistingCluster=true, Compiler connects to the cluster
               Kubernetes API, reads the init-node machine config Secret from
               seam-system, and derives the PKI bundle from the existing CAs.

Output contract:
  --output  Directory receiving:
              seam-mc-{cluster}-{hostname}.yaml — Talos machine config Secret per node.
                Each Secret embeds the full Talos machine config, including all patches
                declared via the patches, registryMirrors, and ciliumPrerequisites fields.
              {cluster-name}.yaml               — TalosCluster CR ready to apply.
              bootstrap-sequence.yaml           — Ordered bootstrap step manifest.

ClusterInput optional fields (set in the --input YAML):
  patches:               []string      — YAML patches deep-merged into every node's machine config.
  ciliumPrerequisites:   bool          — Inject br_netfilter, xt_socket, and rp_filter sysctls.
  registryMirrors:       []            — registry/endpoints pairs injected into registries.mirrors.
  importExistingCluster: bool          — Extract PKI from the running cluster instead of generating fresh.
  machineConfigPaths:    map[str]str   — Hostname to local Talos machine config YAML file path.
                                         When non-empty with importExistingCluster=true, reads CA from
                                         local files instead of querying the Kubernetes API. Init node
                                         entry required. Use for pre-Seam clusters (no seam-mc Secrets).
                                         Example:
                                           machineConfigPaths:
                                             ccs-mgmt-cp1: /path/to/controlplane.yaml

Compile-only: output is a manifest set for human review and GitOps pipeline
application — Compiler never applies, patches, or deletes any resource.
`

// runBootstrapSubcommand parses bootstrap-specific flags and calls compileBootstrap.
// Handles the --kubeconfig flag needed for importExistingCluster mode in addition
// to the standard --input and --output flags. conductor-schema.md §9 Step 1.
func runBootstrapSubcommand(args []string) {
	fs := flag.NewFlagSet("bootstrap", flag.ExitOnError)
	input := fs.String("input", "", "Path to cluster declaration YAML (required)")
	output := fs.String("output", "", "Output directory for manifests (required)")
	kubecfg := fs.String("kubeconfig", "", "Path to kubeconfig for importExistingCluster mode (flag → $KUBECONFIG → ~/.kube/config)")

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, bootstrapHelp)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "compiler bootstrap: flag error: %v\n", err)
		os.Exit(1)
	}
	if *input == "" {
		fmt.Fprintln(os.Stderr, "compiler bootstrap: --input is required")
		os.Exit(1)
	}
	if *output == "" {
		fmt.Fprintln(os.Stderr, "compiler bootstrap: --output is required")
		os.Exit(1)
	}

	if err := compileBootstrap(*input, *output, *kubecfg); err != nil {
		fmt.Fprintf(os.Stderr, "compiler bootstrap: %v\n", err)
		os.Exit(1)
	}
}

// packbuildHelp is the authored per-subcommand help for 'compiler packbuild'.
const packbuildHelp = `Usage: compiler packbuild --input <path> --output <path>

Compile a PackBuild spec file into a ClusterPack CR.

Input contract:
  --input   Path to a PackBuild local spec file (human-authored pack descriptor).
            Declares pack name, version, included resources, and target cluster refs.

Output contract:
  --output  Directory receiving:
              <pack-name>-<version>.yaml  — ClusterPack CR ready to apply

Compile-only: output is a manifest for human review and GitOps pipeline
application — Compiler never applies, patches, or deletes any resource.
`

// runSubcommand parses --input and --output flags then calls fn.
// Bootstrap and packbuild share this flag shape.
func runSubcommand(name string, args []string, helpText string, fn func(input, output string) error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	input := fs.String("input", "", "Path to input spec file (required)")
	output := fs.String("output", "", "Path to output directory (required)")

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, helpText)
		fs.PrintDefaults()
	}

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
	fmt.Fprintln(os.Stderr, "Compiler produces manifests for human review — it never applies resources to any cluster.")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Usage: compiler <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  bootstrap    Compile a cluster declaration into machine configs and bootstrap CRs")
	fmt.Fprintln(os.Stderr, "  launch       Produce the CRD bundle for management cluster bootstrap (Step 2)")
	fmt.Fprintln(os.Stderr, "  enable       Produce the phased deployment manifest bundle (Steps 3–8)")
	fmt.Fprintln(os.Stderr, "  packbuild    Compile a PackBuild spec into a ClusterPack CR")
	fmt.Fprintln(os.Stderr, "  maintenance  Compile a MaintenanceBundle CR with pre-resolved scheduling context")
	fmt.Fprintln(os.Stderr, "  component    Produce RBACProfile CR YAML from the embedded catalog or a descriptor")
	fmt.Fprintln(os.Stderr, "  domain       Reserved — not yet implemented")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Run 'compiler <subcommand> -h' for subcommand-specific flags and contracts.")
}
