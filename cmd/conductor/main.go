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
	"context"
	"flag"
	"fmt"
	"os"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/internal/persistence"
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
// resolves the named capability from the registry, executes it, writes
// OperationResult to the named ConfigMap, and exits.
// conductor-design.md §4.2, conductor-schema.md §8.
func runExecute() {
	execCtx, err := config.BuildExecuteContext()
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: %v\n", err)
		os.Exit(1)
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: build in-cluster config: %v\n", err)
		os.Exit(1)
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: build kube client: %v\n", err)
		os.Exit(1)
	}

	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: build dynamic client: %v\n", err)
		os.Exit(1)
	}

	reg := capability.NewRegistry()
	capability.RegisterAll(reg)

	// TalosClient: constructed from the mounted talosconfig Secret. The
	// TALOSCONFIG_PATH env var points to the file inside the mounted volume.
	// Capabilities that do not require Talos access (pack-deploy, rbac-provision)
	// are unaffected when this is nil — handlers return ValidationFailure if they
	// require it and find it absent.
	var talosClient capability.TalosNodeClient
	if talosconfigPath := os.Getenv("TALOSCONFIG_PATH"); talosconfigPath != "" {
		adapter, err := capability.NewTalosClientAdapter(context.Background(), talosconfigPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "conductor execute: build talos client: %v\n", err)
			os.Exit(1)
		}
		defer adapter.Close() //nolint:errcheck
		talosClient = adapter
	}

	// StorageClient: constructed from S3_REGION (required) and S3_ENDPOINT
	// (optional). Only non-nil when both the CAPABILITY and S3_REGION env vars
	// are set — capabilities that do not need storage (all except etcd-backup,
	// etcd-restore) leave S3_REGION unset.
	var storageClient capability.StorageClient
	if os.Getenv("S3_REGION") != "" {
		adapter, err := capability.NewS3StorageClientAdapter(context.Background())
		if err != nil {
			fmt.Fprintf(os.Stderr, "conductor execute: build S3 client: %v\n", err)
			os.Exit(1)
		}
		storageClient = adapter
	}

	// OCIClient: always constructed; uses only stdlib net/http.
	ociClient := capability.NewOCIRegistryClientAdapter()

	clients := capability.ExecuteClients{
		KubeClient:    kubeClient,
		DynamicClient: dynamicClient,
		TalosClient:   talosClient,
		StorageClient: storageClient,
		OCIClient:     ociClient,
	}

	writer := persistence.NewKubeConfigMapWriter(kubeClient)
	if err := kernel.RunExecute(execCtx, reg, writer, clients); err != nil {
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

	execCtx, err := config.BuildAgentContext(*clusterRef)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: %v\n", err)
		os.Exit(1)
	}

	cfg, err := rest.InClusterConfig()
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: build in-cluster config: %v\n", err)
		os.Exit(1)
	}
	kubeClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: build kube client: %v\n", err)
		os.Exit(1)
	}
	dynamicClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: build dynamic client: %v\n", err)
		os.Exit(1)
	}

	goCtx := context.Background()
	if err := kernel.RunAgent(goCtx, execCtx, kubeClient, dynamicClient); err != nil {
		fmt.Fprintf(os.Stderr, "conductor agent: %v\n", err)
		os.Exit(1)
	}
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
