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
	"net/http"
	"os"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/internal/persistence"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
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

// runExecute implements the execute-mode step sequencer pipeline.
// Reads CLUSTER_REF and POD_NAMESPACE from the environment, loads RunnerConfig
// steps, materialises one capability Job per step in declared order, harvests
// ConfigMap results, and writes StepResults to RunnerConfig status.
// conductor-design.md §4.2, conductor-schema.md §17.
func runExecute() {
	execCtx, err := config.BuildExecuteContext()
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: %v\n", err)
		os.Exit(1)
	}

	// Construct a synthetic single-step RunnerConfig from env vars.
	// BuildExecuteContext populates Capability/ClusterRef/OperationResultCM but
	// leaves RunnerConfig.Steps empty. kernel.RunExecute requires ≥1 step.
	// conductor-schema.md §17.
	execCtx.RunnerConfig = runnerlib.RunnerConfigSpec{
		ClusterRef:  execCtx.ClusterRef,
		RunnerImage: os.Getenv("CONDUCTOR_IMAGE"),
		Steps: []runnerlib.RunnerConfigStep{
			{
				Name:          execCtx.Capability,
				Capability:    execCtx.Capability,
				HaltOnFailure: true,
				Parameters:    buildStepParameters(),
			},
		},
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

	// capabilityStepExecutor wraps the registry and persistence writer to
	// implement kernel.StepExecutor — dispatches each step's capability inline.
	// TODO: replace with a Job-materialising implementation once the sequencer
	// architecture moves to true child-Job creation per step.
	executor := &capabilityStepExecutor{
		reg:     reg,
		writer:  writer,
		clients: clients,
	}

	// NoopStepStatusWriter is used until the RunnerConfig status-write
	// implementation lands. Step results are recorded in the output ConfigMaps
	// and harvested by the owning operator via status reads.
	// TODO: replace with a real RunnerConfig status writer that patches
	// RunnerConfig.status.stepResults via the Kubernetes API.
	statusWriter := kernel.NoopStepStatusWriter{}

	if err := kernel.RunExecute(execCtx, executor, statusWriter); err != nil {
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

	// Start the Prometheus metrics HTTP server on METRICS_ADDR (default :8080).
	// Controller-runtime registers its default collectors on ctrlmetrics.Registry
	// automatically. ServiceMonitor CRDs for Prometheus Operator scrape configuration
	// are deferred to a post-e2e observability session.
	metricsAddr := ":8080"
	if v := os.Getenv("METRICS_ADDR"); v != "" {
		metricsAddr = v
	}
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(ctrlmetrics.Registry, promhttp.HandlerOpts{}))
	go func() {
		server := &http.Server{Addr: metricsAddr, Handler: metricsMux}
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "conductor agent: metrics server: %v\n", err)
		}
	}()

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

// buildStepParameters collects pack-deploy relevant env vars into the
// RunnerConfigStep.Parameters map. Keys match the parameter vocabulary
// consumed by capability handlers. wrapper-schema.md §4, conductor-schema.md §6.
func buildStepParameters() map[string]string {
	params := map[string]string{}
	if v := os.Getenv("PACK_REGISTRY_REF"); v != "" {
		params["registryRef"] = v
	}
	if v := os.Getenv("PACK_CHECKSUM"); v != "" {
		params["checksum"] = v
	}
	if v := os.Getenv("PACK_SIGNATURE"); v != "" {
		params["signature"] = v
	}
	if v := os.Getenv("OPERATION_RESULT_CM"); v != "" {
		params["operationResultCM"] = v
	}
	kubeconfigPath := "/var/run/secrets/kubeconfig/kubeconfig"
	if v := os.Getenv("KUBECONFIG"); v != "" {
		kubeconfigPath = v
	}
	params["kubeconfigPath"] = kubeconfigPath
	return params
}

func printUsage() {
	fmt.Fprintln(os.Stderr, "Usage: conductor <subcommand> [flags]")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Subcommands:")
	fmt.Fprintln(os.Stderr, "  execute              (reads CLUSTER_REF, POD_NAMESPACE from env; steps from RunnerConfig)")
	fmt.Fprintln(os.Stderr, "  agent --cluster-ref <name>")
	fmt.Fprintln(os.Stderr, "")
	fmt.Fprintln(os.Stderr, "Note: compile mode is not supported by the Conductor binary. Use the Compiler binary.")
}

// capabilityStepExecutor implements kernel.StepExecutor by dispatching each step
// to the capability registry directly and writing the result ConfigMap via the
// persistence writer. This is the inline-dispatch production implementation —
// each step runs its capability handler in the same process.
//
// TODO: replace with a Job-materialising implementation once the sequencer
// architecture evolves to create a Kueue child Job per step.
type capabilityStepExecutor struct {
	reg     *capability.Registry
	writer  persistence.ConfigMapWriter
	clients capability.ExecuteClients
}

func (e *capabilityStepExecutor) Execute(
	ctx context.Context,
	step runnerlib.RunnerConfigStep,
	clusterRef, namespace string,
) (runnerlib.RunnerConfigStepResult, error) {
	handler, err := e.reg.Resolve(step.Capability)
	if err != nil {
		// Capability not registered — return a Failed result, not a Go error.
		// The sequencer decides whether to halt based on HaltOnFailure.
		return runnerlib.RunnerConfigStepResult{
			StepName: step.Name,
			Phase:    runnerlib.StepPhaseFailed,
		}, nil
	}

	// Derive a per-step ConfigMap name from step name and clusterRef.
	cmName := fmt.Sprintf("step-%s-%s", step.Name, clusterRef)

	params := capability.ExecuteParams{
		Capability:        step.Capability,
		ClusterRef:        clusterRef,
		OperationResultCM: cmName,
		Namespace:         namespace,
		ExecuteClients:    e.clients,
	}
	result, err := handler.Execute(ctx, params)
	if err != nil {
		return runnerlib.RunnerConfigStepResult{}, fmt.Errorf(
			"capabilityStepExecutor: step %q capability %q: %w", step.Name, step.Capability, err)
	}

	if writeErr := e.writer.WriteResult(ctx, namespace, cmName, result); writeErr != nil {
		return runnerlib.RunnerConfigStepResult{}, fmt.Errorf(
			"capabilityStepExecutor: write ConfigMap for step %q: %w", step.Name, writeErr)
	}

	phase := runnerlib.StepPhaseSucceeded
	if result.Status == runnerlib.ResultFailed {
		phase = runnerlib.StepPhaseFailed
	}

	return runnerlib.RunnerConfigStepResult{
		StepName:  step.Name,
		Phase:     phase,
		OutputRef: runnerlib.ConfigMapRef{Namespace: namespace, Name: cmName},
	}, nil
}
