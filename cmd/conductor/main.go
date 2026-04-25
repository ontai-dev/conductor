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
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	seamv1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
	"github.com/ontai-dev/conductor/internal/capability"
	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
	"github.com/ontai-dev/conductor/internal/persistence"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

var seamScheme = runtime.NewScheme()

func init() {
	if err := seamv1alpha1.AddToScheme(seamScheme); err != nil {
		panic("conductor: failed to register seam-core scheme: " + err.Error())
	}
}

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
	execCtx.RunnerConfig = seamv1alpha1.InfrastructureRunnerConfigSpec{
		ClusterRef:  execCtx.ClusterRef,
		RunnerImage: os.Getenv("CONDUCTOR_IMAGE"),
		Steps: []seamv1alpha1.RunnerConfigStep{
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

	// GuardianClient: always constructed for pack-deploy split path (INV-004, wrapper-schema.md §4).
	// GUARDIAN_BASE_URL defaults to https://guardian.seam-system.svc:443.
	// TLS verification is skipped because the guardian cert is cluster-internal (cert-manager CA).
	guardianBaseURL := os.Getenv("GUARDIAN_BASE_URL")
	if guardianBaseURL == "" {
		guardianBaseURL = "https://guardian.seam-system.svc:443"
	}
	guardianHTTPClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true}, //nolint:gosec
		},
	}
	var guardianClient capability.GuardianIntakeClient = capability.NewGuardianIntakeClientAdapter(
		guardianBaseURL,
		dynamicClient,
		guardianHTTPClient,
	)

	clients := capability.ExecuteClients{
		KubeClient:    kubeClient,
		DynamicClient: dynamicClient,
		TalosClient:   talosClient,
		StorageClient: storageClient,
		OCIClient:     ociClient,
		GuardianClient: guardianClient,
	}

	ctrlClient, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: seamScheme})
	if err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: build ctrl-runtime client: %v\n", err)
		os.Exit(1)
	}

	// Select the appropriate result writer based on which target env var is set.
	// Day-2 path (OPERATION_RESULT_CR): writes InfrastructureTalosClusterOperationResult.
	// Pack path (OPERATION_RESULT_CM): writes PackOperationResult.
	var porWriter persistence.OperationResultWriter
	var tcorWriter persistence.TalosClusterResultWriter
	if execCtx.OperationResultCR != "" {
		tcorWriter = persistence.NewKubeTalosClusterResultWriter(ctrlClient)
	} else {
		porWriter = persistence.NewKubeOperationResultWriter(ctrlClient, execCtx.ClusterRef)
	}

	// capabilityStepExecutor wraps the registry and persistence writer to
	// implement kernel.StepExecutor — dispatches each step's capability inline.
	executor := &capabilityStepExecutor{
		reg:        reg,
		writer:     porWriter,
		tcorWriter: tcorWriter,
		clients:    clients,
	}

	// NoopStepStatusWriter is used until the RunnerConfig status-write
	// implementation lands. Step results are recorded in the output ConfigMaps
	// and harvested by the owning operator via status reads.
	// TODO: replace with a real RunnerConfig status writer that patches
	// RunnerConfig.status.stepResults via the Kubernetes API.
	statusWriter := kernel.NoopStepStatusWriter{}

	execLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(
		slog.String("component", "conductor-execute"),
		slog.String("cluster", execCtx.ClusterRef),
		slog.String("capability", execCtx.Capability),
	)
	execLogger.Info("starting")
	if err := kernel.RunExecute(execCtx, executor, statusWriter); err != nil {
		fmt.Fprintf(os.Stderr, "conductor execute: %v\n", err)
		os.Exit(1)
	}
	execLogger.Info("completed")
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

// buildStepParameters collects execute-mode env vars into the RunnerConfigStep.Parameters
// map. Keys match the parameter vocabulary consumed by capability handlers.
// wrapper-schema.md §4, conductor-schema.md §6.
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
	// Pack-deploy result target (wrapper path).
	if v := os.Getenv("OPERATION_RESULT_CM"); v != "" {
		params["operationResultCM"] = v
	}
	// Day-2 TalosCluster result target (platform path).
	if v := os.Getenv("OPERATION_RESULT_CR"); v != "" {
		params["operationResultCR"] = v
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
// to the capability registry directly and writing the result via the appropriate
// persistence writer. This is the inline-dispatch production implementation.
//
// Two writer paths:
//   - tcorWriter non-nil: day-2 TalosCluster ops — writes InfrastructureTalosClusterOperationResult CR.
//   - writer non-nil: pack-deploy ops — writes PackOperationResult CR.
//
// Exactly one writer is non-nil for a given Job invocation.
type capabilityStepExecutor struct {
	reg        *capability.Registry
	writer     persistence.OperationResultWriter
	tcorWriter persistence.TalosClusterResultWriter
	clients    capability.ExecuteClients
}

func (e *capabilityStepExecutor) Execute(
	ctx context.Context,
	step seamv1alpha1.RunnerConfigStep,
	clusterRef, namespace string,
) (seamv1alpha1.RunnerConfigStepResult, error) {
	handler, err := e.reg.Resolve(step.Capability)
	if err != nil {
		// Capability not registered — return a Failed result, not a Go error.
		// The sequencer decides whether to halt based on HaltOnFailure.
		return seamv1alpha1.RunnerConfigStepResult{
			Name:   step.Name,
			Status: seamv1alpha1.RunnerStepFailed,
		}, nil
	}

	params := capability.ExecuteParams{
		Capability:     step.Capability,
		ClusterRef:     clusterRef,
		Namespace:      namespace,
		ExecuteClients: e.clients,
	}

	// Day-2 path: OPERATION_RESULT_CR is set in step parameters.
	// Pack path: OPERATION_RESULT_CM carries the packExecutionRef.
	operationResultCR := step.Parameters["operationResultCR"]
	packExecutionRef := step.Parameters["operationResultCM"]
	if packExecutionRef == "" && operationResultCR == "" {
		packExecutionRef = fmt.Sprintf("step-%s-%s", step.Name, clusterRef)
	}
	params.OperationResultCM = packExecutionRef

	dispatchLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil)).With(
		slog.String("component", "conductor-execute"),
		slog.String("cluster", clusterRef),
		slog.String("capability", step.Capability),
	)
	dispatchLogger.Info("capability dispatching")
	result, err := handler.Execute(ctx, params)
	if err != nil {
		return seamv1alpha1.RunnerConfigStepResult{}, fmt.Errorf(
			"capabilityStepExecutor: step %q capability %q: %w", step.Name, step.Capability, err)
	}
	dispatchLogger.Info("capability result", slog.String("status", string(result.Status)))

	// Write result to the appropriate persistence target.
	if operationResultCR != "" && e.tcorWriter != nil {
		// Day-2 path: append record to per-cluster TCOR.
		// operationResultCR is the Job name (OPERATION_RESULT_CR env var), used as
		// jobRef so the platform reconciler can correlate the record with the Job it submitted.
		if writeErr := e.tcorWriter.AppendOperationRecord(ctx, clusterRef, operationResultCR, result); writeErr != nil {
			return seamv1alpha1.RunnerConfigStepResult{}, fmt.Errorf(
				"capabilityStepExecutor: append TCOR record for step %q: %w", step.Name, writeErr)
		}
	} else if e.writer != nil {
		// Pack path: write PackOperationResult.
		if writeErr := e.writer.WriteResult(ctx, namespace, packExecutionRef, result); writeErr != nil {
			return seamv1alpha1.RunnerConfigStepResult{}, fmt.Errorf(
				"capabilityStepExecutor: write POR for step %q: %w", step.Name, writeErr)
		}
	}

	status := seamv1alpha1.RunnerStepSucceeded
	if result.Status == runnerlib.ResultFailed {
		status = seamv1alpha1.RunnerStepFailed
	}

	return seamv1alpha1.RunnerConfigStepResult{
		Name:   step.Name,
		Status: status,
	}, nil
}
