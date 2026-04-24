// Package config implements the Configuration Layer: load and validate
// RunnerConfig, produce an immutable ExecutionContext for the entire lifecycle.
// conductor-design.md §2.2.
package config

import (
	"errors"
	"os"

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// Mode is a typed string declaring which execution mode this binary invocation runs in.
// Three modes — no other modes exist. INV-014, CR-INV-001.
type Mode string

const (
	// ModeCompile is the compile-mode pipeline (Compiler binary only).
	ModeCompile Mode = "compile"

	// ModeExecute is the execute-mode pipeline (Conductor binary only).
	ModeExecute Mode = "execute"

	// ModeAgent is the agent-mode pipeline (Conductor binary only).
	ModeAgent Mode = "agent"
)

// Environment variable names read by execute mode on startup.
// conductor-design.md §3.
const (
	// EnvCapability is the named capability this Job must execute.
	// Stamped into the Job spec by the operator. Required in execute mode.
	EnvCapability = "CAPABILITY"

	// EnvClusterRef identifies the cluster this Job targets.
	// Stamped into the Job spec by the operator. Required in execute mode.
	EnvClusterRef = "CLUSTER_REF"

	// EnvOperationResultCM is the ConfigMap name the executor writes
	// OperationResultSpec to before exit. Required in execute mode.
	EnvOperationResultCM = "OPERATION_RESULT_CM"

	// EnvPodNamespace is the Kubernetes downward API variable that carries the
	// namespace the Conductor pod runs in. Defaults to ont-system when absent.
	EnvPodNamespace = "POD_NAMESPACE"

	// DefaultNamespace is the Kubernetes namespace used when POD_NAMESPACE is unset.
	// Execute mode Jobs and agent Deployments always run in ont-system.
	DefaultNamespace = "ont-system"
)

// ExecutionContext is the immutable configuration snapshot for one binary
// invocation. Constructed once at startup. Never mutated after construction.
// conductor-design.md §2.2.
type ExecutionContext struct {
	// Mode is the execution mode for this invocation.
	Mode Mode

	// Capability is the named capability to execute. Non-empty in execute mode only.
	Capability string

	// ClusterRef is the cluster identity for this invocation. Non-empty in execute
	// and agent modes.
	ClusterRef string

	// OperationResultCM is the ConfigMap to write OperationResultSpec to.
	// Non-empty in execute mode only.
	OperationResultCM string

	// Namespace is the Kubernetes namespace this invocation runs in.
	// Populated from POD_NAMESPACE (downward API); defaults to ont-system.
	// Used by execute mode to address the OperationResult ConfigMap and by
	// agent mode to address RunnerConfig and Lease resources.
	Namespace string

	// RunnerConfig is the RunnerConfigSpec loaded from the mounted ConfigMap or
	// environment at startup. Zero value in compile mode.
	RunnerConfig seamcorev1alpha1.InfrastructureRunnerConfigSpec
}

// BuildExecuteContext constructs an ExecutionContext for execute mode.
// Reads CAPABILITY, CLUSTER_REF, and OPERATION_RESULT_CM from the environment.
// Returns a ValidationFailure error if any required variable is absent.
func BuildExecuteContext() (ExecutionContext, error) {
	cap := os.Getenv(EnvCapability)
	if cap == "" {
		return ExecutionContext{}, errors.New(
			"execute mode: CAPABILITY environment variable is required but not set",
		)
	}

	clusterRef := os.Getenv(EnvClusterRef)
	if clusterRef == "" {
		return ExecutionContext{}, errors.New(
			"execute mode: CLUSTER_REF environment variable is required but not set",
		)
	}

	resultCM := os.Getenv(EnvOperationResultCM)
	if resultCM == "" {
		return ExecutionContext{}, errors.New(
			"execute mode: OPERATION_RESULT_CM environment variable is required but not set",
		)
	}

	ns := os.Getenv(EnvPodNamespace)
	if ns == "" {
		ns = DefaultNamespace
	}

	return ExecutionContext{
		Mode:              ModeExecute,
		Capability:        cap,
		ClusterRef:        clusterRef,
		OperationResultCM: resultCM,
		Namespace:         ns,
	}, nil
}

// BuildAgentContext constructs an ExecutionContext for agent mode.
// ClusterRef is resolved from the CLUSTER_REF environment variable first,
// then from the clusterRefFlag CLI argument. The env var takes precedence so
// the Deployment can inject the value via a downward API fieldRef without
// requiring args to change between clusters.
func BuildAgentContext(clusterRefFlag string) (ExecutionContext, error) {
	clusterRef := os.Getenv(EnvClusterRef)
	if clusterRef == "" {
		clusterRef = clusterRefFlag
	}
	if clusterRef == "" {
		return ExecutionContext{}, errors.New(
			"agent mode: cluster ref required -- set CLUSTER_REF env var or --cluster-ref flag",
		)
	}
	ns := os.Getenv(EnvPodNamespace)
	if ns == "" {
		ns = DefaultNamespace
	}

	return ExecutionContext{
		Mode:       ModeAgent,
		ClusterRef: clusterRef,
		Namespace:  ns,
	}, nil
}
