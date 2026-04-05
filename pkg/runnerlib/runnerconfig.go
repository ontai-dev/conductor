package runnerlib

import (
	"encoding/json"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunnerConfigSpec is the operator-generated operational contract for a specific
// cluster or pack. Generated at runtime by platform or wrapper using
// GenerateFromTalosCluster or GenerateFromPackBuild. Never human-authored.
// INV-009, INV-010.
type RunnerConfigSpec struct {
	// ClusterRef identifies the cluster this RunnerConfig governs.
	ClusterRef string

	// RunnerImage is the fully qualified runner image reference including tag.
	// Single source of truth for which runner version handles this cluster's Jobs.
	// Tag convention: v{talosVersion}-r{revision} for stable; dev or dev-rc{N}
	// for development. INV-011, INV-012.
	RunnerImage string

	// Phases declares the applicable execution phases for this RunnerConfig.
	// Management cluster: launch and enable. Tenant clusters: launch and bootstrap.
	Phases []PhaseConfig

	// OperationalHistory is an append-only record of every configuration change
	// applied to this RunnerConfig. Entries are never deleted. Superseded entries
	// are retained as historical record.
	OperationalHistory []OperationalHistoryEntry

	// MaintenanceTargetNodes is the list of node names that are the subject of
	// the operation. Populated by the initiating operator at RunnerConfig creation
	// time. Conductor execute mode uses this list to build NotIn node affinity
	// constraints when SelfOperation is true.
	// conductor-schema.md §13.
	MaintenanceTargetNodes []string

	// OperatorLeaderNode is the node currently hosting the leader pod of the
	// initiating operator. Resolved at RunnerConfig creation time via the
	// Kubernetes downward API (fieldRef: spec.nodeName on the operator's pod).
	// Conductor execute mode excludes this node from Job scheduling when
	// SelfOperation is true, preventing a potential scheduling deadlock if the
	// node were cordoned during the operation.
	// conductor-schema.md §13.
	OperatorLeaderNode string

	// SelfOperation is true when the Job's execution cluster and the target
	// cluster are the same (management cluster self-operations). When true,
	// Conductor execute mode applies NotIn node affinity constraints from
	// MaintenanceTargetNodes and OperatorLeaderNode. When false, exclusion
	// logic is skipped entirely — tenant-targeted operations are exempt.
	// conductor-schema.md §13.
	SelfOperation bool

	// Steps is the ordered list of steps in this multi-step operation intent.
	// The step sequencer in Conductor execute mode processes these in declared
	// order, respecting DependsOn relationships and HaltOnFailure semantics.
	// A RunnerConfig with a single step is the degenerate case — all RunnerConfigs
	// use the steps list. conductor-schema.md §17.
	Steps []RunnerConfigStep
}

// RunnerConfigStep declares one step in a multi-step operation intent.
// Each step maps to exactly one named capability. The sequencer materialises
// one Job per step in declared order.
// conductor-schema.md §17.
type RunnerConfigStep struct {
	// Name is the unique identifier for this step within the RunnerConfig.
	// Used as the key for DependsOn references and StepResult lookup.
	Name string

	// Capability is the named capability identifier Conductor execute mode
	// dispatches for this step. Must match a registered capability name.
	Capability string

	// Parameters is the input parameter map passed to the capability at Job
	// materialisation time.
	Parameters map[string]string

	// DependsOn is an optional reference to a prior step name. The step is
	// not eligible for execution until the referenced step has reached
	// Succeeded state. Empty means no dependency.
	DependsOn string

	// HaltOnFailure controls sequencer behaviour when this step fails.
	// When true, failure terminates the RunnerConfig with terminal condition
	// Failed and no further steps execute. When false, the sequencer records
	// the failure and continues to eligible successor steps.
	HaltOnFailure bool
}

// StepPhase is the lifecycle phase of a RunnerConfig step result.
// conductor-schema.md §17.
type StepPhase string

const (
	// StepPhasePending indicates the step has not yet been dispatched.
	StepPhasePending StepPhase = "Pending"

	// StepPhaseRunning indicates the step Job is currently in flight.
	StepPhaseRunning StepPhase = "Running"

	// StepPhaseSucceeded indicates the step Job completed successfully.
	StepPhaseSucceeded StepPhase = "Succeeded"

	// StepPhaseFailed indicates the step Job reached a failure terminal state.
	StepPhaseFailed StepPhase = "Failed"
)

// ConfigMapRef is a reference to a Kubernetes ConfigMap by namespace and name.
type ConfigMapRef struct {
	// Namespace is the Kubernetes namespace containing the ConfigMap.
	Namespace string

	// Name is the Kubernetes ConfigMap name.
	Name string
}

// RunnerConfigStepResult is the status record for one step written into
// RunnerConfig status by Conductor execute mode after each step completes.
// Conductor writes the result verbatim — it never interprets the payload.
// conductor-schema.md §17.
type RunnerConfigStepResult struct {
	// StepName matches the Name field of the corresponding RunnerConfigStep
	// in spec. Used to correlate results with the declared step list.
	StepName string

	// Phase is the terminal phase reached by this step.
	Phase StepPhase

	// OutputRef is the reference to the ConfigMap in ont-system from which the
	// OperationResult payload was harvested. Garbage-collected after TTL.
	OutputRef ConfigMapRef

	// Result is the raw JSON OperationResult document harvested from the
	// ConfigMap. Conductor writes this verbatim without semantic interpretation.
	// The owning operator reads and interprets this field.
	Result json.RawMessage
}

// PhaseConfig carries per-phase parameters for the runner's execution context.
// Phase names: launch, enable (management only), bootstrap (tenant only), compile.
type PhaseConfig struct {
	// Name identifies the phase. Must be one of: launch, enable, bootstrap, compile.
	Name string

	// Parameters holds phase-specific key-value configuration.
	// An empty map is valid. Never nil after construction.
	Parameters map[string]string
}

// OperationalHistoryEntry is a single append-only audit record describing one
// configuration change applied to this RunnerConfig. The full history is never
// truncated. Newer entries supersede older entries for the same Concern but old
// entries are retained.
type OperationalHistoryEntry struct {
	// AppliedAt is the time this change was applied.
	AppliedAt time.Time

	// Concern identifies what aspect of configuration changed.
	// Example: "RunnerImage", "Phase.launch.Parameters.vmConfig".
	Concern string

	// PreviousValue is the value before the change. Empty for initial entries.
	PreviousValue string

	// NewValue is the value after the change.
	NewValue string

	// AppliedBy identifies who applied the change: a Job name or agent pod name.
	AppliedBy string
}

// SecretRef is a reference to a Kubernetes Secret by name and namespace.
type SecretRef struct {
	// Name is the Kubernetes Secret name.
	Name string

	// Namespace is the Kubernetes namespace containing the Secret.
	Namespace string
}

// RunnerConfigStatus is the status subresource for the RunnerConfig CR.
// Populated exclusively by the runner agent on startup and during control loops.
// No operator writes to this status directly.
type RunnerConfigStatus struct {
	// Capabilities is the self-declared capability manifest published by the agent.
	// Operators read this before submitting Jobs. If a named capability is absent,
	// operators raise CapabilityUnavailable on their operational CR and wait.
	Capabilities CapabilityManifest

	// AgentVersion is the runner image version currently running as agent.
	AgentVersion string

	// AgentLeader is the pod name currently holding the leader election lease.
	AgentLeader string

	// Conditions is the standard Kubernetes condition list for RunnerConfig.
	// Standard condition types: LaunchComplete, EnableComplete, BootstrapComplete,
	// PhaseFailed, CapabilityUnavailable, Completed, Failed.
	Conditions []metav1.Condition

	// StepResults is the ordered list of step result records written by Conductor
	// execute mode as each step in the RunnerConfig's step list completes.
	// Conductor writes each entry verbatim from the harvested ConfigMap payload.
	// The owning operator reads this list after the terminal condition is set.
	// conductor-schema.md §17.
	StepResults []RunnerConfigStepResult
}

