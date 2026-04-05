package runnerlib

import (
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
	// PhaseFailed, CapabilityUnavailable.
	Conditions []metav1.Condition
}

