package runnerlib

import "time"

// CapabilityManifest is the self-declared capability list published by the runner
// agent to RunnerConfig status on startup. Operators read this manifest before
// submitting any Job to confirm the named capability is available. CR-INV-005.
type CapabilityManifest struct {
	// RunnerVersion is the version of the runner binary that published this manifest.
	RunnerVersion string

	// PublishedAt is the time this manifest was written to RunnerConfig status.
	PublishedAt time.Time

	// Entries is the list of named capabilities this runner image supports.
	// Always initialized to a non-nil slice. An empty manifest is valid (agent
	// starting up) but operators will raise CapabilityUnavailable until entries appear.
	Entries []CapabilityEntry
}

// CapabilityEntry declares one named capability supported by this runner image.
// The Name field is the authoritative identifier — it must match a named capability
// constant exactly. Names are permanent. Renaming is forbidden. CR-INV-004.
//
// JSON field names match the CRD status.capabilities array schema exactly:
// name, version, description, parameterSchema. Mode is a Go-only field not
// declared in the CRD and is excluded from serialization via json:"-".
type CapabilityEntry struct {
	// Name is the globally unique, immutable capability identifier.
	// Must match one of the Capability* constants in constants.go exactly.
	Name string `json:"name"`

	// Version is the semantic version of this capability implementation.
	// Tied to the runner version. No independent capability releases. CR-INV-004.
	Version string `json:"version"`

	// Mode declares the execution mode this capability runs in.
	// Named capabilities are always ExecutorMode. Not declared in the CRD schema —
	// excluded from JSON serialization; used only for internal dispatch logic.
	Mode CapabilityMode `json:"-"`

	// ParameterSchema declares the input parameters this capability accepts.
	// Map key is the parameter name. Value is the parameter definition.
	// An empty map is valid for capabilities with no parameters.
	ParameterSchema map[string]ParameterDef `json:"parameterSchema,omitempty"`

	// Description is a human-readable summary of what this capability does.
	Description string `json:"description,omitempty"`
}

// CapabilityMode is a typed string declaring which runner mode a capability
// executes in. The three-mode boundary is absolute. INV-014, CR-INV-001.
type CapabilityMode string

const (
	// ExecutorMode indicates the capability runs as a Kueue Job in executor mode.
	// All named capabilities in the capability table are ExecutorMode.
	ExecutorMode CapabilityMode = "executor"

	// AgentMode indicates the capability is an agent-internal control loop.
	// Not externally invokable. Not submitted as a Job.
	AgentMode CapabilityMode = "agent"

	// CompileMode indicates the capability runs in runner compile mode.
	// Helm and Kustomize goClients are available. Talos goclient is forbidden.
	// INV-014.
	CompileMode CapabilityMode = "compile"
)

// ParameterDef describes the schema for one input parameter of a named capability.
// Operators use this to validate Job specs before submission.
type ParameterDef struct {
	// Type is the parameter value type.
	// One of: string, int, bool, secretRef.
	Type string

	// Required indicates whether this parameter must be present for the capability
	// to execute. Zero value is false (optional).
	Required bool

	// Description is a human-readable explanation of this parameter's purpose.
	Description string

	// Default is the value used when the parameter is absent and Required is false.
	// Empty string means no default — the capability handles absence explicitly.
	Default string
}
