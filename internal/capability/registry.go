// Package capability implements the Capability Engine: static capability registry,
// resolver, and dispatcher. This module exists in the Conductor binary only —
// it is not compiled into the Compiler binary. conductor-design.md §2.3.
//
// Registration is static at build time via RegisterAll. No runtime plugin loading.
// A missing capability causes immediate structured failure before any execution
// begins. CR-INV-004, conductor-design.md §2.3.
package capability

import (
	"context"
	"fmt"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// Handler is the interface every named capability implementation must satisfy.
// Execute is called by the dispatcher with the current execution parameters.
// It returns a completed OperationResultSpec — partial writes are InvariantViolation.
// conductor-design.md §5.4.
type Handler interface {
	// Execute runs the capability and returns the complete operation result.
	// The context carries cancellation from the execute-mode pipeline.
	Execute(ctx context.Context, params ExecuteParams) (runnerlib.OperationResultSpec, error)
}

// ExecuteParams carries all input parameters for one capability execution.
// Constructed by the execute-mode pipeline from the ExecutionContext and the
// injected ExecuteClients. Client fields are nil in unit tests; handlers that
// require a non-nil client return ValidationFailure when absent.
type ExecuteParams struct {
	// Capability is the name of the capability being executed. Matches one of
	// the Capability* constants in pkg/runnerlib/constants.go.
	Capability string

	// ClusterRef identifies the cluster this execution targets.
	ClusterRef string

	// OperationResultCM is the ConfigMap name to write OperationResultSpec to.
	OperationResultCM string

	// Namespace is the Kubernetes namespace the Conductor Job runs in
	// (ont-system). Used to address CRDs and ConfigMaps.
	Namespace string

	// ExecuteClients bundles the injected client dependencies.
	// See ExecuteClients documentation for nil-client semantics.
	ExecuteClients
}

// Registry is the static capability registry. All handlers are registered at
// binary startup via RegisterAll. No handlers may be added after startup.
// conductor-design.md §2.3.
type Registry struct {
	handlers map[string]Handler
}

// NewRegistry allocates an empty Registry. Callers must call RegisterAll (or
// individual Register calls) before any Resolve calls.
func NewRegistry() *Registry {
	return &Registry{handlers: make(map[string]Handler)}
}

// Register adds a named capability handler to the registry. Panics if a handler
// is already registered for name — duplicate registration is a programming error.
func (r *Registry) Register(name string, h Handler) {
	if _, exists := r.handlers[name]; exists {
		panic(fmt.Sprintf("capability: duplicate registration for %q", name))
	}
	r.handlers[name] = h
}

// Resolve returns the handler registered for name. Returns a non-nil error with
// CapabilityUnavailable category when name is not registered — this causes an
// immediate structured failure before any execution begins. CR-INV-004.
func (r *Registry) Resolve(name string) (Handler, error) {
	h, ok := r.handlers[name]
	if !ok {
		return nil, fmt.Errorf("capability: %q is not registered in this Conductor image: %w",
			name, ErrCapabilityUnavailable)
	}
	return h, nil
}

// RegisteredNames returns the sorted list of registered capability names.
// Used by the agent to publish the capability manifest to RunnerConfig status.
func (r *Registry) RegisteredNames() []string {
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// ErrCapabilityUnavailable is the sentinel error returned by Resolve when a
// capability is not in the registry. Callers use errors.Is to detect this.
var ErrCapabilityUnavailable = fmt.Errorf("capability unavailable")
