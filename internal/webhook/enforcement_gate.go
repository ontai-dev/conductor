package webhook

import "sync/atomic"

// EnforcementGate is the in-memory audit/strict enforcement state for the
// tenant cluster RBAC ownership webhook. Starts in audit mode (sweeping);
// transitions to strict after the bootstrap sweep and profile creation complete.
// Safe for concurrent use. guardian-schema.md §3 Step 2, §6.
type EnforcementGate struct {
	strict atomic.Bool
}

// NewEnforcementGate returns an EnforcementGate in audit mode.
func NewEnforcementGate() *EnforcementGate {
	return &EnforcementGate{}
}

// IsStrict reports whether the gate is in strict enforcement mode.
// False (audit) means RBAC resources without the ownership annotation are
// logged but not rejected. True (strict) means they are rejected at admission.
func (g *EnforcementGate) IsStrict() bool {
	return g.strict.Load()
}

// SetStrict transitions the gate from audit to strict. Idempotent.
// Called by TenantBootstrapSweep after the annotation sweep and component
// profile creation complete.
func (g *EnforcementGate) SetStrict() {
	g.strict.Store(true)
}
