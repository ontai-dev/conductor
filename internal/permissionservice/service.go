package permissionservice

import (
	"context"
	"fmt"
	"strings"
)

// ── Request / Response types ──────────────────────────────────────────────────
// Wire format is identical to the guardian management cluster PermissionService
// so that callers can use the same client stub against either endpoint.
// guardian-schema.md §10.

// CheckPermissionRequest is the input for CheckPermission.
type CheckPermissionRequest struct {
	// Principal is the identity name (RBACProfile.Spec.PrincipalRef).
	Principal string `json:"principal"`

	// Cluster is the target cluster name.
	Cluster string `json:"cluster"`

	// APIGroup is the Kubernetes API group. Empty string means core API group.
	APIGroup string `json:"apiGroup"`

	// Resource is the Kubernetes resource type.
	Resource string `json:"resource"`

	// Verb is the requested operation.
	Verb string `json:"verb"`

	// ResourceName restricts the check to a specific resource instance.
	// Empty means the check applies to any resource name.
	// +optional
	ResourceName string `json:"resourceName,omitempty"`
}

// CheckPermissionResponse is the result of CheckPermission.
type CheckPermissionResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// ListPermissionsRequest is the input for ListPermissions.
type ListPermissionsRequest struct {
	Principal string `json:"principal"`
	Cluster   string `json:"cluster"`
}

// ListPermissionsResponse is the result of ListPermissions.
type ListPermissionsResponse struct {
	Rules []EffectiveRuleDTO `json:"rules"`
}

// WhoCanDoRequest is the input for WhoCanDo.
type WhoCanDoRequest struct {
	Cluster  string `json:"cluster"`
	APIGroup string `json:"apiGroup"`
	Resource string `json:"resource"`
	Verb     string `json:"verb"`
}

// WhoCanDoResponse is the result of WhoCanDo.
type WhoCanDoResponse struct {
	Principals []string `json:"principals"`
}

// ExplainDecisionRequest is the input for ExplainDecision.
type ExplainDecisionRequest struct {
	Principal string `json:"principal"`
	Cluster   string `json:"cluster"`
	APIGroup  string `json:"apiGroup"`
	Resource  string `json:"resource"`
	Verb      string `json:"verb"`
}

// ExplainDecisionResponse is the result of ExplainDecision.
type ExplainDecisionResponse struct {
	Allowed     bool           `json:"allowed"`
	Reason      string         `json:"reason"`
	MatchedRule *EffectiveRuleDTO `json:"matchedRule,omitempty"`
}

// EffectiveRuleDTO is the wire representation of a matched allowed operation.
type EffectiveRuleDTO struct {
	APIGroup  string   `json:"apiGroup"`
	Resource  string   `json:"resource"`
	Verbs     []string `json:"verbs"`
	Clusters  []string `json:"clusters,omitempty"`
}

// ── LocalService ─────────────────────────────────────────────────────────────

// LocalService implements PermissionServiceServer backed by SnapshotStore.
// It serves authorization decisions from the local acknowledged PermissionSnapshot
// without requiring management cluster connectivity.
//
// This is a read-only projection of the acknowledged snapshot — it does not
// compute the EPG. EPG computation is exclusively a management cluster function
// in the guardian controller. conductor-schema.md §10 step 6, guardian-schema.md §10.
type LocalService struct {
	store *SnapshotStore
}

// NewLocalService allocates a LocalService backed by the given SnapshotStore.
func NewLocalService(store *SnapshotStore) *LocalService {
	return &LocalService{store: store}
}

// CheckPermission returns whether the principal is permitted to perform verb
// on resource/apiGroup on cluster. Returns denied with reason "not ready" when
// no snapshot has been loaded yet.
func (s *LocalService) CheckPermission(_ context.Context, req *CheckPermissionRequest) (*CheckPermissionResponse, error) {
	entries, _, _, ready := s.store.Get()
	if !ready {
		return &CheckPermissionResponse{
			Allowed: false,
			Reason:  "local permission snapshot not yet loaded — no authorization decision available",
		}, nil
	}

	op, found := s.findMatchingOperation(entries, req.Principal, req.Cluster, req.APIGroup, req.Resource, req.Verb)
	if !found {
		return &CheckPermissionResponse{
			Allowed: false,
			Reason: fmt.Sprintf("no allowed operation grants %s %s/%s to principal %q on cluster %q",
				req.Verb, req.APIGroup, req.Resource, req.Principal, req.Cluster),
		}, nil
	}
	_ = op
	return &CheckPermissionResponse{
		Allowed: true,
		Reason:  "allowed by local permission snapshot",
	}, nil
}

// ListPermissions returns all effective rules for the principal on the cluster.
// Returns an empty list if the principal is unknown or the store is not ready.
func (s *LocalService) ListPermissions(_ context.Context, req *ListPermissionsRequest) (*ListPermissionsResponse, error) {
	entries, _, _, ready := s.store.Get()
	if !ready {
		return &ListPermissionsResponse{Rules: []EffectiveRuleDTO{}}, nil
	}

	for _, entry := range entries {
		if entry.PrincipalRef != req.Principal {
			continue
		}
		var rules []EffectiveRuleDTO
		for _, op := range entry.AllowedOperations {
			// Filter to the requested cluster. An empty Clusters list means the
			// operation applies to all clusters.
			if !clusterMatches(op.Clusters, req.Cluster) {
				continue
			}
			rules = append(rules, toEffectiveRuleDTO(op))
		}
		if rules == nil {
			rules = []EffectiveRuleDTO{}
		}
		return &ListPermissionsResponse{Rules: rules}, nil
	}
	return &ListPermissionsResponse{Rules: []EffectiveRuleDTO{}}, nil
}

// WhoCanDo returns all principals that have the given permission on the cluster.
func (s *LocalService) WhoCanDo(_ context.Context, req *WhoCanDoRequest) (*WhoCanDoResponse, error) {
	entries, _, _, ready := s.store.Get()
	if !ready {
		return &WhoCanDoResponse{Principals: []string{}}, nil
	}

	var matches []string
	for _, entry := range entries {
		for _, op := range entry.AllowedOperations {
			if op.APIGroup != req.APIGroup || op.Resource != req.Resource {
				continue
			}
			if !clusterMatches(op.Clusters, req.Cluster) {
				continue
			}
			if containsVerb(op.Verbs, req.Verb) {
				matches = append(matches, entry.PrincipalRef)
				break
			}
		}
	}
	if matches == nil {
		matches = []string{}
	}
	return &WhoCanDoResponse{Principals: matches}, nil
}

// ExplainDecision returns the authorization decision and the matched allowed
// operation rule (when allowed). This is the QuantAI integration point for
// human-gate review of AI-proposed operations. guardian-schema.md §10.
func (s *LocalService) ExplainDecision(_ context.Context, req *ExplainDecisionRequest) (*ExplainDecisionResponse, error) {
	entries, _, _, ready := s.store.Get()
	if !ready {
		return &ExplainDecisionResponse{
			Allowed: false,
			Reason:  "local permission snapshot not yet loaded — no authorization decision available",
		}, nil
	}

	op, found := s.findMatchingOperation(entries, req.Principal, req.Cluster, req.APIGroup, req.Resource, req.Verb)
	if !found {
		return &ExplainDecisionResponse{
			Allowed: false,
			Reason: fmt.Sprintf("denied: principal %q has no allowed operation granting %s on %s/%s in cluster %q",
				req.Principal, req.Verb, req.APIGroup, req.Resource, req.Cluster),
		}, nil
	}

	dto := toEffectiveRuleDTO(*op)
	return &ExplainDecisionResponse{
		Allowed: true,
		Reason: fmt.Sprintf("allowed: matched operation granting verbs [%s] on %s/%s",
			strings.Join(op.Verbs, ","), op.APIGroup, op.Resource),
		MatchedRule: &dto,
	}, nil
}

// findMatchingOperation searches the entries for an AllowedOperation that
// grants (principal, cluster, apiGroup, resource, verb). Returns (op, true)
// on match; (nil, false) when no operation matches.
func (s *LocalService) findMatchingOperation(
	entries []PrincipalPermissionEntry,
	principal, cluster, apiGroup, resource, verb string,
) (*AllowedOperation, bool) {
	for i, entry := range entries {
		if entry.PrincipalRef != principal {
			continue
		}
		for j, op := range entry.AllowedOperations {
			if op.APIGroup != apiGroup || op.Resource != resource {
				continue
			}
			if !clusterMatches(op.Clusters, cluster) {
				continue
			}
			if containsVerb(op.Verbs, verb) {
				return &entries[i].AllowedOperations[j], true
			}
		}
	}
	return nil, false
}

// clusterMatches returns true when the allowed operation covers the given cluster.
// An empty clusters list means the operation applies to all clusters.
func clusterMatches(clusters []string, cluster string) bool {
	if len(clusters) == 0 {
		return true
	}
	for _, c := range clusters {
		if c == cluster {
			return true
		}
	}
	return false
}

func containsVerb(verbs []string, verb string) bool {
	for _, v := range verbs {
		if v == verb {
			return true
		}
	}
	return false
}

func toEffectiveRuleDTO(op AllowedOperation) EffectiveRuleDTO {
	verbs := make([]string, len(op.Verbs))
	copy(verbs, op.Verbs)
	var clusters []string
	if len(op.Clusters) > 0 {
		clusters = make([]string, len(op.Clusters))
		copy(clusters, op.Clusters)
	}
	return EffectiveRuleDTO{
		APIGroup: op.APIGroup,
		Resource: op.Resource,
		Verbs:    verbs,
		Clusters: clusters,
	}
}
