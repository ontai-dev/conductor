package permissionservice_test

// Unit tests for the local PermissionService (conductor target cluster gRPC
// authorization server). conductor-schema.md §10 step 6, guardian-schema.md §10.

import (
	"context"
	"testing"

	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// makeEntries returns a slice of PrincipalPermissionEntry for use in tests.
// alice has get/list on pods in ccs-dev; bob has create on deployments in all clusters.
func makeEntries() []permissionservice.PrincipalPermissionEntry {
	return []permissionservice.PrincipalPermissionEntry{
		{
			PrincipalRef: "alice",
			AllowedOperations: []permissionservice.AllowedOperation{
				{
					APIGroup: "",
					Resource: "pods",
					Verbs:    []string{"get", "list"},
					Clusters: []string{"ccs-dev"},
				},
			},
		},
		{
			PrincipalRef: "bob",
			AllowedOperations: []permissionservice.AllowedOperation{
				{
					APIGroup: "apps",
					Resource: "deployments",
					Verbs:    []string{"create", "update"},
					Clusters: []string{}, // empty = all clusters
				},
			},
		},
	}
}

// newReadyService returns a LocalService backed by a store pre-populated with makeEntries.
func newReadyService() *permissionservice.LocalService {
	store := permissionservice.NewSnapshotStore()
	store.Update(makeEntries(), "2026-04-04T12:00:00Z", "ccs-dev")
	return permissionservice.NewLocalService(store)
}

// newEmptyService returns a LocalService backed by an unpopulated store.
func newEmptyService() *permissionservice.LocalService {
	store := permissionservice.NewSnapshotStore()
	return permissionservice.NewLocalService(store)
}

// ── SnapshotStore tests ───────────────────────────────────────────────────────

// TestSnapshotStore_InitiallyNotReady verifies that a new store is not ready.
func TestSnapshotStore_InitiallyNotReady(t *testing.T) {
	store := permissionservice.NewSnapshotStore()
	_, _, _, ready := store.Get()
	if ready {
		t.Error("new store must not be ready before Update is called")
	}
}

// TestSnapshotStore_ReadyAfterUpdate verifies that the store becomes ready after Update.
func TestSnapshotStore_ReadyAfterUpdate(t *testing.T) {
	store := permissionservice.NewSnapshotStore()
	store.Update(makeEntries(), "v1", "ccs-dev")
	_, version, cluster, ready := store.Get()
	if !ready {
		t.Error("store must be ready after Update")
	}
	if version != "v1" {
		t.Errorf("version: want %q got %q", "v1", version)
	}
	if cluster != "ccs-dev" {
		t.Errorf("cluster: want %q got %q", "ccs-dev", cluster)
	}
}

// ── CheckPermission tests ─────────────────────────────────────────────────────

// TestCheckPermission_AllowedForGrantedVerb verifies that CheckPermission returns
// Allowed=true when an explicit AllowedOperation covers the request.
func TestCheckPermission_AllowedForGrantedVerb(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.CheckPermission(context.Background(), &permissionservice.CheckPermissionRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "get",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if !resp.Allowed {
		t.Errorf("expected Allowed=true; got reason: %s", resp.Reason)
	}
}

// TestCheckPermission_DeniedForUnknownVerb verifies that a verb not in the
// AllowedOperations list is denied.
func TestCheckPermission_DeniedForUnknownVerb(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.CheckPermission(context.Background(), &permissionservice.CheckPermissionRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "delete",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if resp.Allowed {
		t.Error("expected Allowed=false for verb not in allowed list")
	}
}

// TestCheckPermission_DeniedForWrongCluster verifies that a permission granted
// on a specific cluster does not extend to a different cluster.
func TestCheckPermission_DeniedForWrongCluster(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.CheckPermission(context.Background(), &permissionservice.CheckPermissionRequest{
		Principal: "alice",
		Cluster:   "ccs-test", // alice's pods/get is only in ccs-dev
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "get",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if resp.Allowed {
		t.Error("expected Allowed=false for cluster not in allowed list")
	}
}

// TestCheckPermission_AllClustersOperation verifies that an AllowedOperation with
// empty Clusters applies to any cluster.
func TestCheckPermission_AllClustersOperation(t *testing.T) {
	svc := newReadyService()
	// bob's create on deployments has empty Clusters → applies everywhere
	resp, err := svc.CheckPermission(context.Background(), &permissionservice.CheckPermissionRequest{
		Principal: "bob",
		Cluster:   "ccs-test",
		APIGroup:  "apps",
		Resource:  "deployments",
		Verb:      "create",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if !resp.Allowed {
		t.Errorf("expected Allowed=true for all-cluster operation; reason: %s", resp.Reason)
	}
}

// TestCheckPermission_DeniedUnknownPrincipal verifies that an unknown principal
// is denied.
func TestCheckPermission_DeniedUnknownPrincipal(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.CheckPermission(context.Background(), &permissionservice.CheckPermissionRequest{
		Principal: "unknown-principal",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "get",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if resp.Allowed {
		t.Error("expected Allowed=false for unknown principal")
	}
}

// TestCheckPermission_NotReady verifies that CheckPermission returns Allowed=false
// when the snapshot store has not yet been populated.
func TestCheckPermission_NotReady(t *testing.T) {
	svc := newEmptyService()
	resp, err := svc.CheckPermission(context.Background(), &permissionservice.CheckPermissionRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "get",
	})
	if err != nil {
		t.Fatalf("CheckPermission error: %v", err)
	}
	if resp.Allowed {
		t.Error("expected Allowed=false when store is not ready")
	}
}

// ── ListPermissions tests ─────────────────────────────────────────────────────

// TestListPermissions_ReturnsGrantedRules verifies that ListPermissions returns
// the effective rules for a known principal on their cluster.
func TestListPermissions_ReturnsGrantedRules(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.ListPermissions(context.Background(), &permissionservice.ListPermissionsRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
	})
	if err != nil {
		t.Fatalf("ListPermissions error: %v", err)
	}
	if len(resp.Rules) == 0 {
		t.Fatal("expected non-empty rules for alice on ccs-dev")
	}
	rule := resp.Rules[0]
	if rule.Resource != "pods" {
		t.Errorf("expected resource=pods; got %q", rule.Resource)
	}
}

// TestListPermissions_EmptyForUnknownPrincipal verifies empty list for unknown principal.
func TestListPermissions_EmptyForUnknownPrincipal(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.ListPermissions(context.Background(), &permissionservice.ListPermissionsRequest{
		Principal: "unknown",
		Cluster:   "ccs-dev",
	})
	if err != nil {
		t.Fatalf("ListPermissions error: %v", err)
	}
	if len(resp.Rules) != 0 {
		t.Errorf("expected 0 rules for unknown principal; got %d", len(resp.Rules))
	}
}

// TestListPermissions_FiltersToCluster verifies that ListPermissions only returns
// rules applicable to the requested cluster.
func TestListPermissions_FiltersToCluster(t *testing.T) {
	svc := newReadyService()
	// alice's pods/get is only in ccs-dev — requesting ccs-test must return empty.
	resp, err := svc.ListPermissions(context.Background(), &permissionservice.ListPermissionsRequest{
		Principal: "alice",
		Cluster:   "ccs-test",
	})
	if err != nil {
		t.Fatalf("ListPermissions error: %v", err)
	}
	if len(resp.Rules) != 0 {
		t.Errorf("expected 0 rules for alice on ccs-test; got %d", len(resp.Rules))
	}
}

// TestListPermissions_NotReady verifies empty list when store is not ready.
func TestListPermissions_NotReady(t *testing.T) {
	svc := newEmptyService()
	resp, err := svc.ListPermissions(context.Background(), &permissionservice.ListPermissionsRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
	})
	if err != nil {
		t.Fatalf("ListPermissions error: %v", err)
	}
	if len(resp.Rules) != 0 {
		t.Errorf("expected 0 rules when store not ready; got %d", len(resp.Rules))
	}
}

// ── WhoCanDo tests ─────────────────────────────────────────────────────────────

// TestWhoCanDo_FindsPrincipalWithPermission verifies that WhoCanDo returns
// principals that hold the requested permission.
func TestWhoCanDo_FindsPrincipalWithPermission(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.WhoCanDo(context.Background(), &permissionservice.WhoCanDoRequest{
		Cluster:  "ccs-dev",
		APIGroup: "",
		Resource: "pods",
		Verb:     "list",
	})
	if err != nil {
		t.Fatalf("WhoCanDo error: %v", err)
	}
	if len(resp.Principals) == 0 {
		t.Fatal("expected at least one principal with pods/list on ccs-dev")
	}
	found := false
	for _, p := range resp.Principals {
		if p == "alice" {
			found = true
		}
	}
	if !found {
		t.Errorf("expected alice in WhoCanDo result; got %v", resp.Principals)
	}
}

// TestWhoCanDo_EmptyForNobodyHasPermission verifies empty result when no
// principal holds the permission.
func TestWhoCanDo_EmptyForNobodyHasPermission(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.WhoCanDo(context.Background(), &permissionservice.WhoCanDoRequest{
		Cluster:  "ccs-dev",
		APIGroup: "",
		Resource: "pods",
		Verb:     "delete",
	})
	if err != nil {
		t.Fatalf("WhoCanDo error: %v", err)
	}
	if len(resp.Principals) != 0 {
		t.Errorf("expected empty result; got %v", resp.Principals)
	}
}

// TestWhoCanDo_NotReady verifies empty result when store is not ready.
func TestWhoCanDo_NotReady(t *testing.T) {
	svc := newEmptyService()
	resp, err := svc.WhoCanDo(context.Background(), &permissionservice.WhoCanDoRequest{
		Cluster:  "ccs-dev",
		APIGroup: "",
		Resource: "pods",
		Verb:     "get",
	})
	if err != nil {
		t.Fatalf("WhoCanDo error: %v", err)
	}
	if len(resp.Principals) != 0 {
		t.Errorf("expected empty result when not ready; got %v", resp.Principals)
	}
}

// ── ExplainDecision tests ─────────────────────────────────────────────────────

// TestExplainDecision_AllowedReturnsMatchedRule verifies that ExplainDecision
// returns Allowed=true and a non-nil MatchedRule for a granted permission.
func TestExplainDecision_AllowedReturnsMatchedRule(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.ExplainDecision(context.Background(), &permissionservice.ExplainDecisionRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "get",
	})
	if err != nil {
		t.Fatalf("ExplainDecision error: %v", err)
	}
	if !resp.Allowed {
		t.Errorf("expected Allowed=true; reason: %s", resp.Reason)
	}
	if resp.MatchedRule == nil {
		t.Error("expected non-nil MatchedRule when allowed")
	}
	if resp.MatchedRule.Resource != "pods" {
		t.Errorf("MatchedRule.Resource: want pods got %q", resp.MatchedRule.Resource)
	}
}

// TestExplainDecision_DeniedNoMatchedRule verifies that ExplainDecision returns
// Allowed=false and nil MatchedRule for a denied permission.
func TestExplainDecision_DeniedNoMatchedRule(t *testing.T) {
	svc := newReadyService()
	resp, err := svc.ExplainDecision(context.Background(), &permissionservice.ExplainDecisionRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "delete",
	})
	if err != nil {
		t.Fatalf("ExplainDecision error: %v", err)
	}
	if resp.Allowed {
		t.Error("expected Allowed=false for denied verb")
	}
	if resp.MatchedRule != nil {
		t.Errorf("expected nil MatchedRule when denied; got %+v", resp.MatchedRule)
	}
}

// TestExplainDecision_NotReady verifies that ExplainDecision returns Allowed=false
// when the store is not ready.
func TestExplainDecision_NotReady(t *testing.T) {
	svc := newEmptyService()
	resp, err := svc.ExplainDecision(context.Background(), &permissionservice.ExplainDecisionRequest{
		Principal: "alice",
		Cluster:   "ccs-dev",
		APIGroup:  "",
		Resource:  "pods",
		Verb:      "get",
	})
	if err != nil {
		t.Fatalf("ExplainDecision error: %v", err)
	}
	if resp.Allowed {
		t.Error("expected Allowed=false when store not ready")
	}
}
