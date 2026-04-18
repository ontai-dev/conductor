package permissionservice

import "sync"

// AllowedOperation mirrors security.ontai.dev/v1alpha1 AllowedOperation.
// Guardian writes these into PermissionSnapshot.spec; conductor reads them
// from the locally acknowledged snapshot to serve the local PermissionService.
// guardian-schema.md §7.
type AllowedOperation struct {
	// APIGroup is the Kubernetes API group. Empty string means the core API group.
	// +optional
	APIGroup string `json:"apiGroup,omitempty"`

	// Resource is the Kubernetes resource type.
	Resource string `json:"resource"`

	// Verbs is the list of permitted operations on the resource.
	Verbs []string `json:"verbs"`

	// Clusters is the list of cluster names this operation applies to.
	// +optional
	Clusters []string `json:"clusters,omitempty"`
}

// PrincipalPermissionEntry mirrors security.ontai.dev/v1alpha1 PrincipalPermissionEntry.
type PrincipalPermissionEntry struct {
	// PrincipalRef is the principal name (RBACProfile.Spec.PrincipalRef).
	PrincipalRef string `json:"principalRef"`

	// AllowedOperations is the list of allowed operations for this principal.
	// +optional
	AllowedOperations []AllowedOperation `json:"allowedOperations,omitempty"`
}

// SnapshotStore holds the current in-memory permission snapshot for the local
// cluster. It is populated by the PermissionSnapshot pull loop when a new
// acknowledged snapshot arrives, and queried by LocalService for every
// authorization decision.
//
// The store starts empty (not ready). LocalService returns "not ready" responses
// until Update is called for the first time.
//
// conductor-schema.md §10 step 6, guardian-schema.md §10.
type SnapshotStore struct {
	mu      sync.RWMutex
	entries []PrincipalPermissionEntry
	version string
	cluster string
	ready   bool
}

// NewSnapshotStore allocates an empty SnapshotStore.
func NewSnapshotStore() *SnapshotStore {
	return &SnapshotStore{}
}

// Update replaces the current snapshot entries with a new acknowledged snapshot.
// Called by the PermissionSnapshot pull loop when a new snapshot is verified
// and acknowledged. thread-safe.
func (s *SnapshotStore) Update(entries []PrincipalPermissionEntry, version, cluster string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = entries
	s.version = version
	s.cluster = cluster
	s.ready = true
}

// Get returns the current entries, version, cluster, and whether the store has
// been populated at least once. thread-safe.
func (s *SnapshotStore) Get() (entries []PrincipalPermissionEntry, version, cluster string, ready bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.entries, s.version, s.cluster, s.ready
}

// Clear resets the snapshot store to the not-ready state. Called on receiving a
// RevocationPush from the management Conductor to invalidate the local cache.
// LocalService will return not-ready responses until the next snapshot pull.
// conductor-schema.md §18.
func (s *SnapshotStore) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = nil
	s.version = ""
	s.cluster = ""
	s.ready = false
}
