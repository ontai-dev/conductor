package agent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// SnapshotPullLoop pulls PermissionSnapshot CRs from the management cluster,
// verifies their Ed25519 signatures (INV-026), and populates the local
// SnapshotStore so the local PermissionService can serve authorization decisions
// without requiring management cluster connectivity.
//
// It runs on the target cluster Conductor only. The management cluster Conductor
// signs the snapshots (signing_loop.go); the target cluster Conductor verifies
// and consumes them here. conductor-schema.md §10 step 8, conductor-design.md §2.10.
//
// When pubKey is nil (bootstrap window mode, INV-020), snapshots are accepted
// without signature verification. Real enforcement begins when the key is mounted.
type SnapshotPullLoop struct {
	mgmtClient  dynamic.Interface            // management cluster client for listing snapshots
	localClient dynamic.Interface            // local cluster client for patching receipts
	store       *permissionservice.SnapshotStore
	pubKey      ed25519.PublicKey            // nil during bootstrap window (INV-020)
	clusterName string                       // filters snapshots to this cluster only
	namespace   string                       // local namespace for degraded state patches
}

// NewSnapshotPullLoop constructs a SnapshotPullLoop in bootstrap window mode
// (INV-020). No signature enforcement is applied until a key is mounted.
func NewSnapshotPullLoop(
	mgmtClient, localClient dynamic.Interface,
	store *permissionservice.SnapshotStore,
	clusterName, namespace string,
) *SnapshotPullLoop {
	return &SnapshotPullLoop{
		mgmtClient:  mgmtClient,
		localClient: localClient,
		store:       store,
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// NewSnapshotPullLoopWithKey constructs a SnapshotPullLoop that enforces INV-026
// Ed25519 signature verification. publicKeyPath is the file path to a PKIX
// PEM-encoded Ed25519 public key, typically mounted from a Kubernetes Secret volume.
func NewSnapshotPullLoopWithKey(
	mgmtClient, localClient dynamic.Interface,
	store *permissionservice.SnapshotStore,
	clusterName, namespace, publicKeyPath string,
) (*SnapshotPullLoop, error) {
	keyBytes, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("NewSnapshotPullLoopWithKey: read public key %s: %w", publicKeyPath, err)
	}
	pubKey, err := parseEd25519PublicKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("NewSnapshotPullLoopWithKey: parse Ed25519 public key: %w", err)
	}
	return &SnapshotPullLoop{
		mgmtClient:  mgmtClient,
		localClient: localClient,
		store:       store,
		pubKey:      pubKey,
		clusterName: clusterName,
		namespace:   namespace,
	}, nil
}

// Run runs the pull loop until ctx is cancelled. It fires once immediately
// then repeats on interval. interval must be positive (> 0).
// conductor-schema.md §10 step 8.
func (l *SnapshotPullLoop) Run(ctx context.Context, interval time.Duration) {
	l.pullOnce(ctx)

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.pullOnce(ctx)
		}
	}
}

// pullOnce performs a single pull cycle: list all PermissionSnapshots from the
// management cluster, filter to this cluster, verify each signature, then
// update the local SnapshotStore on success.
func (l *SnapshotPullLoop) pullOnce(ctx context.Context) {
	list, err := l.mgmtClient.Resource(permissionSnapshotGVR).Namespace("").List(ctx, metav1.ListOptions{})
	if err != nil {
		// Management cluster connectivity loss — log and retry next cycle.
		// The pull loop must not crash; the local PermissionService continues
		// serving from the last acknowledged snapshot. conductor-schema.md §10.
		fmt.Printf("snapshot pull loop: cluster=%q list PermissionSnapshots: connectivity error: %v\n",
			l.clusterName, err)
		return
	}

	for _, item := range list.Items {
		// Filter to snapshots intended for this cluster only.
		spec, _, _ := unstructuredNestedMap(item.Object, "spec")
		targetCluster, _ := spec["targetCluster"].(string)
		if targetCluster != l.clusterName {
			continue
		}

		// Verify management cluster signature. INV-026.
		if err := l.verifySnapshotSignature(item.Object); err != nil {
			fmt.Printf("snapshot pull loop: cluster=%q snapshot=%q signature verification failed: %v\n",
				l.clusterName, item.GetName(), err)
			// Patch DegradedSecurityState on the local receipt. Non-fatal — next
			// cycle will retry if the snapshot is updated. conductor-schema.md §10.
			_ = l.patchLocalDegradedSecurityState(ctx, item.GetName(), err.Error())
			continue
		}

		// Parse spec.principalPermissions into typed store entries.
		entries, version, err := l.parseSnapshotSpec(spec)
		if err != nil {
			fmt.Printf("snapshot pull loop: cluster=%q snapshot=%q parse spec error: %v\n",
				l.clusterName, item.GetName(), err)
			continue
		}

		// Populate the local SnapshotStore. LocalService reads from here for all
		// authorization decisions. conductor-schema.md §10 step 6.
		l.store.Update(entries, version, l.clusterName)
		fmt.Printf("snapshot pull loop: cluster=%q snapshot=%q version=%q updated store (%d principals)\n",
			l.clusterName, item.GetName(), version, len(entries))
	}
}

// verifySnapshotSignature verifies the management cluster Ed25519 signature on a
// PermissionSnapshot CR. The signature is base64-encoded in the
// managementSignatureAnnotation. The signed message is json.Marshal(spec).
// INV-026. Bootstrap window mode (pubKey == nil) accepts all snapshots. INV-020.
func (l *SnapshotPullLoop) verifySnapshotSignature(obj map[string]interface{}) error {
	annotations, _, _ := unstructuredNestedMap(obj, "metadata", "annotations")
	sig64, hasSig := annotations[managementSignatureAnnotation].(string)

	// Bootstrap window mode — key not yet mounted. Accept all snapshots. INV-020.
	if l.pubKey == nil {
		return nil
	}

	// Normal operation — key mounted; enforce INV-026.
	if !hasSig || sig64 == "" {
		return fmt.Errorf("snapshot missing required signature annotation %q (INV-026)",
			managementSignatureAnnotation)
	}

	sigBytes, err := base64.StdEncoding.DecodeString(sig64)
	if err != nil {
		return fmt.Errorf("decode signature base64: %w", err)
	}

	spec, _, _ := unstructuredNestedMap(obj, "spec")
	message, err := json.Marshal(spec)
	if err != nil {
		return fmt.Errorf("marshal spec for signature verification: %w", err)
	}

	if !ed25519.Verify(l.pubKey, message, sigBytes) {
		return fmt.Errorf("Ed25519 signature verification failed (INV-026)")
	}
	return nil
}

// patchLocalDegradedSecurityState sets DegradedSecurityState=True on the local
// PermissionSnapshotReceipt CR for the failing snapshot. This signals that the
// snapshot could not be applied due to a signature verification failure.
// conductor-schema.md §10, INV-026.
func (l *SnapshotPullLoop) patchLocalDegradedSecurityState(ctx context.Context, snapshotName, reason string) error {
	patch := map[string]interface{}{
		"status": map[string]interface{}{
			"degradedSecurityState": true,
			"degradedReason":        reason,
		},
	}
	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("patchLocalDegradedSecurityState: marshal patch: %w", err)
	}
	if _, err := l.localClient.Resource(permissionSnapshotReceiptGVR).Namespace(l.namespace).Patch(
		ctx,
		snapshotName,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"status",
	); err != nil {
		return fmt.Errorf("patchLocalDegradedSecurityState %s: %w", snapshotName, err)
	}
	return nil
}

// parseSnapshotSpec extracts the version string and permission entries from a
// PermissionSnapshot spec map.
//
// Priority:
//  1. spec.subjects (formal CRD field, guardian schema §7) — preferred path.
//  2. spec.principalPermissions (legacy compat field) — fallback.
//
// Returns store-typed entries via JSON round-trip.
func (l *SnapshotPullLoop) parseSnapshotSpec(spec map[string]interface{}) ([]permissionservice.PrincipalPermissionEntry, string, error) {
	version, _ := spec["version"].(string)

	// Prefer spec.subjects — the formal CRD field introduced in guardian schema §7.
	// Each SubjectEntry maps to one PrincipalPermissionEntry:
	//   subjectName → PrincipalRef
	//   permissions[] × (apiGroups × resources) → AllowedOperation
	if rawSubjects, ok := spec["subjects"]; ok && rawSubjects != nil {
		entries, err := parseSubjectsField(rawSubjects)
		if err != nil {
			return nil, version, fmt.Errorf("parse spec.subjects: %w", err)
		}
		return entries, version, nil
	}

	// Fallback: spec.principalPermissions for snapshots generated before the
	// formal SubjectEntry schema was adopted.
	raw, ok := spec["principalPermissions"]
	if !ok {
		return nil, version, nil
	}

	// JSON round-trip: unstructured map/slice → typed permissionservice entries.
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, version, fmt.Errorf("marshal principalPermissions: %w", err)
	}
	var entries []permissionservice.PrincipalPermissionEntry
	if err := json.Unmarshal(encoded, &entries); err != nil {
		return nil, version, fmt.Errorf("unmarshal principalPermissions: %w", err)
	}
	return entries, version, nil
}

// parseSubjectsField converts the unstructured spec.subjects value from a
// PermissionSnapshot CR into store-typed PrincipalPermissionEntry values.
//
// Formal CRD schema (guardian schema §7):
//
//	spec.subjects[].subjectName     → PrincipalRef
//	spec.subjects[].permissions[].apiGroups  ([]string, "" = core group)
//	spec.subjects[].permissions[].resources  ([]string)
//	spec.subjects[].permissions[].verbs      ([]string)
//
// Each (apiGroup, resource) pair in a PermissionEntry becomes one AllowedOperation.
// If apiGroups is empty, the empty string "" (core API group) is used.
func parseSubjectsField(raw interface{}) ([]permissionservice.PrincipalPermissionEntry, error) {
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal subjects: %w", err)
	}

	type permEntry struct {
		APIGroups     []string `json:"apiGroups"`
		Resources     []string `json:"resources"`
		Verbs         []string `json:"verbs"`
		ResourceNames []string `json:"resourceNames"`
	}
	type subjectEntry struct {
		SubjectName string      `json:"subjectName"`
		SubjectKind string      `json:"subjectKind"`
		Namespace   string      `json:"namespace"`
		Permissions []permEntry `json:"permissions"`
	}
	var subjects []subjectEntry
	if err := json.Unmarshal(encoded, &subjects); err != nil {
		return nil, fmt.Errorf("unmarshal subjects: %w", err)
	}

	entries := make([]permissionservice.PrincipalPermissionEntry, 0, len(subjects))
	for _, s := range subjects {
		entry := permissionservice.PrincipalPermissionEntry{
			PrincipalRef: s.SubjectName,
		}
		for _, perm := range s.Permissions {
			apiGroups := perm.APIGroups
			if len(apiGroups) == 0 {
				apiGroups = []string{""}
			}
			for _, apiGroup := range apiGroups {
				for _, resource := range perm.Resources {
					entry.AllowedOperations = append(entry.AllowedOperations,
						permissionservice.AllowedOperation{
							APIGroup: apiGroup,
							Resource: resource,
							Verbs:    perm.Verbs,
						})
				}
			}
		}
		entries = append(entries, entry)
	}
	return entries, nil
}
