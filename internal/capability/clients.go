// Package capability — client dependency interfaces for capability handlers.
// Each interface is satisfied by a real client in execute/agent mode and by a
// stub or nil in unit tests. Nil clients cause ValidationFailure, not panic.
package capability

import (
	"context"
	"crypto/ed25519"
	"io"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
)

// TalosNodeClient abstracts Talos machine API operations used by capability
// handlers. Implemented by a real *talos_client.Client adapter in production
// and by a stub in unit tests. conductor-design.md §5, INV-013.
type TalosNodeClient interface {
	// Bootstrap bootstraps etcd on the first control plane node.
	Bootstrap(ctx context.Context) error

	// ApplyConfiguration applies the given machine config bytes to nodes.
	// mode is "auto", "no-reboot", "staged", or "reboot" per Talos API.
	ApplyConfiguration(ctx context.Context, configBytes []byte, mode string) error

	// Upgrade upgrades the Talos OS to the given installer image. Stage=true
	// stages the upgrade for the next reboot instead of applying immediately.
	Upgrade(ctx context.Context, image string, stage bool) error

	// Reboot reboots the node.
	Reboot(ctx context.Context) error

	// Reset performs a factory reset of the node. graceful=true drains workloads first.
	Reset(ctx context.Context, graceful bool) error

	// EtcdSnapshot takes an etcd snapshot and writes it to w.
	EtcdSnapshot(ctx context.Context, w io.Writer) error

	// EtcdRecover recovers etcd from the snapshot in r.
	EtcdRecover(ctx context.Context, r io.Reader) error

	// EtcdDefragment defrags the etcd database on this node.
	EtcdDefragment(ctx context.Context) error

	// GetMachineConfig reads the running machine config from the node.
	// Returns the raw YAML bytes of the machine config as stored at
	// /system/state/config.yaml on the node.
	GetMachineConfig(ctx context.Context) ([]byte, error)

	// Close releases the underlying gRPC connection.
	Close() error
}

// StorageClient abstracts object storage operations for etcd backup and restore.
// Implemented by an S3-compatible client in production.
type StorageClient interface {
	// Upload streams r to bucket/key.
	Upload(ctx context.Context, bucket, key string, r io.Reader) error

	// Download streams bucket/key into the returned ReadCloser.
	// Caller must close the returned reader.
	Download(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

// OCIRegistryClient abstracts OCI registry operations for pack-deploy.
type OCIRegistryClient interface {
	// PullManifests fetches all manifest bytes from the given OCI reference
	// (e.g., "registry.example.com/repo/image@sha256:...").
	// Each element of the returned slice is one Kubernetes manifest in YAML or JSON.
	PullManifests(ctx context.Context, ref string) ([][]byte, error)
}

// GuardianIntakeClient abstracts the guardian /rbac-intake/pack HTTP endpoint.
// Used by pack-deploy to submit the RBAC layer of a ClusterPack for guardian
// wrapping before applying the workload layer. INV-004: guardian owns all RBAC;
// pack-deploy never writes RBAC resources directly. wrapper-schema.md §4.
type GuardianIntakeClient interface {
	// SubmitPackRBACLayer sends YAML manifests from the RBAC OCI layer to
	// guardian's /rbac-intake/pack endpoint. Returns the count of wrapped
	// resources on success.
	SubmitPackRBACLayer(ctx context.Context, componentName string, manifests []string, targetCluster string) (int, error)

	// WaitForRBACProfileProvisioned polls until the RBACProfile for componentName
	// in the seam-tenant-{targetCluster} namespace reaches provisioned=true, or until
	// ctx is cancelled. Returns nil when provisioned. guardian-schema.md §7.
	WaitForRBACProfileProvisioned(ctx context.Context, targetCluster, componentName string) error
}

// ExecuteClients bundles the client dependencies injected into every capability
// execution. Fields may be nil when the handler is running in a test context;
// handlers that require a non-nil client return ValidationFailure when they
// find one absent.
type ExecuteClients struct {
	// KubeClient is the typed Kubernetes client for the target cluster.
	// Used by all capabilities that read or write standard Kubernetes resources.
	KubeClient kubernetes.Interface

	// DynamicClient is the dynamic Kubernetes client for CRD access.
	// Used by capabilities that read Seam CRD instances (PermissionSnapshot,
	// TalosCluster, UpgradePolicy, NodeOperation, NodeMaintenance, etc.).
	DynamicClient dynamic.Interface

	// TalosClient is the Talos machine API client for the target cluster.
	// Used by Platform capabilities that require Talos node operations.
	// nil for pack-deploy and rbac-provision which do not need Talos access.
	TalosClient TalosNodeClient

	// StorageClient is the object storage client for etcd backup/restore.
	// Non-nil only for etcd-backup and etcd-restore.
	StorageClient StorageClient

	// OCIClient is the OCI registry client for ClusterPack artifact fetching.
	// Non-nil only for pack-deploy.
	OCIClient OCIRegistryClient

	// GuardianClient is the guardian intake client for RBAC layer submission.
	// Non-nil only for pack-deploy when the ClusterPack has rbacDigest set.
	// When nil and rbacDigest is absent, pack-deploy uses the legacy path.
	// When nil and rbacDigest is present, pack-deploy returns ValidationFailure.
	// INV-004, wrapper-schema.md §4.
	GuardianClient GuardianIntakeClient

	// SigningPublicKey is the Ed25519 public key used to verify PermissionSnapshot
	// and PackInstance signatures authored by the management cluster Conductor.
	// nil during bootstrap window mode (INV-020) — verification is bypassed.
	// Non-nil in normal operation — INV-026 enforcement applies.
	SigningPublicKey ed25519.PublicKey
}
