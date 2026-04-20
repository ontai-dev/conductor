package runnerlib

// Named capability string constants. These values are permanent and immutable.
// Renaming a capability is forbidden. Fundamental behavior changes require a new
// name. CR-INV-004. Values match conductor-schema.md Section 6 exactly.
//
// The capability name is stamped into the Job spec as the CAPABILITY environment
// variable. The runner reads this on executor startup to resolve the capability
// from the registry.

// platform capabilities — cluster lifecycle and operations.
const (
	// CapabilityBootstrap performs full cluster bootstrap from seed nodes.
	// Multi-step. Uses PVC protocol. Triggered by TalosCluster.
	CapabilityBootstrap = "bootstrap"

	// CapabilityTalosUpgrade performs a rolling Talos OS version upgrade.
	// Triggered by TalosUpgrade CR.
	CapabilityTalosUpgrade = "talos-upgrade"

	// CapabilityKubeUpgrade performs a Kubernetes version upgrade.
	// Triggered by TalosKubeUpgrade CR.
	CapabilityKubeUpgrade = "kube-upgrade"

	// CapabilityStackUpgrade performs a coordinated Talos OS and Kubernetes upgrade.
	// Multi-step. Uses PVC protocol. Triggered by TalosStackUpgrade CR.
	CapabilityStackUpgrade = "stack-upgrade"

	// CapabilityNodePatch applies a machine config patch to one or more nodes.
	// Triggered by TalosNodePatch CR.
	CapabilityNodePatch = "node-patch"

	// CapabilityNodeScaleUp provisions and bootstraps additional nodes into a cluster.
	// Triggered by TalosNodeScaleUp CR.
	CapabilityNodeScaleUp = "node-scale-up"

	// CapabilityNodeDecommission cordons, drains, and removes a node.
	// Triggered by TalosNodeDecommission CR.
	CapabilityNodeDecommission = "node-decommission"

	// CapabilityNodeReboot reboots one or all cluster nodes.
	// Triggered by TalosReboot CR.
	CapabilityNodeReboot = "node-reboot"

	// CapabilityEtcdBackup takes an etcd snapshot and exports machine config to S3.
	// Triggered by TalosBackup CR.
	CapabilityEtcdBackup = "etcd-backup"

	// CapabilityEtcdDefrag performs etcd defragmentation on all members.
	// Triggered by EtcdMaintenance CR with operation=defrag. conductor-schema.md §6.
	CapabilityEtcdDefrag = "etcd-defrag"

	// CapabilityEtcdRestore performs disaster recovery from an S3 etcd snapshot.
	// Triggered by TalosRecovery CR.
	CapabilityEtcdRestore = "etcd-restore"

	// CapabilityPKIRotate rotates PKI certificates and updates talosconfig secret.
	// Triggered by TalosPKIRotation CR.
	CapabilityPKIRotate = "pki-rotate"

	// CapabilityCredentialRotate rotates service account signing keys and OIDC credentials.
	// Triggered by TalosCredentialRotation CR.
	CapabilityCredentialRotate = "credential-rotate"

	// CapabilityHardeningApply applies a TalosHardeningProfile to a running cluster.
	// Triggered by TalosHardeningApply CR.
	CapabilityHardeningApply = "hardening-apply"

	// CapabilityClusterReset performs a destructive factory reset with a human gate.
	// Multi-step. Uses PVC protocol. Triggered by TalosClusterReset CR.
	// Requires annotation ontai.dev/reset-approved=true before execution proceeds.
	// INV-007, INV-015.
	CapabilityClusterReset = "cluster-reset"
)

// Compile mode capabilities — invoked by the conductor binary directly, not by conductor Jobs.
// These capabilities run on the operator's workstation or in a CI/CD pipeline and are never
// submitted to Kueue. They never run on any cluster.
const (
	// CapabilityPackCompile is an conductor compile mode invocation that renders PackBuild
	// inputs (Helm charts, Kustomize overlays, raw manifests) into a ClusterPack OCI artifact.
	// It is invoked by the human or CI/CD pipeline on the workstation — never as a Kueue Job,
	// never via conductor, never on any cluster. The output is a ClusterPack CR YAML emitted
	// for git commit and a ClusterPack OCI artifact pushed to the OCI registry.
	CapabilityPackCompile = "pack-compile"
)

// wrapper capabilities — pack delivery. Execute mode: Kueue Jobs on the management cluster.
const (
	// CapabilityPackDeploy applies a ClusterPack to a target cluster.
	// Triggered by PackExecution CR via wrapper.
	CapabilityPackDeploy = "pack-deploy"
)

// guardian capabilities — RBAC plane.
const (
	// CapabilityRBACProvision provisions RBAC artifacts on a target cluster from
	// the current PermissionSnapshot. Initiated by the security agent control loop.
	CapabilityRBACProvision = "rbac-provision"
)
