package capability

import (
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// RegisterAll populates the registry with the real capability handler
// implementations for every named execute-mode capability declared in
// conductor-schema.md §6. Registration is static at build time.
//
// All handlers accept nil clients and return ValidationFailure when a required
// client is absent — enabling unit tests to call RegisterAll without wiring
// real Kubernetes or Talos clients.
//
// CR-INV-004, conductor-design.md §2.3.
func RegisterAll(reg *Registry) {
	// Platform capabilities -- cluster lifecycle and operations.
	reg.Register(runnerlib.CapabilityBootstrap, &bootstrapHandler{})
	reg.Register(runnerlib.CapabilityTalosUpgrade, &talosUpgradeHandler{})
	reg.Register(runnerlib.CapabilityKubeUpgrade, &kubeUpgradeHandler{})
	reg.Register(runnerlib.CapabilityStackUpgrade, &stackUpgradeHandler{})
	reg.Register(runnerlib.CapabilityNodePatch, &nodePatchHandler{})
	reg.Register(runnerlib.CapabilityNodeScaleUp, &nodeScaleUpHandler{})
	reg.Register(runnerlib.CapabilityNodeDecommission, &nodeDecommissionHandler{})
	reg.Register(runnerlib.CapabilityNodeReboot, &nodeRebootHandler{})
	reg.Register(runnerlib.CapabilityEtcdBackup, &etcdBackupHandler{})
	reg.Register(runnerlib.CapabilityEtcdDefrag, &etcdMaintenanceHandler{})
	reg.Register(runnerlib.CapabilityEtcdRestore, &etcdRestoreHandler{})
	reg.Register(runnerlib.CapabilityPKIRotate, &pkiRotateHandler{})
	reg.Register(runnerlib.CapabilityCredentialRotate, &credentialRotateHandler{})
	reg.Register(runnerlib.CapabilityHardeningApply, &hardeningApplyHandler{})
	reg.Register(runnerlib.CapabilityClusterReset, &clusterResetHandler{})
	reg.Register(runnerlib.CapabilityMachineConfigBackup, &machineConfigBackupHandler{})
	reg.Register(runnerlib.CapabilityMachineConfigRestore, &machineConfigRestoreHandler{})

	// Wrapper capabilities -- pack delivery.
	reg.Register(runnerlib.CapabilityPackDeploy, &packDeployHandler{})

	// Guardian capabilities -- RBAC plane.
	reg.Register(runnerlib.CapabilityRBACProvision, &rbacProvisionHandler{})

	// Note: CapabilityPackCompile is NOT registered here. pack-compile is a
	// Compiler compile-mode invocation only -- it never runs as a Conductor Job.
	// Registering it here would be a schema violation. conductor-schema.md §6.
}
