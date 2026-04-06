// compile_enable.go implements the `compiler enable` subcommand.
//
// Produces the complete management cluster deployment manifest bundle as a
// phased directory structure in the --output directory. The pipeline applies
// phases in strict numerical order, verifying readiness between phases.
//
// Output structure:
//
//	<output>/
//	  00-infrastructure-dependencies/
//	    phase-meta.yaml         — phase metadata and apply order
//	    cnpg-operator.yaml      — CNPG operator manifests (Namespace, CRDs, SA, RBAC, Deployment)
//	    cnpg-cluster.yaml       — CNPG Cluster CR for Guardian database (guardian-db in seam-system)
//	  01-guardian-bootstrap/
//	    phase-meta.yaml         — phase metadata and apply order
//	    namespace-labels.yaml   — seam-system and kube-system webhook-mode=exempt labels
//	    guardian-crds.yaml      — Guardian CRD definitions
//	    guardian-rbac.yaml      — Guardian SA, ClusterRole, ClusterRoleBinding
//	    guardian-rbacprofiles.yaml — Guardian RBACProfile CR
//	  02-guardian-deploy/
//	    phase-meta.yaml
//	    guardian-deployment.yaml — Guardian Deployment manifest
//	  03-platform-wrapper/
//	    phase-meta.yaml
//	    platform-wrapper-crds.yaml      — Platform, Wrapper, seam-core CRD definitions
//	    platform-wrapper-rbac.yaml      — Platform, Wrapper, seam-core RBAC
//	    platform-wrapper-rbacprofiles.yaml — RBACProfiles for Platform, Wrapper, seam-core
//	    platform-wrapper-deployments.yaml  — Platform, Wrapper, seam-core Deployments
//	  04-conductor/
//	    phase-meta.yaml
//	    conductor-crds.yaml     — Conductor (runner.ontai.dev) CRD definitions
//	    conductor-rbac.yaml     — Conductor SA, ClusterRole, ClusterRoleBinding
//	    conductor-rbacprofile.yaml — Conductor RBACProfile CR
//	    conductor-deployment.yaml  — Conductor Deployment (CONDUCTOR_ROLE=management)
//	  05-post-bootstrap/
//	    phase-meta.yaml
//	    leaderelection.yaml     — Leader election Lease resources for all operators
//
// All output is deterministic: no timestamps, no random values.
// Conductor Deployment carries CONDUCTOR_ROLE=management per §15.
//
// conductor-schema.md §9 Step 3, §15 Role Declaration Contract.
// guardian-schema.md §6 (Seam operator RBACProfiles), §16 CNPG Deployment Contract.
// guardian 25c9e93 WS3: namespace-labels.yaml satisfies CheckBootstrapLabels contract.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	"github.com/ontai-dev/conductor/internal/catalog/cnpg"
)

// operatorSpec declares one Seam operator for enable-phase manifest generation.
type operatorSpec struct {
	// Name is the operator's canonical name matching its repository name.
	Name string
	// Namespace is the namespace where the operator Deployment runs.
	Namespace string
	// Image is the operator container image (without tag).
	Image string
	// ServiceAccount is the ServiceAccount name for this operator.
	ServiceAccount string
	// LeaderElectionLease is the name of the leader election Lease.
	LeaderElectionLease string
}

// phaseMeta is the structure written to phase-meta.yaml in each phase directory.
// It declares the phase name, numerical order, readiness gate description, and
// the ordered list of files to apply within the phase.
type phaseMeta struct {
	// Phase is the canonical phase name (e.g., "guardian-bootstrap").
	Phase string `json:"phase" yaml:"phase"`
	// Order is the 1-based application order. Phases must be applied in ascending order.
	Order int `json:"order" yaml:"order"`
	// ReadinessGate is a human-readable description of what must be verified before
	// the next phase is applied by the GitOps pipeline operator.
	ReadinessGate string `json:"readinessGate" yaml:"readinessGate"`
	// ApplyOrder lists the filenames in this phase in the order they must be applied.
	// The pipeline applies files within a phase in this order.
	ApplyOrder []string `json:"applyOrder" yaml:"applyOrder"`
}

// guardianOp returns the operatorSpec for the Guardian operator.
func guardianOp(version string) operatorSpec {
	return operatorSpec{
		Name:                "guardian",
		Namespace:           "seam-system",
		Image:               "registry.ontai.dev/ontai-dev/guardian:" + version,
		ServiceAccount:      "guardian",
		LeaderElectionLease: "guardian-leader",
	}
}

// platformWrapperOps returns operatorSpecs for Platform, Wrapper, and seam-core.
func platformWrapperOps(version string) []operatorSpec {
	return []operatorSpec{
		{
			Name:                "platform",
			Namespace:           "seam-system",
			Image:               "registry.ontai.dev/ontai-dev/platform:" + version,
			ServiceAccount:      "platform",
			LeaderElectionLease: "platform-leader",
		},
		{
			Name:                "wrapper",
			Namespace:           "seam-system",
			Image:               "registry.ontai.dev/ontai-dev/wrapper:" + version,
			ServiceAccount:      "wrapper",
			LeaderElectionLease: "wrapper-leader",
		},
		{
			Name:                "seam-core",
			Namespace:           "seam-system",
			Image:               "registry.ontai.dev/ontai-dev/seam-core:" + version,
			ServiceAccount:      "seam-core",
			LeaderElectionLease: "seam-core-leader",
		},
	}
}

// conductorOp returns the operatorSpec for the Conductor operator.
func conductorOp(version string) operatorSpec {
	return operatorSpec{
		Name:                "conductor",
		Namespace:           "ont-system",
		Image:               "registry.ontai.dev/ontai-dev/conductor:" + version,
		ServiceAccount:      "conductor",
		LeaderElectionLease: "conductor-management",
	}
}

// allOperators returns all operator specs in their original flat order (used
// for leader election leases in phase 5 which covers all operators).
func allOperators(version string) []operatorSpec {
	result := []operatorSpec{conductorOp(version), guardianOp(version)}
	result = append(result, platformWrapperOps(version)...)
	return result
}

const enableHelp = `Usage: compiler enable --output <path> [--version <tag>] [--kubeconfig <path>]

Produce the phased deployment manifest bundle (conductor-schema.md §9 Steps 3–8).

Input contract:
  --version     Conductor/Compiler image tag (default: dev).
  --kubeconfig  Path to kubeconfig (flag → $KUBECONFIG → ~/.kube/config).
                Optional; reserved for compile-time readiness gate validation.

Output contract:
  --output  Directory receiving six phase subdirectories:
              00-infrastructure-dependencies/  — CNPG operator and cluster
              01-guardian-bootstrap/           — Guardian CRDs, RBAC, RBACProfiles
              02-guardian-deploy/              — Guardian Deployment
              03-platform-wrapper/             — Platform, Wrapper, seam-core
              04-conductor/                    — Conductor CRDs, RBAC, Deployment
              05-post-bootstrap/               — DSNS zone, CoreDNS stanza, leader election

            Each phase directory contains phase-meta.yaml declaring apply order
            and readiness gates before the next phase may proceed.

Compile-only: output is a manifest bundle for human review and GitOps pipeline
application — Compiler never applies, patches, or deletes any resource.
`

// runEnableSubcommand parses enable-specific flags and calls compileEnableBundle.
// conductor-schema.md §9 Step 3.
func runEnableSubcommand(args []string) {
	fs := flag.NewFlagSet("enable", flag.ExitOnError)
	output := fs.String("output", "", "Output directory for manifest bundle (required)")
	version := fs.String("version", "dev", "Operator image tag (e.g., v1.9.3-r1). Defaults to \"dev\".")
	_ = fs.String("kubeconfig", "", "Path to kubeconfig (unused in output-only mode; reserved for future validation)")

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, enableHelp)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "compiler enable: flag error: %v\n", err)
		os.Exit(1)
	}
	if *output == "" {
		fmt.Fprintln(os.Stderr, "compiler enable: --output is required")
		fs.Usage()
		os.Exit(1)
	}

	if err := compileEnableBundle(*output, *version); err != nil {
		fmt.Fprintf(os.Stderr, "compiler enable: %v\n", err)
		os.Exit(1)
	}
}

// compileEnableBundle generates the phased management cluster deployment manifest
// bundle and writes it to the output directory.
// conductor-schema.md §9 Step 3, §15.
func compileEnableBundle(output, version string) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	gdn := guardianOp(version)
	pwOps := platformWrapperOps(version)
	cdt := conductorOp(version)

	if err := writePhase0InfrastructureDependencies(output); err != nil {
		return fmt.Errorf("phase 0 infrastructure-dependencies: %w", err)
	}
	if err := writePhase1GuardianBootstrap(output, gdn); err != nil {
		return fmt.Errorf("phase 1 guardian-bootstrap: %w", err)
	}
	if err := writePhase2GuardianDeploy(output, gdn); err != nil {
		return fmt.Errorf("phase 2 guardian-deploy: %w", err)
	}
	if err := writePhase3PlatformWrapper(output, pwOps); err != nil {
		return fmt.Errorf("phase 3 platform-wrapper: %w", err)
	}
	if err := writePhase4Conductor(output, cdt); err != nil {
		return fmt.Errorf("phase 4 conductor: %w", err)
	}
	if err := writePhase5PostBootstrap(output, allOperators(version)); err != nil {
		return fmt.Errorf("phase 5 post-bootstrap: %w", err)
	}

	return nil
}

// --- Phase 0: infrastructure-dependencies ---

// writePhase0InfrastructureDependencies writes the 00-infrastructure-dependencies
// phase directory. This phase provisions the CNPG operator and the guardian-db
// CNPG Cluster CR before any Seam operator is deployed.
//
// Phase 0 is the prerequisite that resolves the CNPG dependency: Guardian's startup
// migration runner connects to CNPG before registering any controller. Phase 0 must
// reach readiness (CNPG Cluster ready) before phase 1 may begin.
// guardian-schema.md §16 CNPG Deployment Contract, conductor-schema.md §9.
func writePhase0InfrastructureDependencies(output string) error {
	dir := filepath.Join(output, "00-infrastructure-dependencies")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{"cnpg-operator.yaml", "cnpg-cluster.yaml"}

	meta := phaseMeta{
		Phase: "infrastructure-dependencies",
		Order: 0,
		ReadinessGate: "Wait for the CNPG Cluster 'guardian-db' in seam-system to reach " +
			"Ready status before applying phase 1 " +
			"(kubectl get cluster guardian-db -n seam-system). " +
			"The CNPG operator Deployment must be Available in cnpg-system. " +
			"Guardian's startup migration runner connects to CNPG before registering " +
			"any controller — phase 0 must be fully operational before Guardian starts. " +
			"guardian-schema.md §16 CNPG Deployment Contract.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	// cnpg-operator.yaml — embedded CNPG v1.24.0 operator manifests.
	if err := writeCNPGOperatorManifest(dir); err != nil {
		return err
	}

	// cnpg-cluster.yaml — CNPG Cluster CR for Guardian's database.
	if err := writeCNPGClusterCR(dir); err != nil {
		return err
	}

	return nil
}

// writeCNPGOperatorManifest writes the embedded CNPG operator manifest to
// cnpg-operator.yaml in the phase directory. The manifest is pinned to
// cnpg.Version and embedded at compile time — no runtime fetch occurs.
// conductor-schema.md §9, guardian-schema.md §16.
func writeCNPGOperatorManifest(dir string) error {
	var buf bytes.Buffer
	buf.WriteString("# CloudNativePG Operator Manifests — pinned to " + cnpg.Version + "\n")
	buf.WriteString("# Generated by: compiler enable (phase 0 infrastructure-dependencies)\n")
	buf.WriteString("# Embedded at compile time from internal/catalog/cnpg/operator.yaml.\n")
	buf.WriteString("# conductor-schema.md §9, guardian-schema.md §16 CNPG Deployment Contract.\n")
	buf.Write(cnpg.OperatorManifest)
	return os.WriteFile(filepath.Join(dir, "cnpg-operator.yaml"), buf.Bytes(), 0644)
}

// writeCNPGClusterCR writes the CNPG Cluster CR for Guardian's database to
// cnpg-cluster.yaml in the phase directory.
//
// The Cluster CR:
//   - name: guardian-db, namespace: seam-system (Guardian's operating namespace)
//   - instances: 3 (HA deployment for management Guardian)
//   - storage size: 50Gi per instance
//   - superuserSecret.name: guardian-db-credentials (read by Guardian startup migration runner)
//   - annotation: governance.infrastructure.ontai.dev/owner=seam-platform
//
// guardian-schema.md §16 CNPG Deployment Contract.
func writeCNPGClusterCR(dir string) error {
	cluster := map[string]interface{}{
		"apiVersion": "postgresql.cnpg.io/v1",
		"kind":       "Cluster",
		"metadata": map[string]interface{}{
			"name":      "guardian-db",
			"namespace": "seam-system",
			"labels": map[string]string{
				"app.kubernetes.io/name":      "guardian-db",
				"app.kubernetes.io/component": "database",
				"ontai.dev/managed-by":        "compiler",
			},
			"annotations": map[string]string{
				"governance.infrastructure.ontai.dev/owner": "seam-platform",
			},
		},
		"spec": map[string]interface{}{
			"instances": 3,
			"storage": map[string]interface{}{
				"size": "50Gi",
			},
			"superuserSecret": map[string]interface{}{
				"name": "guardian-db-credentials",
			},
			"bootstrap": map[string]interface{}{
				"initdb": map[string]interface{}{
					"database": "guardian",
					"owner":    "guardian",
				},
			},
		},
	}

	data, err := yaml.Marshal(cluster)
	if err != nil {
		return fmt.Errorf("marshal CNPG Cluster CR: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# CNPG Cluster CR — Guardian database (guardian-db in seam-system)\n")
	buf.WriteString("# Generated by: compiler enable (phase 0 infrastructure-dependencies)\n")
	buf.WriteString("# guardian-schema.md §16 CNPG Deployment Contract.\n")
	buf.WriteString("# Three instances, 50Gi each. guardian-db-credentials Secret must\n")
	buf.WriteString("# exist in seam-system before applying this CR.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "cnpg-cluster.yaml"), buf.Bytes(), 0644)
}

// --- Phase 1: guardian-bootstrap ---

func writePhase1GuardianBootstrap(output string, gdn operatorSpec) error {
	dir := filepath.Join(output, "01-guardian-bootstrap")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{
		"namespace-labels.yaml",
		"guardian-crds.yaml",
		"guardian-rbac.yaml",
		"guardian-rbacprofiles.yaml",
	}

	meta := phaseMeta{
		Phase: "guardian-bootstrap",
		Order: 1,
		ReadinessGate: "Verify that seam-system and kube-system namespaces carry " +
			"seam.ontai.dev/webhook-mode=exempt before applying phase 2. " +
			"Guardian CRDs must be registered (kubectl get crd | grep security.ontai.dev). " +
			"Guardian RBAC must be present. Guardian RBACProfile must be in the cluster.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	// namespace-labels.yaml — stamps exempt label on seam-system and kube-system.
	// Satisfies guardian 25c9e93 WS3 CheckBootstrapLabels contract.
	if err := writeNamespaceLabels(dir); err != nil {
		return err
	}

	// guardian-crds.yaml — Guardian CRD definitions (security.ontai.dev).
	if err := writeGuardianCRDs(dir); err != nil {
		return err
	}

	// guardian-rbac.yaml — Guardian SA, ClusterRole, ClusterRoleBinding.
	if err := writeOperatorRBACFile(dir, "guardian-rbac.yaml", []operatorSpec{gdn}); err != nil {
		return err
	}

	// guardian-rbacprofiles.yaml — Guardian RBACProfile CR.
	if err := writeOperatorRBACProfilesFile(dir, "guardian-rbacprofiles.yaml", []operatorSpec{gdn}); err != nil {
		return err
	}

	return nil
}

// writeNamespaceLabels writes namespace-labels.yaml containing server-side apply
// patches for seam-system and kube-system, stamping seam.ontai.dev/webhook-mode=exempt.
// This is the bootstrap label required by guardian CheckBootstrapLabels (25c9e93 WS3).
// Uses server-side apply patches: only metadata.name, metadata.labels, and the
// field manager annotation — not full Namespace manifests.
func writeNamespaceLabels(dir string) error {
	var buf bytes.Buffer
	buf.WriteString("# Namespace webhook-mode label patches\n")
	buf.WriteString("# Generated by: compiler enable (phase 1 guardian-bootstrap)\n")
	buf.WriteString("#\n")
	buf.WriteString("# Apply with: kubectl apply --server-side --field-manager=compiler-enable\n")
	buf.WriteString("#\n")
	buf.WriteString("# These patches stamp seam.ontai.dev/webhook-mode=exempt on seam-system\n")
	buf.WriteString("# and kube-system before Guardian is deployed. Guardian's CheckBootstrapLabels\n")
	buf.WriteString("# gate refuses to register the admission webhook if this label is absent.\n")
	buf.WriteString("# guardian 25c9e93 WS3, INV-020, CS-INV-004.\n")

	for _, ns := range namespacesSortedExempt() {
		patch := namespaceLabelPatch(ns)
		data, err := yaml.Marshal(patch)
		if err != nil {
			return fmt.Errorf("marshal namespace patch for %s: %w", ns, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(dir, "namespace-labels.yaml"), buf.Bytes(), 0644)
}

// namespacesSortedExempt returns the canonical list of namespaces to stamp
// with the exempt label, in deterministic order.
func namespacesSortedExempt() []string {
	// kube-system before seam-system — alphabetical, deterministic.
	return []string{"kube-system", "seam-system"}
}

// namespaceLabelPatch returns a server-side apply patch for a Namespace object.
// The patch carries only the fields required for the label operation: apiVersion,
// kind, metadata.name, metadata.labels, and the field manager annotation.
// It is not a full Namespace manifest — no spec, no status.
func namespaceLabelPatch(name string) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Namespace",
		"metadata": map[string]interface{}{
			"name": name,
			"labels": map[string]string{
				"seam.ontai.dev/webhook-mode": "exempt",
			},
			"annotations": map[string]string{
				"kubectl.kubernetes.io/last-applied-configuration": "",
			},
		},
	}
}

// writeGuardianCRDs writes guardian CRD definitions (security.ontai.dev group).
// Uses the embedded CRD bundle from the guardian repository, filtering to
// security.ontai.dev group only.
func writeGuardianCRDs(dir string) error {
	// Extract guardian CRDs from the full CRD bundle (which includes all groups).
	// We generate the full bundle first, then filter to the guardian group.
	var allBuf bytes.Buffer
	if err := writeCRDBundleToBuffer(&allBuf); err != nil {
		return fmt.Errorf("read CRD bundle: %w", err)
	}

	// Split on --- and filter to security.ontai.dev documents.
	guardianCRDs := filterCRDsByGroup(allBuf.String(), "security.ontai.dev")

	var buf bytes.Buffer
	buf.WriteString("# Guardian CRD Definitions (security.ontai.dev)\n")
	buf.WriteString("# Generated by: compiler enable (phase 1 guardian-bootstrap)\n")
	buf.WriteString("# Apply before deploying Guardian. CRDs must be registered before\n")
	buf.WriteString("# Guardian can reconcile any security.ontai.dev resources.\n")
	buf.Write([]byte(guardianCRDs))

	return os.WriteFile(filepath.Join(dir, "guardian-crds.yaml"), buf.Bytes(), 0644)
}

// --- Phase 2: guardian-deploy ---

func writePhase2GuardianDeploy(output string, gdn operatorSpec) error {
	dir := filepath.Join(output, "02-guardian-deploy")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{"guardian-deployment.yaml"}

	meta := phaseMeta{
		Phase: "guardian-deploy",
		Order: 2,
		ReadinessGate: "Wait for the Guardian Deployment to reach Available=True " +
			"(kubectl rollout status deployment/guardian -n seam-system). " +
			"Guardian's admission webhook must be registered and accepting requests. " +
			"Verify with: kubectl get validatingwebhookconfigurations | grep guardian. " +
			"Do not apply phase 3 until Guardian is fully operational — it must be " +
			"present to govern RBAC resources created in subsequent phases.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	if err := writeDeploymentFile(dir, "guardian-deployment.yaml", gdn, "# Guardian Deployment\n# Generated by: compiler enable (phase 2 guardian-deploy)\n"); err != nil {
		return err
	}

	return nil
}

// --- Phase 3: platform-wrapper ---

func writePhase3PlatformWrapper(output string, ops []operatorSpec) error {
	dir := filepath.Join(output, "03-platform-wrapper")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{
		"platform-wrapper-crds.yaml",
		"platform-wrapper-rbac.yaml",
		"platform-wrapper-rbacprofiles.yaml",
		"platform-wrapper-deployments.yaml",
	}

	meta := phaseMeta{
		Phase: "platform-wrapper",
		Order: 3,
		ReadinessGate: "Wait for Platform, Wrapper, and seam-core Deployments to reach " +
			"Available=True. Verify Platform and Wrapper RBACProfiles reach " +
			"provisioned=true (kubectl get rbacprofiles -n seam-system). " +
			"These operators must be operational before Conductor's RBACProfile " +
			"can be provisioned in phase 4.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	// platform-wrapper-crds.yaml — Platform, Wrapper, seam-core CRD definitions.
	if err := writePlatformWrapperCRDs(dir); err != nil {
		return err
	}

	// platform-wrapper-rbac.yaml — SA, ClusterRole, ClusterRoleBinding for all three.
	if err := writeOperatorRBACFile(dir, "platform-wrapper-rbac.yaml", ops); err != nil {
		return err
	}

	// platform-wrapper-rbacprofiles.yaml — RBACProfile CRs for Platform, Wrapper, seam-core.
	if err := writeOperatorRBACProfilesFile(dir, "platform-wrapper-rbacprofiles.yaml", ops); err != nil {
		return err
	}

	// platform-wrapper-deployments.yaml — Deployment manifests.
	if err := writeDeploymentsFile(dir, "platform-wrapper-deployments.yaml", ops,
		"# Platform, Wrapper, seam-core Deployments\n# Generated by: compiler enable (phase 3 platform-wrapper)\n"); err != nil {
		return err
	}

	return nil
}

// writePlatformWrapperCRDs writes CRD definitions for platform, wrapper, and seam-core.
func writePlatformWrapperCRDs(dir string) error {
	var allBuf bytes.Buffer
	if err := writeCRDBundleToBuffer(&allBuf); err != nil {
		return fmt.Errorf("read CRD bundle: %w", err)
	}

	// Filter to platform, infra (wrapper), and infrastructure (seam-core) groups.
	groups := []string{"platform.ontai.dev", "infra.ontai.dev", "infrastructure.ontai.dev"}
	var combined bytes.Buffer
	for _, group := range groups {
		combined.WriteString(filterCRDsByGroup(allBuf.String(), group))
	}

	var buf bytes.Buffer
	buf.WriteString("# Platform, Wrapper, seam-core CRD Definitions\n")
	buf.WriteString("# Generated by: compiler enable (phase 3 platform-wrapper)\n")
	buf.WriteString("# Groups: platform.ontai.dev, infra.ontai.dev, infrastructure.ontai.dev\n")
	buf.Write(combined.Bytes())

	return os.WriteFile(filepath.Join(dir, "platform-wrapper-crds.yaml"), buf.Bytes(), 0644)
}

// --- Phase 4: conductor ---

func writePhase4Conductor(output string, cdt operatorSpec) error {
	dir := filepath.Join(output, "04-conductor")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{
		"conductor-crds.yaml",
		"conductor-rbac.yaml",
		"conductor-rbacprofile.yaml",
		"conductor-deployment.yaml",
	}

	meta := phaseMeta{
		Phase: "conductor",
		Order: 4,
		ReadinessGate: "Wait for the Conductor Deployment to reach Available=True " +
			"(kubectl rollout status deployment/conductor -n ont-system). " +
			"Verify CONDUCTOR_ROLE=management is set in the conductor pod environment. " +
			"Verify Conductor's RBACProfile reaches provisioned=true. " +
			"After this phase completes, the management cluster is fully operational. " +
			"Apply phase 5 (post-bootstrap) at any point after Conductor is ready.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	// conductor-crds.yaml — runner.ontai.dev CRD definitions.
	if err := writeConductorCRDs(dir); err != nil {
		return err
	}

	// conductor-rbac.yaml — Conductor SA, ClusterRole, ClusterRoleBinding.
	if err := writeOperatorRBACFile(dir, "conductor-rbac.yaml", []operatorSpec{cdt}); err != nil {
		return err
	}

	// conductor-rbacprofile.yaml — Conductor RBACProfile CR.
	if err := writeOperatorRBACProfilesFile(dir, "conductor-rbacprofile.yaml", []operatorSpec{cdt}); err != nil {
		return err
	}

	// conductor-deployment.yaml — Conductor Deployment with CONDUCTOR_ROLE=management.
	if err := writeDeploymentFile(dir, "conductor-deployment.yaml", cdt,
		"# Conductor Deployment (CONDUCTOR_ROLE=management)\n# Generated by: compiler enable (phase 4 conductor)\n# conductor-schema.md §15 Role Declaration Contract.\n"); err != nil {
		return err
	}

	return nil
}

// writeConductorCRDs writes runner.ontai.dev CRD definitions.
func writeConductorCRDs(dir string) error {
	var allBuf bytes.Buffer
	if err := writeCRDBundleToBuffer(&allBuf); err != nil {
		return fmt.Errorf("read CRD bundle: %w", err)
	}

	conductorCRDs := filterCRDsByGroup(allBuf.String(), "runner.ontai.dev")

	var buf bytes.Buffer
	buf.WriteString("# Conductor CRD Definitions (runner.ontai.dev)\n")
	buf.WriteString("# Generated by: compiler enable (phase 4 conductor)\n")
	buf.Write([]byte(conductorCRDs))

	return os.WriteFile(filepath.Join(dir, "conductor-crds.yaml"), buf.Bytes(), 0644)
}

// --- Phase 5: post-bootstrap ---

func writePhase5PostBootstrap(output string, operators []operatorSpec) error {
	dir := filepath.Join(output, "05-post-bootstrap")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{
		"dsns-zone-configmap.yaml",
		"coredns-dsns-stanza.yaml",
		"dsns-loadbalancer.yaml",
		"leaderelection.yaml",
	}

	meta := phaseMeta{
		Phase: "post-bootstrap",
		Order: 5,
		ReadinessGate: "No further readiness gate. The management cluster is fully " +
			"operational after this phase. The DSNS zone ConfigMap is created empty " +
			"and populated at runtime by the seam-core DSNSReconciler. Leader election " +
			"Leases are pre-created empty — operators populate them at runtime during " +
			"their first reconcile.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	if err := writeDSNSZoneConfigMapYAML(dir); err != nil {
		return err
	}
	if err := writeCoreDNSDSNSStanzaYAML(dir); err != nil {
		return err
	}
	if err := writeDSNSLoadBalancerYAML(dir); err != nil {
		return err
	}
	if err := writeLeaderElectionYAML(dir, operators); err != nil {
		return err
	}

	return nil
}

// writeDSNSZoneConfigMapYAML writes the empty dsns-zone ConfigMap to 05-post-bootstrap.
// seam-core DSNSReconciler populates zone.db at runtime. seam-core-schema.md §8 Decision 2.
func writeDSNSZoneConfigMapYAML(dir string) error {
	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dsns-zone",
			Namespace: "ont-system",
			Labels: map[string]string{
				"seam.ontai.dev/dsns-zone": "true",
			},
			Annotations: map[string]string{
				"governance.infrastructure.ontai.dev/owner": "seam-core",
			},
		},
		Data: map[string]string{
			"zone.db": "",
		},
	}

	var buf bytes.Buffer
	buf.WriteString("# DSNS Zone ConfigMap — empty at creation; populated at runtime by seam-core DSNSReconciler.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# seam-core-schema.md §8 Decision 2.\n")
	buf.WriteString("---\n")
	data, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal dsns-zone ConfigMap: %w", err)
	}
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "dsns-zone-configmap.yaml"), buf.Bytes(), 0644)
}

// writeCoreDNSDSNSStanzaYAML writes the CoreDNS DSNS stanza ConfigMap patch.
// Apply with: kubectl apply --server-side -f coredns-dsns-stanza.yaml
// Adds file plugin stanza for seam.ontave.dev and reload plugin (5s interval).
// seam-core-schema.md §8 Decision 3.
func writeCoreDNSDSNSStanzaYAML(dir string) error {
	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "kube-dns",
			Namespace: "kube-system",
			Annotations: map[string]string{
				"governance.infrastructure.ontai.dev/owner": "seam-core",
			},
		},
		Data: map[string]string{
			"dsns-stanza": "seam.ontave.dev. {\n    file /etc/coredns/seam-zone.db\n    reload 5s\n}\n",
		},
	}

	var buf bytes.Buffer
	buf.WriteString("# CoreDNS DSNS Stanza Patch — apply with: kubectl apply --server-side -f coredns-dsns-stanza.yaml\n")
	buf.WriteString("# Adds file plugin stanza for seam.ontave.dev and reload plugin (5s interval).\n")
	buf.WriteString("# Human review: integrate dsns-stanza value into the Corefile key before applying.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# seam-core-schema.md §8 Decision 3.\n")
	buf.WriteString("---\n")
	data, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal CoreDNS DSNS stanza ConfigMap: %w", err)
	}
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "coredns-dsns-stanza.yaml"), buf.Bytes(), 0644)
}

// writeDSNSLoadBalancerYAML writes a LoadBalancer Service in ont-system targeting
// CoreDNS pods in kube-system on port 53 UDP+TCP. LoadBalancerIP is the management
// cluster VIP (10.20.0.10). seam-core-schema.md §8 Decision 3.
func writeDSNSLoadBalancerYAML(dir string) error {
	svc := corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dsns-loadbalancer",
			Namespace: "ont-system",
			Annotations: map[string]string{
				"governance.infrastructure.ontai.dev/owner": "seam-core",
			},
		},
		Spec: corev1.ServiceSpec{
			Type:           corev1.ServiceTypeLoadBalancer,
			LoadBalancerIP: "10.20.0.10",
			Selector: map[string]string{
				"k8s-app": "kube-dns",
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "dns-udp",
					Protocol:   corev1.ProtocolUDP,
					Port:       53,
					TargetPort: intstr.FromInt32(53),
				},
				{
					Name:       "dns-tcp",
					Protocol:   corev1.ProtocolTCP,
					Port:       53,
					TargetPort: intstr.FromInt32(53),
				},
			},
		},
	}

	var buf bytes.Buffer
	buf.WriteString("# DSNS LoadBalancer Service — exposes CoreDNS on port 53 UDP+TCP at the management cluster VIP.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# seam-core-schema.md §8 Decision 3.\n")
	buf.WriteString("---\n")
	data, err := yaml.Marshal(svc)
	if err != nil {
		return fmt.Errorf("marshal DSNS LoadBalancer Service: %w", err)
	}
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "dsns-loadbalancer.yaml"), buf.Bytes(), 0644)
}

// --- Shared helpers ---

// writePhaseMeta serialises a phaseMeta struct to phase-meta.yaml in dir.
func writePhaseMeta(dir string, meta phaseMeta) error {
	var buf bytes.Buffer
	buf.WriteString("# Phase metadata — do not edit manually.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	data, err := yaml.Marshal(meta)
	if err != nil {
		return fmt.Errorf("marshal phase-meta for %s: %w", meta.Phase, err)
	}
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "phase-meta.yaml"), buf.Bytes(), 0644)
}

// writeOperatorRBACFile writes ServiceAccounts, ClusterRoles, and ClusterRoleBindings
// for the given operators to the specified filename in dir.
func writeOperatorRBACFile(dir, filename string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator RBAC Resources\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Human review required before GitOps commit.\n")

	for _, op := range operators {
		sa := corev1.ServiceAccount{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      op.ServiceAccount,
				Namespace: op.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name":      op.Name,
					"app.kubernetes.io/component": "operator",
					"ontai.dev/managed-by":        "compiler",
				},
			},
		}
		saData, err := yaml.Marshal(sa)
		if err != nil {
			return fmt.Errorf("marshal ServiceAccount for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(saData)

		cr := rbacv1.ClusterRole{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "rbac.authorization.k8s.io/v1",
				Kind:       "ClusterRole",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: op.Name + "-manager-role",
				Labels: map[string]string{
					"app.kubernetes.io/name":      op.Name,
					"app.kubernetes.io/component": "operator",
					"ontai.dev/managed-by":        "compiler",
				},
				Annotations: map[string]string{
					"ontai.dev/rbac-owner": "guardian",
				},
			},
			Rules: operatorClusterRules(op.Name),
		}
		crData, err := yaml.Marshal(cr)
		if err != nil {
			return fmt.Errorf("marshal ClusterRole for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(crData)

		crb := rbacv1.ClusterRoleBinding{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "rbac.authorization.k8s.io/v1",
				Kind:       "ClusterRoleBinding",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name: op.Name + "-manager-rolebinding",
				Labels: map[string]string{
					"app.kubernetes.io/name":      op.Name,
					"app.kubernetes.io/component": "operator",
					"ontai.dev/managed-by":        "compiler",
				},
				Annotations: map[string]string{
					"ontai.dev/rbac-owner": "guardian",
				},
			},
			RoleRef: rbacv1.RoleRef{
				APIGroup: "rbac.authorization.k8s.io",
				Kind:     "ClusterRole",
				Name:     op.Name + "-manager-role",
			},
			Subjects: []rbacv1.Subject{
				{
					Kind:      "ServiceAccount",
					Name:      op.ServiceAccount,
					Namespace: op.Namespace,
				},
			},
		}
		crbData, err := yaml.Marshal(crb)
		if err != nil {
			return fmt.Errorf("marshal ClusterRoleBinding for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(crbData)
	}

	return os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644)
}

// writeOperatorRBACProfilesFile writes RBACProfile CRs for the given operators
// to the specified filename in dir.
func writeOperatorRBACProfilesFile(dir, filename string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator RBACProfile CRs\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Human review required before GitOps commit. guardian-schema.md §6.\n")
	buf.WriteString("# spec.lineage is controller-managed — do not author manually. CLAUDE.md §14.\n")

	for _, op := range operators {
		profile := buildOperatorRBACProfile(op)
		data, err := yaml.Marshal(profile)
		if err != nil {
			return fmt.Errorf("marshal RBACProfile for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644)
}

// writeDeploymentFile writes a single operator Deployment to filename in dir.
func writeDeploymentFile(dir, filename string, op operatorSpec, header string) error {
	var buf bytes.Buffer
	buf.WriteString(header)

	dep := buildOperatorDeployment(op)
	data, err := yaml.Marshal(dep)
	if err != nil {
		return fmt.Errorf("marshal Deployment for %s: %w", op.Name, err)
	}
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644)
}

// writeDeploymentsFile writes Deployment manifests for multiple operators to filename in dir.
func writeDeploymentsFile(dir, filename string, operators []operatorSpec, header string) error {
	var buf bytes.Buffer
	buf.WriteString(header)

	for _, op := range operators {
		dep := buildOperatorDeployment(op)
		data, err := yaml.Marshal(dep)
		if err != nil {
			return fmt.Errorf("marshal Deployment for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644)
}

// buildOperatorDeployment constructs the Deployment manifest for one operator.
// Conductor Deployment carries CONDUCTOR_ROLE=management. conductor-schema.md §15.
func buildOperatorDeployment(op operatorSpec) appsv1.Deployment {
	replicas := int32(2)
	env := []corev1.EnvVar{
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
	}

	// Conductor Deployment MUST carry CONDUCTOR_ROLE=management.
	// This is a first-class field stamped by compiler enable. §15.
	if op.Name == "conductor" {
		env = append(env, corev1.EnvVar{
			Name:  "CONDUCTOR_ROLE",
			Value: "management",
		})
	}

	return appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      op.Name,
			Namespace: op.Namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      op.Name,
				"app.kubernetes.io/component": "operator",
				"ontai.dev/managed-by":        "compiler",
			},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{
					"app.kubernetes.io/name": op.Name,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app.kubernetes.io/name":      op.Name,
						"app.kubernetes.io/component": "operator",
					},
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: op.ServiceAccount,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtr(true),
					},
					Containers: []corev1.Container{
						{
							Name:            op.Name,
							Image:           op.Image,
							ImagePullPolicy: corev1.PullIfNotPresent,
							Env:             env,
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: boolPtr(false),
								ReadOnlyRootFilesystem:   boolPtr(true),
							},
						},
					},
					TerminationGracePeriodSeconds: int64Ptr(30),
				},
			},
		},
	}
}

// operatorClusterRules returns minimal ClusterRole rules for a Seam operator.
// These are the bootstrap window permissions required for the enable phase.
// Guardian validates and owns all RBAC — this set is the minimum to get
// Guardian operational. guardian-schema.md §4.
func operatorClusterRules(operatorName string) []rbacv1.PolicyRule {
	common := []rbacv1.PolicyRule{
		{
			APIGroups: []string{""},
			Resources: []string{"events"},
			Verbs:     []string{"create", "patch"},
		},
		{
			APIGroups: []string{"coordination.k8s.io"},
			Resources: []string{"leases"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		},
	}

	switch operatorName {
	case "conductor":
		return append(common, rbacv1.PolicyRule{
			APIGroups: []string{"runner.ontai.dev"},
			Resources: []string{"runnerconfigs", "runnerconfigs/status"},
			Verbs:     []string{"get", "list", "watch", "update", "patch"},
		})
	case "guardian":
		return append(common, rbacv1.PolicyRule{
			APIGroups: []string{"security.ontai.dev"},
			Resources: []string{"rbacpolicies", "rbacprofiles", "identitybindings",
				"identityproviders", "permissionsets", "permissionsnapshots"},
			Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		})
	case "platform":
		return append(common, rbacv1.PolicyRule{
			APIGroups: []string{"platform.ontai.dev"},
			Resources: []string{"talosclusters", "talosclusters/status",
				"etcdmaintenances", "nodemaintenances", "pkirotations",
				"clusterresets", "hardeningprofiles", "upgradepolicies",
				"nodeoperations", "clustermaintenances", "maintenancebundles"},
			Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		})
	case "wrapper":
		return append(common, rbacv1.PolicyRule{
			APIGroups: []string{"infra.ontai.dev"},
			Resources: []string{"clusterpacks", "packexecutions", "packinstances",
				"clusterpacks/status", "packexecutions/status", "packinstances/status"},
			Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		})
	case "seam-core":
		return append(common, rbacv1.PolicyRule{
			APIGroups: []string{"infrastructure.ontai.dev"},
			Resources: []string{"infrastructurelineageindices", "infrastructurelineageindices/status"},
			Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		})
	default:
		return common
	}
}

// writeLeaderElectionYAML writes leader election Lease resources in seam-system
// and ont-system for all Seam operators.
func writeLeaderElectionYAML(dir string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator Leader Election Leases\n")
	buf.WriteString("# Generated by: compiler enable (phase 5 post-bootstrap)\n")
	buf.WriteString("# Leases are created empty here; operators populate them at runtime.\n")
	buf.WriteString("# seam-system: guardian, platform, wrapper, seam-core\n")
	buf.WriteString("# ont-system: conductor\n")

	for _, op := range operators {
		lease := coordinationv1.Lease{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "coordination.k8s.io/v1",
				Kind:       "Lease",
			},
			ObjectMeta: metav1.ObjectMeta{
				Name:      op.LeaderElectionLease,
				Namespace: op.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name":      op.Name,
					"app.kubernetes.io/component": "leader-election",
					"ontai.dev/managed-by":        "compiler",
				},
			},
		}
		data, err := yaml.Marshal(lease)
		if err != nil {
			return fmt.Errorf("marshal Lease for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(dir, "leaderelection.yaml"), buf.Bytes(), 0644)
}

// buildOperatorRBACProfile constructs a Guardian RBACProfile CR for one Seam operator SA.
// Uses the security.ontai.dev/v1alpha1 schema from guardian-schema.md §7.
func buildOperatorRBACProfile(op operatorSpec) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACProfile",
		"metadata": map[string]interface{}{
			"name":      "rbac-" + op.Name,
			"namespace": op.Namespace,
			"labels": map[string]string{
				"app.kubernetes.io/name":      op.Name,
				"app.kubernetes.io/component": "operator",
				"ontai.dev/managed-by":        "compiler",
				"ontai.dev/rbac-profile-type": "seam-operator",
			},
			"annotations": map[string]string{
				"ontai.dev/review-required": "true",
			},
		},
		"spec": map[string]interface{}{
			"principalRef":  op.ServiceAccount,
			"rbacPolicyRef": "seam-platform-rbac-policy",
			"targetClusters": []string{
				"management",
			},
			"permissionDeclarations": []map[string]interface{}{
				{
					"permissionSetRef": op.Name + "-permissions",
					"scope":            "cluster",
				},
			},
		},
	}
}

// writeCRDBundleToBuffer generates the complete CRD bundle (same as compiler launch)
// and writes it to the provided buffer. Used by phase-specific CRD writers to
// extract per-group subsets.
func writeCRDBundleToBuffer(buf *bytes.Buffer) error {
	// Use a temp dir to reuse the existing compileLaunchBundle logic.
	tmp, err := os.MkdirTemp("", "compiler-enable-crds-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)

	if err := compileLaunchBundle(tmp); err != nil {
		return err
	}

	data, err := os.ReadFile(filepath.Join(tmp, "crds.yaml"))
	if err != nil {
		return err
	}
	buf.Write(data)
	return nil
}

// filterCRDsByGroup extracts YAML documents from content that contain the given
// API group string. Documents are split on "---" separators.
// Returns a string with only the matching documents, each prefixed with "---\n".
func filterCRDsByGroup(content, group string) string {
	var out bytes.Buffer
	for _, doc := range splitYAMLDocs(content) {
		if containsStr(doc, group) {
			out.WriteString("---\n")
			out.WriteString(doc)
		}
	}
	return out.String()
}

// splitYAMLDocs splits a multi-document YAML string on "---" separators.
// Returns non-empty trimmed document strings.
func splitYAMLDocs(content string) []string {
	var docs []string
	for _, raw := range splitOn(content, "\n---") {
		trimmed := trimLeft(raw, "\n")
		if trimmed != "" {
			docs = append(docs, trimmed)
		}
	}
	return docs
}

// splitOn splits s on each occurrence of sep. The separator is consumed.
func splitOn(s, sep string) []string {
	var parts []string
	for {
		idx := indexOf(s, sep)
		if idx < 0 {
			parts = append(parts, s)
			return parts
		}
		parts = append(parts, s[:idx])
		s = s[idx+len(sep):]
	}
}

// indexOf returns the index of substr in s, or -1 if not present.
func indexOf(s, substr string) int {
	if len(substr) == 0 {
		return 0
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// trimLeft removes leading occurrences of cutset characters from s.
func trimLeft(s, cutset string) string {
	for len(s) > 0 {
		found := false
		for _, c := range cutset {
			if rune(s[0]) == c {
				s = s[1:]
				found = true
				break
			}
		}
		if !found {
			break
		}
	}
	return s
}

// containsStr reports whether s contains substr.
func containsStr(s, substr string) bool {
	if len(substr) == 0 {
		return true
	}
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool { return &b }

// int64Ptr returns a pointer to an int64 value.
func int64Ptr(i int64) *int64 { return &i }
