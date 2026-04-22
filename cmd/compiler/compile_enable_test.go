// compile_enable_test.go tests the compiler enable subcommand.
// Verifies that compileEnableBundle produces the phased directory structure and that
// each phase carries the required content per conductor-schema.md §9 Step 3 and §15.
// All tests are fully offline.
// conductor-schema.md §9, §15, guardian-schema.md §6.
package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// readPhaseFile reads a file within a named phase subdirectory.
func readPhaseFile(t *testing.T, outDir, phase, filename string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(outDir, phase, filename))
	if err != nil {
		t.Fatalf("read %s/%s: %v", phase, filename, err)
	}
	return string(data)
}

// TestEnable_ProducesAllOutputFiles verifies that compileEnableBundle writes all six
// phase subdirectories, each containing a phase-meta.yaml. conductor-schema.md §9 Step 3.
func TestEnable_ProducesAllOutputFiles(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	phases := []struct {
		dir   string
		files []string
	}{
		{"00-infrastructure-dependencies", []string{
			"phase-meta.yaml",
			"prerequisites.yaml",
		}},
		{"00a-namespaces", []string{
			"phase-meta.yaml",
			"namespaces.yaml",
		}},
		{"01-guardian-bootstrap", []string{
			"phase-meta.yaml",
			"namespace-labels.yaml",
			"guardian-crds.yaml",
			"guardian-rbac.yaml",
			"guardian-rbacprofiles.yaml",
			"seam-memberships.yaml",
		}},
		{"02-guardian-deploy", []string{
			"phase-meta.yaml",
			"guardian-webhook-cert.yaml",
			"guardian-service.yaml",
			"guardian-deployment.yaml",
			"guardian-metrics-service.yaml",
			"guardian-rbac-webhook.yaml",
			"guardian-lineage-webhook.yaml",
		}},
		{"03-platform-wrapper", []string{
			"phase-meta.yaml",
			"platform-wrapper-crds.yaml",
			"platform-wrapper-rbac.yaml",
			"platform-wrapper-rbacprofiles.yaml",
			"platform-wrapper-deployments.yaml",
			"platform-wrapper-metrics-services.yaml",
		}},
		{"04-conductor", []string{
			"phase-meta.yaml",
			"conductor-crds.yaml",
			"conductor-rbac.yaml",
			"conductor-rbacprofile.yaml",
			"conductor-signing-key.yaml",
			"conductor-deployment.yaml",
			"conductor-metrics-service.yaml",
		}},
		{"05-post-bootstrap", []string{
			"phase-meta.yaml",
			"dsns-zone-configmap.yaml",
			"coredns-deployment-patch.yaml",
			"dsns-loadbalancer.yaml",
			"leaderelection.yaml",
		}},
	}

	for _, ph := range phases {
		for _, name := range ph.files {
			p := filepath.Join(outDir, ph.dir, name)
			if _, err := os.Stat(p); err != nil {
				t.Errorf("expected file %s/%s not found: %v", ph.dir, name, err)
			}
		}
	}
}

// TestEnable_ConductorDeploymentCarriesManagementRole verifies that the Conductor
// Deployment carries CONDUCTOR_ROLE and CLUSTER_REF via downward API annotation
// fieldRefs, not as static env values. conductor-schema.md §15.
func TestEnable_ConductorDeploymentCarriesManagementRole(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "v1.9.3-r1", defaultRegistry, "", false, "ccs-mgmt", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "04-conductor", "conductor-deployment.yaml")

	// CONDUCTOR_ROLE must be present as a downward API env var. §15.
	assertContainsStr(t, content, "CONDUCTOR_ROLE")
	// CLUSTER_REF must be present as a downward API env var.
	assertContainsStr(t, content, "CLUSTER_REF")
	// Both must use fieldRef (downward API), not static Value.
	assertContainsStr(t, content, "platform.ontai.dev/role")
	assertContainsStr(t, content, "platform.ontai.dev/cluster-ref")
	// The annotations must be stamped on the Deployment.
	assertContainsStr(t, content, "management")
	assertContainsStr(t, content, "ccs-mgmt")
	// --cluster-ref arg must not appear in the Deployment args.
	if strings.Contains(content, "--cluster-ref") {
		t.Error("conductor-deployment.yaml must not contain --cluster-ref arg; CLUSTER_REF is now injected via downward API")
	}
}

// TestEnable_Phase00aNamespacesContent verifies that 00a-namespaces/namespaces.yaml
// contains both canonical Seam namespaces with the required labels.
// CONTEXT.md §4 Namespace Model (locked 2026-04-05).
func TestEnable_Phase00aNamespacesContent(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00a-namespaces", "namespaces.yaml")

	// Both canonical namespaces must be present.
	assertContainsStr(t, content, "name: seam-system")
	assertContainsStr(t, content, "name: ont-system")

	// seam-system must carry the Guardian CheckBootstrapLabels gate label.
	assertContainsStr(t, content, "seam.ontai.dev/webhook-mode: exempt")

	// All namespaces must carry restricted PSA enforcement with warn mirroring enforce.
	assertContainsStr(t, content, "pod-security.kubernetes.io/enforce: restricted")
	assertContainsStr(t, content, "pod-security.kubernetes.io/warn: restricted")

	// ont-system must carry webhook-mode=governed (Guardian gates RBAC after bootstrap).
	assertContainsStr(t, content, "seam.ontai.dev/webhook-mode: governed")

	// seam-tenant namespaces must NOT be pre-created — they are Platform's domain.
	if containsStr(content, "seam-tenant") {
		t.Error("namespaces.yaml must not pre-create seam-tenant-* namespaces — Platform creates them")
	}

	// Phase-meta must declare the readiness gate.
	meta := readPhaseFile(t, outDir, "00a-namespaces", "phase-meta.yaml")
	assertContainsStr(t, meta, "namespaces")
	assertContainsStr(t, meta, "guardian-bootstrap")
}

// TestEnable_Phase00aLexicographicOrder verifies that 00a-namespaces directory
// sorts after 00-infrastructure-dependencies and before 00b-capi-prerequisites
// and 01-guardian-bootstrap — the pipeline applies phases in directory name order.
func TestEnable_Phase00aLexicographicOrder(t *testing.T) {
	// Verify that the directory naming convention produces the correct sort order.
	// "00-" < "00a" < "00b" < "01" lexicographically.
	dirs := []string{
		"00-infrastructure-dependencies",
		"00a-namespaces",
		"00b-capi-prerequisites",
		"01-guardian-bootstrap",
	}
	for i := 1; i < len(dirs); i++ {
		if dirs[i-1] >= dirs[i] {
			t.Errorf("phase directory %q must sort before %q for pipeline ordering",
				dirs[i-1], dirs[i])
		}
	}
}

// TestEnable_ConductorInOntSystem verifies that the Conductor Deployment is in
// ont-system and other operators are in seam-system. CONTEXT.md §4 Namespace Model.
func TestEnable_ConductorInOntSystem(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Conductor must be in ont-system.
	conductorDeploy := readPhaseFile(t, outDir, "04-conductor", "conductor-deployment.yaml")
	assertContainsStr(t, conductorDeploy, "namespace: ont-system")

	// Guardian and platform/wrapper operators must be in seam-system.
	guardianDeploy := readPhaseFile(t, outDir, "02-guardian-deploy", "guardian-deployment.yaml")
	assertContainsStr(t, guardianDeploy, "namespace: seam-system")

	pwDeploy := readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-deployments.yaml")
	assertContainsStr(t, pwDeploy, "namespace: seam-system")
}

// TestEnable_OperatorsYAMLContainsAllDeployments verifies that Deployments for all five
// Seam operators are present across the phase deployment files.
func TestEnable_OperatorsYAMLContainsAllDeployments(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect all deployment content across phases 2, 3, 4.
	content := readPhaseFile(t, outDir, "02-guardian-deploy", "guardian-deployment.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-deployments.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-deployment.yaml")

	assertContainsStr(t, content, "kind: Deployment")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "name: "+name) {
			t.Errorf("deployment files do not contain Deployment for %q", name)
		}
	}
}

// TestEnable_RBACYAMLContainsAllOperators verifies that ServiceAccounts exist for all
// operators and that ClusterRole/ClusterRoleBinding exist ONLY for Guardian.
//
// Non-guardian operators (platform, wrapper, seam-core, conductor) receive their RBAC
// exclusively via Guardian's RBACProfile provisioning mechanism — not via static
// ClusterRole/ClusterRoleBinding. Emitting those for non-guardian operators would
// bypass INV-004 (Guardian owns all RBAC). guardian-schema.md §6.
func TestEnable_RBACYAMLContainsAllOperators(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	guardianRBAC := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbac.yaml")
	platformRBAC := readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbac.yaml")
	conductorRBAC := readPhaseFile(t, outDir, "04-conductor", "conductor-rbac.yaml")

	allContent := guardianRBAC + platformRBAC + conductorRBAC

	// All files together must contain ServiceAccount resources.
	assertContainsStr(t, allContent, "kind: ServiceAccount")

	// Guardian must have a ClusterRole and ClusterRoleBinding (bootstrap window static RBAC).
	assertContainsStr(t, guardianRBAC, "kind: ClusterRole")
	assertContainsStr(t, guardianRBAC, "kind: ClusterRoleBinding")
	assertContainsStr(t, guardianRBAC, "guardian-manager-role")

	// Non-guardian operators must NOT have static ClusterRole/ClusterRoleBinding —
	// they are governed by Guardian's RBACProfile provisioning (INV-004).
	for _, name := range []string{"conductor", "platform", "wrapper", "seam-core"} {
		if strings.Contains(allContent, name+"-manager-role") {
			t.Errorf("RBAC files must not contain static ClusterRole for %q — use RBACProfile provisioning (INV-004)", name)
		}
	}

	// Non-guardian files must have ServiceAccounts but no ClusterRole/ClusterRoleBinding.
	for _, content := range []struct {
		name    string
		content string
	}{
		{"platform-wrapper-rbac.yaml", platformRBAC},
		{"conductor-rbac.yaml", conductorRBAC},
	} {
		assertContainsStr(t, content.content, "kind: ServiceAccount")
		if strings.Contains(content.content, "kind: ClusterRole") {
			t.Errorf("%s must not contain kind: ClusterRole — non-guardian RBAC is provisioned via RBACProfile", content.name)
		}
		if strings.Contains(content.content, "kind: ClusterRoleBinding") {
			t.Errorf("%s must not contain kind: ClusterRoleBinding — non-guardian RBAC is provisioned via RBACProfile", content.name)
		}
	}
}

// TestEnable_LeaderElectionYAMLContainsLeases verifies 05-post-bootstrap/leaderelection.yaml
// contains Lease resources for all operators.
func TestEnable_LeaderElectionYAMLContainsLeases(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "05-post-bootstrap", "leaderelection.yaml")

	assertContainsStr(t, content, "kind: Lease")
	// Conductor lease is in ont-system; others in seam-system.
	assertContainsStr(t, content, "conductor-management")
	assertContainsStr(t, content, "platform-leader")
	assertContainsStr(t, content, "guardian-leader")
}

// TestEnable_RBACProfilesYAMLContainsAllProfiles verifies that RBACProfile CRs for all
// five Seam operator service accounts are present across the phase RBACProfile files.
// guardian-schema.md §6 (Seam operator RBACProfiles).
func TestEnable_RBACProfilesYAMLContainsAllProfiles(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect RBACProfile content across phases 1, 3, 4.
	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-rbacprofile.yaml")

	assertContainsStr(t, content, "apiVersion: security.ontai.dev/v1alpha1")
	assertContainsStr(t, content, "kind: RBACProfile")
	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "rbac-"+name) {
			t.Errorf("RBACProfile files do not contain RBACProfile for %q", name)
		}
	}
}

// TestEnable_RBACProfilesDomainIdentityRef verifies that every RBACProfile CR
// emitted by compiler enable carries spec.domainIdentityRef set to the operator
// name. guardian-schema.md §7, CLAUDE.md §14 Decision 2.
func TestEnable_RBACProfilesDomainIdentityRef(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-rbacprofile.yaml")

	assertContainsStr(t, content, "domainIdentityRef:")

	for _, name := range []string{"conductor", "guardian", "platform", "wrapper", "seam-core"} {
		if !strings.Contains(content, "domainIdentityRef: "+name) {
			t.Errorf("expected domainIdentityRef: %q in RBACProfile output", name)
		}
	}
}

// TestEnable_SeamMembershipsContent verifies that seam-memberships.yaml in phase 01
// contains all five Seam operator SeamMembership CRs with the correct apiVersion,
// tier=infrastructure, and matching domainIdentityRef values.
// infrastructure.ontai.dev/v1alpha1, guardian-schema.md §7.
func TestEnable_SeamMembershipsContent(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "seam-memberships.yaml")

	assertContainsStr(t, content, "apiVersion: infrastructure.ontai.dev/v1alpha1")
	assertContainsStr(t, content, "kind: SeamMembership")
	assertContainsStr(t, content, "tier: infrastructure")

	for _, name := range []string{"guardian", "platform", "wrapper", "conductor", "seam-core"} {
		if !strings.Contains(content, "name: "+name) {
			t.Errorf("seam-memberships.yaml missing SeamMembership for %q", name)
		}
		if !strings.Contains(content, "domainIdentityRef: "+name) {
			t.Errorf("seam-memberships.yaml missing domainIdentityRef: %q", name)
		}
	}
}

// TestEnable_RBACProfilesCarryReviewAnnotation verifies that RBACProfile files
// include the human-review annotation. guardian-schema.md §6.
func TestEnable_RBACProfilesCarryReviewAnnotation(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-rbacprofile.yaml")

	assertContainsStr(t, content, "review-required")
}

// TestEnable_OutputIsDeterministic verifies that successive compileEnableBundle calls
// produce identical output for all phase files. conductor-design.md §1.2.
func TestEnable_OutputIsDeterministic(t *testing.T) {
	out1 := t.TempDir()
	out2 := t.TempDir()

	if err := compileEnableBundle(out1, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("first compileEnableBundle: %v", err)
	}
	if err := compileEnableBundle(out2, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("second compileEnableBundle: %v", err)
	}

	checks := []struct{ phase, file string }{
		{"00-infrastructure-dependencies", "phase-meta.yaml"},
		{"00-infrastructure-dependencies", "prerequisites.yaml"},
		{"00a-namespaces", "phase-meta.yaml"},
		{"00a-namespaces", "namespaces.yaml"},
		{"01-guardian-bootstrap", "phase-meta.yaml"},
		{"01-guardian-bootstrap", "namespace-labels.yaml"},
		{"01-guardian-bootstrap", "guardian-crds.yaml"},
		{"01-guardian-bootstrap", "guardian-rbac.yaml"},
		{"01-guardian-bootstrap", "guardian-rbacprofiles.yaml"},
		{"02-guardian-deploy", "phase-meta.yaml"},
		{"02-guardian-deploy", "guardian-webhook-cert.yaml"},
		{"02-guardian-deploy", "guardian-service.yaml"},
		{"02-guardian-deploy", "guardian-deployment.yaml"},
		{"02-guardian-deploy", "guardian-metrics-service.yaml"},
		{"02-guardian-deploy", "guardian-rbac-webhook.yaml"},
		{"02-guardian-deploy", "guardian-lineage-webhook.yaml"},
		{"03-platform-wrapper", "phase-meta.yaml"},
		{"03-platform-wrapper", "platform-wrapper-crds.yaml"},
		{"03-platform-wrapper", "platform-wrapper-rbac.yaml"},
		{"03-platform-wrapper", "platform-wrapper-rbacprofiles.yaml"},
		{"03-platform-wrapper", "platform-wrapper-deployments.yaml"},
		{"03-platform-wrapper", "platform-wrapper-metrics-services.yaml"},
		{"04-conductor", "phase-meta.yaml"},
		{"04-conductor", "conductor-crds.yaml"},
		{"04-conductor", "conductor-rbac.yaml"},
		{"04-conductor", "conductor-rbacprofile.yaml"},
		{"04-conductor", "conductor-deployment.yaml"},
		{"04-conductor", "conductor-metrics-service.yaml"},
		{"05-post-bootstrap", "phase-meta.yaml"},
		{"05-post-bootstrap", "dsns-zone-configmap.yaml"},
		{"05-post-bootstrap", "coredns-deployment-patch.yaml"},
		{"05-post-bootstrap", "dsns-loadbalancer.yaml"},
		{"05-post-bootstrap", "leaderelection.yaml"},
	}

	for _, c := range checks {
		d1, err1 := os.ReadFile(filepath.Join(out1, c.phase, c.file))
		d2, err2 := os.ReadFile(filepath.Join(out2, c.phase, c.file))
		if err1 != nil || err2 != nil {
			t.Errorf("read %s/%s: err1=%v err2=%v", c.phase, c.file, err1, err2)
			continue
		}
		if string(d1) != string(d2) {
			t.Errorf("%s/%s is not deterministic: successive calls produced different output", c.phase, c.file)
		}
	}
}

// TestEnable_VersionPropagatesIntoImages verifies that the --version flag value
// appears in operator image references across deployment files.
func TestEnable_VersionPropagatesIntoImages(t *testing.T) {
	outDir := t.TempDir()
	const version = "v1.9.3-r5"
	if err := compileEnableBundle(outDir, version, defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Version must appear in all three deployment phase files.
	for _, path := range []struct{ phase, file string }{
		{"02-guardian-deploy", "guardian-deployment.yaml"},
		{"03-platform-wrapper", "platform-wrapper-deployments.yaml"},
		{"04-conductor", "conductor-deployment.yaml"},
	} {
		content := readPhaseFile(t, outDir, path.phase, path.file)
		assertContainsStr(t, content, version)
	}
}

// TestEnable_CRDsYAMLIncludesAllOperatorCRDs verifies that all operator API groups
// are present across the phase CRD files. conductor-schema.md §9 Step 3.
func TestEnable_CRDsYAMLIncludesAllOperatorCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Collect all CRD content across phases 1, 3, 4.
	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "guardian-crds.yaml") +
		readPhaseFile(t, outDir, "03-platform-wrapper", "platform-wrapper-crds.yaml") +
		readPhaseFile(t, outDir, "04-conductor", "conductor-crds.yaml")

	for _, group := range []string{
		"platform.ontai.dev",
		"security.ontai.dev",
		"infra.ontai.dev",
		"infrastructure.ontai.dev",
		"runner.ontai.dev",
	} {
		if !strings.Contains(content, group) {
			t.Errorf("CRD files missing API group %q", group)
		}
	}
}

// --- WS2: namespace-labels.yaml tests ---

// TestEnable_NamespaceLabels_BothNamespacesPresent verifies that namespace-labels.yaml
// contains patches for both kube-system and seam-system.
// guardian 25c9e93 WS3 CheckBootstrapLabels contract.
func TestEnable_NamespaceLabels_BothNamespacesPresent(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "namespace-labels.yaml")

	assertContainsStr(t, content, "kube-system")
	assertContainsStr(t, content, "seam-system")
}

// TestEnable_NamespaceLabels_CorrectKindAndLabel verifies that namespace-labels.yaml
// carries kind: Namespace and the correct seam.ontai.dev/webhook-mode=exempt label.
func TestEnable_NamespaceLabels_CorrectKindAndLabel(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "namespace-labels.yaml")

	assertContainsStr(t, content, "kind: Namespace")
	assertContainsStr(t, content, "seam.ontai.dev/webhook-mode")
	assertContainsStr(t, content, "exempt")
}

// TestEnable_NamespaceLabels_IsSSAPatchOnly verifies that namespace-labels.yaml does NOT
// contain spec or status fields — it must be a server-side apply metadata-only patch,
// not a full Namespace manifest. INV-020, CS-INV-004.
func TestEnable_NamespaceLabels_IsSSAPatchOnly(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "namespace-labels.yaml")

	if strings.Contains(content, "spec:") {
		t.Error("namespace-labels.yaml contains spec: — must be SSA metadata-only patch")
	}
	if strings.Contains(content, "status:") {
		t.Error("namespace-labels.yaml contains status: — must be SSA metadata-only patch")
	}
}

// TestEnable_NamespaceLabels_PhaseMeta verifies that 01-guardian-bootstrap/phase-meta.yaml
// declares the correct phase name, order=1, and lists namespace-labels.yaml first
// in applyOrder.
func TestEnable_NamespaceLabels_PhaseMeta(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "01-guardian-bootstrap", "phase-meta.yaml")

	assertContainsStr(t, content, "phase: guardian-bootstrap")
	assertContainsStr(t, content, `order: "1"`)
	// namespace-labels.yaml must be the first entry in applyOrder.
	namespaceIdx := strings.Index(content, "namespace-labels.yaml")
	guardianCRDsIdx := strings.Index(content, "guardian-crds.yaml")
	if namespaceIdx < 0 {
		t.Error("phase-meta.yaml does not list namespace-labels.yaml in applyOrder")
	}
	if guardianCRDsIdx < 0 {
		t.Error("phase-meta.yaml does not list guardian-crds.yaml in applyOrder")
	}
	if namespaceIdx > 0 && guardianCRDsIdx > 0 && namespaceIdx > guardianCRDsIdx {
		t.Error("namespace-labels.yaml must appear before guardian-crds.yaml in applyOrder")
	}
}

// --- Phase 00: infrastructure-dependencies tests ---

// TestEnable_Phase00_DirectoryExistsAndIsFirst verifies that 00-infrastructure-dependencies
// exists and that its directory name sorts before 01-guardian-bootstrap.
// conductor-schema.md §9 phase 0.
func TestEnable_Phase00_DirectoryExistsAndIsFirst(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	// Directory must exist.
	info, err := os.Stat(filepath.Join(outDir, "00-infrastructure-dependencies"))
	if err != nil {
		t.Fatalf("00-infrastructure-dependencies directory not found: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("00-infrastructure-dependencies is not a directory")
	}

	// Lexicographic order: "00-" sorts before "01-".
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	if len(entries) == 0 {
		t.Fatal("output directory is empty")
	}
	if entries[0].Name() != "00-infrastructure-dependencies" {
		t.Errorf("first directory entry is %q, expected 00-infrastructure-dependencies", entries[0].Name())
	}
}

// TestEnable_Phase00_MetaOrderIsZero verifies that phase-meta.yaml in phase 00
// declares order: 0. conductor-schema.md §9 phase 0.
func TestEnable_Phase00_MetaOrderIsZero(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00-infrastructure-dependencies", "phase-meta.yaml")
	assertContainsStr(t, content, "phase: infrastructure-dependencies")
	assertContainsStr(t, content, `order: "0"`)
}

// TestEnable_Phase00_PrerequisitesIsConfigMap verifies that prerequisites.yaml in
// 00-infrastructure-dependencies is a ConfigMap named seam-platform-prerequisites
// in seam-system with the required label and all four prerequisite categories.
// conductor-schema.md §9.
func TestEnable_Phase00_PrerequisitesIsConfigMap(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00-infrastructure-dependencies", "prerequisites.yaml")
	assertContainsStr(t, content, "kind: ConfigMap")
	assertContainsStr(t, content, "name: seam-platform-prerequisites")
	assertContainsStr(t, content, "namespace: seam-system")
	assertContainsStr(t, content, "seam.ontai.dev/phase: prerequisites")
}

// TestEnable_Phase00_PrerequisitesFourCategories verifies that prerequisites.yaml
// lists all four required prerequisite categories in the ConfigMap data.
// conductor-schema.md §9.
func TestEnable_Phase00_PrerequisitesFourCategories(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00-infrastructure-dependencies", "prerequisites.yaml")
	for _, category := range []string{"database", "job-scheduler", "certificate-manager", "storage"} {
		if !strings.Contains(content, category) {
			t.Errorf("prerequisites.yaml missing category %q", category)
		}
	}
}

// TestEnable_Phase00_PrerequisitesDatabaseDetails verifies that the database
// prerequisite describes CNPG version, required CRD, guardian-db Cluster, and
// guardian-db-credentials Secret with required keys.
// conductor-schema.md §9.
func TestEnable_Phase00_PrerequisitesDatabaseDetails(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00-infrastructure-dependencies", "prerequisites.yaml")
	assertContainsStr(t, content, "CNPG v1.25")
	assertContainsStr(t, content, "clusters.postgresql.cnpg.io")
	assertContainsStr(t, content, "guardian-db")
	assertContainsStr(t, content, "guardian-db-credentials")
}

// TestEnable_Phase00_PrerequisitesApplyOrderListsPrerequisites verifies that
// phase-meta.yaml lists prerequisites.yaml in applyOrder.
// conductor-schema.md §9.
func TestEnable_Phase00_PrerequisitesApplyOrderListsPrerequisites(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	meta := readPhaseFile(t, outDir, "00-infrastructure-dependencies", "phase-meta.yaml")
	assertContainsStr(t, meta, "prerequisites.yaml")
}

// TestEnable_Phase02_GuardianDeploymentCarriesCNPGEnvVars verifies that
// guardian-deployment.yaml carries the CNPG connection env vars and GUARDIAN_ROLE.
// These are required for Guardian to connect to its database after CNPG creates
// the guardian-db-app Secret. guardian-schema.md §16 CNPG Deployment Contract.
func TestEnable_Phase02_GuardianDeploymentCarriesCNPGEnvVars(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "02-guardian-deploy", "guardian-deployment.yaml")
	assertContainsStr(t, content, "CNPG_SECRET_NAME")
	assertContainsStr(t, content, "guardian-db-app")
	assertContainsStr(t, content, "CNPG_SECRET_NAMESPACE")
	assertContainsStr(t, content, "seam-system")
	assertContainsStr(t, content, "GUARDIAN_ROLE")
	assertContainsStr(t, content, "management")
}

// --- Phase 05: post-bootstrap DSNS tests ---

// TestEnable_Phase05_DSNSZoneConfigMapLabelsAndAnnotations verifies that
// dsns-zone-configmap.yaml carries the required label and owner annotation.
// seam-core-schema.md §8 Decision 2.
func TestEnable_Phase05_DSNSZoneConfigMapLabelsAndAnnotations(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "05-post-bootstrap", "dsns-zone-configmap.yaml")

	assertContainsStr(t, content, "name: dsns-zone")
	// kube-system: CoreDNS pods mount this ConfigMap directly — must be co-located.
	assertContainsStr(t, content, "namespace: kube-system")
	assertContainsStr(t, content, "seam.ontai.dev/dsns-zone")
	assertContainsStr(t, content, "governance.infrastructure.ontai.dev/owner")
	assertContainsStr(t, content, "seam-core")
}

// TestEnable_Phase05_DSNSLoadBalancerTargetsPort53 verifies that
// dsns-loadbalancer.yaml is a LoadBalancer Service targeting port 53 UDP and TCP.
// seam-core-schema.md §8 Decision 3.
func TestEnable_Phase05_DSNSLoadBalancerTargetsPort53(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "05-post-bootstrap", "dsns-loadbalancer.yaml")

	assertContainsStr(t, content, "kind: Service")
	assertContainsStr(t, content, "type: LoadBalancer")
	// kube-system: Service selector must be in same namespace as CoreDNS pods.
	assertContainsStr(t, content, "namespace: kube-system")
	// Cilium IPAM annotation replaces deprecated spec.loadBalancerIP.
	assertContainsStr(t, content, "lbipam.cilium.io/ips")
	assertContainsStr(t, content, "port: 53")
	assertContainsStr(t, content, "UDP")
	assertContainsStr(t, content, "TCP")
}

// --- Phase 00b: capi-prerequisites tests ---

// TestEnable_CAPIPhase_AbsentWithoutFlag verifies that the 00b-capi-prerequisites
// directory is NOT emitted when --capi is not set.
// conductor-schema.md §9, platform-schema.md §3.
func TestEnable_CAPIPhase_AbsentWithoutFlag(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	capiDir := filepath.Join(outDir, "00b-capi-prerequisites")
	if _, err := os.Stat(capiDir); err == nil {
		t.Error("00b-capi-prerequisites directory must not be emitted when --capi is not set")
	}
}

// TestEnable_CAPIPhase_PresentWithFlag verifies that the 00b-capi-prerequisites
// directory IS emitted when --capi is set and contains all required files.
// conductor-schema.md §9, platform-schema.md §3.
func TestEnable_CAPIPhase_PresentWithFlag(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	capiDir := filepath.Join(outDir, "00b-capi-prerequisites")
	info, err := os.Stat(capiDir)
	if err != nil {
		t.Fatalf("00b-capi-prerequisites directory missing when --capi set: %v", err)
	}
	if !info.IsDir() {
		t.Fatal("00b-capi-prerequisites is not a directory")
	}

	for _, name := range []string{
		"phase-meta.yaml",
		"capi-core.yaml",
		"capi-talos-bootstrap.yaml",
		"capi-talos-controlplane.yaml",
		"seam-infrastructure-crds.yaml",
	} {
		p := filepath.Join(capiDir, name)
		if _, err := os.Stat(p); err != nil {
			t.Errorf("expected file 00b-capi-prerequisites/%s not found: %v", name, err)
		}
	}
}

// TestEnable_CAPIPhase_PhaseMetaOrder verifies that the 00b-capi-prerequisites phase-meta.yaml
// declares order 0b and sorts lexicographically between 00- and 01- phases.
func TestEnable_CAPIPhase_PhaseMetaOrder(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00b-capi-prerequisites", "phase-meta.yaml")
	assertContainsStr(t, content, "phase: capi-prerequisites")
	assertContainsStr(t, content, "order: 0b")

	// Lexicographic ordering: 00b- sorts after 00- and before 01-.
	entries, err := os.ReadDir(outDir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	var idx00, idx00b, idx01 int = -1, -1, -1
	for i, n := range names {
		switch {
		case n == "00-infrastructure-dependencies":
			idx00 = i
		case n == "00b-capi-prerequisites":
			idx00b = i
		case n == "01-guardian-bootstrap":
			idx01 = i
		}
	}
	if idx00b < 0 {
		t.Fatal("00b-capi-prerequisites not found in directory listing")
	}
	if idx00 >= 0 && idx00b <= idx00 {
		t.Errorf("00b-capi-prerequisites must sort after 00-infrastructure-dependencies")
	}
	if idx01 >= 0 && idx00b >= idx01 {
		t.Errorf("00b-capi-prerequisites must sort before 01-guardian-bootstrap")
	}
}

// TestEnable_CAPIPhase_CAPICoreContainsCRDs verifies that capi-core.yaml in the
// 00b phase contains the expected CAPI core CRDs. platform-schema.md §3.
func TestEnable_CAPIPhase_CAPICoreContainsCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00b-capi-prerequisites", "capi-core.yaml")

	assertContainsStr(t, content, "cluster.x-k8s.io")
	assertContainsStr(t, content, "kind: CustomResourceDefinition")
	assertContainsStr(t, content, "clusters.cluster.x-k8s.io")
	assertContainsStr(t, content, "machines.cluster.x-k8s.io")
	assertContainsStr(t, content, "kind: Deployment")
	assertContainsStr(t, content, "capi-system")
}

// TestEnable_CAPIPhase_TalosBootstrapContainsCRD verifies that capi-talos-bootstrap.yaml
// contains the TalosConfig CRD. platform-schema.md §3.
func TestEnable_CAPIPhase_TalosBootstrapContainsCRD(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00b-capi-prerequisites", "capi-talos-bootstrap.yaml")
	assertContainsStr(t, content, "bootstrap.cluster.x-k8s.io")
	assertContainsStr(t, content, "talosconfigs.bootstrap.cluster.x-k8s.io")
	assertContainsStr(t, content, "kind: Deployment")
}

// TestEnable_CAPIPhase_TalosControlPlaneContainsCRD verifies that capi-talos-controlplane.yaml
// contains the TalosControlPlane CRD. platform-schema.md §3.
func TestEnable_CAPIPhase_TalosControlPlaneContainsCRD(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00b-capi-prerequisites", "capi-talos-controlplane.yaml")
	assertContainsStr(t, content, "controlplane.cluster.x-k8s.io")
	assertContainsStr(t, content, "taloscontrolplanes.controlplane.cluster.x-k8s.io")
	assertContainsStr(t, content, "kind: Deployment")
}

// TestEnable_CAPIPhase_SeamInfrastructureCRDs verifies that seam-infrastructure-crds.yaml
// contains SeamInfrastructureCluster and SeamInfrastructureMachine CRDs.
// platform-schema.md §4 Seam Infrastructure Provider.
func TestEnable_CAPIPhase_SeamInfrastructureCRDs(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "00b-capi-prerequisites", "seam-infrastructure-crds.yaml")
	assertContainsStr(t, content, "infrastructure.cluster.x-k8s.io")
	assertContainsStr(t, content, "seaminfrastructureclusters.infrastructure.cluster.x-k8s.io")
	assertContainsStr(t, content, "seaminfrastructuremachines.infrastructure.cluster.x-k8s.io")
	assertContainsStr(t, content, "SeamInfrastructureCluster")
	assertContainsStr(t, content, "SeamInfrastructureMachine")
}

// TestEnable_CAPIPhase_OtherPhasesStillPresent verifies that enabling --capi does not
// remove any of the standard phases that are always present.
func TestEnable_CAPIPhase_OtherPhasesStillPresent(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", true, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	for _, phase := range []string{
		"00-infrastructure-dependencies",
		"01-guardian-bootstrap",
		"02-guardian-deploy",
		"03-platform-wrapper",
		"04-conductor",
		"05-post-bootstrap",
	} {
		if _, err := os.Stat(filepath.Join(outDir, phase)); err != nil {
			t.Errorf("standard phase %q missing when --capi set: %v", phase, err)
		}
	}
}

// TestEnable_DefaultRegistryInImageReferences verifies that deployment YAMLs
// reference the default local registry (10.20.0.1:5000/ontai-dev) when no
// --registry flag is provided.
func TestEnable_DefaultRegistryInImageReferences(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	for _, path := range []struct{ phase, file string }{
		{"02-guardian-deploy", "guardian-deployment.yaml"},
		{"03-platform-wrapper", "platform-wrapper-deployments.yaml"},
		{"04-conductor", "conductor-deployment.yaml"},
	} {
		content := readPhaseFile(t, outDir, path.phase, path.file)
		assertContainsStr(t, content, "10.20.0.1:5000/ontai-dev")
	}
}

// TestEnable_RegistryFlagOverride verifies that a custom registry prefix appears
// in deployment image references when passed to compileEnableBundle.
func TestEnable_RegistryFlagOverride(t *testing.T) {
	outDir := t.TempDir()
	const customRegistry = "registry.example.com/myproject"
	if err := compileEnableBundle(outDir, "dev", customRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	for _, path := range []struct{ phase, file string }{
		{"02-guardian-deploy", "guardian-deployment.yaml"},
		{"03-platform-wrapper", "platform-wrapper-deployments.yaml"},
		{"04-conductor", "conductor-deployment.yaml"},
	} {
		content := readPhaseFile(t, outDir, path.phase, path.file)
		assertContainsStr(t, content, customRegistry)
		if strings.Contains(content, "10.20.0.1:5000") {
			t.Errorf("%s/%s still references default registry after override", path.phase, path.file)
		}
	}
}

// TestEnable_Phase05_NoDSNSPatchScript verifies that phase 05-post-bootstrap does
// NOT emit coredns-dsns-patch.sh. The CoreDNS Corefile DSNS patch is handled
// automatically by enable-ccs-mgmt.sh Step 7a. Emitting a shell script violates
// INV-001 (no shell scripts anywhere). C-COREDNS-PATCH.
func TestEnable_Phase05_NoDSNSPatchScript(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	scriptPath := filepath.Join(outDir, "05-post-bootstrap", "coredns-dsns-patch.sh")
	if _, err := os.Stat(scriptPath); err == nil {
		t.Errorf("coredns-dsns-patch.sh must not be emitted by compiler enable; " +
			"INV-001 prohibits shell scripts; CoreDNS patch is handled by " +
			"enable-ccs-mgmt.sh Step 7a (C-COREDNS-PATCH)")
	}
}

// TestEnable_Phase05_MetaReferencesCI verifies that the phase 05 ReadinessGate
// references C-COREDNS-PATCH and does not contain the old MANUAL STEP REQUIRED
// language. C-COREDNS-PATCH.
func TestEnable_Phase05_MetaReferencesCI(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	meta := readPhaseFile(t, outDir, "05-post-bootstrap", "phase-meta.yaml")
	if strings.Contains(meta, "MANUAL STEP REQUIRED") {
		t.Errorf("phase 05 ReadinessGate must not say MANUAL STEP REQUIRED; " +
			"CoreDNS patch is now automated by enable-ccs-mgmt.sh (C-COREDNS-PATCH)")
	}
	if !strings.Contains(meta, "C-COREDNS-PATCH") {
		t.Errorf("phase 05 ReadinessGate must reference C-COREDNS-PATCH; got: %s", meta)
	}
}

// TestEnable_WrapperRunnerRole_ContainsPackOperationResultRule verifies that
// wrapper-runner.yaml in 05-post-bootstrap carries the infrastructure.ontai.dev
// packoperationresults rule so Conductor execute mode Jobs can write
// PackOperationResult CRs. WRAPPER-RUNNER-ROLE-PACKOPRESULT.
// conductor-schema.md §5, wrapper-schema.md §4.
func TestEnable_WrapperRunnerRole_ContainsPackOperationResultRule(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "test-cluster", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	content := readPhaseFile(t, outDir, "05-post-bootstrap", "wrapper-runner.yaml")

	assertContainsStr(t, content, "infrastructure.ontai.dev")
	assertContainsStr(t, content, "packoperationresults")

	// Verify the namespace is seam-tenant-{clusterName} not seam-system.
	assertContainsStr(t, content, "seam-tenant-test-cluster")
	if strings.Contains(content, "namespace: seam-system") {
		t.Error("wrapper-runner.yaml must use seam-tenant-{clusterName}, not seam-system")
	}
}

// TestEnable_Phase00_KueueWebhookScopingDocumented verifies that the phase 00
// prerequisites ConfigMap documents Kueue webhook scoping and references
// C-KUEUE-WEBHOOK. The scoping is applied immediately after Kueue is deployed
// in Phase 00 by enable-ccs-mgmt.sh, not as a deferred post-bootstrap step.
// C-KUEUE-WEBHOOK.
func TestEnable_Phase00_KueueWebhookScopingDocumented(t *testing.T) {
	outDir := t.TempDir()
	if err := compileEnableBundle(outDir, "dev", defaultRegistry, "", false, "", ""); err != nil {
		t.Fatalf("compileEnableBundle error: %v", err)
	}

	prereqs := readPhaseFile(t, outDir, "00-infrastructure-dependencies", "prerequisites.yaml")
	if !strings.Contains(prereqs, "C-KUEUE-WEBHOOK") {
		t.Errorf("prerequisites.yaml must reference C-KUEUE-WEBHOOK in the job-scheduler section; " +
			"the Kueue webhook scoping is applied immediately after Kueue deployment in Phase 00")
	}
}
