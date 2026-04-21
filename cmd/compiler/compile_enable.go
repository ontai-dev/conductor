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
//	    prerequisites.yaml      — ConfigMap listing what the operator must provision before phase 1
//	  00a-namespaces/
//	    phase-meta.yaml         — phase metadata and apply order
//	    namespaces.yaml         — seam-system (webhook-mode=exempt, privileged PSA) and ont-system (privileged PSA)
//	  00b-capi-prerequisites/   [emitted only when --capi flag is set]
//	    phase-meta.yaml         — phase metadata and apply order
//	    capi-core.yaml          — CAPI core operator (CRDs, RBAC, Deployment in capi-system)
//	    capi-talos-bootstrap.yaml    — Talos CAPI bootstrap provider
//	    capi-talos-controlplane.yaml — Talos CAPI control plane provider
//	    seam-infrastructure-crds.yaml — SeamInfrastructureCluster and SeamInfrastructureMachine CRDs
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
// guardian-schema.md §6 (Seam operator RBACProfiles).
// guardian 25c9e93 WS3: namespace-labels.yaml satisfies CheckBootstrapLabels contract.
package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/yaml"

	"github.com/ontai-dev/conductor/internal/catalog/capi"
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
	// Args are extra command-line arguments appended after the binary entrypoint.
	// For Conductor: ["agent"]. CLUSTER_REF injected via downward API annotation fieldRef. conductor-schema.md §15.
	Args []string
	// WebhookSecret is the name of the TLS Secret to mount as webhook certs at
	// /tmp/k8s-webhook-server/serving-certs. Non-empty for Guardian, Platform,
	// Wrapper, seam-core (all operators running an admission webhook server).
	WebhookSecret string
	// ConductorRegistry is the OCI registry prefix for Conductor executor Jobs.
	// Injected as CONDUCTOR_REGISTRY env var into the Platform Deployment so the
	// Platform operator can reference the correct Conductor image for Jobs it submits.
	// Non-empty for Platform only.
	ConductorRegistry string
	// DSNSServiceIP is the DSNS LoadBalancer IP injected as DSNS_SERVICE_IP into
	// the seam-core Deployment. Non-empty when --dsns-ip is provided to compiler enable.
	DSNSServiceIP string
	// ClusterRef is the cluster name for Conductor deployments. Stamped as the
	// platform.ontai.dev/cluster-ref annotation on the Deployment and injected as
	// the CLUSTER_REF env var via downward API fieldRef. Non-empty for Conductor only.
	ClusterRef string
	// Role is the operator role for Conductor deployments. Stamped as the
	// platform.ontai.dev/role annotation on the Deployment and injected as the
	// CONDUCTOR_ROLE env var via downward API fieldRef. Non-empty for Conductor only.
	Role string
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

// defaultRegistry is the default OCI registry prefix for all operator images
// produced by compiler enable. Points to the local lab registry. Override via
// the --registry flag to target a different registry (e.g., registry.ontai.dev/ontai-dev).
const defaultRegistry = "10.20.0.1:5000/ontai-dev"

// guardianOp returns the operatorSpec for the Guardian operator.
func guardianOp(version, registry string) operatorSpec {
	return operatorSpec{
		Name:                "guardian",
		Namespace:           "seam-system",
		Image:               registry + "/guardian:" + version,
		ServiceAccount:      "guardian",
		LeaderElectionLease: "guardian-leader",
		WebhookSecret:       "guardian-webhook-cert",
	}
}

// platformWrapperOps returns operatorSpecs for Platform, Wrapper, and seam-core.
// dsnsIP is the DSNS LoadBalancer IP injected into seam-core as DSNS_SERVICE_IP.
// Pass empty string when not providing a DSNS IP (e.g., in tests).
func platformWrapperOps(version, registry, dsnsIP string) []operatorSpec {
	return []operatorSpec{
		{
			Name:                "platform",
			Namespace:           "seam-system",
			Image:               registry + "/platform:" + version,
			ServiceAccount:      "platform",
			LeaderElectionLease: "platform-leader",
			WebhookSecret:       "platform-webhook-cert",
			ConductorRegistry:   registry,
		},
		{
			Name:                "wrapper",
			Namespace:           "seam-system",
			Image:               registry + "/wrapper:" + version,
			ServiceAccount:      "wrapper",
			LeaderElectionLease: "wrapper-leader",
			WebhookSecret:       "wrapper-webhook-cert",
		},
		{
			Name:                "seam-core",
			Namespace:           "seam-system",
			Image:               registry + "/seam-core:" + version,
			ServiceAccount:      "seam-core",
			LeaderElectionLease: "seam-core-leader",
			WebhookSecret:       "seam-core-webhook-cert",
			DSNSServiceIP:       dsnsIP,
		},
	}
}

// conductorOp returns the operatorSpec for the Conductor operator.
// clusterName is the cluster name stamped as platform.ontai.dev/cluster-ref annotation
// and injected as CLUSTER_REF env var via downward API. Pass empty string for
// tests that do not need a real cluster name.
func conductorOp(version, registry, clusterName string) operatorSpec {
	// Conductor lease name is conductor-{clusterName} when clusterName is known,
	// falling back to conductor-management for test/dry-run invocations without
	// a real cluster name. conductor-schema.md §15 Role Declaration Contract.
	leaderLease := "conductor-management"
	if clusterName != "" {
		leaderLease = "conductor-" + clusterName
	}
	op := operatorSpec{
		Name:                "conductor",
		Namespace:           "ont-system",
		Image:               registry + "/conductor:" + version,
		ServiceAccount:      "conductor",
		LeaderElectionLease: leaderLease,
		// Role is always management for compiler-enable-generated deployments.
		// Tenant cluster Conductor deployments are created by Platform.
		Role:      "management",
		ClusterRef: clusterName,
	}
	// Args carries the subcommand only. CLUSTER_REF is injected via downward API
	// annotation fieldRef so the same Deployment template works across clusters.
	// The --cluster-ref CLI flag remains as a development fallback (BuildAgentContext).
	op.Args = []string{"agent"}
	return op
}

// allOperators returns all operator specs in their original flat order (used
// for leader election leases in phase 5 which covers all operators).
func allOperators(version, registry, clusterName, dsnsIP string) []operatorSpec {
	result := []operatorSpec{conductorOp(version, registry, clusterName), guardianOp(version, registry)}
	result = append(result, platformWrapperOps(version, registry, dsnsIP)...)
	return result
}

const enableHelp = `Usage: compiler enable --output <path> [--version <tag>] [--registry <prefix>] [--capi] [--kubeconfig <path>]

Produce the phased deployment manifest bundle (conductor-schema.md §9 Steps 3–8).

Input contract:
  --version     Conductor/Compiler image tag (default: dev).
  --registry    OCI registry prefix for all operator images (default: 10.20.0.1:5000/ontai-dev).
                Images are constructed as {registry}/{operator}:{version}.
                Override for production: --registry registry.ontai.dev/ontai-dev
  --capi        Emit the 00b-capi-prerequisites phase containing CAPI core operator,
                Talos bootstrap provider, Talos control plane provider, and Seam
                Infrastructure CRDs. Required for clusters using the CAPI lifecycle
                path. platform-schema.md §3.
  --kubeconfig  Path to kubeconfig (flag → $KUBECONFIG → ~/.kube/config).
                Optional; reserved for compile-time readiness gate validation.

Output contract:
  --output  Directory receiving phase subdirectories:
              00-infrastructure-dependencies/  — CNPG operator and cluster
              00a-namespaces/                  — seam-system and ont-system Namespace objects
              00b-capi-prerequisites/          — CAPI providers (only when --capi set)
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
	registry := fs.String("registry", defaultRegistry, "OCI registry prefix for operator images (default: 10.20.0.1:5000/ontai-dev).")
	withCAPI := fs.Bool("capi", false, "Emit 00b-capi-prerequisites phase (CAPI core, Talos providers, Seam infra CRDs).")
	kubeconfig := fs.String("kubeconfig", "", "Path to kubeconfig for reading guardian-ca-secret at compile time. Optional — omit to emit with empty caBundle.")
	clusterName := fs.String("cluster-name", "", "Management cluster name stamped into the Conductor --cluster-ref arg. Required for production deployments.")
	dsnsIP := fs.String("dsns-ip", "", "DSNS LoadBalancer IP (lbipam.cilium.io/ips annotation on dsns-loadbalancer Service). Required for production deployments.")

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

	if err := compileEnableBundle(*output, *version, *registry, *kubeconfig, *withCAPI, *clusterName, *dsnsIP); err != nil {
		fmt.Fprintf(os.Stderr, "compiler enable: %v\n", err)
		os.Exit(1)
	}
}

// compileEnableBundle generates the phased management cluster deployment manifest
// bundle and writes it to the output directory.
//
// registry is the OCI registry prefix for all operator images (e.g.,
// "10.20.0.1:5000/ontai-dev"). Images are constructed as {registry}/{operator}:{version}.
// Pass defaultRegistry to use the default local lab registry.
//
// clusterName is the management cluster name stamped into the Conductor Deployment
// as --cluster-ref=<clusterName>. Pass empty string to omit the arg (used in tests).
//
// dsnsIP is the DSNS LoadBalancer IP set via lbipam.cilium.io/ips annotation on the
// DSNS LoadBalancer Service. It is also injected as DSNS_SERVICE_IP into the seam-core
// Deployment. Pass empty string to omit (used in tests without a live cluster).
//
// When withCAPI is true the 00b-capi-prerequisites phase is emitted between phase 0
// and phase 1. This phase is required for clusters using the CAPI lifecycle path.
// platform-schema.md §3 CAPI composition model.
// conductor-schema.md §9 Step 3, §15.
func compileEnableBundle(output, version, registry, kubeconfig string, withCAPI bool, clusterName, dsnsIP string) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	gdn := guardianOp(version, registry)
	pwOps := platformWrapperOps(version, registry, dsnsIP)
	cdt := conductorOp(version, registry, clusterName)

	if err := writePhase0InfrastructureDependencies(output); err != nil {
		return fmt.Errorf("phase 0 infrastructure-dependencies: %w", err)
	}
	if err := writePhase00aNamespaces(output, clusterName); err != nil {
		return fmt.Errorf("phase 00a namespaces: %w", err)
	}
	if withCAPI {
		if err := writePhase00bCAPIPrerequisites(output); err != nil {
			return fmt.Errorf("phase 00b capi-prerequisites: %w", err)
		}
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
	if err := writePhase5PostBootstrap(output, allOperators(version, registry, clusterName, dsnsIP), dsnsIP, clusterName); err != nil {
		return fmt.Errorf("phase 5 post-bootstrap: %w", err)
	}

	return nil
}

// --- Phase 0: infrastructure-dependencies ---

// writePhase0InfrastructureDependencies writes the 00-infrastructure-dependencies
// phase directory. This phase declares the prerequisites the operator must satisfy
// before the enable bundle pipeline may proceed to phase 1.
//
// The compiler does not provision these dependencies — that is the operator's
// responsibility. This phase produces a human-readable declaration of what must
// exist and be healthy before Guardian can start.
//
// conductor-schema.md §9.
func writePhase0InfrastructureDependencies(output string) error {
	dir := filepath.Join(output, "00-infrastructure-dependencies")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{"prerequisites.yaml"}

	meta := phaseMeta{
		Phase: "infrastructure-dependencies",
		Order: 0,
		ReadinessGate: "All prerequisites listed in prerequisites.yaml must be satisfied " +
			"by the operator before applying phase 1 (01-guardian-bootstrap). " +
			"Verify: CNPG operator running in cnpg-system, guardian-db Cluster Ready " +
			"in seam-system, guardian-db-credentials Secret present in seam-system, " +
			"Kueue ClusterQueue and LocalQueue CRDs registered, default StorageClass present.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	return writePrerequisitesConfigMap(dir)
}

// writePrerequisitesConfigMap writes prerequisites.yaml — a ConfigMap in seam-system
// named seam-platform-prerequisites that documents what the operator must provision
// before phase 1 (01-guardian-bootstrap) may be applied.
//
// The ConfigMap is informational: the pipeline applies it as a record of operator
// intent and verifies it exists before advancing. It does not create the listed
// resources — that is the operator's responsibility.
//
// conductor-schema.md §9.
func writePrerequisitesConfigMap(dir string) error {
	cm := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata": map[string]interface{}{
			"name":      "seam-platform-prerequisites",
			"namespace": "seam-system",
			"labels": map[string]string{
				"seam.ontai.dev/phase": "prerequisites",
			},
			"annotations": map[string]string{
				"seam.ontai.dev/apply-before": "01-guardian-bootstrap",
				"seam.ontai.dev/description":  "Prerequisites that must be satisfied by the operator before applying phase 01-guardian-bootstrap.",
			},
		},
		"data": map[string]string{
			"database": "CNPG v1.25 or later. " +
				"CRD clusters.postgresql.cnpg.io must exist. " +
				"One Cluster named guardian-db in seam-system must be Ready. " +
				"A Secret named guardian-db-credentials must exist in seam-system " +
				"with keys username and password.",
			"job-scheduler": "Kueue v0.16.2 or later. " +
				"ClusterQueue and LocalQueue CRDs must exist and be registered. " +
				"Kueue mutating webhook is scoped to ont-managed namespaces immediately " +
				"after kueue-controller.yaml is applied in Phase 00 by enable-ccs-mgmt.sh. " +
				"C-KUEUE-WEBHOOK.",
			"certificate-manager": "cert-manager v1.13 or later if webhook TLS is managed externally. " +
				"Optional if self-signed certificates are used.",
			"storage": "A default StorageClass must exist for CNPG PVCs.",
		},
	}

	data, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal prerequisites ConfigMap: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Platform prerequisites declaration — seam-platform-prerequisites ConfigMap\n")
	buf.WriteString("# Generated by: compiler enable (phase 0 infrastructure-dependencies)\n")
	buf.WriteString("# The operator must provision all listed prerequisites before applying\n")
	buf.WriteString("# phase 01-guardian-bootstrap. The compiler does not create these resources.\n")
	buf.WriteString("# conductor-schema.md §9.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "prerequisites.yaml"), buf.Bytes(), 0644)
}

// --- Phase 00a: namespaces ---

// writePhase00aNamespaces writes the 00a-namespaces phase directory.
// This phase creates the two canonical Seam namespaces before any operator is
// deployed. Namespaces must exist before guardian-bootstrap can apply
// namespace-labels.yaml. CONTEXT.md §4 Namespace Model (locked 2026-04-05).
//
// Namespaces created:
//   - seam-system: operator namespace. Labels: seam.ontai.dev/webhook-mode=exempt
//     (Guardian CheckBootstrapLabels gate) and pod-security.kubernetes.io/enforce=privileged.
//   - ont-system: Conductor agent namespace. Label: pod-security.kubernetes.io/enforce=privileged.
//
// seam-tenant-{cluster-name} namespaces are NOT pre-created here — they are
// created by the Platform operator at target cluster formation time (CP-INV-004).
//
// Lexicographic ordering: "00a-" sorts after "00-" and before "00b-" and "01-"
// — the pipeline applies phases in directory name order.
//
// conductor-schema.md §9, CONTEXT.md §4.
func writePhase00aNamespaces(output, clusterName string) error {
	dir := filepath.Join(output, "00a-namespaces")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	if err := writeNamespacesPhaseMetaYAML(dir); err != nil {
		return err
	}
	return writeNamespacesManifest(dir, clusterName)
}

// writeNamespacesPhaseMetaYAML writes the phase-meta.yaml for the 00a-namespaces phase
// as a Kubernetes ConfigMap. The order field is the string "0a" because the namespaces
// phase sorts between 00 and 01 using lexicographic directory ordering, not an integer.
func writeNamespacesPhaseMetaYAML(dir string) error {
	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "seam-phase-namespaces-meta",
			Namespace: "seam-system",
			Labels: map[string]string{
				"seam.ontai.dev/phase":       "namespaces",
				"seam.ontai.dev/phase-order": "0a",
			},
		},
		Data: map[string]string{
			"phase": "namespaces",
			"order": "0a",
			"readinessGate": "Verify that seam-system and ont-system namespaces exist before applying " +
				"phase 01-guardian-bootstrap. guardian-bootstrap's namespace-labels.yaml " +
				"patches labels onto seam-system — the namespace must pre-exist for the " +
				"patch to succeed. kubectl get ns seam-system ont-system",
			"applyOrder": "namespaces.yaml",
		},
	}
	data, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal phase-meta for namespaces: %w", err)
	}
	var buf bytes.Buffer
	buf.WriteString("# Phase metadata — do not edit manually.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "phase-meta.yaml"), buf.Bytes(), 0644)
}

// writeNamespacesManifest writes namespaces.yaml containing the two canonical
// Seam namespace objects plus, when clusterName is non-empty, the seam-tenant-
// {clusterName} namespace required by the pack delivery chain (wrapper-schema.md §9).
// The seam-tenant namespace carries managed-by=platform, cluster={clusterName}, and
// webhook-mode=governed labels.
func writeNamespacesManifest(dir, clusterName string) error {
	seam := corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "seam-system",
			Labels: map[string]string{
				// Guardian CheckBootstrapLabels startup gate: seam-system must carry
				// webhook-mode=exempt before Guardian's admission webhook registers.
				// guardian-schema.md §4 Bootstrap RBAC Window, guardian 25c9e93 WS3.
				"seam.ontai.dev/webhook-mode": "exempt",
				// Restricted PSA: operator containers must not require privileged escalation.
				// warn mirrors enforce so violations surface before rollout.
				"pod-security.kubernetes.io/enforce": "restricted",
				"pod-security.kubernetes.io/warn":    "restricted",
			},
		},
	}
	ont := corev1.Namespace{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "ont-system",
			Labels: map[string]string{
				// Restricted PSA. warn mirrors enforce.
				"pod-security.kubernetes.io/enforce": "restricted",
				"pod-security.kubernetes.io/warn":    "restricted",
				// webhook-mode=governed: Guardian admission webhook gates RBAC resources
				// in ont-system after the bootstrap window closes.
				"seam.ontai.dev/webhook-mode": "governed",
			},
		},
	}

	seamData, err := yaml.Marshal(seam)
	if err != nil {
		return fmt.Errorf("marshal seam-system Namespace: %w", err)
	}
	ontData, err := yaml.Marshal(ont)
	if err != nil {
		return fmt.Errorf("marshal ont-system Namespace: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Canonical Seam namespaces — 00a-namespaces phase\n")
	buf.WriteString("# Generated by: compiler enable (phase 00a namespaces)\n")
	buf.WriteString("# conductor-schema.md §9, CONTEXT.md §4 Namespace Model (locked 2026-04-05).\n")
	buf.WriteString("#\n")
	buf.WriteString("# seam-system: operator namespace for all Seam operator managers.\n")
	buf.WriteString("#   webhook-mode=exempt: Guardian CheckBootstrapLabels startup gate.\n")
	buf.WriteString("#   restricted PSA: operators must not require privilege escalation.\n")
	buf.WriteString("#\n")
	buf.WriteString("# ont-system: Conductor agent namespace (management and target clusters).\n")
	buf.WriteString("#   restricted PSA, webhook-mode=governed.\n")
	buf.WriteString("#\n")
	if clusterName != "" {
		buf.WriteString("# seam-tenant-" + clusterName + ": pack delivery namespace for the management cluster.\n")
		buf.WriteString("#   managed-by=platform, webhook-mode=governed. wrapper-schema.md §9.\n")
		buf.WriteString("#\n")
	}
	buf.WriteString("---\n")
	buf.Write(seamData)
	buf.WriteString("---\n")
	buf.Write(ontData)

	if clusterName != "" {
		// seam-tenant-{clusterName}: pack delivery namespace created by compiler enable
		// for the management cluster itself. Per-target-cluster tenant namespaces are
		// created by the Platform operator at formation time (CP-INV-004).
		// wrapper-schema.md §9, CONTEXT.md §4 Namespace Model.
		tenant := corev1.Namespace{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Namespace"},
			ObjectMeta: metav1.ObjectMeta{
				Name: "seam-tenant-" + clusterName,
				Labels: map[string]string{
					// Platform owns tenant namespaces — this is the management cluster's own
					// tenant namespace for pack delivery. Labeled for Platform inspection.
					"ontai.dev/managed-by":       "platform",
					"platform.ontai.dev/cluster": clusterName,
					// webhook-mode=governed: Guardian admission webhook gates RBAC resources
					// in this namespace after the bootstrap window closes.
					"seam.ontai.dev/webhook-mode": "governed",
					// Restricted PSA. warn mirrors enforce.
					"pod-security.kubernetes.io/enforce": "restricted",
					"pod-security.kubernetes.io/warn":    "restricted",
				},
			},
		}
		tenantData, err := yaml.Marshal(tenant)
		if err != nil {
			return fmt.Errorf("marshal seam-tenant-%s Namespace: %w", clusterName, err)
		}
		buf.WriteString("---\n")
		buf.Write(tenantData)
	}

	return os.WriteFile(filepath.Join(dir, "namespaces.yaml"), buf.Bytes(), 0644)
}

// --- Phase 00b: capi-prerequisites ---

// writePhase00bCAPIPrerequisites writes the 00b-capi-prerequisites phase directory.
// This phase provisions the CAPI core operator, Talos bootstrap provider, Talos control
// plane provider, and Seam Infrastructure CRDs. It is emitted only when the --capi flag
// is set, and must be applied before phase 1 (guardian-bootstrap) so that CAPI controllers
// are running before the Platform operator starts.
//
// Lexicographic ordering: "00b-" sorts after "00-" and before "01-" — the pipeline
// applies phases in directory name order.
//
// platform-schema.md §3 CAPI composition model, conductor-schema.md §9.
func writePhase00bCAPIPrerequisites(output string) error {
	dir := filepath.Join(output, "00b-capi-prerequisites")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{
		"capi-core.yaml",
		"capi-talos-bootstrap.yaml",
		"capi-talos-controlplane.yaml",
		"seam-infrastructure-crds.yaml",
	}

	// Write phase-meta.yaml directly — the order is the string "0b" to represent
	// the fractional ordering between phase 0 and phase 1. This cannot be expressed
	// as an integer so phase-meta.yaml is written as a raw template.
	if err := writeCAPIPhaseMetaYAML(dir, files); err != nil {
		return err
	}

	// capi-core.yaml — CAPI core operator (Namespace, CRDs, RBAC, Deployment).
	var coreBuf bytes.Buffer
	coreBuf.WriteString("# CAPI Core Operator — pinned to " + capi.CAPIVersion + "\n")
	coreBuf.WriteString("# Generated by: compiler enable (phase 00b capi-prerequisites)\n")
	coreBuf.WriteString("# Embedded at compile time from internal/catalog/capi/capi-core.yaml.\n")
	coreBuf.WriteString("# platform-schema.md §3 CAPI composition model.\n")
	coreBuf.Write(capi.CoreManifest)
	if err := os.WriteFile(filepath.Join(dir, "capi-core.yaml"), coreBuf.Bytes(), 0644); err != nil {
		return err
	}

	// capi-talos-bootstrap.yaml — Talos CAPI bootstrap provider.
	var bootstrapBuf bytes.Buffer
	bootstrapBuf.WriteString("# Talos CAPI Bootstrap Provider — pinned to " + capi.TalosBootstrapVersion + "\n")
	bootstrapBuf.WriteString("# Generated by: compiler enable (phase 00b capi-prerequisites)\n")
	bootstrapBuf.WriteString("# Embedded at compile time from internal/catalog/capi/capi-talos-bootstrap.yaml.\n")
	bootstrapBuf.Write(capi.TalosBootstrapManifest)
	if err := os.WriteFile(filepath.Join(dir, "capi-talos-bootstrap.yaml"), bootstrapBuf.Bytes(), 0644); err != nil {
		return err
	}

	// capi-talos-controlplane.yaml — Talos CAPI control plane provider.
	var cpBuf bytes.Buffer
	cpBuf.WriteString("# Talos CAPI Control Plane Provider — pinned to " + capi.TalosControlPlaneVersion + "\n")
	cpBuf.WriteString("# Generated by: compiler enable (phase 00b capi-prerequisites)\n")
	cpBuf.WriteString("# Embedded at compile time from internal/catalog/capi/capi-talos-controlplane.yaml.\n")
	cpBuf.Write(capi.TalosControlPlaneManifest)
	if err := os.WriteFile(filepath.Join(dir, "capi-talos-controlplane.yaml"), cpBuf.Bytes(), 0644); err != nil {
		return err
	}

	// seam-infrastructure-crds.yaml — SeamInfrastructureCluster and SeamInfrastructureMachine CRDs.
	var infraBuf bytes.Buffer
	infraBuf.WriteString("# Seam Infrastructure Provider CRDs — from platform/config/crd/\n")
	infraBuf.WriteString("# Generated by: compiler enable (phase 00b capi-prerequisites)\n")
	infraBuf.WriteString("# Embedded at compile time from internal/catalog/capi/seam-infrastructure-crds.yaml.\n")
	infraBuf.Write(capi.SeamInfrastructureCRDs)
	if err := os.WriteFile(filepath.Join(dir, "seam-infrastructure-crds.yaml"), infraBuf.Bytes(), 0644); err != nil {
		return err
	}

	return nil
}

// writeCAPIPhaseMetaYAML writes the phase-meta.yaml for the 00b-capi-prerequisites phase
// as a Kubernetes ConfigMap. The order field is the string "0b" because this phase sorts
// between 00a and 01 using lexicographic directory ordering, not an integer.
func writeCAPIPhaseMetaYAML(dir string, applyOrder []string) error {
	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "seam-phase-capi-prerequisites-meta",
			Namespace: "seam-system",
			Labels: map[string]string{
				"seam.ontai.dev/phase":       "capi-prerequisites",
				"seam.ontai.dev/phase-order": "0b",
			},
		},
		Data: map[string]string{
			"phase": "capi-prerequisites",
			"order": "0b",
			"readinessGate": "Wait for all CAPI controller Deployments in capi-system to be Available " +
				"before applying phase 1 (guardian-bootstrap). CAPI controllers must be " +
				"running before the Platform operator starts so that SeamInfrastructureCluster " +
				"and SeamInfrastructureMachine CRDs are registered and CAPI reconcilers can " +
				"process Cluster and Machine objects created by TalosClusterReconciler. " +
				"Check: kubectl get deploy -n capi-system. All must be Available. " +
				"platform-schema.md §3 CAPI composition model.",
			"applyOrder": strings.Join(applyOrder, ","),
		},
	}
	data, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal phase-meta for capi-prerequisites: %w", err)
	}
	var buf bytes.Buffer
	buf.WriteString("# Phase metadata — do not edit manually.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "phase-meta.yaml"), buf.Bytes(), 0644)
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
		"guardian-permissionsets.yaml",
		"guardian-rbacpolicy.yaml",
		"guardian-rbacprofiles.yaml",
		"seam-memberships.yaml",
	}

	meta := phaseMeta{
		Phase: "guardian-bootstrap",
		Order: 1,
		ReadinessGate: "Verify that seam-system and kube-system namespaces carry " +
			"seam.ontai.dev/webhook-mode=exempt before applying phase 2. " +
			"Guardian CRDs must be registered (kubectl get crd | grep security.ontai.dev). " +
			"Guardian RBAC must be present. PermissionSets and RBACPolicy must be in the cluster. " +
			"Guardian RBACProfile must be in the cluster.",
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

	// guardian-permissionsets.yaml — Bootstrap PermissionSet CRs (Gap 7).
	// Includes one PermissionSet per operator plus seam-bootstrap-ceiling (the RBACPolicy ceiling).
	if err := writeBootstrapPermissionSets(dir); err != nil {
		return err
	}

	// guardian-rbacpolicy.yaml — Bootstrap RBACPolicy CR (Gap 6).
	// The seam-platform-rbac-policy governs all operator RBACProfiles during the bootstrap window.
	if err := writeBootstrapRBACPolicy(dir); err != nil {
		return err
	}

	// guardian-rbacprofiles.yaml — Guardian RBACProfile CR.
	if err := writeOperatorRBACProfilesFile(dir, "guardian-rbacprofiles.yaml", []operatorSpec{gdn}); err != nil {
		return err
	}

	// seam-memberships.yaml — SeamMembership CRs for all Seam operators.
	// Applied after RBACProfiles are present so Guardian can validate them
	// immediately on startup. infrastructure.ontai.dev/v1alpha1.
	if err := writeSeamMembershipsFile(dir); err != nil {
		return err
	}

	return nil
}

// writeBootstrapRBACPolicy writes guardian-rbacpolicy.yaml to dir.
// Emits the seam-platform-rbac-policy RBACPolicy CR that governs all operator
// RBACProfiles during the management cluster bootstrap window.
// guardian-schema.md §6.
func writeBootstrapRBACPolicy(dir string) error {
	policy := map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACPolicy",
		"metadata": map[string]interface{}{
			"name":      "seam-platform-rbac-policy",
			"namespace": "seam-system",
			"labels": map[string]string{
				"ontai.dev/managed-by": "compiler",
			},
		},
		"spec": map[string]interface{}{
			// audit mode: Guardian records violations but does not block during bootstrap.
			"enforcementMode": "audit",
			// platform scope: applies to Seam platform operator service accounts.
			"subjectScope": "platform",
			// The ceiling PermissionSet — maximum permissions any RBACProfile under this
			// policy may declare. Full access during bootstrap; tightened post-bootstrap.
			"maximumPermissionSetRef": "seam-bootstrap-ceiling",
			// Bound to the management cluster only.
			"allowedClusters": []string{"management"},
		},
	}

	data, err := yaml.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal bootstrap RBACPolicy: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Bootstrap RBACPolicy — seam-platform-rbac-policy\n")
	buf.WriteString("# Generated by: compiler enable (phase 1 guardian-bootstrap)\n")
	buf.WriteString("# Governs all operator RBACProfiles during the management cluster bootstrap window.\n")
	buf.WriteString("# enforcementMode=audit: Guardian records violations but does not block during bootstrap.\n")
	buf.WriteString("# Tighten maximumPermissionSetRef and enforcementMode post-bootstrap.\n")
	buf.WriteString("# guardian-schema.md §6.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "guardian-rbacpolicy.yaml"), buf.Bytes(), 0644)
}

// writeBootstrapPermissionSets writes guardian-permissionsets.yaml to dir.
// Emits one PermissionSet per Seam operator plus seam-bootstrap-ceiling (the RBACPolicy ceiling).
// All sets grant full access during the bootstrap window; these should be tightened post-bootstrap.
// guardian-schema.md §6.
func writeBootstrapPermissionSets(dir string) error {
	// bootstrapPermissions grants full access to all resources during the bootstrap window.
	// Must be tightened post-bootstrap to least-privilege.
	bootstrapPermissions := []map[string]interface{}{
		{
			"apiGroups": []string{"*"},
			"resources": []string{"*"},
			"verbs":     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
		},
	}

	names := []string{
		"seam-bootstrap-ceiling", // RBACPolicy ceiling reference — must be first
		"guardian-permissions",
		"platform-permissions",
		"wrapper-permissions",
		"seam-core-permissions",
		"conductor-permissions",
	}

	var buf bytes.Buffer
	buf.WriteString("# Bootstrap PermissionSet CRs\n")
	buf.WriteString("# Generated by: compiler enable (phase 1 guardian-bootstrap)\n")
	buf.WriteString("# One PermissionSet per Seam operator plus seam-bootstrap-ceiling (the RBACPolicy ceiling).\n")
	buf.WriteString("# All sets grant full access during the bootstrap window.\n")
	buf.WriteString("# Tighten permissions post-bootstrap to least-privilege. guardian-schema.md §6.\n")

	for _, name := range names {
		ps := map[string]interface{}{
			"apiVersion": "security.ontai.dev/v1alpha1",
			"kind":       "PermissionSet",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": "seam-system",
				"labels": map[string]string{
					"ontai.dev/managed-by":          "compiler",
					"ontai.dev/permission-set-type": "bootstrap",
				},
			},
			"spec": map[string]interface{}{
				"permissions": bootstrapPermissions,
			},
		}
		data, err := yaml.Marshal(ps)
		if err != nil {
			return fmt.Errorf("marshal PermissionSet %s: %w", name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(dir, "guardian-permissionsets.yaml"), buf.Bytes(), 0644)
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
// with the exempt label, in deterministic order. This list must include every
// namespace that hosts Seam system workloads so that Guardian's bootstrap sweep
// and admission webhook do not attempt to annotate or intercept resources in them.
// guardian-schema.md §4, INV-020, CS-INV-004.
func namespacesSortedExempt() []string {
	// Sorted alphabetically for deterministic output.
	return []string{
		"cert-manager",
		"cnpg-system",
		"kube-system",
		"kueue-system",
		"local-path-storage",
		"seam-system",
	}
}

// namespaceLabelPatch returns a server-side apply patch for a Namespace object.
// The patch carries only the fields required for the label operation: apiVersion,
// kind, metadata.name, and metadata.labels.
// No annotations block is emitted — SSA patches own only what they declare and
// the kubectl.kubernetes.io/last-applied-configuration annotation conflicts with
// the server-side apply field manager.
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

	files := []string{
		"guardian-webhook-cert.yaml",
		"guardian-service.yaml",
		"guardian-deployment.yaml",
		"guardian-metrics-service.yaml",
		"guardian-rbac-webhook.yaml",
		"guardian-lineage-webhook.yaml",
	}

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

	// guardian-webhook-cert.yaml — cert-manager Certificate CR for the admission webhook TLS.
	// Must be applied before the Service and Deployment so the Secret exists when Guardian starts.
	if err := writeGuardianWebhookCert(dir); err != nil {
		return err
	}

	// guardian-service.yaml — multi-port Service for Guardian webhook, gRPC, and metrics.
	if err := writeGuardianService(dir, gdn.Namespace); err != nil {
		return err
	}

	if err := writeDeploymentFile(dir, "guardian-deployment.yaml", gdn, "# Guardian Deployment\n# Generated by: compiler enable (phase 2 guardian-deploy)\n"); err != nil {
		return err
	}

	// guardian-metrics-service.yaml — Prometheus metrics Service for Guardian.
	if err := writeMetricsServiceFile(dir, "guardian-metrics-service.yaml", gdn.Name, gdn.Namespace); err != nil {
		return err
	}

	// guardian-rbac-webhook.yaml — ValidatingWebhookConfiguration for RBAC resources.
	if err := writeGuardianRBACWebhook(dir); err != nil {
		return err
	}

	// guardian-lineage-webhook.yaml — ValidatingWebhookConfiguration for lineage immutability.
	if err := writeGuardianLineageWebhook(dir); err != nil {
		return err
	}

	return nil
}

// writeGuardianWebhookCert writes guardian-webhook-cert.yaml to dir.
// Emits a cert-manager Certificate CR that provisions the TLS Secret mounted
// by the Guardian Deployment for its admission webhook server.
// The Certificate is signed by guardian-ca-issuer, a namespaced Issuer in seam-system.
// guardian-schema.md §3.
func writeGuardianWebhookCert(dir string) error {
	cert := map[string]interface{}{
		"apiVersion": "cert-manager.io/v1",
		"kind":       "Certificate",
		"metadata": map[string]interface{}{
			"name":      "guardian-webhook-cert",
			"namespace": "seam-system",
			"labels": map[string]string{
				"app.kubernetes.io/name":      "guardian",
				"app.kubernetes.io/component": "webhook",
				"ontai.dev/managed-by":        "compiler",
			},
		},
		"spec": map[string]interface{}{
			// The resulting Secret name must match the volume secretName in the Deployment.
			"secretName": "guardian-webhook-cert",
			"issuerRef": map[string]interface{}{
				"name": "guardian-ca-issuer",
				"kind": "Issuer",
			},
			"dnsNames": []string{
				"guardian.seam-system.svc",
				"guardian.seam-system.svc.cluster.local",
			},
		},
	}

	data, err := yaml.Marshal(cert)
	if err != nil {
		return fmt.Errorf("marshal guardian webhook Certificate: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Guardian Webhook TLS Certificate\n")
	buf.WriteString("# Generated by: compiler enable (phase 2 guardian-deploy)\n")
	buf.WriteString("# cert-manager Certificate CR — signed by guardian-ca-issuer (namespaced Issuer in seam-system).\n")
	buf.WriteString("# Provisions guardian-webhook-cert Secret mounted at\n")
	buf.WriteString("# /tmp/k8s-webhook-server/serving-certs in the Guardian Deployment.\n")
	buf.WriteString("# Prerequisite: cert-manager must be running and guardian-ca-issuer must exist.\n")
	buf.WriteString("# guardian-schema.md §3.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "guardian-webhook-cert.yaml"), buf.Bytes(), 0644)
}

// writeGuardianService writes guardian-service.yaml to dir.
// Emits a multi-port Service for Guardian: webhook (443→9443), gRPC (9090→9090),
// and metrics (8080→8080). Selects pods labelled app.kubernetes.io/name=guardian.
// guardian-schema.md §3.
func writeGuardianService(dir, namespace string) error {
	svc := map[string]interface{}{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata": map[string]interface{}{
			"name":      "guardian",
			"namespace": namespace,
			"labels": map[string]string{
				"app.kubernetes.io/name":      "guardian",
				"app.kubernetes.io/component": "webhook",
				"ontai.dev/managed-by":        "compiler",
			},
			"annotations": map[string]string{
				"ontai.dev/managed-by": "compiler",
			},
		},
		"spec": map[string]interface{}{
			"selector": map[string]string{
				"app.kubernetes.io/name": "guardian",
			},
			"ports": []map[string]interface{}{
				{
					"name":       "webhook",
					"port":       443,
					"targetPort": 9443,
					"protocol":   "TCP",
				},
				{
					"name":       "grpc",
					"port":       9090,
					"targetPort": 9090,
					"protocol":   "TCP",
				},
				{
					"name":       "metrics",
					"port":       8080,
					"targetPort": 8080,
					"protocol":   "TCP",
				},
			},
		},
	}

	data, err := yaml.Marshal(svc)
	if err != nil {
		return fmt.Errorf("marshal guardian Service: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Guardian Service\n")
	buf.WriteString("# Generated by: compiler enable (phase 2 guardian-deploy)\n")
	buf.WriteString("# Multi-port Service: webhook (443→9443), gRPC (9090→9090), metrics (8080→8080).\n")
	buf.WriteString("# caBundle in ValidatingWebhookConfigurations references this Service.\n")
	buf.WriteString("# guardian-schema.md §3.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "guardian-service.yaml"), buf.Bytes(), 0644)
}

// writeGuardianRBACWebhook writes guardian-rbac-webhook.yaml to dir.
// Emits a ValidatingWebhookConfiguration that gates all RBAC resource writes
// across all non-exempt namespaces. caBundle is injected automatically by
// cert-manager's CA injector via the cert-manager.io/inject-ca-from annotation.
// guardian-schema.md §5. CS-INV-001.
func writeGuardianRBACWebhook(dir string) error {
	vwc := map[string]interface{}{
		"apiVersion": "admissionregistration.k8s.io/v1",
		"kind":       "ValidatingWebhookConfiguration",
		"metadata": map[string]interface{}{
			"name": "guardian-rbac-webhook",
			"annotations": map[string]string{
				"ontai.dev/managed-by":             "compiler",
				"cert-manager.io/inject-ca-from":   "seam-system/guardian-webhook-cert",
			},
		},
		"webhooks": []map[string]interface{}{
			{
				"name":                    "validate-rbac.security.ontai.dev",
				"admissionReviewVersions": []string{"v1"},
				"sideEffects":             "None",
				// FailurePolicy: Fail — policy without enforcement is decoration. CS-INV-001.
				"failurePolicy": "Fail",
				// Namespaces labelled seam.ontai.dev/webhook-mode=exempt are excluded
				// (seam-system, kube-system). All other namespaces are subject to RBAC enforcement.
				// guardian-schema.md §5.
				"namespaceSelector": map[string]interface{}{
					"matchExpressions": []map[string]interface{}{
						{
							"key":      "seam.ontai.dev/webhook-mode",
							"operator": "NotIn",
							"values":   []string{"exempt"},
						},
					},
				},
				"rules": []map[string]interface{}{
					{
						"apiGroups":   []string{"rbac.authorization.k8s.io"},
						"apiVersions": []string{"v1"},
						"operations":  []string{"CREATE", "UPDATE"},
						"resources":   []string{"roles", "clusterroles", "rolebindings", "clusterrolebindings"},
						"scope":       "*",
					},
					{
						"apiGroups":   []string{""},
						"apiVersions": []string{"v1"},
						"operations":  []string{"CREATE", "UPDATE"},
						"resources":   []string{"serviceaccounts"},
						"scope":       "*",
					},
				},
				"clientConfig": map[string]interface{}{
					"service": map[string]interface{}{
						"name":      "guardian",
						"namespace": "seam-system",
						"path":      "/validate-rbac",
						"port":      443,
					},
				},
			},
		},
	}

	data, err := yaml.Marshal(vwc)
	if err != nil {
		return fmt.Errorf("marshal guardian RBAC ValidatingWebhookConfiguration: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Guardian RBAC ValidatingWebhookConfiguration\n")
	buf.WriteString("# Generated by: compiler enable (phase 2 guardian-deploy)\n")
	buf.WriteString("# Gates all RBAC writes in non-exempt namespaces.\n")
	buf.WriteString("# Exempt namespaces carry seam.ontai.dev/webhook-mode=exempt label.\n")
	buf.WriteString("# caBundle injected by cert-manager CA injector via cert-manager.io/inject-ca-from annotation.\n")
	buf.WriteString("# guardian-schema.md §5. CS-INV-001.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "guardian-rbac-webhook.yaml"), buf.Bytes(), 0644)
}

// writeGuardianLineageWebhook writes guardian-lineage-webhook.yaml to dir.
// Emits a ValidatingWebhookConfiguration that enforces lineage immutability on
// all security.ontai.dev root declaration CRDs. caBundle is injected automatically
// by cert-manager's CA injector via the cert-manager.io/inject-ca-from annotation.
// CLAUDE.md §14 Decision 1. guardian-schema.md §5.
func writeGuardianLineageWebhook(dir string) error {
	vwc := map[string]interface{}{
		"apiVersion": "admissionregistration.k8s.io/v1",
		"kind":       "ValidatingWebhookConfiguration",
		"metadata": map[string]interface{}{
			"name": "guardian-lineage-immutability-webhook",
			"annotations": map[string]string{
				"ontai.dev/managed-by":           "compiler",
				"cert-manager.io/inject-ca-from": "seam-system/guardian-webhook-cert",
			},
		},
		"webhooks": []map[string]interface{}{
			{
				"name":                    "validate-lineage.security.ontai.dev",
				"admissionReviewVersions": []string{"v1"},
				"sideEffects":             "None",
				// FailurePolicy: Fail — a missing lineage check is a security breach.
				// CLAUDE.md §14 Decision 1. guardian-schema.md §5.
				"failurePolicy": "Fail",
				// Namespaces labelled seam.ontai.dev/webhook-mode=exempt are excluded
				// (seam-system, kube-system). All other namespaces are subject to
				// lineage immutability enforcement. guardian-schema.md §5.
				"namespaceSelector": map[string]interface{}{
					"matchExpressions": []map[string]interface{}{
						{
							"key":      "seam.ontai.dev/webhook-mode",
							"operator": "NotIn",
							"values":   []string{"exempt"},
						},
					},
				},
				"rules": []map[string]interface{}{
					{
						"apiGroups":   []string{"security.ontai.dev"},
						"apiVersions": []string{"v1alpha1"},
						"operations":  []string{"UPDATE"},
						"resources":   []string{"rbacpolicies", "rbacprofiles", "identitybindings", "identityproviders", "permissionsets"},
						"scope":       "*",
					},
				},
				"clientConfig": map[string]interface{}{
					"service": map[string]interface{}{
						"name":      "guardian",
						"namespace": "seam-system",
						"path":      "/validate-lineage",
						"port":      443,
					},
				},
			},
		},
	}

	data, err := yaml.Marshal(vwc)
	if err != nil {
		return fmt.Errorf("marshal guardian lineage ValidatingWebhookConfiguration: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Guardian Lineage Immutability ValidatingWebhookConfiguration\n")
	buf.WriteString("# Generated by: compiler enable (phase 2 guardian-deploy)\n")
	buf.WriteString("# Enforces spec.lineage immutability on all security.ontai.dev root declarations.\n")
	buf.WriteString("# Rejects any UPDATE that attempts to alter a lineage field after creation.\n")
	buf.WriteString("# caBundle injected by cert-manager CA injector via cert-manager.io/inject-ca-from annotation.\n")
	buf.WriteString("# CLAUDE.md §14 Decision 1. guardian-schema.md §5.\n")
	buf.WriteString("---\n")
	buf.Write(data)

	return os.WriteFile(filepath.Join(dir, "guardian-lineage-webhook.yaml"), buf.Bytes(), 0644)
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
		"platform-wrapper-metrics-services.yaml",
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

	// platform-wrapper-metrics-services.yaml — Prometheus metrics Services for
	// Platform, Wrapper, and seam-core. All run in seam-system.
	if err := writeMetricsServicesFile(dir, "platform-wrapper-metrics-services.yaml", ops); err != nil {
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
		"conductor-signing-key.yaml",
		"conductor-deployment.yaml",
		"conductor-metrics-service.yaml",
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

	// conductor-signing-key.yaml — Ed25519 key pair Secret in ont-system.
	// Emitted BEFORE conductor-deployment.yaml so the volume mount resolves at apply time.
	// INV-026, conductor-schema.md §15.
	if err := writeConductorSigningKeySecret(dir); err != nil {
		return err
	}

	// conductor-deployment.yaml — Conductor Deployment with CONDUCTOR_ROLE=management.
	if err := writeDeploymentFile(dir, "conductor-deployment.yaml", cdt,
		"# Conductor Deployment (CONDUCTOR_ROLE=management)\n# Generated by: compiler enable (phase 4 conductor)\n# conductor-schema.md §15 Role Declaration Contract.\n"); err != nil {
		return err
	}

	// conductor-metrics-service.yaml — Prometheus metrics Service for Conductor.
	// Conductor runs in ont-system (not seam-system). CONTEXT.md §4 Namespace Model.
	if err := writeMetricsServiceFile(dir, "conductor-metrics-service.yaml", cdt.Name, cdt.Namespace); err != nil {
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

func writePhase5PostBootstrap(output string, operators []operatorSpec, dsnsIP, clusterName string) error {
	dir := filepath.Join(output, "05-post-bootstrap")
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	files := []string{
		"dsns-zone-configmap.yaml",
		"coredns-deployment-patch.yaml",
		"dsns-loadbalancer.yaml",
		"leaderelection.yaml",
	}
	if clusterName != "" {
		files = append(files, "pack-deploy-queue.yaml", "wrapper-runner.yaml")
	}

	meta := phaseMeta{
		Phase: "post-bootstrap",
		Order: 5,
		ReadinessGate: "No further readiness gate. The management cluster is fully " +
			"operational after this phase. The DSNS zone ConfigMap is created empty " +
			"and populated at runtime by the seam-core DSNSReconciler. Apply " +
			"coredns-deployment-patch.yaml to mount the dsns-zone ConfigMap into " +
			"CoreDNS pods. Leader election Leases are pre-created empty -- operators " +
			"populate them at runtime during their first reconcile. " +
			"CoreDNS Corefile DSNS stanza is patched automatically by " +
			"enable-ccs-mgmt.sh Step 7a immediately after this phase is applied. " +
			"C-COREDNS-PATCH.",
		ApplyOrder: files,
	}
	if err := writePhaseMeta(dir, meta); err != nil {
		return err
	}

	if err := writeDSNSZoneConfigMapYAML(dir); err != nil {
		return err
	}
	if err := writeCoreDNSDeploymentPatchYAML(dir); err != nil {
		return err
	}
	if err := writeDSNSLoadBalancerYAML(dir, dsnsIP); err != nil {
		return err
	}
	if err := writeLeaderElectionYAML(dir, operators); err != nil {
		return err
	}

	if clusterName != "" {
		// pack-deploy-queue.yaml — Kueue LocalQueue in seam-tenant-{clusterName}.
		// wrapper-schema.md §9 pack delivery chain.
		if err := writePackDeployQueueYAML(dir, clusterName); err != nil {
			return err
		}
		// wrapper-runner.yaml — SA, Role, RoleBinding for pack-deploy Job identity.
		// guardian-schema.md §6, INV-004.
		if err := writeWrapperRunnerRBACYAML(dir, clusterName); err != nil {
			return err
		}
	}

	return nil
}

// writeDSNSZoneConfigMapYAML writes the empty dsns-zone ConfigMap to 05-post-bootstrap.
// seam-core DSNSReconciler populates zone.db at runtime. The ConfigMap lives in
// kube-system so CoreDNS pods can mount it directly via the volume patch emitted by
// writeCoreDNSDeploymentPatchYAML. seam-core-schema.md §8 Decision 2.
func writeDSNSZoneConfigMapYAML(dir string) error {
	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dsns-zone",
			Namespace: "kube-system",
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

// writeDSNSLoadBalancerYAML writes a LoadBalancer Service in kube-system targeting
// CoreDNS pods (selector k8s-app=kube-dns) on port 53 UDP+TCP.
//
// The Service must be in kube-system because CoreDNS pods run there and a cross-
// namespace Service selector is not permitted in Kubernetes. The DSNS IP is
// allocated via Cilium IPAM using the lbipam.cilium.io/ips annotation.
//
// dsnsIP is the IP to assign. When empty, the annotation is omitted (used in tests).
// seam-core-schema.md §8 Decision 3.
func writeDSNSLoadBalancerYAML(dir, dsnsIP string) error {
	annotations := map[string]string{
		"governance.infrastructure.ontai.dev/owner": "seam-core",
	}
	if dsnsIP != "" {
		// Cilium IPAM — allocate the DSNS IP from the LoadBalancer IP pool.
		// lbipam.cilium.io/ips overrides the deprecated spec.loadBalancerIP field.
		annotations["lbipam.cilium.io/ips"] = dsnsIP
	}

	svc := corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "dsns-loadbalancer",
			Namespace: "kube-system",
			Annotations: annotations,
		},
		Spec: corev1.ServiceSpec{
			Type: corev1.ServiceTypeLoadBalancer,
			// Selector targets CoreDNS pods in kube-system.
			// k8s-app=kube-dns is the canonical CoreDNS pod label on Talos clusters.
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
	buf.WriteString("# DSNS LoadBalancer Service — exposes CoreDNS on port 53 UDP+TCP at the DSNS IP.\n")
	buf.WriteString("# Namespace: kube-system (required — Service selector must be in same namespace as pods).\n")
	buf.WriteString("# IP allocated via lbipam.cilium.io/ips annotation (Cilium IPAM). Provide --dsns-ip.\n")
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

// writeCoreDNSDeploymentPatchYAML writes coredns-deployment-patch.yaml to dir.
// This is a strategic merge patch for the CoreDNS Deployment in kube-system.
// It adds the dsns-zone ConfigMap as a volume and mounts it at /etc/coredns/dsns/
// inside the coredns container. CoreDNS file plugin reads zone.db from this path.
//
// Apply with: kubectl patch deployment coredns -n kube-system --patch-file coredns-deployment-patch.yaml
// seam-core-schema.md §8 Decision 3.
func writeCoreDNSDeploymentPatchYAML(dir string) error {
	// Emit as an unstructured map so it renders as a pure strategic merge patch
	// without Kubernetes API defaults filling in omitted fields.
	patch := map[string]interface{}{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata": map[string]interface{}{
			"name":      "coredns",
			"namespace": "kube-system",
		},
		"spec": map[string]interface{}{
			"template": map[string]interface{}{
				"spec": map[string]interface{}{
					"volumes": []map[string]interface{}{
						{
							"name": "dsns-zone",
							"configMap": map[string]interface{}{
								"name": "dsns-zone",
							},
						},
					},
					"containers": []map[string]interface{}{
						{
							// name must match the CoreDNS container name for strategic merge patch.
							"name": "coredns",
							"volumeMounts": []map[string]interface{}{
								{
									"name":      "dsns-zone",
									"mountPath": "/etc/coredns/dsns",
									"readOnly":  true,
								},
							},
						},
					},
				},
			},
		},
	}

	data, err := yaml.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal CoreDNS Deployment patch: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# CoreDNS Deployment Strategic Merge Patch\n")
	buf.WriteString("# Apply with: kubectl patch deployment coredns -n kube-system --patch-file coredns-deployment-patch.yaml\n")
	buf.WriteString("# Adds dsns-zone ConfigMap as volume and mounts at /etc/coredns/dsns/.\n")
	buf.WriteString("# CoreDNS file plugin reads /etc/coredns/dsns/zone.db per the stanza in coredns-dsns-stanza.yaml.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# seam-core-schema.md §8 Decision 3.\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "coredns-deployment-patch.yaml"), buf.Bytes(), 0644)
}

// --- Shared helpers ---

// writeMetricsServiceFile writes a Kubernetes Service manifest for the Prometheus
// metrics endpoint of a single operator. The Service exposes port 8080 named
// "metrics" with selector app.kubernetes.io/name=<operatorName>.
//
// ServiceMonitor CRDs for Prometheus Operator scrape configuration are deferred to
// a post-e2e observability session. This Service provides the endpoint for future
// ServiceMonitor configuration.
func writeMetricsServiceFile(dir, filename, operatorName, namespace string) error {
	svc := corev1.Service{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      operatorName + "-metrics",
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      operatorName,
				"app.kubernetes.io/component": "metrics",
			},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app.kubernetes.io/name": operatorName,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "metrics",
					Port:       8080,
					TargetPort: intstr.FromInt32(8080),
					Protocol:   corev1.ProtocolTCP,
				},
			},
		},
	}
	data, err := yaml.Marshal(svc)
	if err != nil {
		return fmt.Errorf("marshal metrics Service for %s: %w", operatorName, err)
	}
	var buf bytes.Buffer
	buf.WriteString("# Prometheus metrics Service — " + operatorName + " in " + namespace + "\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Port 8080 named 'metrics'. Controlled by METRICS_ADDR in the Deployment.\n")
	buf.WriteString("# ServiceMonitor CRDs for Prometheus Operator scrape configuration are\n")
	buf.WriteString("# deferred to a post-e2e observability session.\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644)
}

// writeMetricsServicesFile writes Prometheus metrics Service manifests for a list
// of operators into a single YAML file in dir. All operators share the same file.
func writeMetricsServicesFile(dir, filename string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Prometheus Metrics Services\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Port 8080 named 'metrics' for each operator. Controlled by METRICS_ADDR.\n")
	buf.WriteString("# ServiceMonitor CRDs for Prometheus Operator scrape configuration are\n")
	buf.WriteString("# deferred to a post-e2e observability session.\n")

	for _, op := range operators {
		svc := corev1.Service{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
			ObjectMeta: metav1.ObjectMeta{
				Name:      op.Name + "-metrics",
				Namespace: op.Namespace,
				Labels: map[string]string{
					"app.kubernetes.io/name":      op.Name,
					"app.kubernetes.io/component": "metrics",
				},
			},
			Spec: corev1.ServiceSpec{
				Selector: map[string]string{
					"app.kubernetes.io/name": op.Name,
				},
				Ports: []corev1.ServicePort{
					{
						Name:       "metrics",
						Port:       8080,
						TargetPort: intstr.FromInt32(8080),
						Protocol:   corev1.ProtocolTCP,
					},
				},
			},
		}
		data, err := yaml.Marshal(svc)
		if err != nil {
			return fmt.Errorf("marshal metrics Service for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}
	return os.WriteFile(filepath.Join(dir, filename), buf.Bytes(), 0644)
}

// writePhaseMeta writes phase-meta.yaml in dir as a Kubernetes ConfigMap.
// The ConfigMap carries the phase metadata fields as string data entries so that
// kubectl apply --dry-run=server accepts the file as a valid Kubernetes resource.
func writePhaseMeta(dir string, meta phaseMeta) error {
	cm := corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "seam-phase-" + meta.Phase + "-meta",
			Namespace: "seam-system",
			Labels: map[string]string{
				"seam.ontai.dev/phase":       meta.Phase,
				"seam.ontai.dev/phase-order": fmt.Sprintf("%d", meta.Order),
			},
		},
		Data: map[string]string{
			"phase":         meta.Phase,
			"order":         fmt.Sprintf("%d", meta.Order),
			"readinessGate": meta.ReadinessGate,
			"applyOrder":    strings.Join(meta.ApplyOrder, ","),
		},
	}
	data, err := yaml.Marshal(cm)
	if err != nil {
		return fmt.Errorf("marshal phase-meta for %s: %w", meta.Phase, err)
	}
	var buf bytes.Buffer
	buf.WriteString("# Phase metadata — do not edit manually.\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "phase-meta.yaml"), buf.Bytes(), 0644)
}

// writeOperatorRBACFile writes ServiceAccounts for all given operators, and
// ClusterRole + ClusterRoleBinding ONLY for Guardian.
//
// Non-guardian operators (platform, wrapper, seam-core, conductor) receive their
// RBAC exclusively via Guardian's RBACProfile provisioning mechanism. Emitting
// static ClusterRole/ClusterRoleBinding for them would bypass Guardian's RBAC
// ownership invariant (INV-004) and create parallel, unmanaged RBAC entries.
//
// Platform and Wrapper also emit executor ServiceAccounts ({op}-executor in
// ont-system). These are the identities under which Kueue-submitted executor Jobs
// run. guardian-schema.md §6.
func writeOperatorRBACFile(dir, filename string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator RBAC Resources\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Human review required before GitOps commit.\n")
	buf.WriteString("# ClusterRole and ClusterRoleBinding are emitted only for Guardian.\n")
	buf.WriteString("# All other operators receive RBAC via Guardian RBACProfile provisioning (INV-004).\n")
	buf.WriteString("# Platform and Wrapper additionally emit executor ServiceAccounts in ont-system.\n")

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

		// Executor ServiceAccounts — Platform and Wrapper submit Kueue Jobs whose
		// pods run under a separate executor SA in ont-system. This separates the
		// operator's control-plane identity from the executor Job identity.
		// conductor-schema.md §5 (execute mode).
		if op.Name == "platform" || op.Name == "wrapper" {
			executorSA := corev1.ServiceAccount{
				TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
				ObjectMeta: metav1.ObjectMeta{
					Name:      op.Name + "-executor",
					Namespace: "ont-system",
					Labels: map[string]string{
						"app.kubernetes.io/name":      op.Name,
						"app.kubernetes.io/component": "executor",
						"ontai.dev/managed-by":        "compiler",
					},
				},
			}
			executorData, err := yaml.Marshal(executorSA)
			if err != nil {
				return fmt.Errorf("marshal executor ServiceAccount for %s: %w", op.Name, err)
			}
			buf.WriteString("---\n")
			buf.Write(executorData)
		}

		// Static ClusterRole and ClusterRoleBinding are the Guardian bootstrap
		// window RBAC only. All other operators are governed by Guardian's own
		// RBACProfile provisioning after Guardian is operational.
		if op.Name != "guardian" {
			continue
		}

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
	var volumes []corev1.Volume
	var volumeMounts []corev1.VolumeMount
	env := []corev1.EnvVar{
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
		}},
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{
			FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"},
		}},
	}

	// Guardian Deployment carries CNPG connection env vars, GUARDIAN_ROLE, and
	// OPERATOR_NAMESPACE (required startup env var — Guardian exits if absent).
	// CNPG_SECRET_NAME/NAMESPACE — Guardian reads the guardian-db-app Secret (the
	// CNPG-generated app user credentials) to connect to its database.
	// GUARDIAN_ROLE — declares management cluster context for the Guardian agent.
	// OPERATOR_NAMESPACE — the namespace where Guardian runs; injected via downward API.
	// guardian-schema.md §16 CNPG Deployment Contract.
	if op.Name == "guardian" {
		env = append(env,
			corev1.EnvVar{Name: "CNPG_SECRET_NAME", Value: "guardian-db-app"},
			corev1.EnvVar{Name: "CNPG_SECRET_NAMESPACE", Value: "seam-system"},
			corev1.EnvVar{Name: "GUARDIAN_ROLE", Value: "management"},
			corev1.EnvVar{
				Name: "OPERATOR_NAMESPACE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
				},
			},
			// TODO(guardian-engineer): wire PERMISSION_SNAPSHOT_FRESHNESS_WINDOW into
			// PermissionSnapshotReconciler freshness validation logic. guardian-schema.md §9.
			corev1.EnvVar{Name: "PERMISSION_SNAPSHOT_FRESHNESS_WINDOW", Value: "3600"},
		)
	}

	// Conductor Deployment carries CONDUCTOR_ROLE and CLUSTER_REF via downward API
	// annotation fieldRefs. The annotations are stamped on the pod template metadata
	// so fieldRef reads resolve from the pod's own annotations at startup.
	// conductor-schema.md §15 Role Declaration Contract.
	if op.Name == "conductor" {
		env = append(env,
			corev1.EnvVar{
				Name: "CONDUCTOR_ROLE",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.annotations['platform.ontai.dev/role']",
					},
				},
			},
			corev1.EnvVar{
				Name: "CLUSTER_REF",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{
						FieldPath: "metadata.annotations['platform.ontai.dev/cluster-ref']",
					},
				},
			},
		)
	}

	// Conductor mounts the Ed25519 signing key Secret emitted in phase 04.
	// SIGNING_PRIVATE_KEY_PATH and SIGNING_PUBLIC_KEY_PATH point to the PEM files
	// within the mount. The Secret (conductor-signing-key) is emitted BEFORE this
	// Deployment in the phase 04 apply order. INV-026, conductor-schema.md §15.
	if op.Name == "conductor" {
		env = append(env,
			corev1.EnvVar{Name: "SIGNING_PRIVATE_KEY_PATH", Value: "/etc/conductor/signing/private.pem"},
			corev1.EnvVar{Name: "SIGNING_PUBLIC_KEY_PATH", Value: "/etc/conductor/signing/public.pem"},
		)
		volumes = append(volumes, corev1.Volume{
			Name: "conductor-signing-key",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: "conductor-signing-key",
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "conductor-signing-key",
			MountPath: "/etc/conductor/signing",
			ReadOnly:  true,
		})
	}

	// Platform, Wrapper, and seam-core carry OPERATOR_NAMESPACE so their webhook
	// servers and controllers can resolve their own namespace without downward API
	// duplication. OPERATOR_NAMESPACE is also required by Guardian admission hooks
	// in the platform and wrapper namespaces. guardian-schema.md §5.
	if op.Name == "platform" || op.Name == "wrapper" || op.Name == "seam-core" {
		env = append(env, corev1.EnvVar{
			Name: "OPERATOR_NAMESPACE",
			ValueFrom: &corev1.EnvVarSource{
				FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"},
			},
		})
	}

	// Platform carries CONDUCTOR_REGISTRY so it can construct Conductor executor Job
	// image references without hardcoding the registry. conductor-schema.md §15.
	if op.ConductorRegistry != "" {
		env = append(env, corev1.EnvVar{
			Name:  "CONDUCTOR_REGISTRY",
			Value: op.ConductorRegistry,
		})
	}

	// seam-core carries DSNS_SERVICE_IP so the DSNSState can seed the static ns
	// glue A record on startup. seam-core-schema.md §8 Decision 2.
	if op.DSNSServiceIP != "" {
		env = append(env, corev1.EnvVar{
			Name:  "DSNS_SERVICE_IP",
			Value: op.DSNSServiceIP,
		})
	}

	// Operators running an admission webhook server mount their TLS certificate Secret
	// at the path controller-runtime reads by default. WebhookSecret is set on all
	// operators that run a webhook: Guardian, Platform, Wrapper, seam-core.
	// guardian-schema.md §3 (webhook TLS).
	if op.WebhookSecret != "" {
		volumes = append(volumes, corev1.Volume{
			Name: "webhook-certs",
			VolumeSource: corev1.VolumeSource{
				Secret: &corev1.SecretVolumeSource{
					SecretName: op.WebhookSecret,
				},
			},
		})
		volumeMounts = append(volumeMounts, corev1.VolumeMount{
			Name:      "webhook-certs",
			MountPath: "/tmp/k8s-webhook-server/serving-certs",
			ReadOnly:  true,
		})
	}

	objMeta := metav1.ObjectMeta{
		Name:      op.Name,
		Namespace: op.Namespace,
		Labels: map[string]string{
			"app.kubernetes.io/name":      op.Name,
			"app.kubernetes.io/component": "operator",
			"ontai.dev/managed-by":        "compiler",
		},
	}
	// Conductor Deployment carries role and cluster-ref as pod template annotations
	// so the downward API fieldRefs in the env block resolve at pod startup.
	// These must be on the pod template metadata, not the Deployment metadata:
	// fieldRef metadata.annotations resolves from the pod's own annotations.
	var podAnnotations map[string]string
	if op.Name == "conductor" {
		podAnnotations = map[string]string{
			"platform.ontai.dev/role": op.Role,
		}
		if op.ClusterRef != "" {
			podAnnotations["platform.ontai.dev/cluster-ref"] = op.ClusterRef
		}
	}

	return appsv1.Deployment{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "apps/v1",
			Kind:       "Deployment",
		},
		ObjectMeta: objMeta,
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
					Annotations: podAnnotations,
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: op.ServiceAccount,
					SecurityContext: &corev1.PodSecurityContext{
						RunAsNonRoot: boolPtr(true),
					},
					Volumes: volumes,
					Containers: []corev1.Container{
						{
							Name:            op.Name,
							Image:           op.Image,
							ImagePullPolicy: corev1.PullAlways,
							Args:            op.Args,
							Env:             env,
							VolumeMounts:    volumeMounts,
							// Container-level security context — defense in depth.
							// Capabilities.Drop=ALL, seccomp RuntimeDefault, no escalation,
							// RunAsNonRoot (belt-and-suspenders with pod-level setting).
							SecurityContext: &corev1.SecurityContext{
								AllowPrivilegeEscalation: boolPtr(false),
								ReadOnlyRootFilesystem:   boolPtr(true),
								RunAsNonRoot:             boolPtr(true),
								Capabilities: &corev1.Capabilities{
									Drop: []corev1.Capability{"ALL"},
								},
								SeccompProfile: &corev1.SeccompProfile{
									Type: corev1.SeccompProfileTypeRuntimeDefault,
								},
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
		return append(common,
			// core API group — ConfigMaps (audit batch staging), Secrets (CNPG creds),
			// Namespaces (webhook-mode label inspection and patching),
			// ServiceAccounts (Step J RBAC materialisation). INV-004.
			rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"serviceaccounts"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"configmaps"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch"},
			},
			rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"secrets"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch"},
			},
			rbacv1.PolicyRule{
				APIGroups: []string{""},
				Resources: []string{"namespaces"},
				Verbs:     []string{"get", "list", "watch", "update", "patch"},
			},
			// security.ontai.dev — all eight Guardian CRD resources plus /status subresources.
			rbacv1.PolicyRule{
				APIGroups: []string{"security.ontai.dev"},
				Resources: []string{
					"rbacpolicies", "rbacpolicies/status",
					"rbacprofiles", "rbacprofiles/status",
					"identitybindings", "identitybindings/status",
					"identityproviders", "identityproviders/status",
					"permissionsets", "permissionsets/status",
					"permissionsnapshots", "permissionsnapshots/status",
					"permissionsnapshotreceipts", "permissionsnapshotreceipts/status",
					"guardians", "guardians/status",
				},
				Verbs: []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			// rbac.authorization.k8s.io — Guardian owns all RBAC on every cluster.
			// bind and escalate are required for ClusterRole aggregation. INV-004.
			rbacv1.PolicyRule{
				APIGroups: []string{"rbac.authorization.k8s.io"},
				Resources: []string{"clusterroles", "clusterrolebindings", "roles", "rolebindings"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete", "bind", "escalate"},
			},
			// runner.ontai.dev — Guardian reads RunnerConfigs in ont-system to validate
			// Conductor is operational before advancing bootstrap state.
			// Gap 10: compiler fix record item 23. guardian-schema.md §15.
			rbacv1.PolicyRule{
				APIGroups: []string{"runner.ontai.dev"},
				Resources: []string{"runnerconfigs"},
				Verbs:     []string{"get"},
			},
		)
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

// writePackDeployQueueYAML emits a Kueue LocalQueue named pack-deploy-queue in
// seam-tenant-{clusterName} referencing ClusterQueue seam-pack-deploy.
// The LocalQueue gates pack-deploy Job admission in the tenant namespace.
// wrapper-schema.md §9, conductor-schema.md §5 (execute mode).
func writePackDeployQueueYAML(dir, clusterName string) error {
	// LocalQueue is a Kueue CRD — emit as unstructured so we do not import kueue.
	lq := map[string]interface{}{
		"apiVersion": "kueue.x-k8s.io/v1beta1",
		"kind":       "LocalQueue",
		"metadata": map[string]interface{}{
			"name":      "pack-deploy-queue",
			"namespace": "seam-tenant-" + clusterName,
			"labels": map[string]interface{}{
				"ontai.dev/managed-by":       "compiler",
				"platform.ontai.dev/cluster": clusterName,
			},
		},
		"spec": map[string]interface{}{
			// clusterQueueName references the cluster-level queue that admits pack-deploy Jobs.
			// The ClusterQueue seam-pack-deploy is provisioned separately (Kueue admin responsibility).
			"clusterQueue": "seam-pack-deploy",
		},
	}

	data, err := yaml.Marshal(lq)
	if err != nil {
		return fmt.Errorf("marshal pack-deploy-queue LocalQueue: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Kueue LocalQueue — pack-deploy-queue in seam-tenant-" + clusterName + "\n")
	buf.WriteString("# References ClusterQueue seam-pack-deploy (Kueue admin must provision the ClusterQueue).\n")
	buf.WriteString("# Required for pack-deploy Job admission in the tenant namespace.\n")
	buf.WriteString("# Generated by: compiler enable (phase 05 post-bootstrap)\n")
	buf.WriteString("# wrapper-schema.md §9, conductor-schema.md §5.\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "pack-deploy-queue.yaml"), buf.Bytes(), 0644)
}

// writeWrapperRunnerRBACYAML emits ServiceAccount, Role, and RoleBinding for the
// wrapper-runner identity in seam-tenant-{clusterName}. The Role is annotated with
// ontai.dev/rbac-owner=guardian per INV-004.
// wrapper-schema.md §9, guardian-schema.md §6.
func writeWrapperRunnerRBACYAML(dir, clusterName string) error {
	ns := "seam-tenant-" + clusterName

	sa := corev1.ServiceAccount{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ServiceAccount"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wrapper-runner",
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "wrapper",
				"app.kubernetes.io/component": "runner",
				"ontai.dev/managed-by":        "compiler",
			},
		},
	}

	role := rbacv1.Role{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "Role",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wrapper-runner",
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "wrapper",
				"app.kubernetes.io/component": "runner",
				"ontai.dev/managed-by":        "compiler",
			},
			Annotations: map[string]string{
				// INV-004: Guardian owns all RBAC. This Role is declared here for
				// bootstrap apply; Guardian governs it after the webhook window closes.
				"ontai.dev/rbac-owner": "guardian",
			},
		},
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"configmaps", "secrets", "serviceaccounts", "services"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"apps"},
				Resources: []string{"deployments", "daemonsets", "statefulsets"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
			{
				APIGroups: []string{"infra.ontai.dev"},
				Resources: []string{"packexecutions", "clusterpacks", "packinstances"},
				Verbs:     []string{"get", "list", "watch"},
			},
			{
				APIGroups: []string{"runner.ontai.dev"},
				Resources: []string{"runnerconfigs"},
				Verbs:     []string{"get", "list", "watch", "patch", "update"},
			},
			{
				APIGroups: []string{"rbac.authorization.k8s.io"},
				Resources: []string{"roles", "rolebindings", "clusterroles", "clusterrolebindings"},
				Verbs:     []string{"get", "list", "watch", "create", "update", "patch", "delete"},
			},
		},
	}

	rb := rbacv1.RoleBinding{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "rbac.authorization.k8s.io/v1",
			Kind:       "RoleBinding",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "wrapper-runner",
			Namespace: ns,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "wrapper",
				"app.kubernetes.io/component": "runner",
				"ontai.dev/managed-by":        "compiler",
			},
			Annotations: map[string]string{
				"ontai.dev/rbac-owner": "guardian",
			},
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "Role",
			Name:     "wrapper-runner",
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      "wrapper-runner",
				Namespace: ns,
			},
		},
	}

	saData, err := yaml.Marshal(sa)
	if err != nil {
		return fmt.Errorf("marshal wrapper-runner ServiceAccount: %w", err)
	}
	roleData, err := yaml.Marshal(role)
	if err != nil {
		return fmt.Errorf("marshal wrapper-runner Role: %w", err)
	}
	rbData, err := yaml.Marshal(rb)
	if err != nil {
		return fmt.Errorf("marshal wrapper-runner RoleBinding: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# wrapper-runner RBAC in seam-tenant-" + clusterName + "\n")
	buf.WriteString("# ServiceAccount, Role, and RoleBinding for the pack-deploy Job identity.\n")
	buf.WriteString("# Role annotation ontai.dev/rbac-owner=guardian: Guardian governs after bootstrap.\n")
	buf.WriteString("# Generated by: compiler enable (phase 05 post-bootstrap)\n")
	buf.WriteString("# wrapper-schema.md §9, guardian-schema.md §6, INV-004.\n")
	buf.WriteString("---\n")
	buf.Write(saData)
	buf.WriteString("---\n")
	buf.Write(roleData)
	buf.WriteString("---\n")
	buf.Write(rbData)
	return os.WriteFile(filepath.Join(dir, "wrapper-runner.yaml"), buf.Bytes(), 0644)
}

// writeConductorSigningKeySecret generates a fresh Ed25519 signing key pair and
// writes it as a Kubernetes Secret named conductor-signing-key in ont-system.
// The Secret carries private.pem (PKCS8 PEM) and public.pem (PKIX PEM).
// Emitted in phase 04 BEFORE conductor-deployment.yaml so the volume mount resolves.
// INV-026, conductor-schema.md §15 (signing key lifecycle).
func writeConductorSigningKeySecret(dir string) error {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return fmt.Errorf("generate Ed25519 signing key: %w", err)
	}

	privateDER, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return fmt.Errorf("marshal PKCS8 private key: %w", err)
	}
	privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})

	publicDER, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return fmt.Errorf("marshal PKIX public key: %w", err)
	}
	publicPEM := pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: publicDER})

	secret := corev1.Secret{
		TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Secret"},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "conductor-signing-key",
			Namespace: "ont-system",
			Labels: map[string]string{
				"app.kubernetes.io/name":      "conductor",
				"app.kubernetes.io/component": "signing",
				"ontai.dev/managed-by":        "compiler",
			},
			Annotations: map[string]string{
				// Human review: rotate this Secret via a new compiler enable run.
				// The Conductor Deployment must be restarted after rotation.
				"ontai.dev/rotate-via": "compiler enable",
			},
		},
		StringData: map[string]string{
			"private.pem": string(privatePEM),
			"public.pem":  string(publicPEM),
		},
	}

	data, err := yaml.Marshal(secret)
	if err != nil {
		return fmt.Errorf("marshal conductor-signing-key Secret: %w", err)
	}

	var buf bytes.Buffer
	buf.WriteString("# Conductor Ed25519 signing key Secret — generated by compiler enable.\n")
	buf.WriteString("# Private key: PKCS8 PEM. Public key: PKIX PEM.\n")
	buf.WriteString("# Mounted at /etc/conductor/signing in the Conductor Deployment.\n")
	buf.WriteString("# Rotate: re-run compiler enable and restart the Conductor Deployment.\n")
	buf.WriteString("# WARNING: generated fresh each compiler enable run — store in GitOps after first enable.\n")
	buf.WriteString("# INV-026, conductor-schema.md §15.\n")
	buf.WriteString("---\n")
	buf.Write(data)
	return os.WriteFile(filepath.Join(dir, "conductor-signing-key.yaml"), buf.Bytes(), 0644)
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
//
// All RBACProfile CRs live in seam-system regardless of the operator's runtime
// namespace. Conductor runs in ont-system but its RBACProfile is in seam-system
// so Guardian can discover it alongside all other operator profiles.
// guardian-schema.md §6.
func buildOperatorRBACProfile(op operatorSpec) map[string]interface{} {
	// RBACProfiles always land in seam-system — Guardian discovers them there.
	// The principalRef still references the operator's actual SA namespace (op.Namespace).
	profileNamespace := "seam-system"

	return map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACProfile",
		"metadata": map[string]interface{}{
			"name":      "rbac-" + op.Name,
			"namespace": profileNamespace,
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
			// principalRef must use the full Kubernetes service account format so Guardian
			// can match the principal against admission webhook subjects.
			// guardian-schema.md §6 RBACProfile principalRef contract.
			"principalRef":  "system:serviceaccount:" + op.Namespace + ":" + op.ServiceAccount,
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
			// domainIdentityRef traces this operator's service account to the
			// DomainIdentity at core.ontai.dev. Set by compiler enable for all
			// Seam family operators. guardian-schema.md §7, CLAUDE.md §14 Decision 2.
			"domainIdentityRef": op.Name,
		},
	}
}

// seamMemberSpec holds the per-operator data needed to emit a SeamMembership CR.
type seamMemberSpec struct {
	// Name is the metadata.name of the SeamMembership (typically the operator name).
	Name string
	// AppIdentityRef is the operator's application-layer identity name.
	AppIdentityRef string
	// DomainIdentityRef references the DomainIdentity at core.ontai.dev.
	DomainIdentityRef string
	// PrincipalRef is the full Kubernetes service account string.
	PrincipalRef string
	// Tier is "infrastructure" or "application".
	Tier string
}

// buildSeamMembership constructs a SeamMembership CR map for one operator.
// infrastructure.ontai.dev/v1alpha1. guardian-schema.md §7, CLAUDE.md §14 Decision 2.
func buildSeamMembership(m seamMemberSpec) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "infrastructure.ontai.dev/v1alpha1",
		"kind":       "SeamMembership",
		"metadata": map[string]interface{}{
			"name":      m.Name,
			"namespace": "seam-system",
			"labels": map[string]string{
				"app.kubernetes.io/name":      m.Name,
				"app.kubernetes.io/component": "seam-membership",
				"ontai.dev/managed-by":        "compiler",
				"ontai.dev/membership-tier":   m.Tier,
			},
		},
		"spec": map[string]interface{}{
			"appIdentityRef":    m.AppIdentityRef,
			"domainIdentityRef": m.DomainIdentityRef,
			"principalRef":      m.PrincipalRef,
			"tier":              m.Tier,
		},
	}
}

// writeSeamMembershipsFile writes seam-memberships.yaml to dir.
// Emits one SeamMembership CR per Seam operator. Applied in phase 01 after
// RBACProfiles so Guardian can validate them immediately. guardian-schema.md §7.
func writeSeamMembershipsFile(dir string) error {
	members := []seamMemberSpec{
		{
			Name: "guardian", AppIdentityRef: "guardian", DomainIdentityRef: "guardian",
			PrincipalRef: "system:serviceaccount:seam-system:guardian",
			Tier:         "infrastructure",
		},
		{
			Name: "platform", AppIdentityRef: "platform", DomainIdentityRef: "platform",
			PrincipalRef: "system:serviceaccount:seam-system:platform",
			Tier:         "infrastructure",
		},
		{
			Name: "wrapper", AppIdentityRef: "wrapper", DomainIdentityRef: "wrapper",
			PrincipalRef: "system:serviceaccount:seam-system:wrapper",
			Tier:         "infrastructure",
		},
		{
			Name: "conductor", AppIdentityRef: "conductor", DomainIdentityRef: "conductor",
			PrincipalRef: "system:serviceaccount:ont-system:conductor",
			Tier:         "infrastructure",
		},
		{
			Name: "seam-core", AppIdentityRef: "seam-core", DomainIdentityRef: "seam-core",
			PrincipalRef: "system:serviceaccount:seam-system:seam-core",
			Tier:         "infrastructure",
		},
	}

	var buf bytes.Buffer
	buf.WriteString("# Seam Operator SeamMembership CRs\n")
	buf.WriteString("# Generated by: compiler enable (phase 1 guardian-bootstrap)\n")
	buf.WriteString("# One SeamMembership per Seam operator. Guardian validates and admits each member.\n")
	buf.WriteString("# Apply after guardian-rbacprofiles.yaml so RBACProfiles are present.\n")
	buf.WriteString("# infrastructure.ontai.dev/v1alpha1 — seam-core CRD. guardian-schema.md §7.\n")

	for _, m := range members {
		cr := buildSeamMembership(m)
		data, err := yaml.Marshal(cr)
		if err != nil {
			return fmt.Errorf("marshal SeamMembership for %s: %w", m.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(dir, "seam-memberships.yaml"), buf.Bytes(), 0644)
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
			// splitYAMLDocs splits on "\n---", consuming the newline that preceded
			// the separator. Documents therefore do not carry a trailing newline.
			// Without this guard the next "---\n" is concatenated directly to the
			// last character of the document, producing e.g. "status: {}---".
			if len(doc) == 0 || doc[len(doc)-1] != '\n' {
				out.WriteByte('\n')
			}
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
