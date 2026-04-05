// compile_enable.go implements the `compiler enable` subcommand.
//
// Produces the complete management cluster deployment manifest bundle as YAML
// output in the --output directory:
//   - crds.yaml           — all Seam CRDs (same as compiler launch)
//   - operators.yaml      — Deployment manifests for all five Seam operators
//   - rbac.yaml           — ServiceAccounts, ClusterRoles, ClusterRoleBindings
//   - leaderelection.yaml — leader election Lease resources in seam-system
//   - rbacprofiles.yaml   — platform-owned RBACProfile CRs for operator SAs
//
// Output is deterministic: no timestamps, no random values.
// Conductor Deployment carries CONDUCTOR_ROLE=management per §15.
//
// conductor-schema.md §9 Step 3, §15 Role Declaration Contract.
// guardian-schema.md §6 (Seam operator RBACProfiles).
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
	"sigs.k8s.io/yaml"
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

// seam operators in enable-phase install order per platform-schema.md §8.
// Conductor (role=management) is listed first as it must be ready before
// Guardian can validate RBACProfiles.
func seamOperators(version string) []operatorSpec {
	return []operatorSpec{
		{
			Name:                "conductor",
			Namespace:           "ont-system",
			Image:               "registry.ontai.dev/ontai-dev/conductor:" + version,
			ServiceAccount:      "conductor",
			LeaderElectionLease: "conductor-management",
		},
		{
			Name:                "guardian",
			Namespace:           "seam-system",
			Image:               "registry.ontai.dev/ontai-dev/guardian:" + version,
			ServiceAccount:      "guardian",
			LeaderElectionLease: "guardian-leader",
		},
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

// runEnableSubcommand parses enable-specific flags and calls compileEnableBundle.
// conductor-schema.md §9 Step 3.
func runEnableSubcommand(args []string) {
	fs := flag.NewFlagSet("enable", flag.ExitOnError)
	output := fs.String("output", "", "Output directory for manifest bundle (required)")
	version := fs.String("version", "dev", "Operator image tag (e.g., v1.9.3-r1). Defaults to \"dev\".")
	_ = fs.String("kubeconfig", "", "Path to kubeconfig (unused in output-only mode; reserved for future validation)")

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

// compileEnableBundle generates the complete management cluster deployment
// manifest bundle and writes it to the output directory.
// conductor-schema.md §9 Step 3, §15.
func compileEnableBundle(output, version string) error {
	if err := os.MkdirAll(output, 0755); err != nil {
		return fmt.Errorf("create output directory: %w", err)
	}

	operators := seamOperators(version)

	// 1. crds.yaml — same CRD bundle as compiler launch.
	if err := compileLaunchBundle(output); err != nil {
		return fmt.Errorf("generate crds.yaml: %w", err)
	}

	// 2. operators.yaml — Deployment manifests for all operators.
	if err := writeOperatorsYAML(output, operators); err != nil {
		return fmt.Errorf("generate operators.yaml: %w", err)
	}

	// 3. rbac.yaml — ServiceAccounts, ClusterRoles, ClusterRoleBindings.
	if err := writeRBACYAML(output, operators); err != nil {
		return fmt.Errorf("generate rbac.yaml: %w", err)
	}

	// 4. leaderelection.yaml — leader election Lease resources.
	if err := writeLeaderElectionYAML(output, operators); err != nil {
		return fmt.Errorf("generate leaderelection.yaml: %w", err)
	}

	// 5. rbacprofiles.yaml — platform-owned RBACProfile CRs for operator SAs.
	if err := writeRBACProfilesYAML(output, operators); err != nil {
		return fmt.Errorf("generate rbacprofiles.yaml: %w", err)
	}

	return nil
}

// writeOperatorsYAML writes Deployment manifests for all Seam operators.
// Conductor carries CONDUCTOR_ROLE=management per conductor-schema.md §15.
func writeOperatorsYAML(output string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator Deployment Manifests\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Apply after crds.yaml and rbac.yaml.\n")
	buf.WriteString("# Conductor Deployment carries CONDUCTOR_ROLE=management per conductor-schema.md §15.\n")
	buf.WriteString("# Human review required before GitOps commit.\n")

	for _, op := range operators {
		deployment := buildOperatorDeployment(op)
		data, err := yaml.Marshal(deployment)
		if err != nil {
			return fmt.Errorf("marshal Deployment for %s: %w", op.Name, err)
		}
		buf.WriteString("---\n")
		buf.Write(data)
	}

	return os.WriteFile(filepath.Join(output, "operators.yaml"), buf.Bytes(), 0644)
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

// writeRBACYAML writes ServiceAccounts, ClusterRoles, and ClusterRoleBindings
// for all Seam operator service accounts.
func writeRBACYAML(output string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator RBAC Resources\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# Apply before operators.yaml. Guardian must be operational before\n")
	buf.WriteString("# any operator SA is used. Review against RBACProfiles in rbacprofiles.yaml.\n")
	buf.WriteString("# Human review required before GitOps commit.\n")

	for _, op := range operators {
		// ServiceAccount
		sa := corev1.ServiceAccount{
			TypeMeta: metav1.TypeMeta{
				APIVersion: "v1",
				Kind:       "ServiceAccount",
			},
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

		// ClusterRole — minimal rules; operators must request permissions via RBACProfile.
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

		// ClusterRoleBinding
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

	return os.WriteFile(filepath.Join(output, "rbac.yaml"), buf.Bytes(), 0644)
}

// operatorClusterRules returns minimal ClusterRole rules for a Seam operator.
// These are the bootstrap window permissions required for the enable phase.
// Guardian validates and owns all RBAC — this set is the minimum to get
// Guardian operational. guardian-schema.md §4.
func operatorClusterRules(operatorName string) []rbacv1.PolicyRule {
	// Common rules for all operators: read their own CRDs, manage leader election.
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
func writeLeaderElectionYAML(output string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator Leader Election Leases\n")
	buf.WriteString("# Generated by: compiler enable\n")
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

	return os.WriteFile(filepath.Join(output, "leaderelection.yaml"), buf.Bytes(), 0644)
}

// writeRBACProfilesYAML writes platform-owned RBACProfile CRs for all Seam
// operator service accounts. These are produced by compiler enable and never
// modified at runtime. guardian-schema.md §6.
func writeRBACProfilesYAML(output string, operators []operatorSpec) error {
	var buf bytes.Buffer
	buf.WriteString("# Seam Operator RBACProfile CRs\n")
	buf.WriteString("# Generated by: compiler enable\n")
	buf.WriteString("# These are the first-class platform-owned RBACProfiles for all Seam operator\n")
	buf.WriteString("# service accounts. Guardian provisions RBAC from these declarations.\n")
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

	return os.WriteFile(filepath.Join(output, "rbacprofiles.yaml"), buf.Bytes(), 0644)
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

// boolPtr returns a pointer to a bool value.
func boolPtr(b bool) *bool { return &b }

// int64Ptr returns a pointer to an int64 value.
func int64Ptr(i int64) *int64 { return &i }
