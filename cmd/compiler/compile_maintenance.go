// compile_maintenance.go implements the `compiler maintenance` subcommand.
//
// Reads cluster scheduling context at compile time (leader node, S3 config)
// and produces a MaintenanceBundle CR YAML that pre-encodes all context
// required for execution. Neither Platform nor Conductor need to perform
// cluster queries at execution time when they consume a MaintenanceBundle.
//
// conductor-schema.md §9, platform-schema.md §10.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	platformv1alpha1 "github.com/ontai-dev/platform/api/v1alpha1"
)

const maintenanceHelp = `Usage: compiler maintenance --operation <type> --cluster <name> --output <path> [flags]

Compile a MaintenanceBundle CR with pre-resolved scheduling context.

Input contract:
  --operation   Maintenance operation type: drain, upgrade, etcd-backup, machineconfig-rotation
  --cluster     TalosCluster resource name
  --targets     Comma-separated list of target node names (optional)
  --namespace   Namespace for the MaintenanceBundle CR (default: seam-system)
  --kubeconfig  Path to kubeconfig (flag → $KUBECONFIG → ~/.kube/config).
                Required: Compiler reads the live cluster to resolve the operator
                leader node and validate target nodes at compile time.

Output contract:
  --output  Directory receiving:
              <cluster>-<operation>.yaml  — MaintenanceBundle CR with pre-encoded
                                            scheduling constraints. Neither Platform
                                            nor Conductor perform cluster queries
                                            at execution time.

Compile-only: output is a manifest for human review and GitOps pipeline
application — Compiler never applies, patches, or deletes any resource.
`

// runMaintenanceSubcommand parses maintenance-specific flags and calls compileMaintenance.
func runMaintenanceSubcommand(args []string) {
	fs := flag.NewFlagSet("maintenance", flag.ExitOnError)
	operation := fs.String("operation", "", "Maintenance operation type: drain, upgrade, etcd-backup, machineconfig-rotation (required)")
	targetsStr := fs.String("targets", "", "Comma-separated list of target node names (optional)")
	cluster := fs.String("cluster", "", "TalosCluster resource name (required)")
	namespace := fs.String("namespace", "seam-system", "Namespace for the MaintenanceBundle CR (default: seam-system)")
	kubeconfig := fs.String("kubeconfig", "", "Path to kubeconfig (default: $KUBECONFIG or ~/.kube/config)")
	output := fs.String("output", "", "Path to output directory (required)")

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, maintenanceHelp)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "compiler maintenance: flag error: %v\n", err)
		os.Exit(1)
	}
	if *operation == "" {
		fmt.Fprintln(os.Stderr, "compiler maintenance: --operation is required")
		os.Exit(1)
	}
	if *cluster == "" {
		fmt.Fprintln(os.Stderr, "compiler maintenance: --cluster is required")
		os.Exit(1)
	}
	if *output == "" {
		fmt.Fprintln(os.Stderr, "compiler maintenance: --output is required")
		os.Exit(1)
	}

	var targets []string
	if *targetsStr != "" {
		for _, t := range strings.Split(*targetsStr, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				targets = append(targets, t)
			}
		}
	}

	if err := compileMaintenance(*operation, targets, *cluster, *namespace, *kubeconfig, *output); err != nil {
		fmt.Fprintf(os.Stderr, "compiler maintenance: %v\n", err)
		os.Exit(1)
	}
}

// compileMaintenance resolves cluster scheduling context and produces a
// MaintenanceBundle CR YAML. conductor-schema.md §9.
func compileMaintenance(operation string, targets []string, clusterName, namespace, kubeconfigPath, output string) error {
	op := platformv1alpha1.MaintenanceBundleOperation(operation)
	switch op {
	case platformv1alpha1.MaintenanceBundleOperationDrain,
		platformv1alpha1.MaintenanceBundleOperationUpgrade,
		platformv1alpha1.MaintenanceBundleOperationEtcdBackup,
		platformv1alpha1.MaintenanceBundleOperationMachineConfigRotation:
		// valid
	default:
		return fmt.Errorf("unknown operation %q: must be one of drain, upgrade, etcd-backup, machineconfig-rotation", operation)
	}

	// Resolve kubeconfig path: flag → $KUBECONFIG → ~/.kube/config.
	kubeconfigPath = resolveKubeconfigPath(kubeconfigPath)

	// Build Kubernetes client.
	k8sClient, err := buildK8sClient(kubeconfigPath)
	if err != nil {
		return fmt.Errorf("build Kubernetes client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Resolve operator leader node from platform-leader Lease in seam-system.
	// conductor-schema.md §13: Compiler fails fast if the Lease is absent or has no holder.
	leaderNode, err := resolveLeaderNodeFromCluster(ctx, k8sClient)
	if err != nil {
		return fmt.Errorf("resolve operator leader node: %w", err)
	}

	// For etcd-backup operations: validate S3 config exists.
	// platform-schema.md §10: A MaintenanceBundle is never committed without a valid S3 reference.
	var s3SecretRef *corev1.SecretReference
	if op == platformv1alpha1.MaintenanceBundleOperationEtcdBackup {
		s3SecretRef, err = resolveS3SecretForCompile(ctx, k8sClient)
		if err != nil {
			return fmt.Errorf("S3 config validation: %w", err)
		}
	}

	// Validate target nodes exist in the cluster.
	if len(targets) > 0 {
		if err := validateTargetNodes(ctx, k8sClient, targets); err != nil {
			return fmt.Errorf("target node validation: %w", err)
		}
	}

	// Build the MaintenanceBundle CR.
	mb := platformv1alpha1.MaintenanceBundle{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "platform.ontai.dev/v1alpha1",
			Kind:       "MaintenanceBundle",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      clusterName + "-" + operation,
			Namespace: namespace,
		},
		Spec: platformv1alpha1.MaintenanceBundleSpec{
			ClusterRef:             platformv1alpha1.LocalObjectRef{Name: clusterName},
			Operation:              op,
			MaintenanceTargetNodes: targets,
			OperatorLeaderNode:     leaderNode,
			S3ConfigSecretRef:      s3SecretRef,
		},
	}

	return writeCRYAML(output, mb.Name, mb)
}

// resolveKubeconfigPath returns the kubeconfig path using the resolution order:
// explicit flag → $KUBECONFIG env → ~/.kube/config.
func resolveKubeconfigPath(flagValue string) string {
	if flagValue != "" {
		return flagValue
	}
	if env := os.Getenv("KUBECONFIG"); env != "" {
		return env
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(home, ".kube", "config")
}

// buildK8sClient builds a Kubernetes typed client from the given kubeconfig path.
func buildK8sClient(kubeconfigPath string) (*kubernetes.Clientset, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %q: %w", kubeconfigPath, err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("create Kubernetes client: %w", err)
	}
	return clientset, nil
}

// resolveLeaderNodeFromCluster reads the platform-leader Lease from seam-system,
// resolves the holder pod, and returns the pod's node name.
// Fails fast if the Lease is absent or has no holder — a safety gate.
// conductor-schema.md §9, CP-INV-007.
func resolveLeaderNodeFromCluster(ctx context.Context, k8s *kubernetes.Clientset) (string, error) {
	lease, err := k8s.CoordinationV1().Leases("seam-system").Get(ctx, "platform-leader", metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("platform-leader Lease not found in seam-system — Platform operator must be running before compiling a MaintenanceBundle")
		}
		return "", fmt.Errorf("get platform-leader Lease: %w", err)
	}
	if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity == "" {
		return "", fmt.Errorf("platform-leader Lease has no holder — Platform operator is not running or has not won election")
	}
	// HolderIdentity format from controller-runtime: "{podName}_{uid}".
	holderIdentity := *lease.Spec.HolderIdentity
	podName := holderIdentity
	if idx := strings.Index(holderIdentity, "_"); idx != -1 {
		podName = holderIdentity[:idx]
	}
	pod, err := k8s.CoreV1().Pods("seam-system").Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", fmt.Errorf("leader pod %q not found in seam-system — platform-leader Lease is stale", podName)
		}
		return "", fmt.Errorf("get leader pod %s/seam-system: %w", podName, err)
	}
	if pod.Spec.NodeName == "" {
		return "", fmt.Errorf("leader pod %q has no NodeName — pod may not have been scheduled yet", podName)
	}
	return pod.Spec.NodeName, nil
}

// resolveS3SecretForCompile validates that the cluster-wide etcd backup S3 Secret
// exists in seam-system. Returns the SecretReference to embed in the MaintenanceBundle.
// Fails fast if the Secret is absent — a MaintenanceBundle for etcd-backup must have
// a valid S3 reference. platform-schema.md §10.
func resolveS3SecretForCompile(ctx context.Context, k8s *kubernetes.Clientset) (*corev1.SecretReference, error) {
	const secretName = "seam-etcd-backup-config"
	const secretNS = "seam-system"
	_, err := k8s.CoreV1().Secrets(secretNS).Get(ctx, secretName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("S3 backup Secret %s/%s not found — create this Secret before compiling an etcd-backup MaintenanceBundle. platform-schema.md §10", secretNS, secretName)
		}
		return nil, fmt.Errorf("get S3 backup Secret %s/%s: %w", secretNS, secretName, err)
	}
	return &corev1.SecretReference{Name: secretName, Namespace: secretNS}, nil
}

// validateTargetNodes checks that each listed node name exists in the cluster.
// Compile-time validation per conductor-schema.md §9.
func validateTargetNodes(ctx context.Context, k8s *kubernetes.Clientset, targets []string) error {
	for _, nodeName := range targets {
		_, err := k8s.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				return fmt.Errorf("target node %q does not exist in the cluster", nodeName)
			}
			return fmt.Errorf("get node %q: %w", nodeName, err)
		}
	}
	return nil
}
