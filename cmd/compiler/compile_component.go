// compile_component.go implements the `compiler component` subcommand.
//
// Emits RBACProfile CR YAML for known ecosystem components from the embedded
// versioned catalog, or renders a scaffold from a human-provided descriptor.
// Guardian's admission webhook enforces what RBACProfiles declare — it never
// generates them. compiler component is the exclusive authorship path.
//
// conductor-schema.md §16, guardian-schema.md §6.
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"

	"github.com/ontai-dev/conductor/internal/catalog"
)

const componentHelp = `Usage: compiler component [--component <name>...] [--descriptor <path>] [--discover] [flags]

Produce RBACProfile CR YAML from the embedded catalog or a custom descriptor.

Input contract:
  Catalog mode (--component):
    --component       Component name from the embedded catalog (repeatable).
                      Known components: cilium, cnpg, kueue, cert-manager, local-path-provisioner
    --namespace       Target namespace for RBACProfile CRs (required)
    --target-clusters Comma-separated list of cluster names (required)
    --rbac-policy-ref Name of the governing RBACPolicy (required)
    --discover        Connect to the cluster to auto-detect deployed resources and
                      report catalog coverage (optional; absent = fully offline)
    --kubeconfig      Path to kubeconfig for --discover (flag → $KUBECONFIG → ~/.kube/config)

  Custom mode (--descriptor):
    --descriptor      Path to a component descriptor YAML file
    --namespace       Target namespace for the RBACProfile CR (required)
    --rbac-policy-ref Name of the governing RBACPolicy (required)

Output contract:
  --output  Catalog mode: directory receiving one RBACProfile YAML per component.
            Custom mode: single file path, or stdout when --output is omitted.

Compile-only: output is a manifest for human review and GitOps pipeline
application — Compiler never applies, patches, or deletes any resource.
`

// runComponentSubcommand parses flags and dispatches to the appropriate mode.
// conductor-schema.md §16.
func runComponentSubcommand(args []string) {
	fs := flag.NewFlagSet("component", flag.ExitOnError)

	var componentFlags multiStringFlag
	fs.Var(&componentFlags, "component",
		"Component name from the embedded catalog (repeatable). "+
			"Known components: "+strings.Join(catalog.AvailableNames(), ", "))

	descriptor := fs.String("descriptor", "",
		"Path to a component descriptor YAML file (custom mode for unlisted components)")
	discover := fs.Bool("discover", false,
		"Enable cluster connectivity to auto-detect deployed third-party resources "+
			"and report catalog coverage (optional; absent = fully offline)")
	output := fs.String("output", "",
		"Output path: directory for catalog mode (one file per component), "+
			"file path for descriptor mode, or stdout when empty")
	namespace := fs.String("namespace", "",
		"Target namespace for RBACProfile CRs (required for catalog and descriptor modes, "+
			"e.g. seam-tenant-management)")
	targetClusters := fs.String("target-clusters", "",
		"Comma-separated list of cluster names for spec.targetClusters (required for catalog mode)")
	rbacPolicyRef := fs.String("rbac-policy-ref", "",
		"Name of the governing RBACPolicy (required for catalog and descriptor modes)")
	kubeconfig := fs.String("kubeconfig", "",
		"Path to kubeconfig for --discover (default: $KUBECONFIG or ~/.kube/config)")

	fs.Usage = func() {
		fmt.Fprint(os.Stderr, componentHelp)
		fs.PrintDefaults()
	}

	if err := fs.Parse(args); err != nil {
		fmt.Fprintf(os.Stderr, "compiler component: flag error: %v\n", err)
		os.Exit(1)
	}

	// Dispatch to operating mode.
	switch {
	case *discover:
		if err := runDiscoverMode(*kubeconfig, *namespace, *output); err != nil {
			fmt.Fprintf(os.Stderr, "compiler component: discover: %v\n", err)
			os.Exit(1)
		}
	case *descriptor != "":
		if err := runDescriptorMode(*descriptor, *output); err != nil {
			fmt.Fprintf(os.Stderr, "compiler component: descriptor: %v\n", err)
			os.Exit(1)
		}
	case len(componentFlags) > 0:
		clusters := parseClusters(*targetClusters)
		if err := runCatalogMode(componentFlags, *namespace, clusters, *rbacPolicyRef, *output); err != nil {
			fmt.Fprintf(os.Stderr, "compiler component: %v\n", err)
			os.Exit(1)
		}
	default:
		fmt.Fprintln(os.Stderr, "compiler component: at least one of --component, --descriptor, or --discover is required")
		fs.Usage()
		os.Exit(1)
	}
}

// runCatalogMode renders RBACProfile YAML for one or more named catalog components.
// Multiple components produce a multi-document YAML separated by ---.
// Unknown component name fails fast with a clear error listing available entries.
// conductor-schema.md §16 Catalog Mode.
func runCatalogMode(names []string, namespace string, targetClusters []string, rbacPolicyRef string, output string) error {
	if namespace == "" {
		return fmt.Errorf("--namespace is required for catalog mode")
	}
	if len(targetClusters) == 0 {
		return fmt.Errorf("--target-clusters is required for catalog mode (comma-separated cluster names)")
	}
	if rbacPolicyRef == "" {
		return fmt.Errorf("--rbac-policy-ref is required for catalog mode")
	}

	params := catalog.RenderParams{
		Namespace:      namespace,
		TargetClusters: targetClusters,
		RBACPolicyRef:  rbacPolicyRef,
	}

	// Resolve and validate all entries before rendering any — fail fast on unknowns.
	var entries []catalog.CatalogEntry
	for _, name := range names {
		entry, ok := catalog.Lookup(name)
		if !ok {
			return fmt.Errorf("unknown component %q — available: %s",
				name, strings.Join(catalog.AvailableNames(), ", "))
		}
		entries = append(entries, entry)
	}

	// Render all entries.
	var docs [][]byte
	for _, entry := range entries {
		rendered, err := entry.Render(params)
		if err != nil {
			return fmt.Errorf("render %q: %w", entry.Name, err)
		}
		docs = append(docs, rendered)
	}

	// Single component and explicit output path: write to that file.
	// Multiple components or directory output: write one file per component.
	// No output path: write to stdout (multi-document with --- separators).
	if output == "" {
		return writeDocsToStdout(entries, docs)
	}

	// Determine if output is a directory path (multiple components) or a file path.
	if len(entries) == 1 {
		return writeDocToFile(output, entries[0].RBACProfileName, docs[0])
	}
	// Multiple components — write to output directory.
	for i, entry := range entries {
		if err := writeDocToFile(output, entry.RBACProfileName, docs[i]); err != nil {
			return err
		}
	}
	return nil
}

// runDescriptorMode renders an RBACProfile scaffold from a custom descriptor file.
// The scaffold carries prominent human-review comment blocks and stubbed required fields.
// conductor-schema.md §16 Custom Mode.
func runDescriptorMode(descriptorPath, output string) error {
	data, err := os.ReadFile(descriptorPath)
	if err != nil {
		return fmt.Errorf("read descriptor %q: %w", descriptorPath, err)
	}

	var desc catalog.ComponentDescriptor
	if err := yaml.Unmarshal(data, &desc); err != nil {
		return fmt.Errorf("parse descriptor %q: %w", descriptorPath, err)
	}
	if desc.Name == "" {
		return fmt.Errorf("descriptor %q: name is required", descriptorPath)
	}
	if desc.Namespace == "" {
		return fmt.Errorf("descriptor %q: namespace is required", descriptorPath)
	}
	// Apply defaults for optional fields.
	if desc.PrincipalRef == "" {
		desc.PrincipalRef = desc.Name
	}
	if desc.RBACPolicyRef == "" {
		desc.RBACPolicyRef = "default-rbac-policy"
	}

	scaffold, err := catalog.DescriptorScaffold(desc)
	if err != nil {
		return fmt.Errorf("generate scaffold: %w", err)
	}

	if output == "" {
		_, err := os.Stdout.Write(scaffold)
		return err
	}
	// If output is a directory, write a named file inside it.
	info, err := os.Stat(output)
	if err == nil && info.IsDir() {
		fileName := "rbac-" + strings.ToLower(strings.ReplaceAll(desc.Name, " ", "-")) + ".yaml"
		return os.WriteFile(filepath.Join(output, fileName), scaffold, 0644)
	}
	return os.WriteFile(output, scaffold, 0644)
}

// runDiscoverMode connects to the cluster and reports catalog coverage of deployed
// third-party ServiceAccounts. Follows the same kubeconfig resolution order as
// compiler maintenance. conductor-schema.md §16 Custom Mode --discover.
func runDiscoverMode(kubeconfigPath, namespace, output string) error {
	// Resolve kubeconfig: flag → $KUBECONFIG → ~/.kube/config.
	kubeconfigPath = resolveKubeconfigPath(kubeconfigPath)

	cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return fmt.Errorf("build kubeconfig: %w", err)
	}
	k8sClient, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return fmt.Errorf("build kubernetes client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// List all ServiceAccounts across the target namespace (or all namespaces).
	listNS := corev1.NamespaceAll
	if namespace != "" {
		listNS = namespace
	}
	saList, err := k8sClient.CoreV1().ServiceAccounts(listNS).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list ServiceAccounts: %w", err)
	}

	// Build a set of deployed ServiceAccount names.
	deployedSAs := make(map[string]bool)
	for _, sa := range saList.Items {
		deployedSAs[sa.Name] = true
	}

	// Cross-reference against catalog entries.
	var covered, uncovered []string
	allEntries := catalog.All()
	for _, entry := range allEntries {
		// The catalog's principalRef is the ServiceAccount name.
		// We check by looking up the rendered principalRef against deployed SAs.
		// Since we don't render here (no params), we use a heuristic by name convention.
		// A real implementation would render each entry with a test namespace and check.
		// For now we report based on SA name convention matching.
		if deployedSAs[principalRefForEntry(entry.Name)] {
			covered = append(covered, entry.Name)
		} else {
			uncovered = append(uncovered, entry.Name)
		}
	}

	report := buildDiscoveryReport(saList.Items, covered, uncovered)
	if output == "" {
		fmt.Print(report)
		return nil
	}
	return os.WriteFile(output, []byte(report), 0644)
}

// principalRefForEntry returns the expected ServiceAccount name for a catalog entry.
// This is the principalRef field from the entry template — used for discover matching.
func principalRefForEntry(entryName string) string {
	switch entryName {
	case "cilium":
		return "cilium"
	case "cnpg":
		return "cnpg-manager"
	case "kueue":
		return "kueue-controller-manager"
	case "cert-manager":
		return "cert-manager"
	case "local-path-provisioner":
		return "local-path-provisioner-service-account"
	default:
		return entryName
	}
}

// buildDiscoveryReport formats the discovery output as a human-readable report.
func buildDiscoveryReport(allSAs []corev1.ServiceAccount, covered, uncovered []string) string {
	var sb strings.Builder
	sb.WriteString("=== compiler component --discover ===\n")
	sb.WriteString(fmt.Sprintf("Deployed ServiceAccounts scanned: %d\n\n", len(allSAs)))

	sb.WriteString("Catalog coverage:\n")
	if len(covered) == 0 {
		sb.WriteString("  (none detected)\n")
	}
	for _, name := range covered {
		e, _ := catalog.Lookup(name)
		sb.WriteString(fmt.Sprintf("  [COVERED]  %-25s  %s\n", name, e.RBACProfileName))
	}

	sb.WriteString("\nNot detected (may require RBACProfile):\n")
	if len(uncovered) == 0 {
		sb.WriteString("  (all catalog components detected)\n")
	}
	for _, name := range uncovered {
		e, _ := catalog.Lookup(name)
		sb.WriteString(fmt.Sprintf("  [MISSING]  %-25s  run: compiler component --component %s\n",
			name, name))
		_ = e
	}

	sb.WriteString("\nNote: Detection is based on ServiceAccount name convention matching.\n")
	sb.WriteString("Verify coverage by inspecting deployed component RBAC against Guardian RBACProfiles.\n")
	return sb.String()
}

// writeDocsToStdout writes multi-document YAML to stdout with --- separators.
func writeDocsToStdout(entries []catalog.CatalogEntry, docs [][]byte) error {
	w := os.Stdout
	for i, doc := range docs {
		if i > 0 {
			if _, err := fmt.Fprintln(w, "---"); err != nil {
				return err
			}
		}
		if _, err := w.Write(doc); err != nil {
			return fmt.Errorf("write %q to stdout: %w", entries[i].Name, err)
		}
	}
	return nil
}

// writeDocToFile writes a single YAML document to the given path.
// If outPath is a directory, writes {rbacProfileName}.yaml inside it.
// Otherwise writes to outPath directly (creating parent directories as needed).
func writeDocToFile(outPath, rbacProfileName string, doc []byte) error {
	info, err := os.Stat(outPath)
	var filePath string
	if err == nil && info.IsDir() {
		filePath = filepath.Join(outPath, rbacProfileName+".yaml")
	} else {
		// Treat as file path — create parent dirs.
		if err := os.MkdirAll(filepath.Dir(outPath), 0755); err != nil {
			return fmt.Errorf("create output dir: %w", err)
		}
		filePath = outPath
	}
	if err := os.WriteFile(filePath, doc, 0644); err != nil {
		return fmt.Errorf("write %s: %w", rbacProfileName, err)
	}
	return nil
}

// parseClusters splits a comma-separated cluster list into a string slice.
func parseClusters(raw string) []string {
	if raw == "" {
		return nil
	}
	var result []string
	for _, s := range strings.Split(raw, ",") {
		s = strings.TrimSpace(s)
		if s != "" {
			result = append(result, s)
		}
	}
	return result
}

// multiStringFlag is a flag.Value that collects multiple --flag values into a slice.
type multiStringFlag []string

func (f *multiStringFlag) String() string {
	return strings.Join(*f, ", ")
}

func (f *multiStringFlag) Set(v string) error {
	*f = append(*f, v)
	return nil
}
