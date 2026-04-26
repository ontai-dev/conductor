// compile_packbuild_helm.go implements the helmCompilePackBuild function.
// Pulls a Helm chart from a URL, renders it with optional values, splits into
// RBAC and workload OCI layers, pushes both layers, and emits a ClusterPack CR
// with the computed digests. Requires INV-014 build tag: helm client is
// compile-mode only. conductor-schema.md §9, wrapper-schema.md §4, INV-001.
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime/debug"
	"sort"
	"strings"

	"helm.sh/helm/v3/pkg/chart/loader"
	"helm.sh/helm/v3/pkg/chartutil"
	"helm.sh/helm/v3/pkg/engine"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/yaml"

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// HelmSource describes a Helm chart source for automated packbuild.
// When present in PackBuildInput, the compiler fetches, renders, and pushes
// the chart automatically instead of requiring pre-built OCI digests.
type HelmSource struct {
	// URL is the full URL to the Helm chart tarball (.tgz).
	// For local chart servers: http://10.20.0.1:8888/charts/nginx-ingress-4.10.0.tgz
	URL string `yaml:"url"`

	// Chart is the chart name (used for Helm release name and rendering context).
	Chart string `yaml:"chart"`

	// Version is the chart version string (used in Helm release metadata).
	Version string `yaml:"version"`

	// ValuesFile is the path to a YAML values file. Optional.
	// Relative paths are resolved from the PackBuildInput file's directory.
	ValuesFile string `yaml:"valuesFile,omitempty"`

	// RegistryCredentialsSecret is the name of a Kubernetes Secret in the
	// management cluster that holds registry credentials. Optional.
	// Not yet implemented — future work for private registries.
	// +optional
	RegistryCredentialsSecret string `yaml:"registryCredentialsSecret,omitempty"`
}

// helmSDKVersion returns the version of the helm.sh/helm/v3 module linked into
// this binary, derived from the build info at runtime. Returns an empty string
// if the build info is unavailable (e.g., in test binaries without module info).
func helmSDKVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return ""
	}
	for _, dep := range info.Deps {
		if dep.Path == "helm.sh/helm/v3" {
			if dep.Replace != nil {
				return dep.Replace.Version
			}
			return dep.Version
		}
	}
	return ""
}

// helmCompilePackBuild implements the helm automation path for packbuild.
// Downloads the Helm chart from HelmSource.URL, renders it, splits RBAC and
// workload manifests, pushes two OCI layers to in.RegistryURL, computes
// checksums, and emits a ClusterPack CR YAML to output.
// inputDir is the directory of the PackBuildInput file (for valuesFile resolution).
func helmCompilePackBuild(ctx context.Context, in PackBuildInput, inputDir, output string) error {
	hs := in.HelmSource
	if hs.URL == "" {
		return fmt.Errorf("helmCompilePackBuild: helmSource.url is required")
	}
	if in.RegistryURL == "" {
		return fmt.Errorf("helmCompilePackBuild: registryUrl is required for helm automation")
	}
	if in.Namespace == "" {
		in.Namespace = "seam-system"
	}

	// Fetch chart tarball.
	chartData, err := fetchURL(ctx, hs.URL)
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: fetch chart %q: %w", hs.URL, err)
	}

	// Load chart from tarball bytes.
	chrt, err := loader.LoadArchive(bytes.NewReader(chartData))
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: load chart archive: %w", err)
	}

	// Load values: chart defaults merged with optional user-supplied values file.
	vals := chrt.Values
	if vals == nil {
		vals = map[string]interface{}{}
	}
	if hs.ValuesFile != "" {
		valsPath := hs.ValuesFile
		if !strings.HasPrefix(valsPath, "/") && inputDir != "" {
			valsPath = inputDir + "/" + valsPath
		}
		valsData, err := os.ReadFile(valsPath)
		if err != nil {
			return fmt.Errorf("helmCompilePackBuild: read values file %q: %w", valsPath, err)
		}
		var userVals map[string]interface{}
		if err := yaml.Unmarshal(valsData, &userVals); err != nil {
			return fmt.Errorf("helmCompilePackBuild: parse values file %q: %w", valsPath, err)
		}
		// Merge user values on top of chart defaults.
		for k, v := range userVals {
			vals[k] = v
		}
	}

	// Render templates.
	chartName := hs.Chart
	if chartName == "" {
		chartName = chrt.Name()
	}
	renderOpts := chartutil.ReleaseOptions{
		Name:      chartName,
		Namespace: "default",
	}
	renderVals, err := chartutil.ToRenderValues(chrt, vals, renderOpts, nil)
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: build render values: %w", err)
	}
	rendered, err := engine.Render(chrt, renderVals)
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: render chart: %w", err)
	}

	// Join rendered templates into a single multi-document YAML string.
	// Skip NOTES.txt files -- they are plain text, not Kubernetes manifests.
	var allYAML strings.Builder
	// Sort by filename for deterministic output.
	keys := make([]string, 0, len(rendered))
	for k := range rendered {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		if strings.HasSuffix(strings.ToLower(k), "notes.txt") {
			continue
		}
		v := strings.TrimSpace(rendered[k])
		if v == "" {
			continue
		}
		allYAML.WriteString("---\n")
		allYAML.WriteString(v)
		allYAML.WriteString("\n")
	}

	// Parse and split manifests into three buckets: RBAC, cluster-scoped, workload.
	manifests, err := ParsePackManifests(allYAML.String())
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: parse rendered manifests: %w", err)
	}
	rbacManifests, clusterScopedManifests, workloadManifests := SplitManifests(manifests)

	// Inject Namespace manifest into workload layer. The Namespace is derived from
	// the first namespace found in workload manifests so it exists on the target
	// cluster before workload resources are applied.
	if ns := detectNamespace(workloadManifests); ns != "" {
		nsMfst := PackManifest{
			Kind: "Namespace",
			Name: ns,
			YAML: buildNamespaceYAML(ns),
		}
		workloadManifests = append([]PackManifest{nsMfst}, workloadManifests...)
	}

	// Serialize layers.
	rbacLayer := serializeManifests(rbacManifests)
	clusterScopedLayer := serializeManifests(clusterScopedManifests)
	workloadLayer := serializeManifests(workloadManifests)

	// Push RBAC layer to OCI registry, tagged as {version}-rbac.
	rbacTag := in.Version + "-rbac"
	rbacDigest, err := ociPushLayer(ctx, in.RegistryURL, rbacTag, []byte(rbacLayer))
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: push RBAC layer: %w", err)
	}

	// Push cluster-scoped layer if non-empty, tagged as {version}-cluster-scoped.
	var clusterScopedDigest string
	if len(clusterScopedManifests) > 0 {
		csTag := in.Version + "-cluster-scoped"
		clusterScopedDigest, err = ociPushLayer(ctx, in.RegistryURL, csTag, []byte(clusterScopedLayer))
		if err != nil {
			return fmt.Errorf("helmCompilePackBuild: push cluster-scoped layer: %w", err)
		}
	}

	// Push workload layer, tagged as {version}-workload.
	workloadTag := in.Version + "-workload"
	workloadDigest, err := ociPushLayer(ctx, in.RegistryURL, workloadTag, []byte(workloadLayer))
	if err != nil {
		return fmt.Errorf("helmCompilePackBuild: push workload layer: %w", err)
	}

	// Compute checksum over all layer content.
	checksum := computeChecksum(rbacLayer + clusterScopedLayer + workloadLayer)

	// Emit ClusterPack CR.
	ns := in.Namespace
	cp := seamcorev1alpha1.InfrastructureClusterPack{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "infrastructure.ontai.dev/v1alpha1",
			Kind:       "InfrastructureClusterPack",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      in.Name,
			Namespace: ns,
		},
		Spec: seamcorev1alpha1.InfrastructureClusterPackSpec{
			Version: in.Version,
			RegistryRef: seamcorev1alpha1.InfrastructurePackRegistryRef{
				URL:    in.RegistryURL,
				Digest: workloadDigest,
			},
			Checksum:            checksum,
			SourceBuildRef:      in.SourceBuildRef,
			TargetClusters:      in.TargetClusters,
			RBACDigest:          rbacDigest,
			WorkloadDigest:      workloadDigest,
			ClusterScopedDigest: clusterScopedDigest,
			BasePackName:        in.BasePackName,
			ChartURL:            hs.URL,
			ChartVersion:        hs.Version,
			ChartName:           chartName,
			HelmVersion:         helmSDKVersion(),
			ValuesFile:          hs.ValuesFile,
		},
	}
	return writeCRYAML(output, in.Name, cp)
}

// fetchURL downloads bytes from a URL using a plain http.Client.
func fetchURL(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetchURL %q: status %d", url, resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

// detectNamespace returns the first namespace found in the rendered manifests,
// preferring explicit Namespace kinds.
func detectNamespace(manifests []PackManifest) string {
	for _, m := range manifests {
		if m.Kind == "Namespace" {
			return m.Name
		}
	}
	for _, m := range manifests {
		if m.Namespace != "" {
			return m.Namespace
		}
	}
	return ""
}

// buildNamespaceYAML builds a minimal Namespace manifest YAML.
func buildNamespaceYAML(ns string) string {
	return fmt.Sprintf("apiVersion: v1\nkind: Namespace\nmetadata:\n  name: %s\n", ns)
}

// serializeManifests joins manifests into a multi-document YAML string.
func serializeManifests(manifests []PackManifest) string {
	var b strings.Builder
	for _, m := range manifests {
		b.WriteString("---\n")
		b.WriteString(m.YAML)
		if !strings.HasSuffix(m.YAML, "\n") {
			b.WriteString("\n")
		}
	}
	return b.String()
}

// computeChecksum returns the hex-encoded SHA256 of the given string.
func computeChecksum(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
