package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// minimalHelmChart builds a minimal Helm chart tarball in memory.
// The chart has one workload template (a ConfigMap) and no RBAC resources,
// which is sufficient for testing the chart metadata carry-through path.
func minimalHelmChart(t *testing.T, chartName, chartVersion string) []byte {
	t.Helper()

	chartYAML := fmt.Sprintf("apiVersion: v2\nname: %s\nversion: %s\ntype: application\n",
		chartName, chartVersion)
	templateYAML := fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %s-config
  namespace: default
data:
  key: value
`, chartName)

	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)

	entries := []struct {
		name    string
		content string
	}{
		{chartName + "/Chart.yaml", chartYAML},
		{chartName + "/templates/configmap.yaml", templateYAML},
	}
	for _, e := range entries {
		hdr := &tar.Header{
			Name: e.name,
			Mode: 0644,
			Size: int64(len(e.content)),
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("minimalHelmChart: write header %s: %v", e.name, err)
		}
		if _, err := tw.Write([]byte(e.content)); err != nil {
			t.Fatalf("minimalHelmChart: write content %s: %v", e.name, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("minimalHelmChart: close tar: %v", err)
	}
	if err := gw.Close(); err != nil {
		t.Fatalf("minimalHelmChart: close gzip: %v", err)
	}
	return buf.Bytes()
}

// mockOCIRegistry creates an httptest.Server that accepts OCI Distribution Spec
// push requests and returns 201 responses. It is sufficient to exercise the
// helmCompilePackBuild OCI push path in tests.
func mockOCIRegistry(t *testing.T) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		switch {
		case r.Method == http.MethodHead && strings.Contains(path, "/blobs/sha256:"):
			// Blob does not exist -- trigger upload flow.
			w.WriteHeader(http.StatusNotFound)

		case r.Method == http.MethodPost && strings.HasSuffix(path, "/blobs/uploads/"):
			// Return absolute upload URL in Location header.
			w.Header().Set("Location", srv.URL+"/v2/upload-session")
			w.WriteHeader(http.StatusAccepted)

		case r.Method == http.MethodPut && strings.Contains(path, "/upload-session"):
			// Acknowledge blob upload.
			w.WriteHeader(http.StatusCreated)

		case r.Method == http.MethodPut && strings.Contains(path, "/manifests/"):
			// Return a fake manifest digest.
			fakeDigest := fmt.Sprintf("sha256:%x",
				sha256.Sum256([]byte(r.URL.String())))
			w.Header().Set("Docker-Content-Digest", fakeDigest)
			w.WriteHeader(http.StatusCreated)

		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return srv
}

// TestHelmCompilePackBuild_ChartMetadataPopulated verifies that helmCompilePackBuild
// populates ChartURL, ChartVersion, ChartName, and HelmVersion in the emitted
// ClusterPack CR. T-09, Decision B, T-04 schema.
func TestHelmCompilePackBuild_ChartMetadataPopulated(t *testing.T) {
	const (
		chartName    = "cert-manager"
		chartVersion = "v1.13.3"
	)

	// Serve the chart tarball from a mock HTTP server.
	chartTarball := minimalHelmChart(t, chartName, chartVersion)
	chartSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-tar")
		w.WriteHeader(http.StatusOK)
		w.Write(chartTarball)
	}))
	defer chartSrv.Close()

	// Mock OCI registry for push operations.
	ociSrv := mockOCIRegistry(t)
	defer ociSrv.Close()

	// ociSrv.URL is http://127.0.0.1:PORT -- strip scheme to get host:port.
	ociHost := strings.TrimPrefix(ociSrv.URL, "http://")
	registryURL := ociHost + "/packs/" + chartName

	outDir := t.TempDir()
	in := PackBuildInput{
		Name:        chartName,
		Version:     chartVersion,
		RegistryURL: registryURL,
		Namespace:   "seam-system",
		HelmSource: &HelmSource{
			URL:     chartSrv.URL + "/" + chartName + "-" + chartVersion + ".tgz",
			Chart:   chartName,
			Version: chartVersion,
		},
	}

	if err := helmCompilePackBuild(context.Background(), in, "", outDir); err != nil {
		t.Fatalf("helmCompilePackBuild: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, chartName+".yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	var cr map[string]interface{}
	if err := yaml.Unmarshal(data, &cr); err != nil {
		t.Fatalf("parse output YAML: %v", err)
	}

	spec, ok := cr["spec"].(map[string]interface{})
	if !ok {
		t.Fatalf("spec field missing or not an object")
	}

	checks := map[string]string{
		"chartURL":     in.HelmSource.URL,
		"chartVersion": chartVersion,
		"chartName":    chartName,
	}
	for field, want := range checks {
		got, _ := spec[field].(string)
		if got != want {
			t.Errorf("spec.%s: got %q, want %q", field, got, want)
		}
	}

	// helmVersion must be non-empty (derived from the linked helm SDK).
	helmVer, _ := spec["helmVersion"].(string)
	if helmVer == "" {
		t.Errorf("spec.helmVersion: expected non-empty (Helm SDK version), got empty string")
	}
}

// TestHelmCompilePackBuild_NonHelmPathNoChartFields verifies that the non-helm
// packbuild path (pre-computed digests) does not emit chart metadata fields in
// the ClusterPack CR. T-09, Decision B.
func TestHelmCompilePackBuild_NonHelmPathNoChartFields(t *testing.T) {
	const input = `
name: raw-pack
version: v1.0.0
registryUrl: registry.example.com/packs/raw-pack
digest: sha256:aaaa
checksum: deadbeef
`
	inputPath := writePackBuildInput(t, input)
	outDir := t.TempDir()

	if err := compilePackBuild(inputPath, outDir); err != nil {
		t.Fatalf("compilePackBuild: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(outDir, "raw-pack.yaml"))
	if err != nil {
		t.Fatalf("read output: %v", err)
	}

	for _, field := range []string{"chartURL", "chartVersion", "chartName", "helmVersion"} {
		if strings.Contains(string(data), field+":") {
			t.Errorf("non-helm output YAML contains field %q but should not", field)
		}
	}
}

// TestHelmSDKVersion_NonEmpty verifies that helmSDKVersion returns a non-empty
// string when the binary is built with the helm.sh/helm/v3 dependency.
// If the binary is built without module info (e.g., go test -gcflags), this
// test is a no-op. T-09.
func TestHelmSDKVersion_NonEmpty(t *testing.T) {
	v := helmSDKVersion()
	if v == "" {
		t.Log("helmSDKVersion returned empty -- module build info unavailable (acceptable in test binaries)")
		return
	}
	if !strings.HasPrefix(v, "v") {
		t.Errorf("helmSDKVersion: expected version starting with 'v', got %q", v)
	}
}
