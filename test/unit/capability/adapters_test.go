package capability_test

// Adapter unit tests for TalosClientAdapter, S3StorageClientAdapter, and
// OCIRegistryClientAdapter. All tests use doubles/stubs — no real Talos nodes,
// S3 buckets, or OCI registries are required. conductor-design.md §5.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/ontai-dev/conductor/internal/capability"
)

// ── S3StorageClientAdapter tests ─────────────────────────────────────────────

// TestNewS3StorageClientAdapter_MissingRegion verifies that constructing an
// S3StorageClientAdapter without S3_REGION set returns an error rather than
// panicking or silently succeeding.
func TestNewS3StorageClientAdapter_MissingRegion(t *testing.T) {
	t.Setenv("S3_REGION", "")
	t.Setenv("S3_ENDPOINT", "")
	_, err := capability.NewS3StorageClientAdapter(context.Background())
	if err == nil {
		t.Fatal("expected error when S3_REGION is not set; got nil")
	}
}

// ── OCIRegistryClientAdapter tests ───────────────────────────────────────────

// ociServer starts a test HTTP server that responds to OCI Distribution API
// requests. It serves one manifest with the given layers, and one blob per
// layer digest.
func ociServer(t *testing.T, name string, layers []struct{ Digest, Content string }) *httptest.Server {
	t.Helper()
	type layer struct {
		Digest    string `json:"digest"`
		MediaType string `json:"mediaType"`
		Size      int64  `json:"size"`
	}
	type manifest struct {
		SchemaVersion int     `json:"schemaVersion"`
		MediaType     string  `json:"mediaType"`
		Layers        []layer `json:"layers"`
	}

	mfstLayers := make([]layer, len(layers))
	blobs := map[string]string{}
	for i, l := range layers {
		mfstLayers[i] = layer{
			Digest:    l.Digest,
			MediaType: "application/vnd.oci.image.layer.v1.tar",
			Size:      int64(len(l.Content)),
		}
		blobs[l.Digest] = l.Content
	}
	mfstJSON, _ := json.Marshal(manifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Layers:        mfstLayers,
	})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == fmt.Sprintf("/v2/%s/manifests/latest", name):
			w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
			w.Write(mfstJSON) //nolint:errcheck
		default:
			for digest, content := range blobs {
				blobPath := fmt.Sprintf("/v2/%s/blobs/%s", name, digest)
				if r.Method == http.MethodGet && r.URL.Path == blobPath {
					w.Write([]byte(content)) //nolint:errcheck
					return
				}
			}
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// TestOCIRegistryClientAdapter_PullManifests_SingleLayer verifies that a
// single-layer OCI image returns one manifest bytes slice with the layer content.
func TestOCIRegistryClientAdapter_PullManifests_SingleLayer(t *testing.T) {
	layers := []struct{ Digest, Content string }{
		{Digest: "sha256:abc123", Content: "apiVersion: v1\nkind: ConfigMap\n"},
	}
	srv := ociServer(t, "myrepo/mypkg", layers)

	// Build a reference using the test server host.
	host := srv.Listener.Addr().String()
	ref := fmt.Sprintf("%s/myrepo/mypkg:latest", host)

	adapter := capability.NewOCIRegistryClientAdapterWithHTTPClient(srv.Client())
	manifests, err := adapter.PullManifests(context.Background(), ref)
	if err != nil {
		t.Fatalf("PullManifests: unexpected error: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("expected 1 manifest; got %d", len(manifests))
	}
	if !bytes.Equal(manifests[0], []byte(layers[0].Content)) {
		t.Errorf("manifest content mismatch: got %q; want %q", manifests[0], layers[0].Content)
	}
}

// TestOCIRegistryClientAdapter_PullManifests_MultipleLayer verifies that a
// multi-layer OCI image returns one manifest bytes slice per layer.
func TestOCIRegistryClientAdapter_PullManifests_MultipleLayer(t *testing.T) {
	layers := []struct{ Digest, Content string }{
		{Digest: "sha256:aaa", Content: "layer-one-content"},
		{Digest: "sha256:bbb", Content: "layer-two-content"},
		{Digest: "sha256:ccc", Content: "layer-three-content"},
	}
	srv := ociServer(t, "myrepo/mypkg", layers)

	host := srv.Listener.Addr().String()
	ref := fmt.Sprintf("%s/myrepo/mypkg:latest", host)

	adapter := capability.NewOCIRegistryClientAdapterWithHTTPClient(srv.Client())
	manifests, err := adapter.PullManifests(context.Background(), ref)
	if err != nil {
		t.Fatalf("PullManifests: unexpected error: %v", err)
	}
	if len(manifests) != len(layers) {
		t.Fatalf("expected %d manifests; got %d", len(layers), len(manifests))
	}
	for i, l := range layers {
		if !bytes.Equal(manifests[i], []byte(l.Content)) {
			t.Errorf("layer %d content mismatch: got %q; want %q", i, manifests[i], l.Content)
		}
	}
}

// TestOCIRegistryClientAdapter_PullManifests_NotFound verifies that a 404 from
// the registry manifests endpoint returns an error.
func TestOCIRegistryClientAdapter_PullManifests_NotFound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	t.Cleanup(srv.Close)

	host := srv.Listener.Addr().String()
	ref := fmt.Sprintf("%s/missing/image:latest", host)

	adapter := capability.NewOCIRegistryClientAdapterWithHTTPClient(srv.Client())
	_, err := adapter.PullManifests(context.Background(), ref)
	if err == nil {
		t.Fatal("expected error for 404 manifest response; got nil")
	}
}

// TestOCIRegistryClientAdapter_PullManifests_MalformedRef verifies that a
// reference string with no slash returns a parse error immediately.
func TestOCIRegistryClientAdapter_PullManifests_MalformedRef(t *testing.T) {
	adapter := capability.NewOCIRegistryClientAdapter()
	_, err := adapter.PullManifests(context.Background(), "no-slash-at-all")
	if err == nil {
		t.Fatal("expected error for malformed reference with no slash; got nil")
	}
}

// TestOCIRegistryClientAdapter_PullManifests_EmptyLayers verifies that an OCI
// image with no layers returns an error rather than an empty successful result.
func TestOCIRegistryClientAdapter_PullManifests_EmptyLayers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		type manifest struct {
			SchemaVersion int           `json:"schemaVersion"`
			Layers        []interface{} `json:"layers"`
		}
		w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
		json.NewEncoder(w).Encode(manifest{SchemaVersion: 2, Layers: nil}) //nolint:errcheck
	}))
	t.Cleanup(srv.Close)

	host := srv.Listener.Addr().String()
	ref := fmt.Sprintf("%s/myrepo/empty:latest", host)

	adapter := capability.NewOCIRegistryClientAdapterWithHTTPClient(srv.Client())
	_, err := adapter.PullManifests(context.Background(), ref)
	if err == nil {
		t.Fatal("expected error for OCI image with no layers; got nil")
	}
}

// ── parseOCIRef tests via PullManifests error path ────────────────────────────

// TestOCIRef_DigestRef verifies that a digest reference (with @sha256:...) is
// routed to the correct manifest URL.
func TestOCIRef_DigestRef(t *testing.T) {
	digest := "sha256:deadbeef"
	requested := ""

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = r.URL.Path
		http.NotFound(w, r) // just capture the URL; don't serve a valid response
	}))
	t.Cleanup(srv.Close)

	host := srv.Listener.Addr().String()
	ref := fmt.Sprintf("%s/myrepo/img@%s", host, digest)

	adapter := capability.NewOCIRegistryClientAdapterWithHTTPClient(srv.Client())
	adapter.PullManifests(context.Background(), ref) //nolint:errcheck

	expected := fmt.Sprintf("/v2/myrepo/img/manifests/%s", digest)
	if requested != expected {
		t.Errorf("manifest URL path: got %q; want %q", requested, expected)
	}
}

// TestOCIRef_TagAndDigestRef verifies that a reference containing both a tag and
// a digest (url:tag@digest format) strips the tag and fetches by digest alone.
// The tag must not appear in the registry URL path. conductor-schema.md §9.
func TestOCIRef_TagAndDigestRef(t *testing.T) {
	digest := "sha256:deadbeef"
	requested := ""

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requested = r.URL.Path
		http.NotFound(w, r) // capture URL; don't serve valid response
	}))
	t.Cleanup(srv.Close)

	host := srv.Listener.Addr().String()
	// Reference with both tag and digest — tag must be stripped from URL path.
	ref := fmt.Sprintf("%s/myrepo/img:v1.0@%s", host, digest)

	adapter := capability.NewOCIRegistryClientAdapterWithHTTPClient(srv.Client())
	adapter.PullManifests(context.Background(), ref) //nolint:errcheck

	expected := fmt.Sprintf("/v2/myrepo/img/manifests/%s", digest)
	if requested != expected {
		t.Errorf("manifest URL path: got %q; want %q", requested, expected)
	}
}


// ── EndpointsFromTalosconfig tests ───────────────────────────────────────────

func TestEndpointsFromTalosconfig_EndpointsFallback(t *testing.T) {
	content := `context: ccs-mgmt
contexts:
  ccs-mgmt:
    endpoints:
      - 10.20.0.2
      - 10.20.0.3
    ca: dGVzdA==
    crt: dGVzdA==
    key: dGVzdA==
`
	f := t.TempDir() + "/talosconfig"
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := capability.EndpointsFromTalosconfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != "10.20.0.2" || got[1] != "10.20.0.3" {
		t.Errorf("unexpected endpoints: %v", got)
	}
}

func TestEndpointsFromTalosconfig_NodesTakePrecedence(t *testing.T) {
	content := `context: ccs-mgmt
contexts:
  ccs-mgmt:
    endpoints:
      - 10.20.0.2
    nodes:
      - 10.20.0.3
    ca: dGVzdA==
    crt: dGVzdA==
    key: dGVzdA==
`
	f := t.TempDir() + "/talosconfig"
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := capability.EndpointsFromTalosconfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || got[0] != "10.20.0.3" {
		t.Errorf("expected nodes to take precedence; got %v", got)
	}
}

func TestEndpointsFromTalosconfig_MissingFile(t *testing.T) {
	_, err := capability.EndpointsFromTalosconfig("/nonexistent/talosconfig")
	if err == nil {
		t.Fatal("expected error for missing file; got nil")
	}
}

func TestEndpointsFromTalosconfig_UnknownContext(t *testing.T) {
	content := `context: other
contexts:
  ccs-mgmt:
    endpoints:
      - 10.20.0.2
`
	f := t.TempDir() + "/talosconfig"
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := capability.EndpointsFromTalosconfig(f)
	if err == nil {
		t.Fatal("expected error for unknown context; got nil")
	}
}

// TestEndpointsFromTalosconfig_ClusterEndpointFiltered verifies that the VIP
// (clusterEndpoint) is removed from the returned endpoints list so that
// per-node Talos operations target only real node IPs and never the VIP.
func TestEndpointsFromTalosconfig_ClusterEndpointFiltered(t *testing.T) {
	content := `context: ccs-mgmt
contexts:
  ccs-mgmt:
    clusterEndpoint: 10.20.0.10
    endpoints:
      - 10.20.0.10
      - 10.20.0.11
      - 10.20.0.12
    ca: dGVzdA==
    crt: dGVzdA==
    key: dGVzdA==
`
	f := t.TempDir() + "/talosconfig"
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := capability.EndpointsFromTalosconfig(f)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0] != "10.20.0.11" || got[1] != "10.20.0.12" {
		t.Errorf("expected VIP (10.20.0.10) filtered from endpoints; got %v", got)
	}
}

// TestEndpointsFromTalosconfig_ClusterEndpointOnlyReturnsError verifies that
// when clusterEndpoint filtering removes all endpoints an error is returned
// rather than an empty list that would silently skip all nodes.
func TestEndpointsFromTalosconfig_ClusterEndpointOnlyReturnsError(t *testing.T) {
	content := `context: ccs-mgmt
contexts:
  ccs-mgmt:
    clusterEndpoint: 10.20.0.10
    endpoints:
      - 10.20.0.10
    ca: dGVzdA==
    crt: dGVzdA==
    key: dGVzdA==
`
	f := t.TempDir() + "/talosconfig"
	if err := os.WriteFile(f, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := capability.EndpointsFromTalosconfig(f)
	if err == nil {
		t.Fatal("expected error when only the VIP remains after filtering; got nil")
	}
}
