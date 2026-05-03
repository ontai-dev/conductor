// compile_oci_push.go implements OCI Distribution Spec v2 blob and manifest
// push helpers used by the helmCompilePackBuild function. Uses net/http only.
// conductor-schema.md §9 (pack-compile), wrapper-schema.md §4.
package main

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// compilerOCIRef holds the parsed components of a registry reference.
type compilerOCIRef struct {
	registry string
	name     string
	ref      string
}

// parseCompilerOCIRef parses "registry/name:tag" into components.
func parseCompilerOCIRef(reference string) (compilerOCIRef, error) {
	slash := strings.Index(reference, "/")
	if slash < 0 {
		return compilerOCIRef{}, fmt.Errorf("parseCompilerOCIRef: no slash in %q", reference)
	}
	registry := reference[:slash]
	remainder := reference[slash+1:]
	if idx := strings.LastIndex(remainder, ":"); idx >= 0 {
		return compilerOCIRef{registry: registry, name: remainder[:idx], ref: remainder[idx+1:]}, nil
	}
	return compilerOCIRef{}, fmt.Errorf("parseCompilerOCIRef: no tag in %q", reference)
}

// ociPushManifest is the minimal OCI Image Manifest written when pushing a
// single-layer pack image. Each layer is one concatenated YAML document.
type ociPushManifest struct {
	SchemaVersion int                  `json:"schemaVersion"`
	MediaType     string               `json:"mediaType"`
	Config        ociPushDescriptor    `json:"config"`
	Layers        []ociPushDescriptor  `json:"layers"`
}

// ociPushDescriptor describes one blob in an OCI manifest.
type ociPushDescriptor struct {
	MediaType string `json:"mediaType"`
	Size      int64  `json:"size"`
	Digest    string `json:"digest"`
}

// packYAMLAsTarGz wraps raw YAML bytes in a gzip-compressed tar archive with a
// single entry named "manifests.yaml". Conductor's extractYAMLsFromTarGz expects
// this format when unpacking OCI layer blobs.
func packYAMLAsTarGz(yamlData []byte) ([]byte, error) {
	var buf bytes.Buffer
	gw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gw)
	hdr := &tar.Header{
		Name: "manifests.yaml",
		Mode: 0644,
		Size: int64(len(yamlData)),
	}
	if err := tw.WriteHeader(hdr); err != nil {
		return nil, fmt.Errorf("packYAMLAsTarGz: write header: %w", err)
	}
	if _, err := tw.Write(yamlData); err != nil {
		return nil, fmt.Errorf("packYAMLAsTarGz: write data: %w", err)
	}
	if err := tw.Close(); err != nil {
		return nil, fmt.Errorf("packYAMLAsTarGz: close tar: %w", err)
	}
	if err := gw.Close(); err != nil {
		return nil, fmt.Errorf("packYAMLAsTarGz: close gzip: %w", err)
	}
	return buf.Bytes(), nil
}

// resolveDockerAuth reads Docker credentials for the given registry hostname
// from the local Docker config. It checks ~/.docker/config.json first, then
// falls back to the snap-installed Docker path at
// ~/snap/docker/current/.docker/config.json. Returns the base64-encoded
// "user:pass" auth string suitable for use as a Basic Authorization header
// value, or an empty string if no credentials are found or any error occurs.
//
// The docker config JSON format is:
//
//	{"auths": {"registry:port": {"auth": "base64user:pass"}}}
//
// The auth field is already base64-encoded by docker credential helpers.
func resolveDockerAuth(registry string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	candidates := []string{
		filepath.Join(home, ".docker", "config.json"),
		filepath.Join(home, "snap", "docker", "current", ".docker", "config.json"),
	}
	for _, path := range candidates {
		data, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		var cfg struct {
			Auths map[string]struct {
				Auth string `json:"auth"`
			} `json:"auths"`
		}
		if err := json.Unmarshal(data, &cfg); err != nil {
			continue
		}
		if entry, ok := cfg.Auths[registry]; ok && entry.Auth != "" {
			return entry.Auth
		}
	}
	return ""
}

// ociPushLayer wraps YAML bytes in a tar.gz archive and pushes it as a single
// OCI blob to the registry. The manifest is also pushed under the given tag.
// Returns the manifest digest (sha256:...) for use in ClusterPack spec fields.
// Conductor's extractYAMLsFromTarGz expects the tar.gz format when pulling layers.
//
// Registry scheme is inferred: http for 10.x and localhost, https otherwise.
// Docker credentials are resolved from ~/.docker/config.json (or the snap
// fallback) and passed as Basic Authorization headers when present.
func ociPushLayer(ctx context.Context, registryURL, tag string, layerData []byte) (digest string, err error) {
	tgzData, err := packYAMLAsTarGz(layerData)
	if err != nil {
		return "", fmt.Errorf("ociPushLayer: pack tar.gz: %w", err)
	}
	layerData = tgzData
	parsed, err := parseCompilerOCIRef(registryURL + ":" + tag)
	if err != nil {
		return "", fmt.Errorf("ociPushLayer: parse registry ref: %w", err)
	}

	scheme := "https"
	if strings.HasPrefix(parsed.registry, "10.") ||
		strings.HasPrefix(parsed.registry, "localhost") ||
		strings.HasPrefix(parsed.registry, "127.") {
		scheme = "http"
	}

	baseURL := fmt.Sprintf("%s://%s/v2/%s", scheme, parsed.registry, parsed.name)
	authHeader := resolveDockerAuth(parsed.registry)

	// Push the layer blob.
	layerDigest := hexDigest(layerData)
	if err := pushBlob(ctx, baseURL, layerDigest, layerData, authHeader); err != nil {
		return "", fmt.Errorf("ociPushLayer: push layer blob: %w", err)
	}

	// Push an empty config blob (required by OCI spec).
	emptyConfig := []byte("{}")
	configDigest := hexDigest(emptyConfig)
	if err := pushBlob(ctx, baseURL, configDigest, emptyConfig, authHeader); err != nil {
		return "", fmt.Errorf("ociPushLayer: push config blob: %w", err)
	}

	mfst := ociPushManifest{
		SchemaVersion: 2,
		MediaType:     "application/vnd.oci.image.manifest.v1+json",
		Config: ociPushDescriptor{
			MediaType: "application/vnd.oci.image.config.v1+json",
			Size:      int64(len(emptyConfig)),
			Digest:    configDigest,
		},
		Layers: []ociPushDescriptor{
			{
				MediaType: "application/vnd.oci.image.layer.v1.tar",
				Size:      int64(len(layerData)),
				Digest:    layerDigest,
			},
		},
	}

	mfstJSON, err := json.Marshal(mfst)
	if err != nil {
		return "", fmt.Errorf("ociPushLayer: marshal manifest: %w", err)
	}

	manifestDigest, err := pushManifest(ctx, baseURL, parsed.ref, mfstJSON, authHeader)
	if err != nil {
		return "", fmt.Errorf("ociPushLayer: push manifest: %w", err)
	}
	return manifestDigest, nil
}

// hexDigest returns "sha256:<hex>" for the given data.
func hexDigest(data []byte) string {
	sum := sha256.Sum256(data)
	return fmt.Sprintf("sha256:%x", sum)
}

// pushBlob uploads blob data to the registry using the Distribution Spec v2
// POST+PUT upload flow. Skips upload if the blob already exists (HEAD check).
// When authHeader is non-empty, it is sent as the Authorization: Basic header
// on all requests to the registry.
func pushBlob(ctx context.Context, baseURL, digest string, data []byte, authHeader string) error {
	headURL := baseURL + "/blobs/" + digest
	headReq, err := http.NewRequestWithContext(ctx, http.MethodHead, headURL, nil)
	if err != nil {
		return fmt.Errorf("pushBlob: HEAD request: %w", err)
	}
	if authHeader != "" {
		headReq.Header.Set("Authorization", "Basic "+authHeader)
	}
	resp, err := http.DefaultClient.Do(headReq)
	if err == nil {
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			return nil
		}
	}

	// POST to get upload URL.
	uploadURL := baseURL + "/blobs/uploads/"
	postReq, err := http.NewRequestWithContext(ctx, http.MethodPost, uploadURL, nil)
	if err != nil {
		return fmt.Errorf("pushBlob: POST upload start: %w", err)
	}
	if authHeader != "" {
		postReq.Header.Set("Authorization", "Basic "+authHeader)
	}
	postResp, err := http.DefaultClient.Do(postReq)
	if err != nil {
		return fmt.Errorf("pushBlob: POST upload start: %w", err)
	}
	postResp.Body.Close()
	if postResp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("pushBlob: unexpected POST status %d", postResp.StatusCode)
	}
	location := postResp.Header.Get("Location")
	if location == "" {
		return fmt.Errorf("pushBlob: POST response missing Location header")
	}

	// PUT to upload URL with digest query param.
	if strings.Contains(location, "?") {
		location += "&digest=" + digest
	} else {
		location += "?digest=" + digest
	}
	putReq, err := http.NewRequestWithContext(ctx, http.MethodPut, location, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("pushBlob: PUT blob: %w", err)
	}
	putReq.Header.Set("Content-Type", "application/octet-stream")
	if authHeader != "" {
		putReq.Header.Set("Authorization", "Basic "+authHeader)
	}
	putReq.ContentLength = int64(len(data))
	putResp, err := http.DefaultClient.Do(putReq)
	if err != nil {
		return fmt.Errorf("pushBlob: PUT blob: %w", err)
	}
	defer putResp.Body.Close()
	body, _ := io.ReadAll(putResp.Body)
	if putResp.StatusCode != http.StatusCreated && putResp.StatusCode != http.StatusOK {
		return fmt.Errorf("pushBlob: PUT to %s returned %d (check registry auth if 401/403): %s", location, putResp.StatusCode, string(body))
	}
	return nil
}

// pushManifest uploads a manifest to the registry under the given reference
// (tag or digest) and returns the manifest's sha256 digest. When authHeader is
// non-empty, it is sent as the Authorization: Basic header on the request.
func pushManifest(ctx context.Context, baseURL, ref string, mfstJSON []byte, authHeader string) (string, error) {
	manifestURL := baseURL + "/manifests/" + ref
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, manifestURL, bytes.NewReader(mfstJSON))
	if err != nil {
		return "", fmt.Errorf("pushManifest: build PUT request: %w", err)
	}
	req.Header.Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
	if authHeader != "" {
		req.Header.Set("Authorization", "Basic "+authHeader)
	}
	req.ContentLength = int64(len(mfstJSON))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("pushManifest: PUT manifest: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("pushManifest: PUT to %s returned %d (check registry auth if 401/403): %s", manifestURL, resp.StatusCode, string(body))
	}

	// Prefer the Docker-Content-Digest header. Fall back to computing it locally.
	if d := resp.Header.Get("Docker-Content-Digest"); d != "" {
		return d, nil
	}
	return hexDigest(mfstJSON), nil
}
