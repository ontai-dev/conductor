// Package capability — concrete client adapter implementations for production use.
//
// Each adapter implements one of the client interfaces in clients.go.
// Adapters are wired in main.go runExecute(). Unit tests use stub doubles.
//
// conductor-schema.md §10, conductor-design.md §5.
package capability

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/dynamic"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	talos_client "github.com/siderolabs/talos/pkg/machinery/client"
	machineapi "github.com/siderolabs/talos/pkg/machinery/api/machine"
	sigsyaml "sigs.k8s.io/yaml"
)

// ── GuardianIntakeClientAdapter ──────────────────────────────────────────────

// rbacProfileGVR is the GroupVersionResource for guardian RBACProfile.
// security.ontai.dev/v1alpha1/rbacprofiles -- guardian-schema.md §7.
var rbacProfileGVR = schema.GroupVersionResource{
	Group:    "security.ontai.dev",
	Version:  "v1alpha1",
	Resource: "rbacprofiles",
}

// rbacProfileProvisionedTimeout is the default time to wait for an RBACProfile
// to reach provisioned=true after submitting the RBAC layer to guardian intake.
const rbacProfileProvisionedTimeout = 120 * time.Second

// rbacProfilePollInterval is the frequency at which WaitForRBACProfileProvisioned
// polls the management cluster for RBACProfile status.
const rbacProfilePollInterval = 5 * time.Second

// GuardianIntakeClientAdapter implements GuardianIntakeClient for production use.
// It calls guardian's /rbac-intake/pack HTTP endpoint for RBAC layer submission
// and polls the management cluster for RBACProfile provisioned status.
// INV-004, wrapper-schema.md §4.
type GuardianIntakeClientAdapter struct {
	httpClient       *http.Client
	guardianBaseURL  string
	managementClient dynamic.Interface
}

// NewGuardianIntakeClientAdapter creates a GuardianIntakeClientAdapter.
// guardianBaseURL is the base URL of the guardian service (e.g., "https://guardian.seam-system.svc:443").
// managementClient is a dynamic client for the management cluster, used to poll RBACProfile status.
// httpClient is optional; nil uses the default http.Client.
func NewGuardianIntakeClientAdapter(guardianBaseURL string, managementClient dynamic.Interface, httpClient *http.Client) *GuardianIntakeClientAdapter {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &GuardianIntakeClientAdapter{
		httpClient:       httpClient,
		guardianBaseURL:  strings.TrimRight(guardianBaseURL, "/"),
		managementClient: managementClient,
	}
}

// SubmitPackRBACLayer POSTs YAML manifests from the RBAC OCI layer to guardian
// /rbac-intake/pack. Returns the count of wrapped resources on success.
func (a *GuardianIntakeClientAdapter) SubmitPackRBACLayer(ctx context.Context, componentName string, manifests []string, targetCluster string) (int, error) {
	body, err := json.Marshal(map[string]interface{}{
		"componentName": componentName,
		"manifests":     manifests,
		"targetCluster": targetCluster,
	})
	if err != nil {
		return 0, fmt.Errorf("marshal pack intake request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.guardianBaseURL+"/rbac-intake/pack", bytes.NewReader(body))
	if err != nil {
		return 0, fmt.Errorf("build pack intake request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return 0, fmt.Errorf("POST /rbac-intake/pack: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("POST /rbac-intake/pack returned HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var result struct {
		Wrapped int `json:"wrapped"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return 0, fmt.Errorf("decode pack intake response: %w", err)
	}
	return result.Wrapped, nil
}

// WaitForRBACProfileProvisioned polls the management cluster until the RBACProfile
// for componentName in seam-tenant-{targetCluster} namespace reaches provisioned=true,
// or until the default timeout elapses. guardian-schema.md §7, INV-004.
func (a *GuardianIntakeClientAdapter) WaitForRBACProfileProvisioned(ctx context.Context, targetCluster, componentName string) error {
	return WaitForRBACProfileProvisioned(
		ctx,
		a.managementClient,
		componentName,
		"seam-tenant-"+targetCluster,
		rbacProfileProvisionedTimeout,
		rbacProfilePollInterval,
	)
}

// WaitForRBACProfileProvisioned polls kubeClient until the RBACProfile named
// rbacProfileName in namespace reaches a Provisioned=True status condition, or
// until timeout elapses. NotFound is retried; any other API error is fatal.
// pollInterval controls how frequently the profile is re-fetched.
// guardian-schema.md §7.
func WaitForRBACProfileProvisioned(
	ctx context.Context,
	kubeClient dynamic.Interface,
	rbacProfileName string,
	namespace string,
	timeout time.Duration,
	pollInterval time.Duration,
) error {
	deadline := time.Now().Add(timeout)
	for {
		profile, err := kubeClient.Resource(rbacProfileGVR).
			Namespace(namespace).
			Get(ctx, rbacProfileName, metav1.GetOptions{})
		if err != nil {
			if apierrors.IsNotFound(err) {
				if time.Now().After(deadline) {
					return fmt.Errorf("RBACProfile %s/%s did not reach provisioned=true within %s",
						namespace, rbacProfileName, timeout)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(pollInterval):
					continue
				}
			}
			return fmt.Errorf("get RBACProfile %s/%s: %w", namespace, rbacProfileName, err)
		}

		// Accept provisioned=true either via the Provisioned condition or the
		// status.provisioned boolean (guardian may set the boolean without updating
		// the condition when the RBACPolicy arrives asynchronously).
		if provisioned, _, _ := unstructured.NestedBool(profile.Object, "status", "provisioned"); provisioned {
			return nil
		}
		conditions, _, _ := unstructuredList(profile.Object, "status", "conditions")
		for _, c := range conditions {
			cond, ok := c.(map[string]interface{})
			if !ok {
				continue
			}
			if cond["type"] == "Provisioned" && cond["status"] == "True" {
				return nil
			}
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("RBACProfile %s/%s did not reach provisioned=true within %s",
				namespace, rbacProfileName, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// ── TalosClientAdapter ───────────────────────────────────────────────────────

// TalosClientAdapter wraps the Talos machinery *client.Client and implements
// TalosNodeClient. It reads the talosconfig from the path passed at construction
// time — typically the path to a mounted Secret volume in a Kueue Job pod.
// INV-013: talos goclient is executor/agent mode only.
type TalosClientAdapter struct {
	inner *talos_client.Client
	nodes []string // endpoint IPs parsed from talosconfig at construction time
}

// NewTalosClientAdapter creates a TalosClientAdapter by reading the talosconfig
// from talosconfigPath. Returns an error if the config cannot be read or the
// gRPC connection cannot be established.
func NewTalosClientAdapter(ctx context.Context, talosconfigPath string) (*TalosClientAdapter, error) {
	nodes, _ := EndpointsFromTalosconfig(talosconfigPath) // empty slice on parse error
	c, err := talos_client.New(ctx, talos_client.WithConfigFromFile(talosconfigPath))
	if err != nil {
		return nil, fmt.Errorf("TalosClientAdapter: open talosconfig %s: %w", talosconfigPath, err)
	}
	return &TalosClientAdapter{inner: c, nodes: nodes}, nil
}

// Bootstrap bootstraps etcd on the first control plane node.
func (a *TalosClientAdapter) Bootstrap(ctx context.Context) error {
	return a.inner.Bootstrap(ctx, &machineapi.BootstrapRequest{})
}

// applyConfigMode maps the human-readable mode string used in RunnerConfig
// parameters to the Talos protobuf enum value.
func applyConfigMode(mode string) (machineapi.ApplyConfigurationRequest_Mode, error) {
	switch strings.ToLower(mode) {
	case "reboot":
		return machineapi.ApplyConfigurationRequest_REBOOT, nil
	case "auto":
		return machineapi.ApplyConfigurationRequest_AUTO, nil
	case "no-reboot":
		return machineapi.ApplyConfigurationRequest_NO_REBOOT, nil
	case "staged":
		return machineapi.ApplyConfigurationRequest_STAGED, nil
	default:
		return 0, fmt.Errorf("unknown apply-configuration mode %q; must be reboot, auto, no-reboot, or staged", mode)
	}
}

// ApplyConfiguration applies configBytes to the node using the given mode.
func (a *TalosClientAdapter) ApplyConfiguration(ctx context.Context, configBytes []byte, mode string) error {
	modeEnum, err := applyConfigMode(mode)
	if err != nil {
		return err
	}
	_, err = a.inner.ApplyConfiguration(ctx, &machineapi.ApplyConfigurationRequest{
		Data: configBytes,
		Mode: modeEnum,
	})
	return err
}

// Nodes returns the endpoint IPs parsed from the talosconfig at construction time.
func (a *TalosClientAdapter) Nodes() []string { return a.nodes }

// Health calls the Talos Version RPC as a lightweight liveness check.
func (a *TalosClientAdapter) Health(ctx context.Context) error {
	_, err := a.inner.Version(ctx)
	return err
}

// Upgrade upgrades the Talos OS to the given installer image.
// force is always false — forced upgrades require explicit operator intervention
// beyond what the capability parameter schema supports.
func (a *TalosClientAdapter) Upgrade(ctx context.Context, image string, stage bool) error {
	_, err := a.inner.Upgrade(ctx, image, stage, false)
	return err
}

// Reboot reboots the node.
func (a *TalosClientAdapter) Reboot(ctx context.Context) error {
	return a.inner.Reboot(ctx)
}

// Reset performs a factory reset of the node. reboot is always false;
// the caller controls any subsequent reboot via a separate Reboot capability.
func (a *TalosClientAdapter) Reset(ctx context.Context, graceful bool) error {
	return a.inner.Reset(ctx, graceful, false)
}

// EtcdSnapshot takes an etcd snapshot and writes it to w.
func (a *TalosClientAdapter) EtcdSnapshot(ctx context.Context, w io.Writer) error {
	rc, err := a.inner.EtcdSnapshot(ctx, &machineapi.EtcdSnapshotRequest{})
	if err != nil {
		return fmt.Errorf("EtcdSnapshot: %w", err)
	}
	defer rc.Close()
	if _, err := io.Copy(w, rc); err != nil {
		return fmt.Errorf("EtcdSnapshot: copy snapshot data: %w", err)
	}
	return nil
}

// EtcdRecover recovers etcd from the snapshot in r.
func (a *TalosClientAdapter) EtcdRecover(ctx context.Context, r io.Reader) error {
	_, err := a.inner.EtcdRecover(ctx, r)
	return err
}

// EtcdDefragment defrags the etcd database on this node.
func (a *TalosClientAdapter) EtcdDefragment(ctx context.Context) error {
	_, err := a.inner.EtcdDefragment(ctx)
	return err
}

// GetMachineConfig reads the running machine config from /system/state/config.yaml
// on the Talos node. Returns raw YAML bytes. Used after node operations to capture
// the post-operation config state.
func (a *TalosClientAdapter) GetMachineConfig(ctx context.Context) ([]byte, error) {
	rc, err := a.inner.Read(ctx, "/system/state/config.yaml")
	if err != nil {
		return nil, fmt.Errorf("GetMachineConfig: read /system/state/config.yaml: %w", err)
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, fmt.Errorf("GetMachineConfig: read response: %w", err)
	}
	return data, nil
}

// Kubeconfig generates a fresh admin kubeconfig from the cluster Talos API.
// Returns raw kubeconfig YAML bytes signed by the cluster Kubernetes CA.
// Used by pkiRotateHandler to refresh the stored kubeconfig Secret after a
// PKI rotation cycle. platform-schema.md §13.
func (a *TalosClientAdapter) Kubeconfig(ctx context.Context) ([]byte, error) {
	b, err := a.inner.Kubeconfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("Kubeconfig: %w", err)
	}
	return b, nil
}

// Close releases the underlying gRPC connection.
func (a *TalosClientAdapter) Close() error {
	return a.inner.Close()
}

// ── S3StorageClient ───────────────────────────────────────────────────────────

// S3StorageClientAdapter implements StorageClient against an S3-compatible
// object store. Constructor reads S3_ENDPOINT (optional) and S3_REGION
// (required) from the environment.
type S3StorageClientAdapter struct {
	inner *s3.Client
}

// NewS3StorageClientAdapter creates an S3StorageClientAdapter from the environment.
// S3_REGION must be set. S3_ENDPOINT is optional; when set it overrides the AWS
// regional endpoint (e.g., for MinIO or a local S3 gateway).
func NewS3StorageClientAdapter(ctx context.Context) (*S3StorageClientAdapter, error) {
	region := os.Getenv("S3_REGION")
	if region == "" {
		return nil, fmt.Errorf("S3StorageClientAdapter: S3_REGION environment variable not set")
	}

	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("S3StorageClientAdapter: load AWS config: %w", err)
	}

	var opts []func(*s3.Options)
	if endpoint := os.Getenv("S3_ENDPOINT"); endpoint != "" {
		opts = append(opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(endpoint)
			o.UsePathStyle = true
		})
	}

	return &S3StorageClientAdapter{inner: s3.NewFromConfig(cfg, opts...)}, nil
}

// Upload streams r to bucket/key.
func (a *S3StorageClientAdapter) Upload(ctx context.Context, bucket, key string, r io.Reader) error {
	_, err := a.inner.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   r,
	})
	if err != nil {
		return fmt.Errorf("S3 Upload %s/%s: %w", bucket, key, err)
	}
	return nil
}

// Download streams bucket/key into the returned ReadCloser. Caller must close.
func (a *S3StorageClientAdapter) Download(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	out, err := a.inner.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("S3 Download %s/%s: %w", bucket, key, err)
	}
	return out.Body, nil
}

// ── OCIRegistryClientAdapter ──────────────────────────────────────────────────

// OCIRegistryClientAdapter implements OCIRegistryClient against any OCI
// Distribution Spec-compliant registry using plain HTTP or HTTPS.
// Uses net/http only — no third-party OCI library required.
type OCIRegistryClientAdapter struct {
	httpClient *http.Client
}

// NewOCIRegistryClientAdapter creates an OCIRegistryClientAdapter with a default
// HTTP client.
func NewOCIRegistryClientAdapter() *OCIRegistryClientAdapter {
	return &OCIRegistryClientAdapter{httpClient: &http.Client{}}
}

// NewOCIRegistryClientAdapterWithHTTPClient creates an OCIRegistryClientAdapter
// that uses the provided HTTP client. Intended for use in tests where a
// *httptest.Server client is injected to route requests to the test server.
func NewOCIRegistryClientAdapterWithHTTPClient(c *http.Client) *OCIRegistryClientAdapter {
	return &OCIRegistryClientAdapter{httpClient: c}
}

// ociRef holds the parsed components of an OCI image reference.
type ociRef struct {
	registry string // e.g., "registry.example.com" or "10.20.0.1:5000"
	name     string // e.g., "ontai-dev/myplugin"
	ref      string // tag or digest, e.g., "v1.2.3" or "sha256:abc123"
}

// parseOCIRef parses a reference string of the form
// "registry/name:tag" or "registry/name@sha256:digest" into its components.
func parseOCIRef(reference string) (ociRef, error) {
	// Split off the registry (first path component that contains a colon or dot
	// before the first slash — standard OCI reference grammar).
	slash := strings.Index(reference, "/")
	if slash < 0 {
		return ociRef{}, fmt.Errorf("parseOCIRef: malformed reference %q: no slash found", reference)
	}
	registry := reference[:slash]
	remainder := reference[slash+1:]

	// Separate digest (@sha256:...) from tag (:tag).
	if idx := strings.Index(remainder, "@"); idx >= 0 {
		namePart := remainder[:idx]
		// Strip any trailing tag (:tag) from the name. When a digest is present the
		// tag is redundant and must not appear in the registry URL path — the path
		// segment after the repository name must go directly to /manifests/{digest}.
		// e.g. "ontai-dev/test-pack:v0.1.0@sha256:..." → name="ontai-dev/test-pack"
		if colonIdx := strings.LastIndex(namePart, ":"); colonIdx >= 0 {
			namePart = namePart[:colonIdx]
		}
		return ociRef{
			registry: registry,
			name:     namePart,
			ref:      remainder[idx+1:], // e.g., "sha256:abc123"
		}, nil
	}
	if idx := strings.LastIndex(remainder, ":"); idx >= 0 {
		return ociRef{
			registry: registry,
			name:     remainder[:idx],
			ref:      remainder[idx+1:],
		}, nil
	}
	return ociRef{}, fmt.Errorf("parseOCIRef: malformed reference %q: no tag or digest", reference)
}

// ociManifest is a minimal OCI Image Manifest (schema v2) for JSON decoding.
type ociManifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	MediaType     string `json:"mediaType"`
	// Layers contains the content blobs. For pack images each layer is one
	// Kubernetes manifest file (YAML or JSON).
	Layers []struct {
		Digest    string `json:"digest"`
		MediaType string `json:"mediaType"`
		Size      int64  `json:"size"`
	} `json:"layers"`
}

// PullManifests fetches all manifest bytes from the given OCI reference.
// Each element of the returned slice is one Kubernetes manifest (YAML or JSON)
// stored as a layer blob in the OCI image.
func (a *OCIRegistryClientAdapter) PullManifests(ctx context.Context, ref string) ([][]byte, error) {
	parsed, err := parseOCIRef(ref)
	if err != nil {
		return nil, err
	}

	// Determine scheme — use http:// for local registry addresses (no TLS).
	scheme := "https"
	if strings.HasPrefix(parsed.registry, "10.") ||
		strings.HasPrefix(parsed.registry, "localhost") ||
		strings.HasPrefix(parsed.registry, "127.") {
		scheme = "http"
	}

	manifestURL := fmt.Sprintf("%s://%s/v2/%s/manifests/%s",
		scheme, parsed.registry, parsed.name, parsed.ref)

	mfst, err := a.fetchManifest(ctx, manifestURL)
	if err != nil {
		return nil, fmt.Errorf("PullManifests: fetch manifest: %w", err)
	}

	if len(mfst.Layers) == 0 {
		return nil, fmt.Errorf("PullManifests: OCI image %s has no layers", ref)
	}

	var results [][]byte
	for _, layer := range mfst.Layers {
		blobURL := fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
			scheme, parsed.registry, parsed.name, layer.Digest)
		data, err := a.fetchBlob(ctx, blobURL)
		if err != nil {
			return nil, fmt.Errorf("PullManifests: fetch blob %s: %w", layer.Digest, err)
		}
		results = append(results, data)
	}
	return results, nil
}

// fetchManifest fetches and decodes an OCI image manifest from manifestURL.
func (a *OCIRegistryClientAdapter) fetchManifest(ctx context.Context, manifestURL string) (*ociManifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, manifestURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept",
		"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json")

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", manifestURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned HTTP %d", manifestURL, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read manifest body: %w", err)
	}

	var mfst ociManifest
	if err := json.Unmarshal(body, &mfst); err != nil {
		return nil, fmt.Errorf("decode manifest JSON: %w", err)
	}
	return &mfst, nil
}

// fetchBlob downloads a blob from blobURL and returns its bytes.
func (a *OCIRegistryClientAdapter) fetchBlob(ctx context.Context, blobURL string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, blobURL, nil)
	if err != nil {
		return nil, err
	}

	resp, err := a.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", blobURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("GET %s returned HTTP %d", blobURL, resp.StatusCode)
	}

	var buf bytes.Buffer
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return nil, fmt.Errorf("read blob body: %w", err)
	}
	return buf.Bytes(), nil
}

// ── Per-node Talos helpers ────────────────────────────────────────────────────

// talosConfigFile is the minimal structure needed to read endpoints from a talosconfig.
// The talosconfig format is defined by the Talos machinery client.
type talosConfigFile struct {
	Context  string                       `json:"context"`
	Contexts map[string]talosConfigCtx    `json:"contexts"`
}

type talosConfigCtx struct {
	// ClusterEndpoint is the virtual IP or load-balancer address that routes to
	// the control-plane nodes. When set, EndpointsFromTalosconfig filters it from
	// the returned node list so that per-node operations are never sent to the VIP.
	// Populate this field in the talosconfig when the endpoints list contains the
	// cluster VIP alongside individual node IPs.
	ClusterEndpoint string   `json:"clusterEndpoint,omitempty"`
	Endpoints       []string `json:"endpoints"`
	Nodes           []string `json:"nodes"`
}

// EndpointsFromTalosconfig parses the talosconfig at path and returns the list
// of individual node targets for the active context.
//
// Priority order:
//  1. ctx.nodes — explicit physical node IPs, returned as-is (no VIP expected).
//  2. ctx.endpoints filtered by ctx.clusterEndpoint — removes the virtual IP or
//     load-balancer entry so that per-node operations (GetMachineConfig,
//     ApplyConfiguration) target only real nodes and never the VIP.
//
// If clusterEndpoint filtering removes every endpoint, an error is returned:
// the caller must either add ctx.nodes or correct the endpoints list so that
// at least one individual node IP remains after filtering.
func EndpointsFromTalosconfig(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read talosconfig %s: %w", path, err)
	}
	var cfg talosConfigFile
	if err := sigsyaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse talosconfig: %w", err)
	}
	ctx, ok := cfg.Contexts[cfg.Context]
	if !ok {
		return nil, fmt.Errorf("talosconfig context %q not found", cfg.Context)
	}
	// Explicit node list takes precedence; individual node IPs never include the VIP.
	if len(ctx.Nodes) > 0 {
		return ctx.Nodes, nil
	}
	if len(ctx.Endpoints) == 0 {
		return nil, fmt.Errorf("talosconfig context %q has no endpoints or nodes", cfg.Context)
	}
	// Filter the cluster endpoint (VIP) from the fallback endpoints list.
	// Sending per-node Talos operations to the VIP address causes GetMachineConfig
	// to read from the current VIP holder and ApplyConfiguration to target only
	// that node, silently skipping all other control-plane nodes.
	if ctx.ClusterEndpoint != "" {
		var filtered []string
		for _, ep := range ctx.Endpoints {
			if ep != ctx.ClusterEndpoint {
				filtered = append(filtered, ep)
			}
		}
		if len(filtered) == 0 {
			return nil, fmt.Errorf("talosconfig context %q: after filtering cluster endpoint %q, no node IPs remain; add individual node IPs to ctx.nodes", cfg.Context, ctx.ClusterEndpoint)
		}
		return filtered, nil
	}
	return ctx.Endpoints, nil
}

// NodeContext returns a context that targets exactly one Talos node. The context
// is passed to GetMachineConfig and ApplyConfiguration so the Talos RPC
// infrastructure routes each call to the specified node only. This is the
// correct way to perform per-node operations when the Talos client has multiple
// endpoints configured -- without it, the client targets all endpoints and
// GetMachineConfig returns only the first responding node's config while
// ApplyConfiguration applies to every node.
func NodeContext(ctx context.Context, nodeIP string) context.Context {
	return talos_client.WithNode(ctx, nodeIP)
}
