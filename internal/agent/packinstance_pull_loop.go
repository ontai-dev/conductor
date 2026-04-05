package agent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"
	"os"
	"strings"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/dynamic"
)

// PackInstancePullLoop pulls signed PackInstance artifacts from the management
// cluster (stored as Secrets by the SigningLoop), verifies their Ed25519
// signatures (INV-026), and creates or updates PackReceipt CRs on the local
// tenant cluster.
//
// It runs on tenant clusters only — gated on MGMT_KUBECONFIG_PATH being set,
// consistent with SnapshotPullLoop. conductor-schema.md §10, Gap 28.
//
// When pubKey is nil (bootstrap window mode, INV-020), artifacts are accepted
// without signature verification and PackReceipts are created with Verified=True.
// Real enforcement begins when the key is mounted.
type PackInstancePullLoop struct {
	mgmtClient  dynamic.Interface // management cluster client for listing Secrets
	localClient dynamic.Interface // local cluster client for PackReceipt CRs
	pubKey      ed25519.PublicKey // nil during bootstrap window (INV-020)
	clusterName string            // this cluster's name — used to scope Secret lookups
	namespace   string            // local namespace for PackReceipt CRs (ont-system)
}

// NewPackInstancePullLoop constructs a PackInstancePullLoop in bootstrap window
// mode (INV-020). No signature enforcement is applied until a key is mounted.
func NewPackInstancePullLoop(
	mgmtClient, localClient dynamic.Interface,
	clusterName, namespace string,
) *PackInstancePullLoop {
	return &PackInstancePullLoop{
		mgmtClient:  mgmtClient,
		localClient: localClient,
		clusterName: clusterName,
		namespace:   namespace,
	}
}

// NewPackInstancePullLoopWithKey constructs a PackInstancePullLoop that enforces
// INV-026 Ed25519 signature verification. publicKeyPath is the file path to a
// PKIX PEM-encoded Ed25519 public key, typically mounted from a Kubernetes
// Secret volume.
func NewPackInstancePullLoopWithKey(
	mgmtClient, localClient dynamic.Interface,
	clusterName, namespace, publicKeyPath string,
) (*PackInstancePullLoop, error) {
	keyBytes, err := os.ReadFile(publicKeyPath)
	if err != nil {
		return nil, fmt.Errorf("NewPackInstancePullLoopWithKey: read public key %s: %w", publicKeyPath, err)
	}
	pubKey, err := parseEd25519PublicKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("NewPackInstancePullLoopWithKey: parse Ed25519 public key: %w", err)
	}
	return &PackInstancePullLoop{
		mgmtClient:  mgmtClient,
		localClient: localClient,
		pubKey:      pubKey,
		clusterName: clusterName,
		namespace:   namespace,
	}, nil
}

// Run runs the pull loop until ctx is cancelled. It fires once immediately
// then repeats on interval. interval must be positive (> 0).
// conductor-schema.md §10, Gap 28.
func (l *PackInstancePullLoop) Run(ctx context.Context, interval time.Duration) {
	l.pullOnce(ctx)

	if ctx.Err() != nil {
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.pullOnce(ctx)
		}
	}
}

// pullOnce performs a single pull cycle: list all signed artifact Secrets in
// seam-tenant-{clusterName} on the management cluster, verify each Ed25519
// signature (INV-026), and create or update the corresponding PackReceipt on
// the local cluster. conductor-schema.md §10.
func (l *PackInstancePullLoop) pullOnce(ctx context.Context) {
	secretNS := fmt.Sprintf("seam-tenant-%s", l.clusterName)
	namePrefix := fmt.Sprintf("seam-pack-signed-%s-", l.clusterName)

	list, err := l.mgmtClient.Resource(secretGVR).Namespace(secretNS).List(ctx, metav1.ListOptions{})
	if err != nil {
		// Management cluster connectivity loss — log and retry next cycle.
		// Local PackReceipts continue to reflect the last verified state.
		fmt.Printf("packinstance pull loop: cluster=%q list Secrets in %s: connectivity error: %v\n",
			l.clusterName, secretNS, err)
		return
	}

	for _, item := range list.Items {
		secretName := item.GetName()
		if !strings.HasPrefix(secretName, namePrefix) {
			// Secret not produced by the signing loop for this cluster — skip.
			continue
		}

		// Extract PackInstance name from the Secret name suffix.
		packInstanceName := strings.TrimPrefix(secretName, namePrefix)

		// Read artifact and signature from Secret data.
		data, _, _ := unstructuredNestedMap(item.Object, "data")
		artifactB64, _ := data["artifact"].(string)
		sigB64, _ := data["signature"].(string)

		if artifactB64 == "" || sigB64 == "" {
			fmt.Printf("packinstance pull loop: cluster=%q secret=%q missing artifact or signature field\n",
				l.clusterName, secretName)
			continue
		}

		// Decode artifact from base64 back to raw JSON bytes.
		artifactJSON, err := base64.StdEncoding.DecodeString(artifactB64)
		if err != nil {
			fmt.Printf("packinstance pull loop: cluster=%q secret=%q decode artifact base64: %v\n",
				l.clusterName, secretName, err)
			continue
		}

		// Verify Ed25519 signature. INV-026. Bootstrap window accepts all. INV-020.
		verified, failureReason := l.verifyArtifact(artifactJSON, sigB64)

		// Create or update the local PackReceipt.
		l.upsertPackReceipt(ctx, packInstanceName, sigB64, secretName, verified, failureReason)
	}
}

// verifyArtifact verifies the Ed25519 signature of the artifact bytes.
// Returns (true, "") on success or (false, reason) on failure.
//
// Bootstrap window mode (pubKey == nil) accepts all artifacts without
// verification and returns (true, ""). INV-020.
func (l *PackInstancePullLoop) verifyArtifact(artifactJSON []byte, sigB64 string) (bool, string) {
	// Bootstrap window mode — key not yet mounted. Accept all artifacts. INV-020.
	if l.pubKey == nil {
		return true, ""
	}

	// Normal operation — key mounted; enforce INV-026.
	if sigB64 == "" {
		return false, "missing signature (INV-026)"
	}

	sigBytes, err := base64.StdEncoding.DecodeString(sigB64)
	if err != nil {
		return false, fmt.Sprintf("decode signature base64: %v", err)
	}

	if !ed25519.Verify(l.pubKey, artifactJSON, sigBytes) {
		return false, "Ed25519 signature verification failed (INV-026)"
	}
	return true, ""
}

// upsertPackReceipt creates or updates a PackReceipt CR on the local cluster
// recording the verification result for the given PackInstance artifact.
//
// Idempotency: if the existing PackReceipt already carries the same verified
// status and signature reference, the call is a no-op.
//
// Gap 28, conductor-schema.md §10.
func (l *PackInstancePullLoop) upsertPackReceipt(
	ctx context.Context,
	packInstanceName, sigB64, signatureRef string,
	verified bool,
	failureReason string,
) {
	receiptName := packInstanceName

	specPayload := map[string]interface{}{
		"packInstanceRef": packInstanceName,
		"signatureRef":    signatureRef,
	}
	statusPayload := map[string]interface{}{
		"verified":  verified,
		"signature": sigB64,
	}
	if !verified && failureReason != "" {
		statusPayload["verificationFailedReason"] = failureReason
	}

	receipt := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infra.ontai.dev/v1alpha1",
			"kind":       "PackReceipt",
			"metadata": map[string]interface{}{
				"name":      receiptName,
				"namespace": l.namespace,
			},
			"spec":   specPayload,
			"status": statusPayload,
		},
	}

	// Try to get existing PackReceipt.
	existing, err := l.localClient.Resource(packReceiptGVR).Namespace(l.namespace).Get(
		ctx, receiptName, metav1.GetOptions{},
	)
	if k8serrors.IsNotFound(err) {
		// Does not exist — create it.
		if _, err := l.localClient.Resource(packReceiptGVR).Namespace(l.namespace).Create(
			ctx, receipt, metav1.CreateOptions{},
		); err != nil {
			fmt.Printf("packinstance pull loop: cluster=%q create PackReceipt %q: %v\n",
				l.clusterName, receiptName, err)
		} else {
			fmt.Printf("packinstance pull loop: cluster=%q created PackReceipt %q verified=%v\n",
				l.clusterName, receiptName, verified)
		}
		return
	}
	if err != nil {
		// Real Get error — log and skip; next cycle retries.
		fmt.Printf("packinstance pull loop: cluster=%q get PackReceipt %q: %v\n",
			l.clusterName, receiptName, err)
		return
	}

	// Exists — check idempotency: compare verified status and signature.
	existingStatus, _, _ := unstructuredNestedMap(existing.Object, "status")
	existingVerified, _ := existingStatus["verified"].(bool)
	existingSig, _ := existingStatus["signature"].(string)
	if existingVerified == verified && existingSig == sigB64 {
		// Content unchanged — skip update. Gap 28 idempotency.
		return
	}

	// Content differs — update the PackReceipt.
	receipt.SetResourceVersion(existing.GetResourceVersion())
	if _, err := l.localClient.Resource(packReceiptGVR).Namespace(l.namespace).Update(
		ctx, receipt, metav1.UpdateOptions{},
	); err != nil {
		fmt.Printf("packinstance pull loop: cluster=%q update PackReceipt %q: %v\n",
			l.clusterName, receiptName, err)
	} else {
		fmt.Printf("packinstance pull loop: cluster=%q updated PackReceipt %q verified=%v\n",
			l.clusterName, receiptName, verified)
	}
}
