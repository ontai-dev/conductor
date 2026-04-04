// Package webhook — Conductor admission webhook handlers.
//
// SealedCausalChainWebhook rejects UPDATE requests that modify the spec.lineage
// field on any Seam-managed CRD. This enforces the immutability guarantee of the
// sealed causal chain: lineage is authored once at creation time and may never
// be altered. seam-core-schema.md §5, CLAUDE.md §14 Decision 1.
package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// SealedCausalChainWebhook implements the admission webhook handler that enforces
// lineage immutability. It is registered as a ValidatingAdmissionWebhook by the
// platform's webhook configuration manifest.
//
// Admission logic:
//   - Operation != UPDATE → allow (create/delete are not restricted by this webhook).
//   - spec.lineage absent in old object → allow (pre-lineage object, not enforced).
//   - spec.lineage changed from old to new → deny with HTTP 200 + Allowed=false.
//   - spec.lineage unchanged or absent in new object → allow.
type SealedCausalChainWebhook struct{}

// NewSealedCausalChainWebhook constructs a SealedCausalChainWebhook.
func NewSealedCausalChainWebhook() *SealedCausalChainWebhook {
	return &SealedCausalChainWebhook{}
}

// ServeHTTP handles admission review requests at the /validate/sealed-causal-chain path.
// It always responds with HTTP 200 — the admission decision is encoded in the
// AdmissionReview.Response.Allowed field per the Kubernetes webhook protocol.
func (w *SealedCausalChainWebhook) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(resp, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		writeError(resp, fmt.Errorf("read request body: %w", err))
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		writeError(resp, fmt.Errorf("decode AdmissionReview: %w", err))
		return
	}

	admissionResp := w.admit(review.Request)
	review.Response = admissionResp
	review.Response.UID = review.Request.UID

	out, err := json.Marshal(review)
	if err != nil {
		writeError(resp, fmt.Errorf("encode AdmissionReview response: %w", err))
		return
	}

	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	resp.Write(out) //nolint:errcheck
}

// admit evaluates the admission request and returns an AdmissionResponse.
// Always returns a non-nil response.
func (w *SealedCausalChainWebhook) admit(req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req == nil {
		return allow()
	}

	// Only enforce on UPDATE operations — CREATE is the lineage authoring path.
	if req.Operation != admissionv1.Update {
		return allow()
	}

	// Decode old and new objects as unstructured maps.
	var oldObj, newObj map[string]interface{}
	if err := json.Unmarshal(req.OldObject.Raw, &oldObj); err != nil {
		// Cannot parse old object — allow rather than block on parse error.
		return allow()
	}
	if err := json.Unmarshal(req.Object.Raw, &newObj); err != nil {
		return allow()
	}

	oldLineage := extractLineage(oldObj)
	if oldLineage == nil {
		// No lineage in old object — pre-lineage CRD, not enforced.
		return allow()
	}

	newLineage := extractLineage(newObj)

	if !reflect.DeepEqual(oldLineage, newLineage) {
		return deny("SealedCausalChain is immutable after creation: spec.lineage may not be modified (seam-core-schema.md §5)")
	}
	return allow()
}

// extractLineage extracts the spec.lineage field from an unstructured object.
// Returns nil if the field is absent.
func extractLineage(obj map[string]interface{}) interface{} {
	spec, ok := obj["spec"].(map[string]interface{})
	if !ok {
		return nil
	}
	lineage, ok := spec["lineage"]
	if !ok {
		return nil
	}
	return lineage
}

// allow returns an AdmissionResponse that permits the operation.
func allow() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: true,
	}
}

// deny returns an AdmissionResponse that rejects the operation with the given message.
func deny(message string) *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusForbidden,
			Message: message,
			Reason:  metav1.StatusReasonForbidden,
		},
	}
}

// writeError writes a plain text HTTP 500 response. Used only when the
// admission framework itself fails (parse error, etc.).
func writeError(resp http.ResponseWriter, err error) {
	http.Error(resp, err.Error(), http.StatusInternalServerError)
}

// WebhookServer wraps the admission webhook HTTP server lifecycle.
// TLS is required for Kubernetes admission webhooks. Cert and key paths are
// read from WEBHOOK_TLS_CERT_PATH and WEBHOOK_TLS_KEY_PATH environment variables.
type WebhookServer struct {
	inner *http.Server
}

// NewWebhookServer constructs a WebhookServer listening on addr with the given
// TLS cert and key. addr is typically ":8443". certFile and keyFile are paths
// to the TLS certificate and private key (PEM), typically from mounted Secrets.
func NewWebhookServer(addr, certFile, keyFile string) *WebhookServer {
	mux := http.NewServeMux()
	mux.Handle("/validate/sealed-causal-chain", NewSealedCausalChainWebhook())

	return &WebhookServer{
		inner: &http.Server{
			Addr:    addr,
			Handler: mux,
		},
	}
}

// Start starts the HTTPS server. Blocks until the server terminates or ctx
// is cancelled. Returns nil on clean shutdown via ctx cancellation.
func (s *WebhookServer) Start(ctx context.Context, certFile, keyFile string) error {
	errCh := make(chan error, 1)
	go func() {
		if err := s.inner.ListenAndServeTLS(certFile, keyFile); err != nil && err != http.ErrServerClosed {
			errCh <- err
		} else {
			errCh <- nil
		}
	}()

	select {
	case <-ctx.Done():
		return s.inner.Shutdown(context.Background())
	case err := <-errCh:
		return err
	}
}
