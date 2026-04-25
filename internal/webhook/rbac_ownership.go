package webhook

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Annotation constants for RBAC ownership enforcement on tenant clusters.
// Must match guardian/internal/webhook/decision.go exactly.
// guardian-schema.md §5, CS-INV-001.
const (
	rbacOwnerAnnotation      = "ontai.dev/rbac-owner"
	rbacOwnerAnnotationValue = "guardian"
)

// rbacInterceptedKinds is the set of resource kinds subject to RBAC ownership
// enforcement. Matches guardian's InterceptedKinds set. guardian-schema.md §5.
var rbacInterceptedKinds = map[string]bool{
	"Role":               true,
	"ClusterRole":        true,
	"RoleBinding":        true,
	"ClusterRoleBinding": true,
	"ServiceAccount":     true,
}

// TenantRBACOwnershipWebhook enforces the ontai.dev/rbac-owner=guardian annotation
// on RBAC resources on tenant clusters. Mirrors the management cluster Guardian
// webhook behavior from the Conductor agent process.
//
// In audit mode (EnforcementGate.IsStrict()==false): logs resources lacking the
// annotation but admits them. Used during the bootstrap sweep phase.
//
// In strict mode (EnforcementGate.IsStrict()==true): rejects resources lacking
// the annotation. Active after the sweep + profile creation complete.
//
// guardian-schema.md §5, conductor-schema.md §15. CS-INV-001.
type TenantRBACOwnershipWebhook struct {
	gate *EnforcementGate
}

// NewTenantRBACOwnershipWebhook constructs a TenantRBACOwnershipWebhook backed by
// the given EnforcementGate. The webhook starts in audit mode until gate.SetStrict()
// is called by TenantBootstrapSweep.
func NewTenantRBACOwnershipWebhook(gate *EnforcementGate) *TenantRBACOwnershipWebhook {
	return &TenantRBACOwnershipWebhook{gate: gate}
}

// ServeHTTP handles admission review requests at /validate/rbac-ownership.
// Always responds with HTTP 200 — the decision is encoded in Allowed.
func (w *TenantRBACOwnershipWebhook) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if req.Method != http.MethodPost {
		http.Error(resp, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		writeAdmissionError(resp, fmt.Errorf("read request body: %w", err))
		return
	}

	var review admissionv1.AdmissionReview
	if err := json.Unmarshal(body, &review); err != nil {
		writeAdmissionError(resp, fmt.Errorf("decode AdmissionReview: %w", err))
		return
	}

	review.Response = w.admit(req.Context(), review.Request)
	review.Response.UID = review.Request.UID

	resp.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(resp).Encode(review); err != nil {
		fmt.Printf("rbac-ownership webhook: encode response: %v\n", err)
	}
}

// admit evaluates the admission request and returns the decision.
func (w *TenantRBACOwnershipWebhook) admit(_ context.Context, req *admissionv1.AdmissionRequest) *admissionv1.AdmissionResponse {
	if req == nil {
		return allowAdmission()
	}

	// Non-intercepted kind: always admit.
	if !rbacInterceptedKinds[req.Kind.Kind] {
		return allowAdmission()
	}

	// Kubernetes system namespaces and known exempt namespaces are never blocked.
	if isExemptNamespace(req) {
		return allowAdmission()
	}

	// Cluster-scoped resources (ClusterRole, ClusterRoleBinding) have empty
	// namespace. Kubernetes built-in names start with "system:" — these must
	// never be blocked regardless of enforcement mode (upgrades, bootstrapping).
	if req.Namespace == "" && strings.HasPrefix(req.Name, "system:") {
		return allowAdmission()
	}

	// Check for ownership annotation.
	annotations := extractAnnotations(req.Object.Raw)
	if annotations[rbacOwnerAnnotation] == rbacOwnerAnnotationValue {
		return allowAdmission()
	}

	// Resource lacks ownership annotation.
	reason := fmt.Sprintf(
		"resource %s/%s must carry annotation %s=%s; "+
			"all RBAC resources on this cluster are governed by guardian "+
			"(CS-INV-001, conductor-schema.md §15)",
		req.Kind.Kind, req.Name, rbacOwnerAnnotation, rbacOwnerAnnotationValue,
	)

	if !w.gate.IsStrict() {
		// Audit mode: log and admit.
		fmt.Printf("rbac-ownership webhook [AUDIT]: %s\n", reason)
		return allowAdmission()
	}

	// Strict mode: reject.
	return &admissionv1.AdmissionResponse{
		Allowed: false,
		Result: &metav1.Status{
			Code:    http.StatusForbidden,
			Message: reason,
		},
	}
}

// extractAnnotations parses raw object JSON and returns its metadata.annotations map.
// Returns an empty map on any parse error (safe — unannotated resources are governed).
func extractAnnotations(raw []byte) map[string]string {
	if len(raw) == 0 {
		return nil
	}
	var obj struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil
	}
	return obj.Metadata.Annotations
}

// isExemptNamespace returns true if the request's namespace is a well-known
// Kubernetes or Seam system namespace. These namespaces are never subject to
// RBAC ownership enforcement — Kubernetes and platform bootstrap RBAC must land
// unconditionally. Cluster-scoped resources (empty namespace) are NOT exempted
// here; the system: prefix check in admit() covers those.
func isExemptNamespace(req *admissionv1.AdmissionRequest) bool {
	switch req.Namespace {
	case "kube-system", "kube-public", "kube-node-lease",
		"ont-system", "seam-system":
		return true
	}
	return false
}

func allowAdmission() *admissionv1.AdmissionResponse {
	return &admissionv1.AdmissionResponse{Allowed: true}
}

func writeAdmissionError(resp http.ResponseWriter, err error) {
	review := admissionv1.AdmissionReview{
		Response: &admissionv1.AdmissionResponse{
			Allowed: false,
			Result: &metav1.Status{
				Code:    http.StatusInternalServerError,
				Message: err.Error(),
			},
		},
	}
	resp.Header().Set("Content-Type", "application/json")
	resp.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(resp).Encode(review)
}
