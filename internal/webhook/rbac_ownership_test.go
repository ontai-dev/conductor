package webhook

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// makeAdmissionReview builds a minimal AdmissionReview for the given kind,
// namespace, operation, and annotations (nil = no annotations).
func makeAdmissionReview(kind, ns, op string, annotations map[string]string) []byte {
	objMeta := struct {
		Metadata struct {
			Annotations map[string]string `json:"annotations,omitempty"`
		} `json:"metadata"`
	}{}
	objMeta.Metadata.Annotations = annotations
	objRaw, _ := json.Marshal(objMeta)

	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admission.k8s.io/v1",
			Kind:       "AdmissionReview",
		},
		Request: &admissionv1.AdmissionRequest{
			UID:       "test-uid",
			Namespace: ns,
			Operation: admissionv1.Operation(op),
			Kind:      metav1.GroupVersionKind{Kind: kind},
			Name:      "test-resource",
			Object:    runtime.RawExtension{Raw: objRaw},
		},
	}
	b, _ := json.Marshal(review)
	return b
}

// admitRequest calls the TenantRBACOwnershipWebhook and returns the AdmissionResponse.
func admitRequest(wh *TenantRBACOwnershipWebhook, body []byte) *admissionv1.AdmissionResponse {
	req := httptest.NewRequest(http.MethodPost, "/validate/rbac-ownership", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	wh.ServeHTTP(w, req)

	var review admissionv1.AdmissionReview
	_ = json.NewDecoder(w.Body).Decode(&review)
	return review.Response
}

// TestRBACOwnership_AdmitWithAnnotation verifies that a Role with the ownership
// annotation is admitted in both audit and strict mode.
func TestRBACOwnership_AdmitWithAnnotation(t *testing.T) {
	for _, strict := range []bool{false, true} {
		gate := NewEnforcementGate()
		if strict {
			gate.SetStrict()
		}
		wh := NewTenantRBACOwnershipWebhook(gate)

		body := makeAdmissionReview("Role", "app-ns", "CREATE", map[string]string{
			rbacOwnerAnnotation: rbacOwnerAnnotationValue,
		})
		resp := admitRequest(wh, body)
		if !resp.Allowed {
			t.Errorf("strict=%v: annotated Role should be admitted, got denied: %v", strict, resp.Result)
		}
	}
}

// TestRBACOwnership_AuditMode_AdmitsWithoutAnnotation verifies that in audit mode
// a Role without the ownership annotation is admitted (logged, not rejected).
func TestRBACOwnership_AuditMode_AdmitsWithoutAnnotation(t *testing.T) {
	gate := NewEnforcementGate() // audit mode
	wh := NewTenantRBACOwnershipWebhook(gate)

	body := makeAdmissionReview("Role", "app-ns", "CREATE", nil)
	resp := admitRequest(wh, body)
	if !resp.Allowed {
		t.Errorf("audit mode: unannotated Role should be admitted, got denied: %v", resp.Result)
	}
}

// TestRBACOwnership_StrictMode_RejectsWithoutAnnotation verifies that in strict
// mode a Role without the ownership annotation is rejected at admission.
func TestRBACOwnership_StrictMode_RejectsWithoutAnnotation(t *testing.T) {
	gate := NewEnforcementGate()
	gate.SetStrict()
	wh := NewTenantRBACOwnershipWebhook(gate)

	body := makeAdmissionReview("Role", "app-ns", "CREATE", nil)
	resp := admitRequest(wh, body)
	if resp.Allowed {
		t.Error("strict mode: unannotated Role should be rejected, got admitted")
	}
}

// TestRBACOwnership_KubeSystemAlwaysAdmitted verifies that resources in kube-system
// are always admitted regardless of annotation or enforcement mode.
func TestRBACOwnership_KubeSystemAlwaysAdmitted(t *testing.T) {
	gate := NewEnforcementGate()
	gate.SetStrict()
	wh := NewTenantRBACOwnershipWebhook(gate)

	body := makeAdmissionReview("Role", "kube-system", "CREATE", nil)
	resp := admitRequest(wh, body)
	if !resp.Allowed {
		t.Error("kube-system Role should always be admitted")
	}
}

// TestRBACOwnership_NonInterceptedKindAdmitted verifies that non-RBAC kinds
// (e.g., Deployment) are admitted without annotation checks.
func TestRBACOwnership_NonInterceptedKindAdmitted(t *testing.T) {
	gate := NewEnforcementGate()
	gate.SetStrict()
	wh := NewTenantRBACOwnershipWebhook(gate)

	body := makeAdmissionReview("Deployment", "app-ns", "CREATE", nil)
	resp := admitRequest(wh, body)
	if !resp.Allowed {
		t.Error("Deployment (non-intercepted kind) should always be admitted")
	}
}

// TestRBACOwnership_AllInterceptedKindsEnforced verifies that all five RBAC kinds
// are subject to ownership enforcement in strict mode.
func TestRBACOwnership_AllInterceptedKindsEnforced(t *testing.T) {
	gate := NewEnforcementGate()
	gate.SetStrict()
	wh := NewTenantRBACOwnershipWebhook(gate)

	for _, kind := range []string{"Role", "ClusterRole", "RoleBinding", "ClusterRoleBinding", "ServiceAccount"} {
		body := makeAdmissionReview(kind, "app-ns", "CREATE", nil)
		resp := admitRequest(wh, body)
		if resp.Allowed {
			t.Errorf("strict mode: unannotated %s should be rejected", kind)
		}
	}
}

// TestEnforcementGate_StartsInAuditMode verifies the gate is in audit mode on construction.
func TestEnforcementGate_StartsInAuditMode(t *testing.T) {
	gate := NewEnforcementGate()
	if gate.IsStrict() {
		t.Error("gate should start in audit mode (IsStrict=false)")
	}
}

// TestEnforcementGate_SetStrictIsIdempotent verifies SetStrict is safe to call multiple times.
func TestEnforcementGate_SetStrictIsIdempotent(t *testing.T) {
	gate := NewEnforcementGate()
	gate.SetStrict()
	gate.SetStrict()
	if !gate.IsStrict() {
		t.Error("gate should be strict after SetStrict")
	}
}
