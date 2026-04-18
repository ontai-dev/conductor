package webhook_test

// Unit tests for SealedCausalChainWebhook.
//
// The webhook must:
//   - Allow CREATE operations without inspecting fields.
//   - Allow DELETE operations.
//   - Allow UPDATE operations where spec.lineage is unchanged.
//   - Allow UPDATE operations where old object has no spec.lineage (pre-lineage CRDs).
//   - Deny UPDATE operations where spec.lineage changed.
//
// seam-core-schema.md §5, CLAUDE.md §14 Decision 1.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ontai-dev/conductor/internal/webhook"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// makeAdmissionReview builds an AdmissionReview request with the given
// operation, old object, and new object.
func makeAdmissionReview(t *testing.T, op admissionv1.Operation, oldObj, newObj map[string]interface{}) []byte {
	t.Helper()

	oldRaw, _ := json.Marshal(oldObj)
	newRaw, _ := json.Marshal(newObj)

	review := admissionv1.AdmissionReview{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "admission.k8s.io/v1",
			Kind:       "AdmissionReview",
		},
		Request: &admissionv1.AdmissionRequest{
			UID:       types.UID("test-uid-1"),
			Operation: op,
			OldObject: runtime.RawExtension{Raw: oldRaw},
			Object:    runtime.RawExtension{Raw: newRaw},
		},
	}
	b, err := json.Marshal(review)
	if err != nil {
		t.Fatalf("marshal AdmissionReview: %v", err)
	}
	return b
}

// postWebhook posts an AdmissionReview body to the handler and returns the
// decoded AdmissionReview response.
func postWebhook(t *testing.T, body []byte) admissionv1.AdmissionReview {
	t.Helper()
	h := webhook.NewSealedCausalChainWebhook()
	req := httptest.NewRequest(http.MethodPost, "/validate/sealed-causal-chain", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("webhook returned HTTP %d; want 200", rr.Code)
	}

	var out admissionv1.AdmissionReview
	if err := json.NewDecoder(rr.Body).Decode(&out); err != nil {
		t.Fatalf("decode AdmissionReview response: %v", err)
	}
	return out
}

// lineageField returns a spec.lineage map for use in test objects.
func lineageField(rootName, rootKind string) map[string]interface{} {
	return map[string]interface{}{
		"rootKind": rootKind,
		"rootName": rootName,
	}
}

// objectWithLineage builds a test CR object with a spec.lineage field.
func objectWithLineage(lineage map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACProfile",
		"metadata":   map[string]interface{}{"name": "test-profile"},
		"spec": map[string]interface{}{
			"lineage":     lineage,
			"rbacPolicy":  "default",
		},
	}
}

// objectWithoutLineage builds a test CR object with no spec.lineage field.
func objectWithoutLineage() map[string]interface{} {
	return map[string]interface{}{
		"apiVersion": "security.ontai.dev/v1alpha1",
		"kind":       "RBACProfile",
		"metadata":   map[string]interface{}{"name": "test-profile"},
		"spec": map[string]interface{}{
			"rbacPolicy": "default",
		},
	}
}

// ── tests ─────────────────────────────────────────────────────────────────────

// TestSealedCausalChainWebhook_CreateAlwaysAllowed verifies that CREATE
// operations are always permitted (lineage is authored at creation time).
func TestSealedCausalChainWebhook_CreateAlwaysAllowed(t *testing.T) {
	newObj := objectWithLineage(lineageField("talos-cluster-1", "TalosCluster"))
	body := makeAdmissionReview(t, admissionv1.Create, map[string]interface{}{}, newObj)
	resp := postWebhook(t, body)

	if resp.Response == nil {
		t.Fatal("expected non-nil Response")
	}
	if !resp.Response.Allowed {
		t.Errorf("CREATE must be allowed; got denied: %v", resp.Response.Result)
	}
}

// TestSealedCausalChainWebhook_DeleteAlwaysAllowed verifies that DELETE
// operations are always permitted.
func TestSealedCausalChainWebhook_DeleteAlwaysAllowed(t *testing.T) {
	oldObj := objectWithLineage(lineageField("talos-cluster-1", "TalosCluster"))
	body := makeAdmissionReview(t, admissionv1.Delete, oldObj, map[string]interface{}{})
	resp := postWebhook(t, body)

	if !resp.Response.Allowed {
		t.Errorf("DELETE must be allowed; got denied: %v", resp.Response.Result)
	}
}

// TestSealedCausalChainWebhook_UpdateUnchangedLineageAllowed verifies that an
// UPDATE where spec.lineage is identical in old and new objects is allowed.
func TestSealedCausalChainWebhook_UpdateUnchangedLineageAllowed(t *testing.T) {
	lineage := lineageField("talos-cluster-1", "TalosCluster")
	oldObj := objectWithLineage(lineage)
	newObj := objectWithLineage(lineage) // same lineage
	// Change an unrelated spec field to simulate a real update.
	newObj["spec"].(map[string]interface{})["rbacPolicy"] = "production"

	body := makeAdmissionReview(t, admissionv1.Update, oldObj, newObj)
	resp := postWebhook(t, body)

	if !resp.Response.Allowed {
		t.Errorf("UPDATE with unchanged lineage must be allowed; got denied: %v", resp.Response.Result)
	}
}

// TestSealedCausalChainWebhook_UpdateChangedLineageDenied verifies that an
// UPDATE where spec.lineage differs from old to new is denied.
func TestSealedCausalChainWebhook_UpdateChangedLineageDenied(t *testing.T) {
	oldObj := objectWithLineage(lineageField("talos-cluster-1", "TalosCluster"))
	// Attempt to change rootName in lineage — must be rejected.
	newObj := objectWithLineage(lineageField("tampered-root", "TalosCluster"))

	body := makeAdmissionReview(t, admissionv1.Update, oldObj, newObj)
	resp := postWebhook(t, body)

	if resp.Response.Allowed {
		t.Error("UPDATE with changed spec.lineage must be denied; got allowed")
	}
	if resp.Response.Result == nil {
		t.Error("denied response must carry a Result status")
	}
}

// TestSealedCausalChainWebhook_UpdateAddedLineageDenied verifies that an
// UPDATE that adds a spec.lineage field to an object that previously had one
// (change from one value to another) is denied.
func TestSealedCausalChainWebhook_UpdateAddedLineageDenied(t *testing.T) {
	oldObj := objectWithLineage(lineageField("root-1", "TalosCluster"))
	// New object has a completely different lineage.
	newObj := objectWithLineage(map[string]interface{}{
		"rootKind": "PackExecution",
		"rootName": "pack-exec-1",
	})

	body := makeAdmissionReview(t, admissionv1.Update, oldObj, newObj)
	resp := postWebhook(t, body)

	if resp.Response.Allowed {
		t.Error("UPDATE changing lineage kind must be denied; got allowed")
	}
}

// TestSealedCausalChainWebhook_UpdatePreLineageCRDAllowed verifies that an
// UPDATE on an object with no spec.lineage in the old object is always allowed.
// This handles pre-lineage CRDs that were created before the lineage field
// was introduced. seam-core-schema.md §14 Decision 6.
func TestSealedCausalChainWebhook_UpdatePreLineageCRDAllowed(t *testing.T) {
	oldObj := objectWithoutLineage()
	newObj := objectWithLineage(lineageField("talos-cluster-1", "TalosCluster"))

	body := makeAdmissionReview(t, admissionv1.Update, oldObj, newObj)
	resp := postWebhook(t, body)

	if !resp.Response.Allowed {
		t.Errorf("UPDATE on pre-lineage CRD must be allowed; got denied: %v", resp.Response.Result)
	}
}

// TestSealedCausalChainWebhook_UpdateBothNoLineageAllowed verifies that an
// UPDATE where neither old nor new object has spec.lineage is allowed.
func TestSealedCausalChainWebhook_UpdateBothNoLineageAllowed(t *testing.T) {
	oldObj := objectWithoutLineage()
	newObj := objectWithoutLineage()

	body := makeAdmissionReview(t, admissionv1.Update, oldObj, newObj)
	resp := postWebhook(t, body)

	if !resp.Response.Allowed {
		t.Errorf("UPDATE with no lineage in either object must be allowed; got denied: %v", resp.Response.Result)
	}
}

// TestSealedCausalChainWebhook_UIDPropagatesToResponse verifies that the
// request UID is copied to the response UID per the Kubernetes webhook protocol.
func TestSealedCausalChainWebhook_UIDPropagatesToResponse(t *testing.T) {
	lineage := lineageField("talos-cluster-1", "TalosCluster")
	oldObj := objectWithLineage(lineage)
	newObj := objectWithLineage(lineage)

	body := makeAdmissionReview(t, admissionv1.Update, oldObj, newObj)
	resp := postWebhook(t, body)

	if resp.Response.UID != "test-uid-1" {
		t.Errorf("response UID must match request UID; got %q", resp.Response.UID)
	}
}

// TestSealedCausalChainWebhook_NonPostMethodRejected verifies that non-POST
// HTTP methods are rejected with 405.
func TestSealedCausalChainWebhook_NonPostMethodRejected(t *testing.T) {
	h := webhook.NewSealedCausalChainWebhook()
	req := httptest.NewRequest(http.MethodGet, "/validate/sealed-causal-chain", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET must return 405; got %d", rr.Code)
	}
}

// TestSealedCausalChainWebhook_MalformedBodyRejected verifies that a
// non-JSON request body returns 500 (internal error from parse failure).
func TestSealedCausalChainWebhook_MalformedBodyRejected(t *testing.T) {
	h := webhook.NewSealedCausalChainWebhook()
	req := httptest.NewRequest(http.MethodPost, "/validate/sealed-causal-chain",
		bytes.NewReader([]byte("this is not json")))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusInternalServerError {
		t.Errorf("malformed body must return 500; got %d", rr.Code)
	}
}
