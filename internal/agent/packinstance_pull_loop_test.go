// Package agent -- T-10 unit tests for PackReceipt metadata carry-through.
//
// Tests cover:
//   - extractPackMetadataFromArtifact: extracts clusterPackRef and chart fields
//     from a PackInstance artifact JSON.
//   - Chart fields present: all four fields returned when set in PackInstance spec.
//   - Chart fields absent: zero-value metadata returned for kustomize/raw packs.
//   - Invalid JSON: returns zero-value metadata without panicking.
//   - upsertPackReceipt spec payload: chart fields omitted when empty, included
//     when non-empty.
//
// T-10, Decision B, conductor-schema.md §10.
package agent

import (
	"encoding/json"
	"testing"
)

// TestExtractPackMetadataFromArtifact_HelmPack verifies that chart provenance
// fields are extracted from a PackInstance artifact JSON produced by a helm pack.
func TestExtractPackMetadataFromArtifact_HelmPack(t *testing.T) {
	artifact := map[string]interface{}{
		"apiVersion": "infra.ontai.dev/v1alpha1",
		"kind":       "PackInstance",
		"metadata":   map[string]interface{}{"name": "cert-manager-ccs-mgmt"},
		"spec": map[string]interface{}{
			"clusterPackRef":   "cert-manager-v1.13.3-r1",
			"targetClusterRef": "ccs-mgmt",
			"chartVersion":     "v1.13.3",
			"chartURL":         "https://charts.jetstack.io",
			"chartName":        "cert-manager",
			"helmVersion":      "v3.14.0",
		},
	}
	artifactJSON, _ := json.Marshal(artifact)

	clusterPackRef, meta := extractPackMetadataFromArtifact(artifactJSON)

	if clusterPackRef != "cert-manager-v1.13.3-r1" {
		t.Errorf("clusterPackRef: got %q, want %q", clusterPackRef, "cert-manager-v1.13.3-r1")
	}
	if meta.ChartVersion != "v1.13.3" {
		t.Errorf("ChartVersion: got %q, want %q", meta.ChartVersion, "v1.13.3")
	}
	if meta.ChartURL != "https://charts.jetstack.io" {
		t.Errorf("ChartURL: got %q, want %q", meta.ChartURL, "https://charts.jetstack.io")
	}
	if meta.ChartName != "cert-manager" {
		t.Errorf("ChartName: got %q, want %q", meta.ChartName, "cert-manager")
	}
	if meta.HelmVersion != "v3.14.0" {
		t.Errorf("HelmVersion: got %q, want %q", meta.HelmVersion, "v3.14.0")
	}
}

// TestExtractPackMetadataFromArtifact_RawPack verifies that zero-value metadata
// is returned for a PackInstance artifact without chart provenance fields (raw or
// kustomize category packs where helm fields are absent).
func TestExtractPackMetadataFromArtifact_RawPack(t *testing.T) {
	artifact := map[string]interface{}{
		"apiVersion": "infra.ontai.dev/v1alpha1",
		"kind":       "PackInstance",
		"metadata":   map[string]interface{}{"name": "raw-pack-ccs-mgmt"},
		"spec": map[string]interface{}{
			"clusterPackRef":   "raw-pack-v1.0.0-r1",
			"targetClusterRef": "ccs-mgmt",
			// No chart fields -- raw pack.
		},
	}
	artifactJSON, _ := json.Marshal(artifact)

	clusterPackRef, meta := extractPackMetadataFromArtifact(artifactJSON)

	if clusterPackRef != "raw-pack-v1.0.0-r1" {
		t.Errorf("clusterPackRef: got %q, want %q", clusterPackRef, "raw-pack-v1.0.0-r1")
	}
	if meta.ChartVersion != "" || meta.ChartURL != "" || meta.ChartName != "" || meta.HelmVersion != "" {
		t.Errorf("expected all chart fields empty for raw pack; got %+v", meta)
	}
}

// TestExtractPackMetadataFromArtifact_InvalidJSON verifies that invalid JSON
// returns zero-value metadata without panicking.
func TestExtractPackMetadataFromArtifact_InvalidJSON(t *testing.T) {
	clusterPackRef, meta := extractPackMetadataFromArtifact([]byte("not-json"))
	if clusterPackRef != "" {
		t.Errorf("clusterPackRef: expected empty for invalid JSON, got %q", clusterPackRef)
	}
	if meta.ChartVersion != "" || meta.RBACDigest != "" {
		t.Errorf("expected zero-value metadata for invalid JSON; got %+v", meta)
	}
}

// TestUpsertPackReceiptSpecPayload_HelmFields verifies that non-empty metadata
// fields are included in the spec payload passed to upsertPackReceipt. This
// is a whitebox test of the spec construction logic.
func TestUpsertPackReceiptSpecPayload_HelmFields(t *testing.T) {
	meta := packDeliveryMetadata{
		RBACDigest:     "sha256:rbac",
		WorkloadDigest: "sha256:workload",
		ChartVersion:   "v1.13.3",
		ChartURL:       "https://charts.jetstack.io",
		ChartName:      "cert-manager",
		HelmVersion:    "v3.14.0",
	}

	specPayload := buildReceiptSpecPayload("cert-manager-ccs-mgmt", "sec-ref", meta)

	checks := map[string]string{
		"rbacDigest":     meta.RBACDigest,
		"workloadDigest": meta.WorkloadDigest,
		"chartVersion":   meta.ChartVersion,
		"chartURL":       meta.ChartURL,
		"chartName":      meta.ChartName,
		"helmVersion":    meta.HelmVersion,
	}
	for field, want := range checks {
		got, _ := specPayload[field].(string)
		if got != want {
			t.Errorf("specPayload[%q]: got %q, want %q", field, got, want)
		}
	}
}

// TestUpsertPackReceiptSpecPayload_EmptyFieldsOmitted verifies that empty metadata
// fields are not included in the spec payload (omitted, not zero-value).
func TestUpsertPackReceiptSpecPayload_EmptyFieldsOmitted(t *testing.T) {
	meta := packDeliveryMetadata{} // all zero-value

	specPayload := buildReceiptSpecPayload("raw-pack-ccs-mgmt", "sec-ref", meta)

	for _, field := range []string{"rbacDigest", "workloadDigest", "chartVersion", "chartURL", "chartName", "helmVersion"} {
		if _, ok := specPayload[field]; ok {
			t.Errorf("specPayload[%q] present but should be omitted for empty metadata", field)
		}
	}
}
