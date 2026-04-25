// Package runnerlib -- T-08 unit tests for InfrastructurePackReceiptSpec fields.
//
// Tests cover:
//   - InfrastructurePackReceiptSpec carries RBACDigest, WorkloadDigest, ChartVersion,
//     ChartURL, ChartName, HelmVersion.
//   - All six fields survive a JSON serialization round-trip.
//   - Fields are zero-value (empty string) when not populated.
//
// T-08, Decision B, T-04 schema. Types migrated to seam-core in T-2B-6.
package runnerlib_test

import (
	"encoding/json"
	"testing"

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

func TestPackReceiptSpec_NewFields_RoundTrip(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructurePackReceiptSpec{
		ClusterPackRef:   "cert-manager-v1.13.3-r1",
		TargetClusterRef: "ccs-mgmt",
		PackSignature:    "sig==",
		SignatureVerified: true,
		RBACDigest:       "sha256:aabbcc",
		WorkloadDigest:   "sha256:ddeeff",
		ChartVersion:     "v1.13.3",
		ChartURL:         "https://charts.jetstack.io",
		ChartName:        "cert-manager",
		HelmVersion:      "v3.14.0",
	}

	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal InfrastructurePackReceiptSpec: %v", err)
	}

	var got seamcorev1alpha1.InfrastructurePackReceiptSpec
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal InfrastructurePackReceiptSpec: %v", err)
	}

	checks := map[string][2]string{
		"RBACDigest":     {got.RBACDigest, spec.RBACDigest},
		"WorkloadDigest": {got.WorkloadDigest, spec.WorkloadDigest},
		"ChartVersion":   {got.ChartVersion, spec.ChartVersion},
		"ChartURL":       {got.ChartURL, spec.ChartURL},
		"ChartName":      {got.ChartName, spec.ChartName},
		"HelmVersion":    {got.HelmVersion, spec.HelmVersion},
	}
	for field, pair := range checks {
		if pair[0] != pair[1] {
			t.Errorf("%s: got %q, want %q", field, pair[0], pair[1])
		}
	}
}

func TestPackReceiptSpec_NewFields_ZeroWhenAbsent(t *testing.T) {
	spec := seamcorev1alpha1.InfrastructurePackReceiptSpec{
		ClusterPackRef:   "raw-pack-v1.0.0-r1",
		TargetClusterRef: "ccs-mgmt",
		SignatureVerified: true,
		// No digest or chart fields -- raw or kustomize pack.
	}

	if spec.RBACDigest != "" {
		t.Errorf("RBACDigest: expected empty, got %q", spec.RBACDigest)
	}
	if spec.WorkloadDigest != "" {
		t.Errorf("WorkloadDigest: expected empty, got %q", spec.WorkloadDigest)
	}
	if spec.ChartVersion != "" {
		t.Errorf("ChartVersion: expected empty, got %q", spec.ChartVersion)
	}
	if spec.ChartURL != "" {
		t.Errorf("ChartURL: expected empty, got %q", spec.ChartURL)
	}
	if spec.ChartName != "" {
		t.Errorf("ChartName: expected empty, got %q", spec.ChartName)
	}
	if spec.HelmVersion != "" {
		t.Errorf("HelmVersion: expected empty, got %q", spec.HelmVersion)
	}
}
