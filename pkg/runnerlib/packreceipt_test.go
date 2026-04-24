// Package runnerlib -- T-08 unit tests for PackReceiptSpec new fields.
//
// Tests cover:
//   - PackReceiptSpec carries RBACDigest, WorkloadDigest, ChartVersion,
//     ChartURL, ChartName, HelmVersion.
//   - All six fields survive a JSON serialization round-trip.
//   - Fields are zero-value (empty string) when not populated.
//
// T-08, Decision B, T-04 schema.
package runnerlib

import (
	"encoding/json"
	"testing"
)

func TestPackReceiptSpec_NewFields_RoundTrip(t *testing.T) {
	spec := PackReceiptSpec{
		ClusterPackRef: ClusterPackRef{
			Name:    "cert-manager-v1.13.3-r1",
			Version: "v1.13.3",
		},
		TargetClusterRef:  "ccs-mgmt",
		PackSignature:     "sig==",
		SignatureVerified:  true,
		RBACDigest:        "sha256:aabbcc",
		WorkloadDigest:    "sha256:ddeeff",
		ChartVersion:      "v1.13.3",
		ChartURL:          "https://charts.jetstack.io",
		ChartName:         "cert-manager",
		HelmVersion:       "v3.14.0",
	}

	data, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal PackReceiptSpec: %v", err)
	}

	var got PackReceiptSpec
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal PackReceiptSpec: %v", err)
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
	spec := PackReceiptSpec{
		ClusterPackRef: ClusterPackRef{
			Name:    "raw-pack-v1.0.0-r1",
			Version: "v1.0.0",
		},
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
