package agent

import (
	"context"
	"fmt"
	"testing"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/dynamic/fake"
)

// setupCapabilityPublisherScheme builds the scheme for CapabilityPublisher tests.
func setupCapabilityPublisherScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureRunnerConfig",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureRunnerConfigList",
	}, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignal",
	}, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "DriftSignalList",
	}, &unstructured.UnstructuredList{})
	return s
}

// TestCapabilityPublisher_EmitsDriftSignalAfterMissingThreshold verifies that
// emitRunnerConfigMissingSignal creates a DriftSignal in the management cluster
// seam-tenant-{clusterRef} namespace. T-23.
//
// The maintainPublication goroutine is not tested directly (it uses timers that
// make synchronous testing impractical). Instead this test verifies the signal
// emission function directly, which maintainPublication calls after the threshold.
func TestCapabilityPublisher_EmitsDriftSignalAfterMissingThreshold(t *testing.T) {
	scheme := setupCapabilityPublisherScheme()
	clusterRef := "ccs-dev"

	c := fake.NewSimpleDynamicClient(scheme)
	p := NewCapabilityPublisher(c, "ont-system")

	// Call emitRunnerConfigMissingSignal directly as maintainPublication would.
	p.emitRunnerConfigMissingSignal(context.Background(), clusterRef)

	// DriftSignal must be created in seam-tenant-{clusterRef}.
	tenantNS := "seam-tenant-" + clusterRef
	signalName := "drift-runnerconfig-" + clusterRef

	got, err := c.Resource(driftSignalGVR).Namespace(tenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("expected DriftSignal to be created, got error: %v", err)
	}

	// Verify spec fields.
	spec, _, _ := unstructuredNestedMap(got.Object, "spec")
	if state, _ := spec["state"].(string); state != "pending" {
		t.Errorf("DriftSignal.spec.state = %q, want %q", state, "pending")
	}
	affectedRef, _, _ := unstructuredNestedMap(spec, "affectedCRRef")
	if kind, _ := affectedRef["kind"].(string); kind != "InfrastructureRunnerConfig" {
		t.Errorf("affectedCRRef.kind = %q, want %q", kind, "InfrastructureRunnerConfig")
	}
	if name, _ := affectedRef["name"].(string); name != clusterRef {
		t.Errorf("affectedCRRef.name = %q, want %q", name, clusterRef)
	}
}

// TestCapabilityPublisher_EmitDriftSignal_IdempotentOnAlreadyExists verifies that
// emitRunnerConfigMissingSignal is a no-op when the DriftSignal already exists.
// This prevents duplicate signal storms. T-23.
func TestCapabilityPublisher_EmitDriftSignal_IdempotentOnAlreadyExists(t *testing.T) {
	scheme := setupCapabilityPublisherScheme()
	clusterRef := "ccs-dev"
	tenantNS := "seam-tenant-" + clusterRef
	signalName := "drift-runnerconfig-" + clusterRef

	existing := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "DriftSignal",
			"metadata": map[string]interface{}{
				"name":            signalName,
				"namespace":       tenantNS,
				"resourceVersion": "1",
			},
			"spec": map[string]interface{}{"state": "queued"},
		},
	}

	c := fake.NewSimpleDynamicClient(scheme, existing)
	p := NewCapabilityPublisher(c, "ont-system")

	// Should not panic or return an error -- AlreadyExists is silently ignored.
	p.emitRunnerConfigMissingSignal(context.Background(), clusterRef)

	// Original signal must remain unchanged (state=queued, not overwritten to pending).
	got, err := c.Resource(driftSignalGVR).Namespace(tenantNS).Get(
		context.Background(), signalName, metav1.GetOptions{},
	)
	if err != nil {
		t.Fatalf("get DriftSignal: %v", err)
	}
	spec, _, _ := unstructuredNestedMap(got.Object, "spec")
	if state, _ := spec["state"].(string); state != "queued" {
		t.Errorf("existing DriftSignal state overwritten: got %q, want %q", state, "queued")
	}
}

// TestCapabilityPublisher_IsPublishNotFound verifies the NotFound detection helper. T-23.
func TestCapabilityPublisher_IsPublishNotFound(t *testing.T) {
	scheme := setupCapabilityPublisherScheme()
	c := fake.NewSimpleDynamicClient(scheme)
	p := NewCapabilityPublisher(c, "ont-system")

	notFoundErr := k8serrors.NewNotFound(schema.GroupResource{Group: "infrastructure.ontai.dev", Resource: "infrastructurerunnerconfigs"}, "ccs-dev")
	wrappedNotFound := fmt.Errorf("capability publisher: patch RunnerConfig %q status in %q: %w", "ccs-dev", "ont-system", notFoundErr)
	transientErr := fmt.Errorf("connection refused")

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"notFound", notFoundErr, true},
		{"wrappedNotFound", wrappedNotFound, true},
		{"transient", transientErr, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := p.isPublishNotFound(tc.err); got != tc.want {
				t.Errorf("isPublishNotFound(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
