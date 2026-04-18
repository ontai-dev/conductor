package agent_test

import (
	"context"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"

	"github.com/ontai-dev/conductor/internal/agent"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// newFakeDynamicClient creates a fake dynamic client configured to know about
// the RunnerConfig resource so Patch calls succeed.
func newFakeDynamicClient(scheme *runtime.Scheme) *dynamicfake.FakeDynamicClient {
	// Register the RunnerConfig GVR in the RESTMapper by adding it to the scheme.
	// dynamicfake uses the scheme to resolve GVKs; we add a dummy unstructured type.
	gvk := schema.GroupVersionKind{
		Group:   "runner.ontai.dev",
		Version: "v1alpha1",
		Kind:    "RunnerConfig",
	}
	scheme.AddKnownTypeWithName(gvk, &runtime.Unknown{})
	gvkList := schema.GroupVersionKind{
		Group:   "runner.ontai.dev",
		Version: "v1alpha1",
		Kind:    "RunnerConfigList",
	}
	scheme.AddKnownTypeWithName(gvkList, &runtime.Unknown{})
	_ = meta.NewDefaultRESTMapper(nil)
	return dynamicfake.NewSimpleDynamicClient(scheme)
}

// TestBuildManifest_ContainsAllCapabilities verifies that BuildManifest produces
// a []CapabilityEntry with all provided capability names and the correct mode.
// The returned slice is used directly as status.capabilities in the RunnerConfig CR.
// conductor-design.md §2.10.
func TestBuildManifest_ContainsAllCapabilities(t *testing.T) {
	caps := []string{"bootstrap", "node-patch", "etcd-backup"}
	entries := agent.BuildManifest(caps, "dev")

	if len(entries) != len(caps) {
		t.Errorf("expected %d entries; got %d", len(caps), len(entries))
	}
	for i, entry := range entries {
		if entry.Name != caps[i] {
			t.Errorf("entry[%d].Name: expected %q; got %q", i, caps[i], entry.Name)
		}
		if entry.Mode != runnerlib.ExecutorMode {
			t.Errorf("entry[%d].Mode: expected ExecutorMode; got %q", i, entry.Mode)
		}
		if entry.ParameterSchema == nil {
			t.Errorf("entry[%d].ParameterSchema must not be nil", i)
		}
	}
}

// TestBuildManifest_EmptyCapabilitiesIsValid verifies that an empty capabilities
// list produces a non-nil, zero-length []CapabilityEntry. conductor-schema.md §5.
func TestBuildManifest_EmptyCapabilitiesIsValid(t *testing.T) {
	entries := agent.BuildManifest(nil, "dev")
	if entries == nil {
		t.Error("BuildManifest: result must not be nil even for empty capabilities list")
	}
	if len(entries) != 0 {
		t.Errorf("expected 0 entries; got %d", len(entries))
	}
}

// TestCapabilityPublisher_PublishAcceptsCapabilitySlice verifies that Publish
// accepts a []CapabilityEntry slice (matching the CRD status.capabilities array
// type) and does not panic when the RunnerConfig CR does not exist in the fake store.
// conductor-schema.md §5.
func TestCapabilityPublisher_PublishAcceptsCapabilitySlice(t *testing.T) {
	caps := []string{"node-patch"}
	entries := agent.BuildManifest(caps, "dev")

	scheme := runtime.NewScheme()
	fakeClient := newFakeDynamicClient(scheme)
	pub := agent.NewCapabilityPublisher(fakeClient, "ont-system")
	// Publish will error (CR not in fake) but must not panic.
	_ = pub.Publish(context.Background(), "ccs-test", "dev", "pod-1", entries)
}

// TestCapabilityPublisher_ConstructsWithoutPanic verifies that NewCapabilityPublisher
// does not panic given valid inputs.
func TestCapabilityPublisher_ConstructsWithoutPanic(t *testing.T) {
	scheme := runtime.NewScheme()
	fakeClient := dynamicfake.NewSimpleDynamicClient(scheme)
	pub := agent.NewCapabilityPublisher(fakeClient, "ont-system")
	if pub == nil {
		t.Error("expected non-nil CapabilityPublisher")
	}
}
