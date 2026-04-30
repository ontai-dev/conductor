package agent_test

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/ontai-dev/conductor/internal/agent"
	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

var runnerConfigGVR = schema.GroupVersionResource{
	Group:   "infrastructure.ontai.dev",
	Version: "v1alpha1",
	Resource: "infrastructurerunnerconfigs",
}

// makeRunnerConfig constructs an Unstructured RunnerConfig with optional capabilities
// already set in status (to simulate a pre-populated RunnerConfig).
func makeRunnerConfig(name, namespace string, hasCaps bool) *unstructured.Unstructured {
	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureRunnerConfig",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": namespace,
			},
			"spec": map[string]interface{}{
				"clusterRef": name,
			},
		},
	}
	if hasCaps {
		obj.Object["status"] = map[string]interface{}{
			"capabilities": []interface{}{
				map[string]interface{}{"name": "bootstrap", "version": "dev"},
			},
		}
	}
	return obj
}

// newFakeDynamicClient creates a fake dynamic client configured to know about
// the RunnerConfig resource so Patch calls succeed.
func newFakeDynamicClient(scheme *runtime.Scheme) *dynamicfake.FakeDynamicClient {
	// Register the RunnerConfig GVR in the RESTMapper by adding it to the scheme.
	// dynamicfake uses the scheme to resolve GVKs; we add a dummy unstructured type.
	gvk := schema.GroupVersionKind{
		Group:   "infrastructure.ontai.dev",
		Version: "v1alpha1",
		Kind:    "InfrastructureRunnerConfig",
	}
	scheme.AddKnownTypeWithName(gvk, &runtime.Unknown{})
	gvkList := schema.GroupVersionKind{
		Group:   "infrastructure.ontai.dev",
		Version: "v1alpha1",
		Kind:    "InfrastructureRunnerConfigList",
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

// newAllFakeDynamicClient creates a fake dynamic client that supports List on
// the RunnerConfig GVR. Uses NewSimpleDynamicClientWithCustomListKinds so the
// fake tracker knows the list kind mapping.
func newAllFakeDynamicClient(scheme *runtime.Scheme) *dynamicfake.FakeDynamicClient {
	scheme.AddKnownTypeWithName(schema.GroupVersionKind{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Kind: "InfrastructureRunnerConfig",
	}, &unstructured.Unstructured{})
	_ = meta.NewDefaultRESTMapper(nil)
	return dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme,
		map[schema.GroupVersionResource]string{runnerConfigGVR: "InfrastructureRunnerConfigList"},
	)
}

// TestPublishAllWithRetry_PublishesToEmptyRunnerConfigs verifies that PublishAllWithRetry
// issues a patch action for each RunnerConfig that has no capabilities.
func TestPublishAllWithRetry_PublishesToEmptyRunnerConfigs(t *testing.T) {
	scheme := runtime.NewScheme()
	fakeClient := newAllFakeDynamicClient(scheme)
	rc1 := makeRunnerConfig("ccs-mgmt", "ont-system", false)
	rc2 := makeRunnerConfig("ccs-dev", "ont-system", false)
	ctx := context.Background()
	for _, rc := range []*unstructured.Unstructured{rc1, rc2} {
		if _, err := fakeClient.Resource(runnerConfigGVR).Namespace("ont-system").Create(ctx, rc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("setup create %s: %v", rc.GetName(), err)
		}
	}
	fakeClient.ClearActions()

	pub := agent.NewCapabilityPublisher(fakeClient, "ont-system")
	entries := agent.BuildManifest([]string{"bootstrap"}, "dev")

	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pub.PublishAllWithRetry(tctx, "dev", "", entries)
	time.Sleep(200 * time.Millisecond)

	patchedNames := map[string]bool{}
	for _, a := range fakeClient.Actions() {
		if a.GetVerb() == "patch" && a.GetSubresource() == "status" {
			if pa, ok := a.(k8stesting.PatchAction); ok {
				patchedNames[pa.GetName()] = true
			}
		}
	}
	for _, name := range []string{"ccs-mgmt", "ccs-dev"} {
		if !patchedNames[name] {
			t.Errorf("expected patch action for RunnerConfig %q, got none; patched: %v", name, patchedNames)
		}
	}
}

// TestPublishAllWithRetry_PublishesToAllRunnerConfigsRegardlessOfExistingCaps verifies
// that PublishAllWithRetry publishes to every RunnerConfig including those already
// populated, ensuring stale or partial capability sets are always overwritten with
// the full management conductor manifest.
func TestPublishAllWithRetry_PublishesToAllRunnerConfigsRegardlessOfExistingCaps(t *testing.T) {
	scheme := runtime.NewScheme()
	fakeClient := newAllFakeDynamicClient(scheme)
	rc1 := makeRunnerConfig("ccs-mgmt", "ont-system", true) // already has caps
	rc2 := makeRunnerConfig("ccs-dev", "ont-system", false)  // empty
	ctx := context.Background()
	for _, rc := range []*unstructured.Unstructured{rc1, rc2} {
		if _, err := fakeClient.Resource(runnerConfigGVR).Namespace("ont-system").Create(ctx, rc, metav1.CreateOptions{}); err != nil {
			t.Fatalf("setup create %s: %v", rc.GetName(), err)
		}
	}
	fakeClient.ClearActions()

	pub := agent.NewCapabilityPublisher(fakeClient, "ont-system")
	entries := agent.BuildManifest([]string{"bootstrap"}, "dev")

	tctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	pub.PublishAllWithRetry(tctx, "dev", "", entries)
	time.Sleep(200 * time.Millisecond)

	patchedNames := map[string]bool{}
	for _, a := range fakeClient.Actions() {
		if a.GetVerb() == "patch" && a.GetSubresource() == "status" {
			if pa, ok := a.(k8stesting.PatchAction); ok {
				patchedNames[pa.GetName()] = true
			}
		}
	}
	for _, name := range []string{"ccs-mgmt", "ccs-dev"} {
		if !patchedNames[name] {
			t.Errorf("expected patch action for RunnerConfig %q (always publish), got none; patched: %v", name, patchedNames)
		}
	}
}
