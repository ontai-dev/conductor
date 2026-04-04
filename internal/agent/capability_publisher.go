package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// runnerConfigGVR is the GroupVersionResource for RunnerConfig CRs.
// API group runner.ontai.dev, schema version v1alpha1. conductor-schema.md §5.
var runnerConfigGVR = schema.GroupVersionResource{
	Group:    "runner.ontai.dev",
	Version:  "v1alpha1",
	Resource: "runnerconfigs",
}

// CapabilityPublisher writes the Conductor capability manifest to the RunnerConfig
// status subresource. This is the agent's self-declaration: operators read
// RunnerConfig status before submitting any Job. conductor-schema.md §10 step 3.
type CapabilityPublisher struct {
	client    dynamic.Interface
	namespace string
}

// NewCapabilityPublisher constructs a CapabilityPublisher that writes to the
// RunnerConfig CRs in the given namespace.
func NewCapabilityPublisher(client dynamic.Interface, namespace string) *CapabilityPublisher {
	return &CapabilityPublisher{client: client, namespace: namespace}
}

// Publish writes the manifest to the RunnerConfig named after the clusterRef.
// It targets the status subresource so only status fields are changed.
// conductor-schema.md §5 (RunnerConfigStatus), conductor-design.md §2.10.
func (p *CapabilityPublisher) Publish(ctx context.Context, clusterRef, agentVersion, agentLeader string, manifest runnerlib.CapabilityManifest) error {
	manifest.PublishedAt = time.Now().UTC()

	// Build a strategic merge patch that updates only the status fields.
	statusPatch := map[string]interface{}{
		"status": map[string]interface{}{
			"capabilities": manifest,
			"agentVersion": agentVersion,
			"agentLeader":  agentLeader,
		},
	}
	patchBytes, err := json.Marshal(statusPatch)
	if err != nil {
		return fmt.Errorf("capability publisher: marshal status patch: %w", err)
	}

	_, err = p.client.Resource(runnerConfigGVR).Namespace(p.namespace).Patch(
		ctx,
		clusterRef,
		types.MergePatchType,
		patchBytes,
		metav1.PatchOptions{},
		"status",
	)
	if err != nil {
		return fmt.Errorf("capability publisher: patch RunnerConfig %q status in %q: %w",
			clusterRef, p.namespace, err)
	}
	return nil
}

// BuildManifest constructs the CapabilityManifest from the registered execute-mode
// capabilities. All capabilities are declared with ExecutorMode and version "dev".
// Production builds will stamp the real version via ldflags.
// conductor-design.md §2.10, conductor-schema.md §5.
func BuildManifest(capabilities []string, version string) runnerlib.CapabilityManifest {
	entries := make([]runnerlib.CapabilityEntry, 0, len(capabilities))
	for _, name := range capabilities {
		entries = append(entries, runnerlib.CapabilityEntry{
			Name:            name,
			Version:         version,
			Mode:            runnerlib.ExecutorMode,
			ParameterSchema: map[string]runnerlib.ParameterDef{},
		})
	}
	return runnerlib.CapabilityManifest{
		RunnerVersion: version,
		Entries:       entries,
	}
}
