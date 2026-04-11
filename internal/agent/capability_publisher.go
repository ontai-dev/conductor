package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// capabilityPublishRetryInterval is the interval between retry attempts when
// the RunnerConfig does not yet exist or the patch fails transiently.
const capabilityPublishRetryInterval = 30 * time.Second

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

// Publish writes the capability list to the RunnerConfig named after the clusterRef.
// It targets the status subresource so only status fields are changed.
// capabilities is a flat []CapabilityEntry slice matching the CRD definition
// (status.capabilities: array). conductor-schema.md §5, conductor-design.md §2.10.
func (p *CapabilityPublisher) Publish(ctx context.Context, clusterRef, agentVersion, agentLeader string, capabilities []runnerlib.CapabilityEntry) error {
	log := slog.Default().With("component", "capability-publisher", "clusterRef", clusterRef, "namespace", p.namespace)

	// Build a strategic merge patch that updates only the status fields.
	statusPatch := map[string]interface{}{
		"status": map[string]interface{}{
			"capabilities": capabilities,
			"agentVersion": agentVersion,
			"agentLeader":  agentLeader,
		},
	}
	patchBytes, err := json.Marshal(statusPatch)
	if err != nil {
		return fmt.Errorf("capability publisher: marshal status patch: %w", err)
	}

	log.Info("patching RunnerConfig status with capability manifest", "capabilityCount", len(capabilities))
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
	log.Info("capability manifest published to RunnerConfig", "capabilityCount", len(capabilities))
	return nil
}

// PublishWithRetry attempts an initial Publish and, if it fails, starts a
// background goroutine that retries every capabilityPublishRetryInterval until
// the publish succeeds or ctx is cancelled. The goroutine exits on first
// success — there is no need to republish unless the RunnerConfig changes.
//
// This handles the race where Conductor starts before Platform has created the
// RunnerConfig: the initial attempt fails with NotFound, and the retry loop
// picks it up once Platform creates the CR. conductor-schema.md §10 step 3.
func (p *CapabilityPublisher) PublishWithRetry(ctx context.Context, clusterRef, agentVersion, agentLeader string, capabilities []runnerlib.CapabilityEntry) {
	log := slog.Default().With("component", "capability-publisher", "clusterRef", clusterRef, "namespace", p.namespace)

	if err := p.Publish(ctx, clusterRef, agentVersion, agentLeader, capabilities); err == nil {
		return // success on first attempt — no retry loop needed
	} else {
		log.Warn("initial capability publish failed — starting retry loop",
			"error", err, "retryInterval", capabilityPublishRetryInterval)
	}

	go func() {
		ticker := time.NewTicker(capabilityPublishRetryInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				log.Info("capability publish retry loop cancelled", "reason", ctx.Err())
				return
			case <-ticker.C:
				if err := p.Publish(ctx, clusterRef, agentVersion, agentLeader, capabilities); err != nil {
					log.Warn("capability publish retry failed", "error", err)
				} else {
					log.Info("capability manifest published successfully after retry")
					return
				}
			}
		}
	}()
}

// BuildManifest constructs the []CapabilityEntry slice from the registered
// execute-mode capabilities. All capabilities are declared with ExecutorMode
// and the given version string. Production builds stamp the real version via ldflags.
// The returned slice maps directly to the CRD status.capabilities array field.
// conductor-design.md §2.10, conductor-schema.md §5.
func BuildManifest(capabilities []string, version string) []runnerlib.CapabilityEntry {
	entries := make([]runnerlib.CapabilityEntry, 0, len(capabilities))
	for _, name := range capabilities {
		entries = append(entries, runnerlib.CapabilityEntry{
			Name:            name,
			Version:         version,
			Mode:            runnerlib.ExecutorMode,
			ParameterSchema: map[string]runnerlib.ParameterDef{},
		})
	}
	return entries
}
