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

// capabilityWatchInterval is how often the maintenance loop polls RunnerConfig
// for UID changes after a successful publish.
const capabilityWatchInterval = 15 * time.Second

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

// PublishWithRetry attempts an initial Publish and starts a background
// maintenance goroutine. The goroutine retries failed publishes every
// capabilityPublishRetryInterval, then after a successful publish polls
// RunnerConfig every capabilityWatchInterval. If the RunnerConfig is deleted
// or recreated (UID changes), the goroutine republishes the capability manifest.
//
// This handles two races:
//  1. Conductor starts before Platform creates RunnerConfig (NotFound on initial
//     attempt — retry loop picks it up). conductor-schema.md §10 step 3.
//  2. RunnerConfig is deleted and recreated (e.g. Platform restarts) — UID-based
//     watch detects the recreation and republishes immediately.
func (p *CapabilityPublisher) PublishWithRetry(ctx context.Context, clusterRef, agentVersion, agentLeader string, capabilities []runnerlib.CapabilityEntry) {
	log := slog.Default().With("component", "capability-publisher", "clusterRef", clusterRef, "namespace", p.namespace)

	if err := p.Publish(ctx, clusterRef, agentVersion, agentLeader, capabilities); err != nil {
		log.Warn("initial capability publish failed — background maintenance loop will retry",
			"error", err, "retryInterval", capabilityPublishRetryInterval)
	}

	go p.maintainPublication(ctx, clusterRef, agentVersion, agentLeader, capabilities)
}

// maintainPublication is the long-running background goroutine started by
// PublishWithRetry. It cycles through two phases until ctx is cancelled:
//
//  1. Publish phase: retries Publish every capabilityPublishRetryInterval until
//     success, then records the RunnerConfig UID.
//  2. Watch phase: polls RunnerConfig every capabilityWatchInterval. On UID
//     change or NotFound, transitions back to the publish phase.
func (p *CapabilityPublisher) maintainPublication(ctx context.Context, clusterRef, agentVersion, agentLeader string, capabilities []runnerlib.CapabilityEntry) {
	log := slog.Default().With("component", "capability-publisher", "clusterRef", clusterRef, "namespace", p.namespace)

	// Seed currentUID from a RunnerConfig that may already exist from the
	// synchronous initial attempt in PublishWithRetry.
	currentUID, err := p.fetchRunnerConfigUID(ctx, clusterRef)
	if err != nil {
		currentUID = "" // will enter publish phase immediately
	}

	for {
		// Publish phase: retry until Publish succeeds and we have a valid UID.
		if currentUID == "" {
			retryTicker := time.NewTicker(capabilityPublishRetryInterval)
		publishLoop:
			for {
				if err := p.Publish(ctx, clusterRef, agentVersion, agentLeader, capabilities); err != nil {
					log.Warn("capability publish failed — retrying",
						"error", err, "retryInterval", capabilityPublishRetryInterval)
				} else {
					uid, err := p.fetchRunnerConfigUID(ctx, clusterRef)
					if err != nil {
						log.Warn("published but failed to read RunnerConfig UID — will re-check next tick", "error", err)
					} else {
						log.Info("capability manifest published", "uid", uid)
						currentUID = uid
					}
					retryTicker.Stop()
					break publishLoop
				}
				select {
				case <-ctx.Done():
					retryTicker.Stop()
					log.Info("capability publish loop cancelled", "reason", ctx.Err())
					return
				case <-retryTicker.C:
					// retry
				}
			}
		}

		// Watch phase: poll for RunnerConfig deletion or recreation.
		watchTicker := time.NewTicker(capabilityWatchInterval)
	watchLoop:
		for {
			select {
			case <-ctx.Done():
				watchTicker.Stop()
				log.Info("capability watch loop cancelled", "reason", ctx.Err())
				return
			case <-watchTicker.C:
				uid, err := p.fetchRunnerConfigUID(ctx, clusterRef)
				if err != nil {
					watchTicker.Stop()
					log.Info("RunnerConfig not found — republishing capabilities", "clusterRef", clusterRef)
					currentUID = ""
					break watchLoop
				}
				if uid != currentUID {
					watchTicker.Stop()
					log.Info("RunnerConfig UID changed — republishing capabilities",
						"clusterRef", clusterRef, "oldUID", currentUID, "newUID", uid)
					currentUID = ""
					break watchLoop
				}
			}
		}
		// Loop back to publish phase.
	}
}

// fetchRunnerConfigUID fetches the RunnerConfig named clusterRef and returns its
// UID. Returns an error if the resource is not found or the Get call fails.
func (p *CapabilityPublisher) fetchRunnerConfigUID(ctx context.Context, clusterRef string) (string, error) {
	rc, err := p.client.Resource(runnerConfigGVR).Namespace(p.namespace).Get(ctx, clusterRef, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("capability publisher: get RunnerConfig %q in %q: %w",
			clusterRef, p.namespace, err)
	}
	return string(rc.GetUID()), nil
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
