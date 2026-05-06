// Package integration_test contains envtest integration tests for the conductor
// binary's execute-mode step sequencer and role-gated behavior.
//
// These tests use envtest to spin up a real API server and etcd, verifying that
// the step sequencer correctly persists StepResults and terminal conditions to
// RunnerConfig status in the real Kubernetes API — behavior that the unit tests'
// recording StepStatusWriter cannot validate (no real status patch ordering,
// no etcd visibility).
//
// envtest binaries are required. From the ontai root:
//
//	make envtest-setup
//	export KUBEBUILDER_ASSETS=$(make -s envtest-path)
package integration_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	seamcorev1alpha1 "github.com/ontai-dev/seam-core/api/v1alpha1"
)

// runnerConfigGVR is the GroupVersionResource for InfrastructureRunnerConfig CRs.
// The conductor uses dynamic clients to interact with RunnerConfig -- the types
// in runnerlib are plain Go structs (not controller-runtime managed objects),
// so all CRD interactions use unstructured.Unstructured.
var runnerConfigGVR = schema.GroupVersionResource{
	Group:    "infrastructure.ontai.dev",
	Version:  "v1alpha1",
	Resource: "infrastructurerunnerconfigs",
}

var (
	testEnv       *envtest.Environment
	restCfg       *rest.Config
	dynamicClient dynamic.Interface
)

func TestMain(m *testing.M) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		fmt.Fprintln(os.Stderr, "KUBEBUILDER_ASSETS not set; skipping integration suite (requires KUBEBUILDER_ASSETS and CONDUCTOR-ENVTEST-BINARIES closed)")
		os.Exit(0)
	}

	ctrl.SetLogger(zap.New(zap.UseDevMode(true)))

	crdPath := filepath.Join("..", "..", "config", "crd")

	testEnv = &envtest.Environment{
		CRDDirectoryPaths:     []string{crdPath},
		ErrorIfCRDPathMissing: true,
	}

	var err error
	restCfg, err = testEnv.Start()
	if err != nil {
		panic("failed to start envtest: " + err.Error())
	}

	dynamicClient, err = dynamic.NewForConfig(restCfg)
	if err != nil {
		panic("failed to create dynamic client: " + err.Error())
	}

	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

// poll waits up to timeout for condition to return true, checking every 200ms.
func poll(t *testing.T, timeout time.Duration, condition func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return true
		}
		time.Sleep(200 * time.Millisecond)
	}
	return false
}

// createRunnerConfig creates a RunnerConfig CR in the real API server with the
// given namespace, name, and spec. Returns the UID of the created object.
func createRunnerConfig(ctx context.Context, t *testing.T, ns, name string, spec seamcorev1alpha1.InfrastructureRunnerConfigSpec) string {
	t.Helper()

	specBytes, err := json.Marshal(spec)
	if err != nil {
		t.Fatalf("marshal RunnerConfigSpec: %v", err)
	}

	var specMap map[string]interface{}
	if err := json.Unmarshal(specBytes, &specMap); err != nil {
		t.Fatalf("unmarshal RunnerConfigSpec to map: %v", err)
	}

	obj := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "infrastructure.ontai.dev/v1alpha1",
			"kind":       "InfrastructureRunnerConfig",
			"metadata": map[string]interface{}{
				"name":      name,
				"namespace": ns,
			},
			"spec": specMap,
		},
	}

	created, err := dynamicClient.Resource(runnerConfigGVR).Namespace(ns).Create(ctx, obj, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create RunnerConfig %s/%s: %v", ns, name, err)
	}

	t.Cleanup(func() {
		_ = dynamicClient.Resource(runnerConfigGVR).Namespace(ns).Delete(
			context.Background(), name, metav1.DeleteOptions{})
	})

	return string(created.GetUID())
}

// getRunnerConfig retrieves a RunnerConfig by namespace/name.
func getRunnerConfig(ctx context.Context, t *testing.T, ns, name string) *unstructured.Unstructured {
	t.Helper()
	obj, err := dynamicClient.Resource(runnerConfigGVR).Namespace(ns).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get RunnerConfig %s/%s: %v", ns, name, err)
	}
	return obj
}

// k8sStepStatusWriter is a real StepStatusWriter that patches RunnerConfig
// status via the dynamic Kubernetes client. This replaces the recording
// StepStatusWriter used in unit tests with one that writes to the actual
// API server, allowing envtest to observe the side effects.
//
// This is the integration-test implementation of the TODO marked in
// cmd/conductor/main.go: "replace with a real RunnerConfig status writer
// that patches RunnerConfig.status.stepResults via the Kubernetes API."
type k8sStepStatusWriter struct {
	client    dynamic.Interface
	namespace string
	name      string
}

func (w *k8sStepStatusWriter) WriteStepResult(ctx context.Context, result seamcorev1alpha1.RunnerConfigStepResult) error {
	return w.patchStatus(ctx, func(status map[string]interface{}) {
		results, _ := status["stepResults"].([]interface{})
		resultMap := map[string]interface{}{
			"name":   result.Name,
			"status": string(result.Status),
		}
		status["stepResults"] = append(results, resultMap)
	})
}

func (w *k8sStepStatusWriter) WriteCompleted(ctx context.Context) error {
	return w.patchStatus(ctx, func(status map[string]interface{}) {
		conditions, _ := status["conditions"].([]interface{})
		conditions = appendCondition(conditions, "Completed", "True", "StepsCompleted",
			"All steps completed successfully.")
		status["conditions"] = conditions
	})
}

func (w *k8sStepStatusWriter) WriteFailed(ctx context.Context, failedStep string) error {
	return w.patchStatus(ctx, func(status map[string]interface{}) {
		conditions, _ := status["conditions"].([]interface{})
		conditions = appendCondition(conditions, "Failed", "True", "StepFailed",
			fmt.Sprintf("Step %q failed with HaltOnFailure=true.", failedStep))
		status["conditions"] = conditions
	})
}

// patchStatus fetches the RunnerConfig, applies mutFn to status, and patches it back.
func (w *k8sStepStatusWriter) patchStatus(ctx context.Context, mutFn func(map[string]interface{})) error {
	obj, err := w.client.Resource(runnerConfigGVR).Namespace(w.namespace).Get(ctx, w.name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("k8sStepStatusWriter get %s/%s: %w", w.namespace, w.name, err)
	}

	status, _ := obj.Object["status"].(map[string]interface{})
	if status == nil {
		status = map[string]interface{}{}
	}
	mutFn(status)
	obj.Object["status"] = status

	statusBytes, err := json.Marshal(map[string]interface{}{
		"status": status,
	})
	if err != nil {
		return fmt.Errorf("k8sStepStatusWriter marshal status: %w", err)
	}

	_, err = w.client.Resource(runnerConfigGVR).Namespace(w.namespace).Patch(
		ctx, w.name, types.MergePatchType, statusBytes,
		metav1.PatchOptions{},
		"status",
	)
	return err
}

// appendCondition adds a condition to the conditions slice (replaces if same type exists).
func appendCondition(conditions []interface{}, condType, status, reason, message string) []interface{} {
	now := metav1.Now().UTC().Format(time.RFC3339)
	newCond := map[string]interface{}{
		"type":               condType,
		"status":             status,
		"reason":             reason,
		"message":            message,
		"lastTransitionTime": now,
	}
	for i, c := range conditions {
		cm, _ := c.(map[string]interface{})
		if cm["type"] == condType {
			conditions[i] = newCond
			return conditions
		}
	}
	return append(conditions, newCond)
}

// getRunnerConfigStatus retrieves the status map from a RunnerConfig object.
func getRunnerConfigStatus(obj *unstructured.Unstructured) map[string]interface{} {
	status, _ := obj.Object["status"].(map[string]interface{})
	if status == nil {
		return map[string]interface{}{}
	}
	return status
}

// hasCondition returns true if the conditions slice contains a condition of
// condType with the given status value.
func hasCondition(obj *unstructured.Unstructured, condType, condStatus string) bool {
	status := getRunnerConfigStatus(obj)
	conditions, _ := status["conditions"].([]interface{})
	for _, c := range conditions {
		cm, _ := c.(map[string]interface{})
		if cm["type"] == condType && cm["status"] == condStatus {
			return true
		}
	}
	return false
}

// getStepResults returns the stepResults slice from a RunnerConfig status.
func getStepResults(obj *unstructured.Unstructured) []map[string]interface{} {
	status := getRunnerConfigStatus(obj)
	raw, _ := status["stepResults"].([]interface{})
	results := make([]map[string]interface{}, 0, len(raw))
	for _, r := range raw {
		if rm, ok := r.(map[string]interface{}); ok {
			results = append(results, rm)
		}
	}
	return results
}

