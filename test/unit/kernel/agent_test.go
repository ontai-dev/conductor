package kernel_test

import (
	"context"
	"testing"

	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/ontai-dev/conductor/internal/config"
	"github.com/ontai-dev/conductor/internal/kernel"
)

// TestRunAgent_ValidContextReturnsNil verifies that RunAgent returns nil for a
// valid agent context when the caller cancels the context before leader election
// can proceed (clean shutdown path). conductor-design.md §4.3.
func TestRunAgent_ValidContextReturnsNil(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel: leader election returns immediately on cancelled ctx

	execCtx := config.ExecutionContext{
		Mode:       config.ModeAgent,
		ClusterRef: "ccs-test",
		Namespace:  "ont-system",
	}

	fakeClient := fake.NewSimpleClientset()
	fakeDynamic := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())

	if err := kernel.RunAgent(goCtx, execCtx, fakeClient, fakeDynamic); err != nil {
		t.Errorf("expected nil error for valid agent context with cancelled ctx; got %v", err)
	}
}

// TestRunAgent_ModeAgentWithEmptyClusterRefNoPanic verifies that RunAgent does not
// panic with an empty ClusterRef — the agent prints it but the leader election
// leaseName degrades gracefully to "conductor-".
func TestRunAgent_ModeAgentWithEmptyClusterRefNoPanic(t *testing.T) {
	goCtx, cancel := context.WithCancel(context.Background())
	cancel()

	execCtx := config.ExecutionContext{
		Mode:       config.ModeAgent,
		ClusterRef: "",
		Namespace:  "ont-system",
	}

	fakeClient := fake.NewSimpleClientset()
	fakeDynamic := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())

	if err := kernel.RunAgent(goCtx, execCtx, fakeClient, fakeDynamic); err != nil {
		t.Errorf("expected nil error even with empty ClusterRef; got %v", err)
	}
}
