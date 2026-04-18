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
	t.Setenv("CONDUCTOR_ROLE", "management")

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
	t.Setenv("CONDUCTOR_ROLE", "management")

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

// TestManagementRole_SkipsPermissionService verifies that the management cluster
// Conductor does not start the local PermissionService gRPC server.
// PermissionService on the management cluster is owned by Guardian — starting
// a competing instance would conflict with Guardian's authorization plane.
// conductor-schema.md §15, §10 step 6.
func TestManagementRole_SkipsPermissionService(t *testing.T) {
	if kernel.PermissionServiceEnabled(kernel.RoleManagement) {
		t.Error("management role must not start PermissionService: it is owned by Guardian on the management cluster")
	}
}

// TestManagementRole_SkipsWebhook verifies that the management cluster Conductor
// does not start the admission webhook server.
// The admission webhook on the management cluster is owned by Guardian —
// starting a competing instance would intercept requests Guardian must own.
// conductor-schema.md §15.
func TestManagementRole_SkipsWebhook(t *testing.T) {
	if kernel.WebhookEnabled(kernel.RoleManagement) {
		t.Error("management role must not start the admission webhook: it is owned by Guardian on the management cluster")
	}
}

// TestTenantRole_EnablesPermissionService verifies that the tenant cluster
// Conductor starts the local PermissionService gRPC server.
// Tenant PermissionService serves authorization decisions from the acknowledged
// PermissionSnapshot without requiring management cluster connectivity.
// conductor-schema.md §15, §10 step 6.
func TestTenantRole_EnablesPermissionService(t *testing.T) {
	if !kernel.PermissionServiceEnabled(kernel.RoleTenant) {
		t.Error("tenant role must start PermissionService")
	}
}

// TestTenantRole_EnablesWebhook verifies that the tenant cluster Conductor starts
// the admission webhook server (when TLS certs are configured).
// conductor-schema.md §15.
func TestTenantRole_EnablesWebhook(t *testing.T) {
	if !kernel.WebhookEnabled(kernel.RoleTenant) {
		t.Error("tenant role must start the admission webhook")
	}
}

// TestRunAgent_TenantRole_ReturnsNilOnCancelledContext verifies that RunAgent
// with role=tenant returns nil on a pre-cancelled context. The PermissionService
// and webhook goroutines start but see the cancelled context and shut down.
// WEBHOOK_TLS_CERT_PATH is not set so the webhook server is skipped in dev mode.
func TestRunAgent_TenantRole_ReturnsNilOnCancelledContext(t *testing.T) {
	t.Setenv("CONDUCTOR_ROLE", "tenant")

	goCtx, cancel := context.WithCancel(context.Background())
	cancel()

	execCtx := config.ExecutionContext{
		Mode:       config.ModeAgent,
		ClusterRef: "ccs-dev",
		Namespace:  "ont-system",
	}

	fakeClient := fake.NewSimpleClientset()
	fakeDynamic := dynamicfake.NewSimpleDynamicClient(runtime.NewScheme())

	if err := kernel.RunAgent(goCtx, execCtx, fakeClient, fakeDynamic); err != nil {
		t.Errorf("expected nil error for tenant role with cancelled ctx; got %v", err)
	}
}

// TestRunAgent_MissingRole_ParseRoleReturnsError verifies that ParseRole returns
// a non-nil error when CONDUCTOR_ROLE is absent. RunAgent calls ParseRole and then
// calls ExitInvariantViolation when an error is returned — testing the os.Exit path
// directly is not practical in-process. This test verifies the decision gate that
// drives the structured exit. conductor-schema.md §15.
func TestRunAgent_MissingRole_ParseRoleReturnsError(t *testing.T) {
	// Simulate absent CONDUCTOR_ROLE.
	_, err := kernel.ParseRole(func(_ string) string { return "" })
	if err == nil {
		t.Error("expected ParseRole to return error for absent CONDUCTOR_ROLE; RunAgent calls ExitInvariantViolation on this error")
	}
}

// TestRunAgent_UnrecognizedRole_ParseRoleReturnsError verifies that ParseRole
// returns a non-nil error for an unrecognised CONDUCTOR_ROLE value. RunAgent calls
// ExitInvariantViolation on this error. conductor-schema.md §15.
func TestRunAgent_UnrecognizedRole_ParseRoleReturnsError(t *testing.T) {
	_, err := kernel.ParseRole(func(key string) string {
		if key == "CONDUCTOR_ROLE" {
			return "controller"
		}
		return ""
	})
	if err == nil {
		t.Error(`expected ParseRole to return error for CONDUCTOR_ROLE="controller"`)
	}
}
