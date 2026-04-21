package config

import (
	"os"
	"testing"
)

func TestBuildAgentContext_EnvVarTakesPrecedence(t *testing.T) {
	t.Setenv(EnvClusterRef, "ccs-from-env")
	t.Setenv(EnvPodNamespace, "ont-system")

	ctx, err := BuildAgentContext("ccs-from-flag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx.ClusterRef != "ccs-from-env" {
		t.Errorf("ClusterRef = %q, want %q", ctx.ClusterRef, "ccs-from-env")
	}
}

func TestBuildAgentContext_FlagFallbackWhenEnvAbsent(t *testing.T) {
	os.Unsetenv(EnvClusterRef)
	t.Setenv(EnvPodNamespace, "ont-system")

	ctx, err := BuildAgentContext("ccs-from-flag")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx.ClusterRef != "ccs-from-flag" {
		t.Errorf("ClusterRef = %q, want %q", ctx.ClusterRef, "ccs-from-flag")
	}
}

func TestBuildAgentContext_ErrorWhenBothAbsent(t *testing.T) {
	os.Unsetenv(EnvClusterRef)
	t.Setenv(EnvPodNamespace, "ont-system")

	_, err := BuildAgentContext("")
	if err == nil {
		t.Fatal("expected error when both env and flag are empty, got nil")
	}
}

func TestBuildAgentContext_NamespaceDefaultsToOntSystem(t *testing.T) {
	os.Unsetenv(EnvClusterRef)
	os.Unsetenv(EnvPodNamespace)

	ctx, err := BuildAgentContext("ccs-mgmt")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ctx.Namespace != DefaultNamespace {
		t.Errorf("Namespace = %q, want %q", ctx.Namespace, DefaultNamespace)
	}
}
