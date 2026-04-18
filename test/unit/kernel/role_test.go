package kernel_test

import (
	"testing"

	"github.com/ontai-dev/conductor/internal/kernel"
)

// WS1 — Role enforcement unit tests.
// ParseRole is tested directly because process exit on invalid role cannot be
// asserted in a unit test. These tests verify the four role inputs the
// conductor binary must handle: management, tenant, absent, invalid.

// TestParseRole_Management verifies that CONDUCTOR_ROLE=management returns
// RoleManagement with no error. The management role activates the signing
// authority (PackInstance and PermissionSnapshot signing loops) and starts the
// FederationServer listener. Validation authority is inactive. INV-026.
func TestParseRole_Management(t *testing.T) {
	role, err := kernel.ParseRole(func(key string) string {
		if key == "CONDUCTOR_ROLE" {
			return "management"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("ParseRole returned unexpected error: %v", err)
	}
	if role != kernel.RoleManagement {
		t.Errorf("expected RoleManagement; got %q", role)
	}
}

// TestParseRole_Tenant verifies that CONDUCTOR_ROLE=tenant returns RoleTenant
// with no error. The tenant role activates validation authority (PermissionSnapshot
// and PackInstance pull loops) and starts the FederationClient toward
// MGMT_FEDERATION_ADDR. Signing authority is inactive. INV-026.
func TestParseRole_Tenant(t *testing.T) {
	role, err := kernel.ParseRole(func(key string) string {
		if key == "CONDUCTOR_ROLE" {
			return "tenant"
		}
		return ""
	})
	if err != nil {
		t.Fatalf("ParseRole returned unexpected error: %v", err)
	}
	if role != kernel.RoleTenant {
		t.Errorf("expected RoleTenant; got %q", role)
	}
}

// TestParseRole_Absent verifies that an absent CONDUCTOR_ROLE (empty env var)
// returns a non-nil error. The process must exit before any reconciliation loop
// when the role is not declared — an unresolved role is an InvariantViolation.
func TestParseRole_Absent(t *testing.T) {
	_, err := kernel.ParseRole(func(_ string) string { return "" })
	if err == nil {
		t.Error("expected non-nil error for absent CONDUCTOR_ROLE; got nil")
	}
}

// TestParseRole_Invalid verifies that an unrecognised CONDUCTOR_ROLE value
// returns a non-nil error. The valid set is fixed: "management" and "tenant".
// Any other string — including partial matches or misspellings — is rejected.
func TestParseRole_Invalid(t *testing.T) {
	_, err := kernel.ParseRole(func(key string) string {
		if key == "CONDUCTOR_ROLE" {
			return "worker"
		}
		return ""
	})
	if err == nil {
		t.Errorf(`expected non-nil error for CONDUCTOR_ROLE="worker"; got nil`)
	}
}
