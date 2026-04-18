package kernel

import "fmt"

// Role identifies the operational role of a Conductor agent deployment.
// The role determines which control loops are active and which federation
// endpoint is started. conductor-design.md §4.3.
type Role string

const (
	// RoleManagement identifies the management cluster Conductor.
	// Signing authority is active: PackInstance and PermissionSnapshot signing
	// loops run. FederationServer listener is initialised. Validation authority
	// (PermissionSnapshot pull from management cluster) is inactive.
	// INV-026.
	RoleManagement Role = "management"

	// RoleTenant identifies a tenant cluster Conductor.
	// Validation authority is active: PermissionSnapshot and PackInstance pull
	// loops run. FederationClient is started toward MGMT_FEDERATION_ADDR.
	// Signing authority is inactive.
	// INV-026.
	RoleTenant Role = "tenant"
)

// ParseRole reads the CONDUCTOR_ROLE environment variable and returns the
// corresponding Role constant. Returns a non-nil error when the variable is
// absent or carries an unrecognised value.
//
// The caller must exit before entering any reconciliation loop when an error
// is returned — an unresolved role is an InvariantViolation.
//
// The env parameter is the environment lookup function (typically os.Getenv).
// It is injected to enable unit testing without modifying the process environment.
func ParseRole(env func(string) string) (Role, error) {
	r := env("CONDUCTOR_ROLE")
	switch r {
	case string(RoleManagement):
		return RoleManagement, nil
	case string(RoleTenant):
		return RoleTenant, nil
	case "":
		return "", fmt.Errorf("CONDUCTOR_ROLE is not set; must be %q or %q", RoleManagement, RoleTenant)
	default:
		return "", fmt.Errorf("unknown CONDUCTOR_ROLE %q; must be %q or %q", r, RoleManagement, RoleTenant)
	}
}
