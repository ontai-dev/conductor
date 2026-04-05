package e2e_test

// Scenario: PermissionSnapshot pull loop
//
// Pre-conditions required for this test to run:
//   - ccs-mgmt and ccs-test both provisioned and running (both kubeconfigs set)
//   - Guardian running in seam-system on ccs-mgmt
//   - Management cluster Conductor running with SIGNING_PRIVATE_KEY_PATH set
//   - A PermissionSnapshot CR exists in security-system on ccs-mgmt, signed
//   - Target cluster Conductor on ccs-test running with MGMT_KUBECONFIG_PATH
//     and SIGNING_PUBLIC_KEY_PATH set
//
// What this test verifies (conductor-schema.md §6, guardian-schema.md §9, INV-026):
//   - Target cluster Conductor pull loop discovers PermissionSnapshot on ccs-mgmt
//   - Conductor verifies the Ed25519 signature on the snapshot
//   - Valid signature: PermissionSnapshotReceipt created/updated in ont-system on ccs-test
//     with SyncStatus=InSync and matching lastAckedVersion
//   - Invalid signature: receipt shows SyncStatus=DegradedSecurityState
//     (blocks new authorization decisions, guardian-schema.md §9)
//   - PermissionService on ccs-test serves decisions from acknowledged snapshot

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("PermissionSnapshot pull loop", func() {
	It("pull loop discovers PermissionSnapshot on management cluster and verifies signature", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("valid snapshot signature results in PermissionSnapshotReceipt SyncStatus=InSync", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("lastAckedVersion in receipt matches the snapshot version on management cluster", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("invalid signature results in SyncStatus=DegradedSecurityState", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("local PermissionService on ccs-test serves decisions after snapshot is acknowledged", func() {
		Skip("lab cluster not yet provisioned")
	})
})
