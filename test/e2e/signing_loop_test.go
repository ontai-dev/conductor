package e2e_test

// Scenario: Signing loop artifact storage
//
// Pre-conditions required for this test to run:
//   - ccs-mgmt fully provisioned (MGMT_KUBECONFIG set)
//   - Conductor agent Deployment running in ont-system on ccs-mgmt
//   - SIGNING_PRIVATE_KEY_PATH set in the Conductor Deployment env (management cluster)
//   - A PackInstance CR exists in infra-system (or seam-tenant-ccs-test) on ccs-mgmt
//     with no existing signature annotation
//
// What this test verifies (conductor-schema.md §6, session/38 WS1, INV-026):
//   - Management cluster Conductor signing loop detects unsigned PackInstance CR
//   - Conductor signs the PackInstance with the platform Ed25519 private key
//   - Signed artifact is stored as Secret seam-pack-signed-{clusterName}-{packInstanceName}
//     in seam-tenant-{clusterName} on ccs-mgmt
//   - Secret data contains artifact (base64 JSON) and signature (base64 Ed25519)
//   - Second signing cycle is a no-op (idempotent — signature matches, no re-write)

import (
	. "github.com/onsi/ginkgo/v2"
)

var _ = Describe("signing loop artifact storage", func() {
	It("signing loop detects unsigned PackInstance and writes signature Secret", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("signed Secret name follows seam-pack-signed-{clusterName}-{packInstanceName} convention", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("Secret data.artifact is base64-encoded JSON of PackInstance spec", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("Secret data.signature is a valid Ed25519 signature over the artifact bytes", func() {
		Skip("lab cluster not yet provisioned")
	})

	It("second signing cycle is idempotent — no update when signature already matches", func() {
		Skip("lab cluster not yet provisioned")
	})
})
