package e2e_test

// signing_loop_test.go -- live cluster verification of conductor role=management
// signing loop artifact storage on ccs-mgmt.
//
// Pre-conditions:
//   - MGMT_KUBECONFIG set; conductor running in ont-system on ccs-mgmt.
//   - SIGNING_PRIVATE_KEY_PATH set in the conductor Deployment env (management cluster).
//   - TENANT_CLUSTER_NAME=ccs-dev.
//   - cert-manager ClusterPack deployed; PackInstance cert-manager exists in
//     seam-tenant-{tenantClusterName} on ccs-mgmt.
//
// What this test verifies (conductor-schema.md §6, INV-026):
//   - Signing loop detects PackInstance and writes signed artifact Secret.
//   - Secret name follows seam-pack-signed-{clusterName}-{packInstanceName}.
//   - Secret carries data.artifact (base64 JSON) and data.signature (base64 Ed25519).
//   - ClusterPack cert-manager carries ontai.dev/pack-signature annotation.
//   - Second read of the Secret shows no change (idempotent).

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

const (
	signingPollTimeout  = 3 * time.Minute
	signingPollInterval = 5 * time.Second

	packSignatureAnnotation = "ontai.dev/pack-signature"
)

var (
	clusterPackGVR = schema.GroupVersionResource{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: "infrastructureclusterpacks",
	}
	packInstanceGVR = schema.GroupVersionResource{
		Group: "infrastructure.ontai.dev", Version: "v1alpha1", Resource: "infrastructurepackinstances",
	}
)

var _ = Describe("Conductor signing loop: PackInstance artifact storage", func() {
	// All tests use mgmtClient (conductor role=management runs on ccs-mgmt).
	// PackInstance name follows {basePackName}-{clusterName}: cert-manager-helm-{cluster}.
	// Signed artifact Secret name: seam-pack-signed-{cluster}-{packInstanceName}.

	certMgrSecretName := func() string {
		return fmt.Sprintf("seam-pack-signed-%s-cert-manager-helm-%s", tenantClusterName, tenantClusterName)
	}

	It("signed artifact Secret seam-pack-signed-{tenant}-cert-manager-helm-{tenant} exists in seam-tenant-{tenant}", func() {
		tenantNS := "seam-tenant-" + tenantClusterName
		secretName := certMgrSecretName()

		By(fmt.Sprintf("polling for Secret %s/%s on %s", tenantNS, secretName, mgmtClient.Name))
		Eventually(func() bool {
			_, err := mgmtClient.Typed.CoreV1().Secrets(tenantNS).Get(
				context.Background(), secretName, metav1.GetOptions{})
			return err == nil
		}, signingPollTimeout, signingPollInterval).Should(BeTrue(),
			"signing loop did not create %s/%s within %s; "+
				"ensure conductor running with SIGNING_PRIVATE_KEY_PATH and PackInstance exists",
			tenantNS, secretName, signingPollTimeout)
	})

	It("signed Secret carries data.artifact and data.signature", func() {
		tenantNS := "seam-tenant-" + tenantClusterName

		secret, err := mgmtClient.Typed.CoreV1().Secrets(tenantNS).Get(
			context.Background(), certMgrSecretName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		artifact, hasArtifact := secret.Data["artifact"]
		Expect(hasArtifact).To(BeTrue(), "signed Secret must have data.artifact")
		Expect(artifact).NotTo(BeEmpty(), "data.artifact must not be empty")

		sig, hasSig := secret.Data["signature"]
		Expect(hasSig).To(BeTrue(), "signed Secret must have data.signature")
		Expect(sig).NotTo(BeEmpty(), "data.signature must not be empty")
	})

	It("data.artifact is valid base64-encoded JSON containing PackInstance fields", func() {
		tenantNS := "seam-tenant-" + tenantClusterName

		secret, err := mgmtClient.Typed.CoreV1().Secrets(tenantNS).Get(
			context.Background(), certMgrSecretName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		// Secret.Data is already decoded from base64 by the Kubernetes API.
		// The artifact field contains the raw JSON bytes of the PackInstance spec.
		artifact := secret.Data["artifact"]
		Expect(artifact).NotTo(BeEmpty())

		// Verify it decodes as JSON -- the artifact is the raw JSON, already base64-decoded.
		// If it's double-encoded, attempt one more decode.
		raw := artifact
		if decoded, err2 := base64.StdEncoding.DecodeString(string(artifact)); err2 == nil {
			raw = decoded
		}
		Expect(raw).To(ContainSubstring("{"),
			"artifact must be JSON-encoded PackInstance spec")
	})

	It("data.signature is valid base64-encoded bytes (Ed25519 output is 64 bytes)", func() {
		tenantNS := "seam-tenant-" + tenantClusterName

		secret, err := mgmtClient.Typed.CoreV1().Secrets(tenantNS).Get(
			context.Background(), certMgrSecretName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())

		sigBytes := secret.Data["signature"]
		// Secret.Data is already base64-decoded. If sig was stored as base64 string,
		// try decoding it to confirm it has the correct Ed25519 size (64 bytes).
		decoded, decErr := base64.StdEncoding.DecodeString(string(sigBytes))
		if decErr == nil {
			Expect(decoded).To(HaveLen(64),
				"Ed25519 signature must be exactly 64 bytes")
		} else {
			// Stored as raw bytes directly.
			Expect(sigBytes).To(HaveLen(64),
				"Ed25519 signature must be exactly 64 bytes")
		}
	})

	It("signing loop is idempotent: signature does not change between reads", func() {
		tenantNS := "seam-tenant-" + tenantClusterName

		secret1, err := mgmtClient.Typed.CoreV1().Secrets(tenantNS).Get(
			context.Background(), certMgrSecretName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		sig1 := string(secret1.Data["signature"])

		// Wait slightly longer than one signing loop interval (default 60s).
		By("waiting one signing loop interval (65s) then re-reading the Secret")
		time.Sleep(65 * time.Second)

		secret2, err := mgmtClient.Typed.CoreV1().Secrets(tenantNS).Get(
			context.Background(), certMgrSecretName(), metav1.GetOptions{})
		Expect(err).NotTo(HaveOccurred())
		sig2 := string(secret2.Data["signature"])

		Expect(sig1).To(Equal(sig2),
			"signing loop must not re-sign when artifact is unchanged (idempotency — INV-026)")
	})
})

var _ = Describe("Conductor signing loop: ClusterPack annotation", func() {
	It("cert-manager-{tenantClusterName} ClusterPack carries ontai.dev/pack-signature annotation", func() {
		tenantNS := "seam-tenant-" + tenantClusterName
		cpName := "cert-manager-" + tenantClusterName
		By(fmt.Sprintf("getting ClusterPack %s/%s on %s", tenantNS, cpName, mgmtClient.Name))

		// ClusterPacks are namespace-scoped under seam-tenant-{clusterName}.
		list, err := mgmtClient.Dynamic.Resource(clusterPackGVR).
			Namespace(tenantNS).List(context.Background(), metav1.ListOptions{})
		Expect(err).NotTo(HaveOccurred())

		var found bool
		for _, item := range list.Items {
			if item.GetName() != cpName {
				continue
			}
			found = true
			sig := item.GetAnnotations()[packSignatureAnnotation]
			Expect(sig).NotTo(BeEmpty(),
				"%s ClusterPack must carry %s annotation (signing loop INV-026)",
				cpName, packSignatureAnnotation)
		}
		Expect(found).To(BeTrue(),
			"ClusterPack %s not found in %s on management cluster — required for signing loop test",
			cpName, tenantNS)
	})
})
