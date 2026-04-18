package runnerlib_test

import (
	"testing"

	"github.com/ontai-dev/conductor/pkg/runnerlib"
)

// TestJobSpecZeroValueNoPanic verifies that the zero value of JobSpec is
// accessible without panic.
func TestJobSpecZeroValueNoPanic(t *testing.T) {
	var s runnerlib.JobSpec

	_ = s.Name
	_ = s.Namespace
	_ = s.Image
	_ = s.Capability
	_ = s.ClusterRef
	_ = s.QueueName
	_ = s.OperationResultConfigMap
	_ = s.SecretVolumes
	_ = s.TTLSecondsAfterFinished
	_ = s.ServiceAccountName
}

// TestSecretVolumeReadOnlyConvention documents the convention that SecretVolume
// ReadOnly must always be set to true by callers. The builder enforces this
// invariant — callers must not assume they can set ReadOnly=false. Any code
// setting ReadOnly=false on a SecretVolume constructed via WithSecretVolume is
// an error: the builder will override it to true.
//
// This is a convention test — not enforced by the type. The builder enforces it.
func TestSecretVolumeReadOnlyConvention(t *testing.T) {
	// A SecretVolume produced by the builder must always have ReadOnly=true.
	spec, err := runnerlib.NewJobSpecBuilder().
		WithCapability(runnerlib.CapabilityBootstrap).
		WithRunnerImage("registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1").
		WithSecretVolume("talosconfig-ccs-dev", "/mnt/talosconfig").
		Build()
	if err != nil {
		t.Fatalf("Build failed unexpectedly: %v", err)
	}

	if len(spec.SecretVolumes) != 1 {
		t.Fatalf("expected 1 SecretVolume, got %d", len(spec.SecretVolumes))
	}
	// Convention enforcement: ReadOnly must be true regardless of caller intent.
	if !spec.SecretVolumes[0].ReadOnly {
		t.Error("SecretVolume.ReadOnly must be true: builder must enforce this invariant")
	}
}

// TestJobSpecBuilderBuildRequiredFields verifies that Build returns an error
// when required fields are missing.
func TestJobSpecBuilderBuildRequiredFields(t *testing.T) {
	t.Run("missing RunnerImage", func(t *testing.T) {
		_, err := runnerlib.NewJobSpecBuilder().
			WithCapability(runnerlib.CapabilityBootstrap).
			Build()
		if err == nil {
			t.Error("Build must return an error when RunnerImage is not set")
		}
	})

	t.Run("missing Capability", func(t *testing.T) {
		_, err := runnerlib.NewJobSpecBuilder().
			WithRunnerImage("registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1").
			Build()
		if err == nil {
			t.Error("Build must return an error when Capability is not set")
		}
	})

	t.Run("missing both RunnerImage and Capability", func(t *testing.T) {
		_, err := runnerlib.NewJobSpecBuilder().Build()
		if err == nil {
			t.Error("Build must return an error when both RunnerImage and Capability are not set")
		}
	})
}

// TestJobSpecBuilderFieldPropagation verifies that all fields set via With*
// methods are correctly present in the produced JobSpec.
func TestJobSpecBuilderFieldPropagation(t *testing.T) {
	const (
		capability    = "bootstrap"
		clusterRef    = "10.20.0.10"
		runnerImage   = "registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1"
		queueName     = "platform-queue"
		resultCM      = "bootstrap-result-ccs-dev"
		secretName    = "talosconfig-ccs-dev"
		mountPath     = "/mnt/talosconfig"
		ttl     int32 = 300
	)

	spec, err := runnerlib.NewJobSpecBuilder().
		WithCapability(capability).
		WithClusterRef(clusterRef).
		WithRunnerImage(runnerImage).
		WithQueueName(queueName).
		WithOperationResultConfigMap(resultCM).
		WithSecretVolume(secretName, mountPath).
		WithTTL(ttl).
		Build()

	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if spec.Capability != capability {
		t.Errorf("Capability: got %q, want %q", spec.Capability, capability)
	}
	if spec.ClusterRef != clusterRef {
		t.Errorf("ClusterRef: got %q, want %q", spec.ClusterRef, clusterRef)
	}
	if spec.Image != runnerImage {
		t.Errorf("Image: got %q, want %q", spec.Image, runnerImage)
	}
	if spec.QueueName != queueName {
		t.Errorf("QueueName: got %q, want %q", spec.QueueName, queueName)
	}
	if spec.OperationResultConfigMap != resultCM {
		t.Errorf("OperationResultConfigMap: got %q, want %q", spec.OperationResultConfigMap, resultCM)
	}
	if spec.TTLSecondsAfterFinished != ttl {
		t.Errorf("TTLSecondsAfterFinished: got %d, want %d", spec.TTLSecondsAfterFinished, ttl)
	}
	if spec.ServiceAccountName != runnerlib.ServiceAccountName {
		t.Errorf("ServiceAccountName: got %q, want %q", spec.ServiceAccountName, runnerlib.ServiceAccountName)
	}
	if len(spec.SecretVolumes) != 1 {
		t.Fatalf("SecretVolumes: got %d volumes, want 1", len(spec.SecretVolumes))
	}
	if spec.SecretVolumes[0].SecretName != secretName {
		t.Errorf("SecretVolume.SecretName: got %q, want %q", spec.SecretVolumes[0].SecretName, secretName)
	}
	if spec.SecretVolumes[0].MountPath != mountPath {
		t.Errorf("SecretVolume.MountPath: got %q, want %q", spec.SecretVolumes[0].MountPath, mountPath)
	}
}

// TestJobSpecBuilderDefaultTTL verifies that the default TTL is applied when
// WithTTL is not called.
func TestJobSpecBuilderDefaultTTL(t *testing.T) {
	spec, err := runnerlib.NewJobSpecBuilder().
		WithCapability(runnerlib.CapabilityBootstrap).
		WithRunnerImage("registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}

	if spec.TTLSecondsAfterFinished != runnerlib.DefaultTTLSecondsAfterFinished {
		t.Errorf("default TTL: got %d, want %d",
			spec.TTLSecondsAfterFinished, runnerlib.DefaultTTLSecondsAfterFinished)
	}
}

// TestJobSpecBuilderValueSemantics verifies that the builder uses value semantics:
// calling a With* method does not modify the original builder.
func TestJobSpecBuilderValueSemantics(t *testing.T) {
	base := runnerlib.NewJobSpecBuilder().
		WithCapability(runnerlib.CapabilityBootstrap).
		WithRunnerImage("registry.ontai.dev/ontai-dev/conductor:v1.9.3-r1")

	// Branch from base — should not affect base.
	branch1 := base.WithClusterRef("cluster-a")
	branch2 := base.WithClusterRef("cluster-b")

	spec1, err := branch1.Build()
	if err != nil {
		t.Fatalf("branch1 Build failed: %v", err)
	}
	spec2, err := branch2.Build()
	if err != nil {
		t.Fatalf("branch2 Build failed: %v", err)
	}

	if spec1.ClusterRef != "cluster-a" {
		t.Errorf("branch1 ClusterRef: got %q, want cluster-a", spec1.ClusterRef)
	}
	if spec2.ClusterRef != "cluster-b" {
		t.Errorf("branch2 ClusterRef: got %q, want cluster-b", spec2.ClusterRef)
	}
}
