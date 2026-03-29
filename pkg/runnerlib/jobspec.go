package runnerlib

import "errors"

// ServiceAccountName is the default Kubernetes ServiceAccount used by runner Jobs.
// All runner Jobs run under this service account unless overridden.
const ServiceAccountName = "ont-runner"

// DefaultTTLSecondsAfterFinished is the default TTL applied to completed Jobs
// when WithTTL is not called on the builder. 600 seconds = 10 minutes.
const DefaultTTLSecondsAfterFinished int32 = 600

// JobSpecBuilder is the interface for constructing a JobSpec for a runner executor
// Job. Each operator uses this builder (via the shared library) to produce the
// Job spec it submits to Kueue. The builder enforces invariants at Build time.
//
// Usage:
//
//	spec, err := NewJobSpecBuilder().
//	    WithCapability(CapabilityBootstrap).
//	    WithClusterRef("my-cluster").
//	    WithRunnerImage("registry.ontai.dev/ontai-dev/ont-runner:v1.9.3-r1").
//	    WithQueueName("platform-system-queue").
//	    WithOperationResultConfigMap("bootstrap-result-my-cluster").
//	    Build()
type JobSpecBuilder interface {
	// WithCapability sets the named capability this Job will execute.
	// Required. Build returns an error if not set.
	WithCapability(name string) JobSpecBuilder

	// WithClusterRef sets the cluster identity passed to the runner as CLUSTER_REF.
	WithClusterRef(clusterRef string) JobSpecBuilder

	// WithRunnerImage sets the fully qualified runner image including tag.
	// Required. Build returns an error if not set.
	WithRunnerImage(image string) JobSpecBuilder

	// WithQueueName sets the Kueue LocalQueue name this Job is submitted to.
	WithQueueName(queueName string) JobSpecBuilder

	// WithOperationResultConfigMap sets the name of the ConfigMap the runner will
	// write OperationResultSpec to before exit.
	WithOperationResultConfigMap(name string) JobSpecBuilder

	// WithSecretVolume adds a Secret mount to the Job. The Secret is always mounted
	// read-only. This method may be called multiple times to add multiple mounts.
	WithSecretVolume(secretName string, mountPath string) JobSpecBuilder

	// WithTTL sets the TTL in seconds after Job completion before the Job is
	// garbage collected. Default is DefaultTTLSecondsAfterFinished (600s).
	WithTTL(seconds int32) JobSpecBuilder

	// Build validates all required fields and produces a JobSpec.
	// Returns an error if RunnerImage, Capability, or Namespace is empty.
	Build() (JobSpec, error)
}

// JobSpec is the value type produced by JobSpecBuilder.Build(). It contains all
// fields needed for the operator to construct a Kueue-compatible batch/v1 Job.
// Operators map this to the actual Kubernetes Job manifest — this type is the
// shared contract, not a Kubernetes API type.
type JobSpec struct {
	// Name is the Job name. Derived from the capability and clusterRef by the builder.
	Name string

	// Namespace is the Kubernetes namespace for this Job.
	// Defaults to ont-system for management cluster operations.
	Namespace string

	// Image is the fully qualified runner image reference.
	Image string

	// Capability is the named capability this Job executes.
	Capability string

	// ClusterRef is the cluster identity passed as CLUSTER_REF env var.
	ClusterRef string

	// QueueName is the Kueue LocalQueue this Job targets.
	QueueName string

	// OperationResultConfigMap is the name of the ConfigMap the runner writes
	// OperationResultSpec to before exit.
	OperationResultConfigMap string

	// SecretVolumes is the list of Secret mounts for this Job.
	SecretVolumes []SecretVolume

	// TTLSecondsAfterFinished is the Job TTL after completion.
	TTLSecondsAfterFinished int32

	// ServiceAccountName is the ServiceAccount the Job pod runs under.
	ServiceAccountName string
}

// SecretVolume declares a Secret to be mounted into the runner Job pod.
//
// ReadOnly is always true by convention — runner Jobs must never modify
// mounted secrets. This invariant is enforced by the builder regardless of
// what the caller passes to WithSecretVolume.
type SecretVolume struct {
	// SecretName is the name of the Kubernetes Secret to mount.
	SecretName string

	// MountPath is the filesystem path where the Secret is mounted in the pod.
	MountPath string

	// ReadOnly is always true. The builder enforces this invariant.
	// Runner Jobs must never modify mounted secrets.
	ReadOnly bool
}

// jobSpecBuilder is the unexported concrete implementation of JobSpecBuilder.
// The builder uses value semantics — each With* method returns a new copy with
// the field set, never mutating the original. This makes the builder safe to
// branch and safe to pass between goroutines.
type jobSpecBuilder struct {
	capability               string
	clusterRef               string
	runnerImage              string
	queueName                string
	operationResultConfigMap string
	secretVolumes            []SecretVolume
	ttl                      *int32
	namespace                string
}

// NewJobSpecBuilder returns a new JobSpecBuilder with all fields at zero value.
// At minimum, WithCapability, WithRunnerImage must be called before Build.
func NewJobSpecBuilder() JobSpecBuilder {
	return &jobSpecBuilder{}
}

func (b *jobSpecBuilder) WithCapability(name string) JobSpecBuilder {
	c := *b
	c.capability = name
	return &c
}

func (b *jobSpecBuilder) WithClusterRef(clusterRef string) JobSpecBuilder {
	c := *b
	c.clusterRef = clusterRef
	return &c
}

func (b *jobSpecBuilder) WithRunnerImage(image string) JobSpecBuilder {
	c := *b
	c.runnerImage = image
	return &c
}

func (b *jobSpecBuilder) WithQueueName(queueName string) JobSpecBuilder {
	c := *b
	c.queueName = queueName
	return &c
}

func (b *jobSpecBuilder) WithOperationResultConfigMap(name string) JobSpecBuilder {
	c := *b
	c.operationResultConfigMap = name
	return &c
}

func (b *jobSpecBuilder) WithSecretVolume(secretName string, mountPath string) JobSpecBuilder {
	c := *b
	// Copy the slice to maintain value semantics — do not share backing array.
	vols := make([]SecretVolume, len(b.secretVolumes), len(b.secretVolumes)+1)
	copy(vols, b.secretVolumes)
	// ReadOnly is always true — enforced here regardless of caller intent.
	vols = append(vols, SecretVolume{
		SecretName: secretName,
		MountPath:  mountPath,
		ReadOnly:   true,
	})
	c.secretVolumes = vols
	return &c
}

func (b *jobSpecBuilder) WithTTL(seconds int32) JobSpecBuilder {
	c := *b
	c.ttl = &seconds
	return &c
}

// Build validates required fields and produces a JobSpec.
// Returns an error if RunnerImage or Capability is empty.
// Applies defaults: Namespace=ont-system if empty, TTL=600s, ServiceAccountName="ont-runner".
func (b *jobSpecBuilder) Build() (JobSpec, error) {
	if b.runnerImage == "" {
		return JobSpec{}, errors.New("runnerlib: JobSpecBuilder.Build: RunnerImage is required")
	}
	if b.capability == "" {
		return JobSpec{}, errors.New("runnerlib: JobSpecBuilder.Build: Capability is required")
	}

	ns := b.namespace
	if ns == "" {
		ns = "ont-system"
	}

	ttl := DefaultTTLSecondsAfterFinished
	if b.ttl != nil {
		ttl = *b.ttl
	}

	vols := b.secretVolumes
	if vols == nil {
		vols = []SecretVolume{}
	}

	return JobSpec{
		Namespace:                ns,
		Image:                    b.runnerImage,
		Capability:               b.capability,
		ClusterRef:               b.clusterRef,
		QueueName:                b.queueName,
		OperationResultConfigMap: b.operationResultConfigMap,
		SecretVolumes:            vols,
		TTLSecondsAfterFinished:  ttl,
		ServiceAccountName:       ServiceAccountName,
	}, nil
}
