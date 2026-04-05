package federation

// MessageType identifies the type of message in a federation envelope.
// The stable contract of message types is locked in conductor-schema.md §18.
// Adding new message types requires a Platform Governor directive.
type MessageType string

const (
	// TypeRunnerConfigValidationRequest is sent from tenant to management to
	// validate RunnerConfig parameters before Job materialisation.
	TypeRunnerConfigValidationRequest MessageType = "RunnerConfigValidationRequest"

	// TypeRunnerConfigValidationResponse is sent from management to tenant with
	// the validation result (approved or rejected with structured reason).
	TypeRunnerConfigValidationResponse MessageType = "RunnerConfigValidationResponse"

	// TypeAuditEventBatch is sent from tenant to management carrying a batch of
	// sequenced audit events for the management AuditSinkReconciler.
	TypeAuditEventBatch MessageType = "AuditEventBatch"

	// TypeAuditEventAck is sent from management to tenant acknowledging receipt
	// of an AuditEventBatch by sequence number.
	TypeAuditEventAck MessageType = "AuditEventAck"

	// TypeRevocationPush is sent from management to tenant to initiate revocation
	// of a PackInstance or PermissionSnapshot. This is the architectural reason
	// a persistent bidirectional stream is used over individual RPCs.
	TypeRevocationPush MessageType = "RevocationPush"

	// TypeHeartBeat is sent by both sides at 30-second intervals as a liveness
	// signal. Three consecutive missed HeartBeat acknowledgments degrade the channel.
	TypeHeartBeat MessageType = "HeartBeat"
)

// Envelope is the typed outer wrapper carried on every federation stream message.
// All fields are present in every message. Type-specific payload is in Data.
//
// SequenceNumber is monotonically increasing per sender. ClusterID identifies
// the tenant cluster for audit correlation — on the management side it is the
// cluster ID extracted from the client certificate SAN.
type Envelope struct {
	// Type is the discriminator that determines how Data is interpreted.
	Type MessageType `json:"type"`

	// SequenceNumber is monotonically increasing per sender, starting at 1.
	SequenceNumber uint64 `json:"sequenceNumber"`

	// ClusterID is the tenant cluster ID. Set by the tenant on outbound messages.
	// Set by the management side from the client cert SAN on inbound messages.
	ClusterID string `json:"clusterID"`

	// Data carries the type-specific payload as a JSON-encoded byte slice.
	Data []byte `json:"data,omitempty"`
}

// ── Message payload types ─────────────────────────────────────────────────────

// RunnerConfigValidationRequest is the payload for TypeRunnerConfigValidationRequest.
type RunnerConfigValidationRequest struct {
	// RunnerConfigName is the name of the RunnerConfig to validate.
	RunnerConfigName string `json:"runnerConfigName"`
	// Namespace is the namespace of the RunnerConfig.
	Namespace string `json:"namespace"`
}

// RunnerConfigValidationResponse is the payload for TypeRunnerConfigValidationResponse.
type RunnerConfigValidationResponse struct {
	// RunnerConfigName mirrors the request field for correlation.
	RunnerConfigName string `json:"runnerConfigName"`
	// Approved is true when the RunnerConfig parameters are valid.
	Approved bool `json:"approved"`
	// Reason carries the rejection reason when Approved is false.
	Reason string `json:"reason,omitempty"`
}

// AuditEvent is a single audit event in an AuditEventBatch.
type AuditEvent struct {
	SequenceNumber int64  `json:"sequenceNumber"`
	Subject        string `json:"subject"`
	Action         string `json:"action"`
	Resource       string `json:"resource"`
	Decision       string `json:"decision"`
	MatchedPolicy  string `json:"matchedPolicy"`
}

// AuditEventBatch is the payload for TypeAuditEventBatch.
// Events are sequenced and deduplicated by the management AuditSinkReconciler
// on (cluster_id, sequence_number). Replays are safe and expected.
type AuditEventBatch struct {
	// Events is the ordered list of audit events in this batch.
	Events []AuditEvent `json:"events"`
}

// AuditEventAck is the payload for TypeAuditEventAck.
// Management sends one ack per received AuditEventBatch.
type AuditEventAck struct {
	// AckedSequenceNumber is the SequenceNumber of the acknowledged Envelope.
	AckedSequenceNumber uint64 `json:"ackedSequenceNumber"`
}

// RevocationTarget identifies what is being revoked.
type RevocationTarget string

const (
	// RevocationTargetPackInstance revokes a PackInstance artifact.
	RevocationTargetPackInstance RevocationTarget = "PackInstance"
	// RevocationTargetPermissionSnapshot revokes a PermissionSnapshot.
	RevocationTargetPermissionSnapshot RevocationTarget = "PermissionSnapshot"
)

// RevocationPush is the payload for TypeRevocationPush.
// Sent management → tenant without waiting for the tenant to initiate.
// This is the architectural justification for a persistent bidirectional stream
// over individual RPCs. conductor-schema.md §18.
type RevocationPush struct {
	// Target identifies whether a PackInstance or PermissionSnapshot is revoked.
	Target RevocationTarget `json:"target"`
	// ResourceName is the name of the revoked resource CR.
	ResourceName string `json:"resourceName"`
	// Namespace is the namespace of the revoked resource.
	Namespace string `json:"namespace"`
}

// HeartBeat is the payload for TypeHeartBeat.
// Both sides send HeartBeat messages at 30-second intervals.
// Three consecutive missed HeartBeat acknowledgments degrade the channel.
// conductor-schema.md §18.
type HeartBeat struct {
	// IsAck is true when this HeartBeat is an acknowledgment of a received HeartBeat.
	IsAck bool `json:"isAck,omitempty"`
}
