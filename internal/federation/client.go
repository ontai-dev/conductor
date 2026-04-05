package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// FederationChannelDegraded is the condition type set on the tenant Conductor
// RunnerConfig status when the federation channel is not operational.
// conductor-schema.md §18.
const FederationChannelDegraded = "FederationChannelDegraded"

// FederationClient is the tenant-side federation gRPC client.
// It connects to the management Conductor federation port, presents a client
// certificate with the cluster ID as DNS SAN, and maintains a persistent
// bidirectional stream. conductor-schema.md §18.
type FederationClient struct {
	// mgmtAddr is the management Conductor federation address (host:port).
	mgmtAddr string

	// tlsCfg is the client TLS configuration (CA cert + client cert/key).
	tlsCfg *tlsClientConfig

	// clusterID is the cluster ID of this tenant Conductor instance.
	clusterID string

	// snapshotStore is the local PermissionService snapshot store. On receiving
	// a RevocationPush the client invalidates the snapshot cache.
	snapshotStore *permissionservice.SnapshotStore

	// mu guards degraded and seqCounter.
	mu sync.Mutex
	// degraded is true when the channel has been marked degraded due to missed heartbeats.
	degraded bool
	// seqCounter is the next outgoing envelope sequence number.
	seqCounter uint64
	// missedHeartbeatACKs counts consecutive missed heartbeat ACKs from management.
	missedHeartbeatACKs int
	// lastHeartbeatACKAt is the time the last heartbeat ACK was received.
	lastHeartbeatACKAt time.Time

	// pendingAuditBatches is a channel of Envelopes ready to send on the stream.
	// The WAL layer writes to this; the stream loop reads from it.
	pendingAuditBatches chan *Envelope

	// revocationCh is notified when a RevocationPush is received.
	revocationCh chan RevocationPush

	// sendCh is the internal channel used to pass outbound envelopes to the stream goroutine.
	sendCh chan *Envelope
}

// tlsClientConfig holds the raw paths for lazy config construction.
type tlsClientConfig struct {
	caCertPath     string
	clientCertPath string
	clientKeyPath  string
}

// NewFederationClient constructs a FederationClient.
//
// - mgmtAddr: management Conductor federation address (e.g. "10.20.0.10:9091")
// - clientCertPath, clientKeyPath: PEM files for this tenant's client cert/key
// - caCertPath: PEM file for the management CA cert (verifies server)
// - clusterID: this tenant's cluster ID (must match DNS SAN in client cert)
// - snapshotStore: local PermissionService store; nil skips cache invalidation
func NewFederationClient(
	mgmtAddr, clientCertPath, clientKeyPath, caCertPath, clusterID string,
	snapshotStore *permissionservice.SnapshotStore,
) *FederationClient {
	return &FederationClient{
		mgmtAddr:            mgmtAddr,
		tlsCfg:              &tlsClientConfig{caCertPath: caCertPath, clientCertPath: clientCertPath, clientKeyPath: clientKeyPath},
		clusterID:           clusterID,
		snapshotStore:       snapshotStore,
		pendingAuditBatches: make(chan *Envelope, 256),
		revocationCh:        make(chan RevocationPush, 16),
		sendCh:              make(chan *Envelope, 256),
	}
}

// IsDegraded returns true when the federation channel has been marked degraded
// due to consecutive missed heartbeat ACKs. conductor-schema.md §18.
func (c *FederationClient) IsDegraded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.degraded
}

// RevocationCh returns a read-only channel that emits RevocationPush messages
// received from the management Conductor.
func (c *FederationClient) RevocationCh() <-chan RevocationPush {
	return c.revocationCh
}

// SendAuditBatch queues an AuditEventBatch envelope for transmission on the stream.
// The WAL layer calls this after writing the batch to the WAL.
func (c *FederationClient) SendAuditBatch(env *Envelope) {
	select {
	case c.pendingAuditBatches <- env:
	default:
		// Channel full — drop. WAL prevents loss (WAL_MAX_BYTES check is upstream).
	}
}

// Run connects to the management Conductor federation port and maintains the
// bidirectional stream, including heartbeats and reconnection on failure.
// Run blocks until ctx is cancelled. conductor-schema.md §18.
func (c *FederationClient) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if err := c.runOnce(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			fmt.Printf("federation client: cluster=%q stream error: %v — reconnecting in 5s\n",
				c.clusterID, err)
		}

		// Wait before reconnecting to avoid a tight reconnect storm.
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}

// runOnce establishes one connection + stream session. Returns when the stream
// ends (cleanly or due to error).
func (c *FederationClient) runOnce(ctx context.Context) error {
	tlsCfg, err := BuildClientTLSConfig(c.tlsCfg.caCertPath, c.tlsCfg.clientCertPath, c.tlsCfg.clientKeyPath)
	if err != nil {
		return fmt.Errorf("build TLS config: %w", err)
	}

	creds := credentials.NewTLS(tlsCfg)
	conn, err := grpc.NewClient(c.mgmtAddr, grpc.WithTransportCredentials(creds))
	if err != nil {
		c.markDegraded()
		return fmt.Errorf("dial %q: %w", c.mgmtAddr, err)
	}
	defer conn.Close()

	// Open the bidirectional stream.
	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/conductor.federation.v1alpha1.FederationService/Stream",
	)
	if err != nil {
		c.markDegraded()
		return fmt.Errorf("open federation stream: %w", err)
	}

	// Clear degraded on successful connection.
	c.mu.Lock()
	c.degraded = false
	c.missedHeartbeatACKs = 0
	c.lastHeartbeatACKAt = time.Now()
	c.mu.Unlock()

	fmt.Printf("federation client: cluster=%q connected to management federation at %s\n",
		c.clusterID, c.mgmtAddr)

	// Run heartbeat sender and audit batch relay in goroutines; receive loop in this goroutine.
	streamCtx, streamCancel := context.WithCancel(ctx)
	defer streamCancel()

	go c.heartbeatSender(streamCtx, stream)
	go c.heartbeatMonitor(streamCtx)
	go c.auditBatchRelay(streamCtx, stream)

	return c.receiveLoop(streamCtx, stream, streamCancel)
}

// receiveLoop reads inbound envelopes from the management side.
func (c *FederationClient) receiveLoop(ctx context.Context, stream grpc.ClientStream, cancel context.CancelFunc) error {
	for {
		env := &Envelope{}
		if err := stream.RecvMsg(env); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if status.Code(err) == codes.Canceled || status.Code(err) == codes.Unavailable {
				c.markDegraded()
				cancel()
				return fmt.Errorf("stream closed by management: %w", err)
			}
			cancel()
			return fmt.Errorf("recv: %w", err)
		}

		switch env.Type {
		case TypeHeartBeat:
			// Reset missed ACK counter on any heartbeat from management.
			c.mu.Lock()
			c.missedHeartbeatACKs = 0
			c.lastHeartbeatACKAt = time.Now()
			c.mu.Unlock()

		case TypeAuditEventAck:
			var ack AuditEventAck
			if err := json.Unmarshal(env.Data, &ack); err == nil {
				// WAL layer will advance read pointer past ack.AckedSequenceNumber.
				// (Handled by WAL in WS3.)
				_ = ack.AckedSequenceNumber
			}

		case TypeRevocationPush:
			var push RevocationPush
			if err := json.Unmarshal(env.Data, &push); err != nil {
				fmt.Printf("federation client: cluster=%q malformed RevocationPush: %v\n", c.clusterID, err)
				continue
			}
			// Invalidate local PermissionService snapshot cache.
			if c.snapshotStore != nil && push.Target == RevocationTargetPermissionSnapshot {
				c.snapshotStore.Clear()
			}
			// Deliver to consumer.
			select {
			case c.revocationCh <- push:
			default:
			}
			fmt.Printf("federation client: cluster=%q received RevocationPush target=%s resource=%s\n",
				c.clusterID, push.Target, push.ResourceName)

		case TypeRunnerConfigValidationResponse:
			// Future: route to validation call waiter.
			_ = env

		default:
			fmt.Printf("federation client: cluster=%q unknown message type %q\n", c.clusterID, env.Type)
		}
	}
}

// heartbeatSender sends a HeartBeat every HeartbeatInterval.
func (c *FederationClient) heartbeatSender(ctx context.Context, stream grpc.ClientStream) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			c.seqCounter++
			seq := c.seqCounter
			c.mu.Unlock()

			env := &Envelope{
				Type:           TypeHeartBeat,
				SequenceNumber: seq,
				ClusterID:      c.clusterID,
				Data:           mustMarshal(HeartBeat{IsAck: false}),
			}
			if err := stream.SendMsg(env); err != nil {
				fmt.Printf("federation client: cluster=%q heartbeat send error: %v\n", c.clusterID, err)
				return
			}
		}
	}
}

// heartbeatMonitor checks for missed heartbeat ACKs and marks the channel degraded
// if HeartbeatMissedThreshold is exceeded.
func (c *FederationClient) heartbeatMonitor(ctx context.Context) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			// If we haven't received a heartbeat ACK in the expected window,
			// increment the missed counter.
			if time.Since(c.lastHeartbeatACKAt) > HeartbeatInterval {
				c.missedHeartbeatACKs++
				missed := c.missedHeartbeatACKs
				c.mu.Unlock()

				if missed >= HeartbeatMissedThreshold {
					fmt.Printf("federation client: cluster=%q heartbeat ACK missed %d times — marking degraded\n",
						c.clusterID, missed)
					c.markDegraded()
					return
				}
			} else {
				c.mu.Unlock()
			}
		}
	}
}

// auditBatchRelay reads from pendingAuditBatches and sends via the stream.
func (c *FederationClient) auditBatchRelay(ctx context.Context, stream grpc.ClientStream) {
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-c.pendingAuditBatches:
			if err := stream.SendMsg(env); err != nil {
				fmt.Printf("federation client: cluster=%q audit batch send error: %v\n", c.clusterID, err)
				// Put back in queue so WAL replay can resend.
				select {
				case c.pendingAuditBatches <- env:
				default:
				}
				return
			}
		}
	}
}

// markDegraded marks the federation channel as degraded.
func (c *FederationClient) markDegraded() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.degraded {
		fmt.Printf("federation client: cluster=%q channel degraded\n", c.clusterID)
		c.degraded = true
	}
}

// nextSeq returns the next outgoing sequence number.
func (c *FederationClient) nextSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.seqCounter++
	return c.seqCounter
}

// NewAuditBatchEnvelope creates an Envelope from an AuditEventBatch.
func (c *FederationClient) NewAuditBatchEnvelope(batch AuditEventBatch) *Envelope {
	return &Envelope{
		Type:           TypeAuditEventBatch,
		SequenceNumber: c.nextSeq(),
		ClusterID:      c.clusterID,
		Data:           mustMarshal(batch),
	}
}
