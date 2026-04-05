package federation

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// HeartbeatInterval is the interval between heartbeat messages.
// conductor-schema.md §18.
const HeartbeatInterval = 30 * time.Second

// HeartbeatMissedThreshold is the number of consecutive missed heartbeat ACKs
// before a channel is marked degraded on the management side.
const HeartbeatMissedThreshold = 3

// AuditBatchStagingNamespace is the namespace where audit batch ConfigMaps are
// created by the FederationServer for the Guardian AuditSinkReconciler.
// conductor-schema.md §18, guardian-schema.md §15.
const AuditBatchStagingNamespace = "seam-system"

// AuditBatchLabelKey is the label applied to ConfigMaps created from AuditEventBatch
// messages. Guardian's AuditSinkReconciler watches for this label.
const AuditBatchLabelKey = "seam.ontai.dev/audit-batch"

// StreamHandlerFunc is called by the server for each accepted federation stream.
// clusterID is extracted from the client certificate SAN before this is called.
type StreamHandlerFunc func(ctx context.Context, clusterID string, stream grpc.ServerStream) error

// clusterStatus tracks heartbeat state per connected tenant.
type clusterStatus struct {
	mu             sync.Mutex
	missedHeartbeats int
}

// FederationServer is the management-side federation gRPC server.
// It listens on the federation port with mutual TLS, extracts cluster IDs
// from client certificate SANs, and maintains the bidirectional stream with
// each connected tenant Conductor. conductor-schema.md §18.
type FederationServer struct {
	// tlsCfg is the server TLS configuration (built from CA/cert/key paths).
	tlsCfg *tls.Config

	// kubeClient is used to write AuditEventBatch ConfigMaps to seam-system.
	// May be nil in tests.
	kubeClient kubernetes.Interface

	// mu guards connectedClusters.
	mu sync.RWMutex
	// connectedClusters maps clusterID → stream status for heartbeat tracking.
	connectedClusters map[string]*clusterStatus
}

// NewFederationServer constructs a FederationServer from certificate paths.
// The server does not start until Start is called.
// conductor-schema.md §18.
func NewFederationServer(caCertPath, serverCertPath, serverKeyPath string, kubeClient kubernetes.Interface) (*FederationServer, error) {
	tlsCfg, err := BuildServerTLSConfig(caCertPath, serverCertPath, serverKeyPath)
	if err != nil {
		return nil, fmt.Errorf("federation server TLS config: %w", err)
	}
	return &FederationServer{
		tlsCfg:            tlsCfg,
		kubeClient:        kubeClient,
		connectedClusters: make(map[string]*clusterStatus),
	}, nil
}

// NewFederationServerFromTLS constructs a FederationServer from an already-built
// tls.Config. Used in tests to inject a test TLS config directly.
func NewFederationServerFromTLS(tlsCfg *tls.Config, kubeClient kubernetes.Interface) *FederationServer {
	return &FederationServer{
		tlsCfg:            tlsCfg,
		kubeClient:        kubeClient,
		connectedClusters: make(map[string]*clusterStatus),
	}
}

// Start begins listening on addr and serves the federation gRPC stream with mutual TLS.
// A plain TCP listener is used — gRPC handles TLS negotiation (including ALPN h2)
// via the transport credentials. Blocks until ctx is cancelled or a fatal error.
// conductor-schema.md §18.
func (s *FederationServer) Start(ctx context.Context, addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("federation server listen %q: %w", addr, err)
	}
	return s.startOnListener(ctx, lis)
}

// StartWithListener starts the server on the provided plain TCP listener.
// The listener must NOT be pre-wrapped with TLS — gRPC applies TLS via credentials.
// Used in tests to bind to a random port. conductor-schema.md §18.
func (s *FederationServer) StartWithListener(ctx context.Context, lis net.Listener) error {
	return s.startOnListener(ctx, lis)
}

func (s *FederationServer) startOnListener(ctx context.Context, lis net.Listener) error {
	creds := credentials.NewTLS(s.tlsCfg)
	srv := grpc.NewServer(grpc.Creds(creds))
	srv.RegisterService(&federationServiceDesc, s)

	go func() {
		<-ctx.Done()
		srv.GracefulStop()
	}()

	fmt.Printf("federation server: listening on %s\n", lis.Addr())
	return srv.Serve(lis)
}

// ConnectedClusterIDs returns the cluster IDs of all currently connected tenants.
// Used for heartbeat health observability.
func (s *FederationServer) ConnectedClusterIDs() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ids := make([]string, 0, len(s.connectedClusters))
	for id := range s.connectedClusters {
		ids = append(ids, id)
	}
	return ids
}

// federationStream handles a single bidirectional stream from a connected tenant.
// It implements the grpc.ServerStream interface handler for the FederationService/Stream method.
func (s *FederationServer) federationStream(stream grpc.ServerStream) error {
	// Extract cluster ID from the peer TLS certificate SAN.
	clusterID, err := s.clusterIDFromStream(stream)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "cluster ID extraction: %v", err)
	}

	// Register this cluster as connected.
	cs := &clusterStatus{}
	s.mu.Lock()
	s.connectedClusters[clusterID] = cs
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.connectedClusters, clusterID)
		s.mu.Unlock()
	}()

	fmt.Printf("federation server: cluster %q connected\n", clusterID)
	defer fmt.Printf("federation server: cluster %q disconnected\n", clusterID)

	// Run the stream receive loop.
	return s.handleStream(stream.Context(), clusterID, cs, stream)
}

// handleStream is the main receive/send loop for a connected tenant stream.
// It processes incoming envelopes and responds appropriately.
func (s *FederationServer) handleStream(ctx context.Context, clusterID string, cs *clusterStatus, stream grpc.ServerStream) error {
	for {
		// Check for context cancellation.
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// Receive the next envelope from the tenant.
		env := &Envelope{}
		if err := stream.RecvMsg(env); err != nil {
			// Stream ended — tenant disconnected or context cancelled.
			return nil
		}

		// Stamp the cluster ID from the cert (authoritative) onto the envelope.
		env.ClusterID = clusterID

		switch env.Type {
		case TypeHeartBeat:
			// Reset missed heartbeat counter and send an ACK.
			cs.mu.Lock()
			cs.missedHeartbeats = 0
			cs.mu.Unlock()

			ack := &Envelope{
				Type:      TypeHeartBeat,
				ClusterID: clusterID,
				Data:      mustMarshal(HeartBeat{IsAck: true}),
			}
			if err := stream.SendMsg(ack); err != nil {
				return fmt.Errorf("send heartbeat ack to %q: %w", clusterID, err)
			}

		case TypeAuditEventBatch:
			// Write audit events as a ConfigMap for Guardian AuditSinkReconciler.
			if err := s.handleAuditBatch(ctx, clusterID, env); err != nil {
				fmt.Printf("federation server: cluster %q audit batch error: %v\n", clusterID, err)
				// Non-fatal — continue receiving; still send ack so tenant advances WAL.
			}

			// Send AuditEventAck.
			ack := &Envelope{
				Type:      TypeAuditEventAck,
				ClusterID: clusterID,
				Data:      mustMarshal(AuditEventAck{AckedSequenceNumber: env.SequenceNumber}),
			}
			if err := stream.SendMsg(ack); err != nil {
				return fmt.Errorf("send audit ack to %q: %w", clusterID, err)
			}

		case TypeRunnerConfigValidationRequest:
			// Respond with an approved response (stub — full validation in future session).
			var req RunnerConfigValidationRequest
			_ = json.Unmarshal(env.Data, &req)
			resp := &Envelope{
				Type:      TypeRunnerConfigValidationResponse,
				ClusterID: clusterID,
				Data: mustMarshal(RunnerConfigValidationResponse{
					RunnerConfigName: req.RunnerConfigName,
					Approved:         true,
				}),
			}
			if err := stream.SendMsg(resp); err != nil {
				return fmt.Errorf("send validation response to %q: %w", clusterID, err)
			}

		default:
			// Unknown message type — log and continue.
			fmt.Printf("federation server: cluster %q unknown message type %q\n", clusterID, env.Type)
		}
	}
}

// handleAuditBatch writes an AuditEventBatch to seam-system as a ConfigMap for
// the Guardian AuditSinkReconciler. conductor-schema.md §18.
func (s *FederationServer) handleAuditBatch(ctx context.Context, clusterID string, env *Envelope) error {
	if s.kubeClient == nil {
		return nil // tests without kube client
	}

	// The data field IS the events JSON (already in the format AuditSink expects).
	// Re-wrap as a ConfigMap.
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: fmt.Sprintf("fed-audit-batch-%s-", clusterID),
			Namespace:    AuditBatchStagingNamespace,
			Labels: map[string]string{
				AuditBatchLabelKey: "true",
			},
			Annotations: map[string]string{
				"seam.ontai.dev/cluster-id":       clusterID,
				"seam.ontai.dev/federation-seq":   fmt.Sprintf("%d", env.SequenceNumber),
			},
		},
		Data: map[string]string{
			"events": string(env.Data),
		},
	}

	if _, err := s.kubeClient.CoreV1().ConfigMaps(AuditBatchStagingNamespace).Create(ctx, cm, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create audit batch ConfigMap for cluster %q: %w", clusterID, err)
	}
	return nil
}

// clusterIDFromStream extracts the cluster ID from the peer TLS certificate.
func (s *FederationServer) clusterIDFromStream(stream grpc.ServerStream) (string, error) {
	p, ok := peer.FromContext(stream.Context())
	if !ok {
		return "", fmt.Errorf("no peer info in stream context")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return "", fmt.Errorf("peer auth info is not TLS — unexpected for federation stream")
	}
	if len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return "", fmt.Errorf("no verified certificate chain in TLS handshake")
	}
	clientCert := tlsInfo.State.VerifiedChains[0][0]
	return ClusterIDFromCert(clientCert)
}

// mustMarshal marshals v to JSON, panicking on error (only used for well-typed structs).
func mustMarshal(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("federation mustMarshal: %v", err))
	}
	return b
}

// ── gRPC service descriptor ───────────────────────────────────────────────────

// federationServiceServer is the interface type for the gRPC service descriptor.
type federationServiceServer interface {
	federationStream(stream grpc.ServerStream) error
}

// federationServiceDesc is the gRPC service descriptor for the FederationService.
// The single streaming method "Stream" establishes the bidirectional channel.
// Hand-written to avoid proto codegen dependency. conductor-schema.md §18.
var federationServiceDesc = grpc.ServiceDesc{
	ServiceName: "conductor.federation.v1alpha1.FederationService",
	HandlerType: (*federationServiceServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Stream",
			Handler:       _federationStreamHandler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "conductor/federation/v1alpha1/federation.proto",
}

func _federationStreamHandler(srv any, stream grpc.ServerStream) error {
	return srv.(federationServiceServer).federationStream(stream)
}

// SendRevocationPush sends a RevocationPush message to a connected tenant.
// Returns an error if the cluster is not currently connected.
// This is a management-side push — it does not wait for tenant initiation.
// conductor-schema.md §18.
func (s *FederationServer) SendRevocationPush(clusterID string, push RevocationPush, stream grpc.ServerStream) error {
	env := &Envelope{
		Type:      TypeRevocationPush,
		ClusterID: clusterID,
		Data:      mustMarshal(push),
	}
	return stream.SendMsg(env)
}

// MissedHeartbeats returns the missed heartbeat count for a cluster, or -1 if
// the cluster is not connected.
func (s *FederationServer) MissedHeartbeats(clusterID string) int {
	s.mu.RLock()
	cs, ok := s.connectedClusters[clusterID]
	s.mu.RUnlock()
	if !ok {
		return -1
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	return cs.missedHeartbeats
}

// IncrementMissedHeartbeats increments the missed heartbeat count for a cluster.
// Called by a background monitor when a HeartBeat ACK is not received within the
// expected interval. conductor-schema.md §18.
func (s *FederationServer) IncrementMissedHeartbeats(clusterID string) int {
	s.mu.RLock()
	cs, ok := s.connectedClusters[clusterID]
	s.mu.RUnlock()
	if !ok {
		return 0
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.missedHeartbeats++
	return cs.missedHeartbeats
}

// IsDegraded returns true if a cluster's missed heartbeat count has reached the
// degradation threshold. conductor-schema.md §18.
func (s *FederationServer) IsDegraded(clusterID string) bool {
	return s.MissedHeartbeats(clusterID) >= HeartbeatMissedThreshold
}
