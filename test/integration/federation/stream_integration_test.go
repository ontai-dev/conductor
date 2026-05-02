// Package federation_test contains integration tests for the federation
// bidirectional gRPC stream between tenant and management Conductor.
//
// Tests spin up a real FederationServer on a random port with in-process TLS
// certificates, connect a raw gRPC client (or FederationClient) and verify the
// full message exchange. No external certificate files are required.
//
// conductor-schema.md §18.
package federation_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/ontai-dev/conductor/internal/federation"
	"github.com/ontai-dev/conductor/internal/permissionservice"
)

// ── In-process certificate helpers ───────────────────────────────────────────

type streamTestCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func newStreamTestCA(t *testing.T) *streamTestCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(10),
		Subject:               pkix.Name{CommonName: "stream-test-ca"},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}
	return &streamTestCA{cert: cert, certDER: der, key: key}
}

func (ca *streamTestCA) issueServerCert(t *testing.T) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(11),
		Subject:      pkix.Name{CommonName: "stream-test-server"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create server cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal server key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func (ca *streamTestCA) issueClientCert(t *testing.T, clusterID string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(12),
		Subject:      pkix.Name{CommonName: clusterID},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{clusterID},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		t.Fatalf("create client cert: %v", err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatalf("marshal client key: %v", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func (ca *streamTestCA) pemBytes() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER})
}

// writeTempStreamCerts writes cert/key/CA PEM files to a temp dir and returns paths.
func writeTempStreamCerts(t *testing.T, certPEM, keyPEM, caPEM []byte) (certPath, keyPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	caPath = filepath.Join(dir, "ca.pem")
	must(t, os.WriteFile(certPath, certPEM, 0o600), "write cert")
	must(t, os.WriteFile(keyPath, keyPEM, 0o600), "write key")
	must(t, os.WriteFile(caPath, caPEM, 0o600), "write ca")
	return
}

func must(t *testing.T, err error, msg string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", msg, err)
	}
}

// startStreamServer starts a FederationServer (no kubeClient) on a random port.
// Returns the address and a cancel function.
func startStreamServer(t *testing.T, srv *federation.FederationServer) (addr string, cancel context.CancelFunc) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = lis.Addr().String()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = srv.StartWithListener(ctx, lis) }()
	time.Sleep(50 * time.Millisecond)
	return addr, cancel
}

// openRawStream opens a raw bidirectional gRPC stream to addr using the supplied client TLS config.
func openRawStream(t *testing.T, addr string, clientTLS *tls.Config) (grpc.ClientStream, *grpc.ClientConn) {
	t.Helper()
	creds := credentials.NewTLS(clientTLS)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	ctx := context.Background()
	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/conductor.federation.v1alpha1.FederationService/Stream",
		federation.ForceCodecOption(),
	)
	if err != nil {
		conn.Close()
		t.Fatalf("open stream: %v", err)
	}
	return stream, conn
}

// mustMarshalJSON marshals v to JSON in tests.
func mustMarshalJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return b
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestStream_HeartBeat_ServerRespondsWithACK sends a HeartBeat to the server and
// verifies the server responds with an ACK HeartBeat.
func TestStream_HeartBeat_ServerRespondsWithACK(t *testing.T) {
	ca := newStreamTestCA(t)
	serverCertPEM, serverKeyPEM := ca.issueServerCert(t)
	clientCertPEM, clientKeyPEM := ca.issueClientCert(t, "ccs-hb-test")
	caPEM := ca.pemBytes()

	serverCertPath, serverKeyPath, caPath := writeTempStreamCerts(t, serverCertPEM, serverKeyPEM, caPEM)
	clientCertPath, clientKeyPath, _ := writeTempStreamCerts(t, clientCertPEM, clientKeyPEM, caPEM)

	serverTLS, err := federation.BuildServerTLSConfig(caPath, serverCertPath, serverKeyPath)
	if err != nil {
		t.Fatalf("server TLS: %v", err)
	}
	srv := federation.NewFederationServerFromTLS(serverTLS, nil)
	addr, _ := startStreamServer(t, srv)

	clientTLS, err := federation.BuildClientTLSConfig(caPath, clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("client TLS: %v", err)
	}
	stream, conn := openRawStream(t, addr, clientTLS)
	defer conn.Close()

	// Send a HeartBeat to the server.
	hb := &federation.Envelope{
		Type:           federation.TypeHeartBeat,
		SequenceNumber: 1,
		ClusterID:      "ccs-hb-test",
		Data:           mustMarshalJSON(t, federation.HeartBeat{IsAck: false}),
	}
	if err := stream.SendMsg(hb); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}

	// The server should respond with an ACK heartbeat.
	resp := &federation.Envelope{}
	if err := stream.RecvMsg(resp); err != nil {
		t.Fatalf("recv heartbeat ACK: %v", err)
	}
	if resp.Type != federation.TypeHeartBeat {
		t.Errorf("response type = %q; want %q", resp.Type, federation.TypeHeartBeat)
	}
	var ackHB federation.HeartBeat
	if err := json.Unmarshal(resp.Data, &ackHB); err != nil {
		t.Fatalf("unmarshal heartbeat ACK: %v", err)
	}
	if !ackHB.IsAck {
		t.Error("HeartBeat response IsAck = false; want true")
	}
}

// TestStream_AuditEventBatch_ServerRespondsWithAck sends an AuditEventBatch to the
// server and verifies the server responds with an AuditEventAck bearing the correct
// sequence number.
func TestStream_AuditEventBatch_ServerRespondsWithAck(t *testing.T) {
	ca := newStreamTestCA(t)
	serverCertPEM, serverKeyPEM := ca.issueServerCert(t)
	clientCertPEM, clientKeyPEM := ca.issueClientCert(t, "ccs-audit-test")
	caPEM := ca.pemBytes()

	serverCertPath, serverKeyPath, caPath := writeTempStreamCerts(t, serverCertPEM, serverKeyPEM, caPEM)
	clientCertPath, clientKeyPath, _ := writeTempStreamCerts(t, clientCertPEM, clientKeyPEM, caPEM)

	serverTLS, err := federation.BuildServerTLSConfig(caPath, serverCertPath, serverKeyPath)
	if err != nil {
		t.Fatalf("server TLS: %v", err)
	}
	// kubeClient is nil — server skips ConfigMap creation but still ACKs.
	srv := federation.NewFederationServerFromTLS(serverTLS, nil)
	addr, _ := startStreamServer(t, srv)

	clientTLS, err := federation.BuildClientTLSConfig(caPath, clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("client TLS: %v", err)
	}
	stream, conn := openRawStream(t, addr, clientTLS)
	defer conn.Close()

	const testSeq = uint64(42)
	batch := &federation.Envelope{
		Type:           federation.TypeAuditEventBatch,
		SequenceNumber: testSeq,
		ClusterID:      "ccs-audit-test",
		Data: mustMarshalJSON(t, federation.AuditEventBatch{
			Events: []federation.AuditEvent{
				{SequenceNumber: 1, Subject: "alice", Action: "rbacpolicy.validated", Resource: "pol-a", Decision: "allow"},
			},
		}),
	}
	if err := stream.SendMsg(batch); err != nil {
		t.Fatalf("send audit batch: %v", err)
	}

	resp := &federation.Envelope{}
	if err := stream.RecvMsg(resp); err != nil {
		t.Fatalf("recv audit ack: %v", err)
	}
	if resp.Type != federation.TypeAuditEventAck {
		t.Errorf("response type = %q; want %q", resp.Type, federation.TypeAuditEventAck)
	}
	var ack federation.AuditEventAck
	if err := json.Unmarshal(resp.Data, &ack); err != nil {
		t.Fatalf("unmarshal ack: %v", err)
	}
	if ack.AckedSequenceNumber != testSeq {
		t.Errorf("AckedSequenceNumber = %d; want %d", ack.AckedSequenceNumber, testSeq)
	}
}

// TestStream_RevocationPush_ClientReceivesOnRevocationCh connects a FederationClient
// with a SnapshotStore, sends a RevocationPush from the server side, and verifies
// the push appears on RevocationCh and the snapshot store is cleared.
//
// This test uses FederationClient.Run and a helper goroutine to inject a
// RevocationPush via a second raw stream connection that bypasses Run's
// reconnect loop.
func TestStream_ClusterID_ExtractedFromClientCert(t *testing.T) {
	// This test verifies that the server stamps the clusterID from the TLS cert
	// onto inbound envelopes, not from the client-supplied ClusterID field.
	ca := newStreamTestCA(t)
	serverCertPEM, serverKeyPEM := ca.issueServerCert(t)
	clientCertPEM, clientKeyPEM := ca.issueClientCert(t, "ccs-id-test")
	caPEM := ca.pemBytes()

	serverCertPath, serverKeyPath, caPath := writeTempStreamCerts(t, serverCertPEM, serverKeyPEM, caPEM)
	clientCertPath, clientKeyPath, _ := writeTempStreamCerts(t, clientCertPEM, clientKeyPEM, caPEM)

	serverTLS, err := federation.BuildServerTLSConfig(caPath, serverCertPath, serverKeyPath)
	if err != nil {
		t.Fatalf("server TLS: %v", err)
	}
	srv := federation.NewFederationServerFromTLS(serverTLS, nil)
	addr, _ := startStreamServer(t, srv)

	clientTLS, err := federation.BuildClientTLSConfig(caPath, clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("client TLS: %v", err)
	}
	stream, conn := openRawStream(t, addr, clientTLS)
	defer conn.Close()

	// Send a HeartBeat claiming a WRONG cluster ID — the server must ignore the
	// ClusterID field and use the cert SAN instead; the ACK should carry the cert SAN.
	hb := &federation.Envelope{
		Type:           federation.TypeHeartBeat,
		SequenceNumber: 1,
		ClusterID:      "wrong-cluster-id",
		Data:           mustMarshalJSON(t, federation.HeartBeat{IsAck: false}),
	}
	if err := stream.SendMsg(hb); err != nil {
		t.Fatalf("send heartbeat: %v", err)
	}
	resp := &federation.Envelope{}
	if err := stream.RecvMsg(resp); err != nil {
		t.Fatalf("recv: %v", err)
	}
	// The server should stamp the ClusterID from the cert SAN, not from the envelope.
	if resp.ClusterID != "ccs-id-test" {
		t.Errorf("server ACK ClusterID = %q; want %q (from cert SAN)", resp.ClusterID, "ccs-id-test")
	}
}

// TestStream_WALReplay_OnReconnect writes 3 entries to a WAL, ACKs 1, then
// runs a FederationClient against a test server and verifies that 2 unacknowledged
// entries are replayed after the stream connects.
func TestStream_WALReplay_OnReconnect(t *testing.T) {
	ca := newStreamTestCA(t)
	serverCertPEM, serverKeyPEM := ca.issueServerCert(t)
	clientCertPEM, clientKeyPEM := ca.issueClientCert(t, "ccs-wal-replay")
	caPEM := ca.pemBytes()

	serverCertPath, serverKeyPath, caPath := writeTempStreamCerts(t, serverCertPEM, serverKeyPEM, caPEM)
	clientCertPath, clientKeyPath, clientCAPath := writeTempStreamCerts(t, clientCertPEM, clientKeyPEM, caPEM)

	serverTLS, err := federation.BuildServerTLSConfig(caPath, serverCertPath, serverKeyPath)
	if err != nil {
		t.Fatalf("server TLS: %v", err)
	}
	srv := federation.NewFederationServerFromTLS(serverTLS, nil)
	addr, _ := startStreamServer(t, srv)

	// Pre-populate WAL with 3 entries; ACK sequence 1.
	walFile := filepath.Join(t.TempDir(), "test.wal")
	wal, err := federation.OpenWAL(walFile, 1024*1024)
	if err != nil {
		t.Fatalf("open WAL: %v", err)
	}
	for seq := uint64(1); seq <= 3; seq++ {
		entry := federation.WALEntry{
			SequenceNumber: seq,
			Data:           mustMarshalJSON(t, federation.AuditEventBatch{Events: []federation.AuditEvent{{SequenceNumber: int64(seq)}}}),
		}
		if err := wal.Write(entry); err != nil {
			t.Fatalf("WAL write seq=%d: %v", seq, err)
		}
	}
	wal.Ack(1) // seq 1 already acknowledged; replay should return seq 2 and 3
	wal.Close()

	// Open WAL again to simulate a reconnect scenario (client re-opens the file).
	wal2, err := federation.OpenWAL(walFile, 1024*1024)
	if err != nil {
		t.Fatalf("reopen WAL: %v", err)
	}
	wal2.Ack(1)

	// Run FederationClient against the test server with a short context.
	store := permissionservice.NewSnapshotStore()
	client := federation.NewFederationClient(addr, clientCertPath, clientKeyPath, clientCAPath, "ccs-wal-replay", store)
	client.SetWAL(wal2)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go client.Run(ctx)

	// Wait until ACKs for seq 2 and seq 3 are received by the client (WAL advances).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if wal2.AckSeq() >= 3 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	cancel()
	_ = wal2.Close()

	if wal2.AckSeq() < 3 {
		t.Errorf("WAL ackSeq = %d; want >= 3 (seq 2 and 3 replayed and ACKed)", wal2.AckSeq())
	}
}
