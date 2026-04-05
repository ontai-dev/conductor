// Package federation_test covers the federation TLS setup, connection lifecycle,
// and cluster ID extraction. Tests use in-process self-signed certificates to
// avoid requiring external certificate files.
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
)

// ── Certificate helpers ───────────────────────────────────────────────────────

// testCA holds a self-signed CA and its key for test certificate issuance.
type testCA struct {
	cert    *x509.Certificate
	certDER []byte
	key     *ecdsa.PrivateKey
}

func newTestCA(t *testing.T) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate CA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test-federation-ca"},
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
	return &testCA{cert: cert, certDER: der, key: key}
}

// issueServerCert issues a server TLS certificate signed by the CA.
func (ca *testCA) issueServerCert(t *testing.T, dnsNames []string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate server key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test-federation-server"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     dnsNames,
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
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

// issueClientCert issues a client TLS certificate signed by the CA.
// The clusterID is embedded as a DNS SAN.
func (ca *testCA) issueClientCert(t *testing.T, clusterID string) (certPEM, keyPEM []byte) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("generate client key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(3),
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
	certPEM = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM = pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	return
}

// caPEM returns the PEM-encoded CA certificate.
func (ca *testCA) caPEM() []byte {
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca.certDER})
}

// writeTempCerts writes certificate and key PEM bytes to temp files and
// returns their paths. Files are cleaned up when the test completes.
func writeTempCerts(t *testing.T, certPEM, keyPEM, caPEM []byte) (certPath, keyPath, caPath string) {
	t.Helper()
	dir := t.TempDir()
	certPath = filepath.Join(dir, "cert.pem")
	keyPath = filepath.Join(dir, "key.pem")
	caPath = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(certPath, certPEM, 0o600); err != nil {
		t.Fatalf("write cert: %v", err)
	}
	if err := os.WriteFile(keyPath, keyPEM, 0o600); err != nil {
		t.Fatalf("write key: %v", err)
	}
	if err := os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	return
}

// startTestServer starts a FederationServer on a random port using plain TCP.
// Returns the listener address and a cancel function.
func startTestServer(t *testing.T, srv *federation.FederationServer) (addr string, cancel context.CancelFunc) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr = lis.Addr().String()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	go func() { _ = srv.StartWithListener(ctx, lis) }()
	time.Sleep(30 * time.Millisecond) // allow server to start accepting
	return addr, cancel
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestBuildServerTLSConfig_ValidPaths verifies that BuildServerTLSConfig
// succeeds with valid cert/key/CA paths and produces a config with RequireAndVerifyClientCert.
func TestBuildServerTLSConfig_ValidPaths(t *testing.T) {
	ca := newTestCA(t)
	serverCert, serverKey := ca.issueServerCert(t, []string{"localhost"})
	certPath, keyPath, caPath := writeTempCerts(t, serverCert, serverKey, ca.caPEM())

	cfg, err := federation.BuildServerTLSConfig(caPath, certPath, keyPath)
	if err != nil {
		t.Fatalf("BuildServerTLSConfig returned error: %v", err)
	}
	if cfg.ClientAuth != tls.RequireAndVerifyClientCert {
		t.Errorf("expected RequireAndVerifyClientCert, got %v", cfg.ClientAuth)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("expected TLS 1.3 minimum, got 0x%04x", cfg.MinVersion)
	}
}

// TestBuildServerTLSConfig_MissingCA verifies that a missing CA cert file returns
// an error.
func TestBuildServerTLSConfig_MissingCA(t *testing.T) {
	ca := newTestCA(t)
	serverCert, serverKey := ca.issueServerCert(t, []string{"localhost"})
	certPath, keyPath, _ := writeTempCerts(t, serverCert, serverKey, ca.caPEM())

	_, err := federation.BuildServerTLSConfig("/no/such/ca.pem", certPath, keyPath)
	if err == nil {
		t.Fatal("expected error for missing CA cert, got nil")
	}
}

// TestBuildClientTLSConfig_ValidPaths verifies client TLS config construction.
func TestBuildClientTLSConfig_ValidPaths(t *testing.T) {
	ca := newTestCA(t)
	clientCert, clientKey := ca.issueClientCert(t, "ccs-dev")
	certPath, keyPath, caPath := writeTempCerts(t, clientCert, clientKey, ca.caPEM())

	cfg, err := federation.BuildClientTLSConfig(caPath, certPath, keyPath)
	if err != nil {
		t.Fatalf("BuildClientTLSConfig returned error: %v", err)
	}
	if cfg.MinVersion != tls.VersionTLS13 {
		t.Errorf("expected TLS 1.3 minimum, got 0x%04x", cfg.MinVersion)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client certificate, got %d", len(cfg.Certificates))
	}
}

// TestClusterIDFromCert_DNSSANPresent verifies that the first DNS SAN is returned
// as the cluster ID.
func TestClusterIDFromCert_DNSSANPresent(t *testing.T) {
	ca := newTestCA(t)
	clientCert, _ := ca.issueClientCert(t, "ccs-test")
	block, _ := pem.Decode(clientCert)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse cert: %v", err)
	}

	clusterID, err := federation.ClusterIDFromCert(cert)
	if err != nil {
		t.Fatalf("ClusterIDFromCert returned error: %v", err)
	}
	if clusterID != "ccs-test" {
		t.Errorf("expected clusterID=ccs-test, got %q", clusterID)
	}
}

// TestClusterIDFromCert_NoDNSSAN_FallsBackToCommonName verifies that CommonName is
// used when no DNS SAN is present.
func TestClusterIDFromCert_NoDNSSAN_FallsBackToCommonName(t *testing.T) {
	ca := newTestCA(t)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(99),
		Subject:      pkix.Name{CommonName: "ccs-fallback"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		// No DNSNames
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	cert, _ := x509.ParseCertificate(der)

	clusterID, err := federation.ClusterIDFromCert(cert)
	if err != nil {
		t.Fatalf("ClusterIDFromCert returned error: %v", err)
	}
	if clusterID != "ccs-fallback" {
		t.Errorf("expected clusterID=ccs-fallback, got %q", clusterID)
	}
}

// TestClusterIDFromCert_NoSANNoCN verifies that an error is returned when neither
// DNS SAN nor CommonName is present.
func TestClusterIDFromCert_NoSANNoCN(t *testing.T) {
	ca := newTestCA(t)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(99),
		Subject:      pkix.Name{}, // No CN
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
	}
	der, _ := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	cert, _ := x509.ParseCertificate(der)

	_, err := federation.ClusterIDFromCert(cert)
	if err == nil {
		t.Fatal("expected error when no SAN and no CN, got nil")
	}
}

// TestFederationServer_gRPC_AcceptsValidCert verifies that a gRPC stream can be
// established when the client presents a valid management CA-signed certificate.
func TestFederationServer_gRPC_AcceptsValidCert(t *testing.T) {
	ca := newTestCA(t)

	serverCertPEM, serverKeyPEM := ca.issueServerCert(t, []string{"localhost"})
	serverCertPath, serverKeyPath, caPath := writeTempCerts(t, serverCertPEM, serverKeyPEM, ca.caPEM())

	srv, err := federation.NewFederationServer(caPath, serverCertPath, serverKeyPath, nil)
	if err != nil {
		t.Fatalf("NewFederationServer: %v", err)
	}

	addr, cancel := startTestServer(t, srv)
	defer cancel()

	// Connect with a valid client cert.
	clientCertPEM, clientKeyPEM := ca.issueClientCert(t, "ccs-dev")
	clientCertPath, clientKeyPath, _ := writeTempCerts(t, clientCertPEM, clientKeyPEM, ca.caPEM())
	clientTLSCfg, err := federation.BuildClientTLSConfig(caPath, clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	clientTLSCfg.ServerName = "127.0.0.1"

	creds := credentials.NewTLS(clientTLSCfg)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	// Attempt to open the bidirectional stream — valid client should be accepted.
	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/conductor.federation.v1alpha1.FederationService/Stream",
	)
	if err != nil {
		t.Fatalf("open stream with valid cert: %v", err)
	}
	// Stream opened successfully — close cleanly.
	_ = stream.CloseSend()
}

// TestFederationServer_gRPC_RejectsNoCert verifies that a gRPC connection
// attempt without a client certificate fails with an authentication error.
func TestFederationServer_gRPC_RejectsNoCert(t *testing.T) {
	ca := newTestCA(t)

	serverCertPEM, serverKeyPEM := ca.issueServerCert(t, []string{"localhost"})
	serverCertPath, serverKeyPath, caPath := writeTempCerts(t, serverCertPEM, serverKeyPEM, ca.caPEM())

	srv, err := federation.NewFederationServer(caPath, serverCertPath, serverKeyPath, nil)
	if err != nil {
		t.Fatalf("NewFederationServer: %v", err)
	}

	addr, cancel := startTestServer(t, srv)
	defer cancel()

	// Connect with NO client cert — just trust the server CA.
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(ca.caPEM())
	noCertTLSCfg := &tls.Config{
		RootCAs:    caPool,
		ServerName: "127.0.0.1",
		MinVersion: tls.VersionTLS13,
		// No Certificates — no client cert presented
	}

	creds := credentials.NewTLS(noCertTLSCfg)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		// Rejected at dial time — expected.
		return
	}
	defer conn.Close()

	ctx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	// Opening a stream should fail since no client cert was presented.
	stream, streamErr := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/conductor.federation.v1alpha1.FederationService/Stream",
	)
	if streamErr != nil {
		// Expected — rejected.
		return
	}
	// If stream opened, sending should fail.
	sendErr := stream.SendMsg(&struct{}{})
	if sendErr == nil {
		t.Error("expected connection without client cert to be rejected")
	}
}

// TestFederationServer_gRPC_RejectsWrongCA verifies that a client certificate
// signed by a different (rogue) CA is rejected by the server.
func TestFederationServer_gRPC_RejectsWrongCA(t *testing.T) {
	serverCA := newTestCA(t)
	rogueCA := newTestCA(t)

	serverCertPEM, serverKeyPEM := serverCA.issueServerCert(t, []string{"localhost"})
	serverCertPath, serverKeyPath, caPath := writeTempCerts(t, serverCertPEM, serverKeyPEM, serverCA.caPEM())

	srv, err := federation.NewFederationServer(caPath, serverCertPath, serverKeyPath, nil)
	if err != nil {
		t.Fatalf("NewFederationServer: %v", err)
	}

	addr, cancel := startTestServer(t, srv)
	defer cancel()

	// Connect with a rogue CA-signed client cert, trusting the server CA for server verification.
	rogueClientCertPEM, rogueClientKeyPEM := rogueCA.issueClientCert(t, "attacker")
	rogueClientCertPath, rogueClientKeyPath, _ := writeTempCerts(t, rogueClientCertPEM, rogueClientKeyPEM, rogueCA.caPEM())

	// Build client TLS config using the rogue client cert but trust the real server CA.
	caPool := x509.NewCertPool()
	caPool.AppendCertsFromPEM(serverCA.caPEM())
	clientKeyPair, err := tls.LoadX509KeyPair(rogueClientCertPath, rogueClientKeyPath)
	if err != nil {
		t.Fatalf("load rogue key pair: %v", err)
	}
	rogueTLSCfg := &tls.Config{
		Certificates: []tls.Certificate{clientKeyPair},
		RootCAs:      caPool,
		ServerName:   "127.0.0.1",
		MinVersion:   tls.VersionTLS13,
	}

	creds := credentials.NewTLS(rogueTLSCfg)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		// Rejected at dial — expected.
		return
	}
	defer conn.Close()

	ctx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	stream, streamErr := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/conductor.federation.v1alpha1.FederationService/Stream",
	)
	if streamErr != nil {
		// Expected — rogue cert rejected.
		return
	}
	sendErr := stream.SendMsg(&struct{}{})
	if sendErr == nil {
		t.Error("expected rogue CA-signed client cert to be rejected by server")
	}
}

// TestFederationClient_IsDegraded_InitiallyFalse verifies that a newly created
// FederationClient is not degraded before any connection attempt.
func TestFederationClient_IsDegraded_InitiallyFalse(t *testing.T) {
	c := federation.NewFederationClient(
		"localhost:9091", "/no/cert", "/no/key", "/no/ca", "ccs-dev", nil,
	)
	if c.IsDegraded() {
		t.Error("expected new FederationClient to not be degraded")
	}
}

// TestFederationClient_ClusterIDExtraction verifies end-to-end cluster ID
// extraction from a client certificate through the gRPC stream context.
func TestFederationClient_ClusterIDExtraction(t *testing.T) {
	ca := newTestCA(t)

	serverCertPEM, serverKeyPEM := ca.issueServerCert(t, []string{"localhost"})
	serverCertPath, serverKeyPath, caPath := writeTempCerts(t, serverCertPEM, serverKeyPEM, ca.caPEM())

	srv, err := federation.NewFederationServer(caPath, serverCertPath, serverKeyPath, nil)
	if err != nil {
		t.Fatalf("NewFederationServer: %v", err)
	}

	addr, cancel := startTestServer(t, srv)
	defer cancel()

	// Connect with a specific cluster ID in the client cert SAN.
	clientCertPEM, clientKeyPEM := ca.issueClientCert(t, "ccs-prod")
	clientCertPath, clientKeyPath, _ := writeTempCerts(t, clientCertPEM, clientKeyPEM, ca.caPEM())
	clientTLSCfg, err := federation.BuildClientTLSConfig(caPath, clientCertPath, clientKeyPath)
	if err != nil {
		t.Fatalf("client TLS config: %v", err)
	}
	clientTLSCfg.ServerName = "127.0.0.1"

	creds := credentials.NewTLS(clientTLSCfg)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer conn.Close()

	ctx, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	stream, err := conn.NewStream(ctx,
		&grpc.StreamDesc{ServerStreams: true, ClientStreams: true},
		"/conductor.federation.v1alpha1.FederationService/Stream",
	)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	// The server extracts "ccs-prod" from the client cert SAN.
	// The fact that the stream opened without being rejected confirms the
	// cluster ID was successfully extracted (error path rejects with Unauthenticated).
	_ = stream.CloseSend()
}
