// Package federation implements the Conductor federation channel — the
// persistent bidirectional gRPC stream connecting tenant Conductor agents to
// the management Conductor agent. conductor-schema.md §18.
//
// The federation port (FEDERATION_PORT, default :9091) is distinct from the
// PermissionService internal port. Mutual TLS is enforced: every connection must
// present a client certificate signed by the management CA. The connecting
// cluster's ID is extracted from the client certificate Subject Alternative Name
// (DNS SAN). Any connection without a valid management CA cert is rejected at the
// TLS handshake layer — no application code runs for unauthenticated connections.
package federation

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"os"
)

// DefaultFederationPort is the TCP port the FederationServer listens on when
// FEDERATION_PORT is not set. conductor-schema.md §18.
const DefaultFederationPort = ":9091"

// ClusterIDFromCert extracts the cluster ID from the first DNS Subject
// Alternative Name of the given certificate.
//
// The convention is: the client certificate CN or first DNS SAN carries the
// cluster ID (e.g. "ccs-dev"). An absent or empty SAN causes an error so the
// caller can reject the connection.
func ClusterIDFromCert(cert *x509.Certificate) (string, error) {
	if len(cert.DNSNames) > 0 && cert.DNSNames[0] != "" {
		return cert.DNSNames[0], nil
	}
	// Fall back to CommonName for compatibility.
	if cert.Subject.CommonName != "" {
		return cert.Subject.CommonName, nil
	}
	return "", fmt.Errorf("client certificate carries no DNS SAN and no CommonName — cannot determine cluster ID")
}

// BuildServerTLSConfig constructs a tls.Config for the FederationServer.
//
// - caCertPath: PEM-encoded CA certificate used to verify client certificates.
// - serverCertPath, serverKeyPath: PEM-encoded server certificate and key.
//
// The config requires client authentication (tls.RequireAndVerifyClientCert).
// Any connection that cannot present a valid client certificate is rejected at
// the TLS handshake. conductor-schema.md §18.
func BuildServerTLSConfig(caCertPath, serverCertPath, serverKeyPath string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %q: %w", caCertPath, err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse CA cert %q: no valid PEM certificates found", caCertPath)
	}

	serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load server cert/key (%q, %q): %w", serverCertPath, serverKeyPath, err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
		MinVersion:   tls.VersionTLS13,
		// h2 is required by gRPC v1.67+ ALPN enforcement.
		NextProtos: []string{"h2"},
	}, nil
}

// BuildClientTLSConfig constructs a tls.Config for the FederationClient.
//
// - caCertPath: PEM-encoded management CA certificate used to verify the server.
// - clientCertPath, clientKeyPath: PEM-encoded client certificate and key.
//
// The cluster ID must be embedded as a DNS SAN in the client certificate.
// conductor-schema.md §18.
func BuildClientTLSConfig(caCertPath, clientCertPath, clientKeyPath string) (*tls.Config, error) {
	caPEM, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("read CA cert %q: %w", caCertPath, err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("parse CA cert %q: no valid PEM certificates found", caCertPath)
	}

	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("load client cert/key (%q, %q): %w", clientCertPath, clientKeyPath, err)
	}

	return &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}

// ListenTLS opens a TLS listener on addr using cfg.
func ListenTLS(addr string, cfg *tls.Config) (net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %q: %w", addr, err)
	}
	return tls.NewListener(lis, cfg), nil
}
