package data

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/keepalive"

	lcmV1 "github.com/go-tangra/go-tangra-lcm/gen/go/lcm/service/v1"
)

// LcmClient holds the LCM service gRPC client for the deployer
type LcmClient struct {
	conn *grpc.ClientConn
	log  *log.Helper

	CertificateJobService lcmV1.LcmCertificateJobServiceClient
}

// NewLcmClient creates a new LcmClient instance for the deployer service
func NewLcmClient(ctx *bootstrap.Context) (*LcmClient, func(), error) {
	l := ctx.NewLoggerHelper("deployer/lcm-client")

	// Get LCM endpoint from environment variable, fallback to default
	endpoint := os.Getenv("LCM_GRPC_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:9100" // Default LCM gRPC endpoint
	}

	l.Infof("Connecting to LCM service at: %s", endpoint)

	// Load TLS credentials for mTLS connection to LCM service
	creds, err := loadDeployerTLSCredentials(l)
	if err != nil {
		l.Warnf("Failed to load TLS credentials, LCM service will not be available: %v", err)
		return nil, func() {}, nil // Return nil clients, service will be unavailable
	}

	// Configure connection parameters for automatic reconnection
	connectParams := grpc.ConnectParams{
		Backoff: backoff.Config{
			BaseDelay:  1 * time.Second,
			Multiplier: 1.5,
			Jitter:     0.2,
			MaxDelay:   30 * time.Second,
		},
		MinConnectTimeout: 5 * time.Second,
	}

	// Configure keepalive to detect dead connections
	// Note: gRPC servers enforce a minimum ping interval (default 5 minutes)
	// Setting Time too low causes ENHANCE_YOUR_CALM errors
	keepaliveParams := keepalive.ClientParameters{
		Time:                5 * time.Minute,  // Send pings every 5 minutes if no activity
		Timeout:             20 * time.Second, // Wait 20 seconds for ping ack before considering connection dead
		PermitWithoutStream: false,            // Don't send pings without active streams (reduces unnecessary traffic)
	}

	// Create gRPC connection with TLS and reconnection settings
	conn, err := grpc.NewClient(
		endpoint,
		grpc.WithTransportCredentials(creds),
		grpc.WithConnectParams(connectParams),
		grpc.WithKeepaliveParams(keepaliveParams),
		grpc.WithDefaultServiceConfig(`{
			"loadBalancingConfig": [{"round_robin":{}}],
			"methodConfig": [{
				"name": [{"service": ""}],
				"waitForReady": true,
				"retryPolicy": {
					"MaxAttempts": 3,
					"InitialBackoff": "0.5s",
					"MaxBackoff": "5s",
					"BackoffMultiplier": 2,
					"RetryableStatusCodes": ["UNAVAILABLE", "RESOURCE_EXHAUSTED"]
				}
			}]
		}`),
	)
	if err != nil {
		l.Errorf("Failed to connect to LCM service: %v", err)
		return nil, func() {}, err
	}

	client := &LcmClient{
		conn:                  conn,
		log:                   l,
		CertificateJobService: lcmV1.NewLcmCertificateJobServiceClient(conn),
	}

	cleanup := func() {
		if err := conn.Close(); err != nil {
			l.Errorf("Failed to close LCM connection: %v", err)
		}
	}

	l.Info("LCM client initialized successfully")

	return client, cleanup, nil
}

// IsConnected checks if the LCM client is connected
func (c *LcmClient) IsConnected(ctx context.Context) bool {
	if c == nil || c.conn == nil {
		return false
	}
	return c.conn.GetState().String() == "READY"
}

// CertificateData contains the certificate data fetched from LCM
type CertificateData struct {
	JobID            string
	CertificatePEM   string
	CACertificatePEM string
	PrivateKeyPEM    string
	SerialNumber     string
	CommonName       string
	SANs             []string
	ExpiresAt        int64
}

// GetCertificateByJobID fetches a certificate from LCM by its job ID
func (c *LcmClient) GetCertificateByJobID(ctx context.Context, jobID string, includePrivateKey bool) (*CertificateData, error) {
	if c == nil || c.CertificateJobService == nil {
		return nil, fmt.Errorf("LCM client not available")
	}

	resp, err := c.CertificateJobService.GetJobResult(ctx, &lcmV1.GetJobResultRequest{
		JobId:             jobID,
		IncludePrivateKey: &includePrivateKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to get certificate from LCM: %w", err)
	}

	// Check if job is completed
	if resp.GetStatus() != lcmV1.CertificateJobStatus_CERTIFICATE_JOB_STATUS_COMPLETED {
		return nil, fmt.Errorf("certificate job is not completed, status: %s", resp.GetStatus().String())
	}

	certData := &CertificateData{
		JobID:            resp.GetJobId(),
		CertificatePEM:   resp.GetCertificatePem(),
		CACertificatePEM: resp.GetCaCertificatePem(),
		PrivateKeyPEM:    resp.GetPrivateKeyPem(),
		SerialNumber:     resp.GetSerialNumber(),
	}

	// Parse the certificate PEM to extract CommonName and SANs
	if certData.CertificatePEM != "" {
		if err := certData.parseCertificatePEM(); err != nil {
			c.log.Warnf("Failed to parse certificate PEM: %v", err)
		}
	}

	// Get expiration from response if available
	if resp.GetExpiresAt() != nil {
		certData.ExpiresAt = resp.GetExpiresAt().AsTime().Unix()
	}

	return certData, nil
}

// parseCertificatePEM parses the certificate PEM and extracts CommonName and SANs
func (cd *CertificateData) parseCertificatePEM() error {
	block, _ := pem.Decode([]byte(cd.CertificatePEM))
	if block == nil {
		return fmt.Errorf("failed to decode certificate PEM")
	}

	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	cd.CommonName = cert.Subject.CommonName
	cd.SANs = cert.DNSNames
	if cd.ExpiresAt == 0 {
		cd.ExpiresAt = cert.NotAfter.Unix()
	}

	return nil
}

// loadDeployerTLSCredentials loads TLS credentials for connecting to LCM service
func loadDeployerTLSCredentials(l *log.Helper) (credentials.TransportCredentials, error) {
	// Get certificate paths from environment or use defaults
	caCertPath := os.Getenv("LCM_CA_CERT_PATH")
	if caCertPath == "" {
		caCertPath = "./data/ca/ca.crt"
	}
	clientCertPath := os.Getenv("LCM_CLIENT_CERT_PATH")
	if clientCertPath == "" {
		clientCertPath = "./data/deployer/deployer.crt"
	}
	clientKeyPath := os.Getenv("LCM_CLIENT_KEY_PATH")
	if clientKeyPath == "" {
		clientKeyPath = "./data/deployer/deployer.key"
	}

	// Load CA certificate
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		l.Errorf("Failed to read CA cert from %s: %v", caCertPath, err)
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		l.Errorf("Failed to parse CA cert from %s", caCertPath)
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	// Load client certificate and key
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		l.Errorf("Failed to load client cert/key from %s, %s: %v", clientCertPath, clientKeyPath, err)
		return nil, err
	}

	// Create TLS config
	tlsConfig := &tls.Config{
		Certificates: []tls.Certificate{clientCert},
		RootCAs:      caCertPool,
		ServerName:   "localhost", // Must match a SAN in the server certificate
		MinVersion:   tls.VersionTLS12,
	}

	l.Infof("Loaded TLS credentials: CA=%s, Cert=%s", caCertPath, clientCertPath)

	return credentials.NewTLS(tlsConfig), nil
}
