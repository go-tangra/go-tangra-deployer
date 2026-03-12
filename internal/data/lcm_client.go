package data

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/grpc"

	"github.com/go-tangra/go-tangra-common/grpcx"

	lcmV1 "github.com/go-tangra/go-tangra-lcm/gen/go/lcm/service/v1"
)

// LcmClient holds the LCM service gRPC client for the deployer.
// It resolves the LCM endpoint lazily via ModuleDialer on first use.
type LcmClient struct {
	dialer *grpcx.ModuleDialer
	log    *log.Helper

	once                  sync.Once
	conn                  *grpc.ClientConn
	CertificateJobService lcmV1.LcmCertificateJobServiceClient
	initErr               error
}

// NewLcmClient creates a new LcmClient that resolves via ModuleDialer.
func NewLcmClient(ctx *bootstrap.Context, dialer *grpcx.ModuleDialer) (*LcmClient, func(), error) {
	l := ctx.NewLoggerHelper("deployer/lcm-client")

	client := &LcmClient{
		dialer: dialer,
		log:    l,
	}

	cleanup := func() {
		if client.conn != nil {
			if err := client.conn.Close(); err != nil {
				l.Errorf("Failed to close LCM connection: %v", err)
			}
		}
	}

	l.Info("LCM client created (will resolve endpoint on first use)")
	return client, cleanup, nil
}

// resolve lazily connects to the LCM service via ModuleDialer.
func (c *LcmClient) resolve() error {
	c.once.Do(func() {
		c.log.Info("Resolving lcm module endpoint...")
		conn, err := c.dialer.DialModule(context.Background(), "lcm", 30, 5*time.Second)
		if err != nil {
			c.initErr = fmt.Errorf("resolve lcm: %w", err)
			c.log.Errorf("Failed to resolve lcm: %v", err)
			return
		}
		c.conn = conn
		c.CertificateJobService = lcmV1.NewLcmCertificateJobServiceClient(conn)
		c.log.Info("LCM client connected via ModuleDialer")
	})
	return c.initErr
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
	if c == nil {
		return nil, fmt.Errorf("LCM client not available")
	}

	if err := c.resolve(); err != nil {
		return nil, err
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
