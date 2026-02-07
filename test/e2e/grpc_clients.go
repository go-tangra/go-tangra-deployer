package e2e

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
	lcmV1 "github.com/go-tangra/go-tangra-lcm/gen/go/lcm/service/v1"
)

// LCMClients holds LCM gRPC service clients
type LCMClients struct {
	conn           *grpc.ClientConn
	LcmClient      lcmV1.LcmClientServiceClient
	Issuer         lcmV1.LcmIssuerServiceClient
	CertificateJob lcmV1.LcmCertificateJobServiceClient
}

// DeployerClients holds Deployer gRPC service clients
type DeployerClients struct {
	conn       *grpc.ClientConn
	Target     deployerV1.DeploymentTargetServiceClient
	Job        deployerV1.DeploymentJobServiceClient
	Deployment deployerV1.DeploymentServiceClient
}

// ClientCredentials holds mTLS credentials
type ClientCredentials struct {
	CertFile   string
	KeyFile    string
	PrivateKey *ecdsa.PrivateKey
}

// NewLCMClientsWithTLS creates LCM clients with TLS (optionally mTLS)
func NewLCMClientsWithTLS(serverAddr, caFile string, creds *ClientCredentials, timeout time.Duration) (*LCMClients, error) {
	// Load CA certificate
	caCert, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %w", err)
	}

	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to parse CA certificate")
	}

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	// Load client certificate if provided (for mTLS)
	if creds != nil && creds.CertFile != "" && creds.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(creds.CertFile, creds.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, serverAddr,
		grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to LCM server: %w", err)
	}

	return &LCMClients{
		conn:           conn,
		LcmClient:      lcmV1.NewLcmClientServiceClient(conn),
		Issuer:         lcmV1.NewLcmIssuerServiceClient(conn),
		CertificateJob: lcmV1.NewLcmCertificateJobServiceClient(conn),
	}, nil
}

// NewDeployerClients creates Deployer clients (insecure for local testing)
func NewDeployerClients(serverAddr string, timeout time.Duration) (*DeployerClients, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := grpc.DialContext(ctx, serverAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Deployer server: %w", err)
	}

	return &DeployerClients{
		conn:       conn,
		Target:     deployerV1.NewDeploymentTargetServiceClient(conn),
		Job:        deployerV1.NewDeploymentJobServiceClient(conn),
		Deployment: deployerV1.NewDeploymentServiceClient(conn),
	}, nil
}

// Close closes the LCM connection
func (c *LCMClients) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// Close closes the Deployer connection
func (c *DeployerClients) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// RegisterAndGetMTLSCert registers with LCM and obtains mTLS certificate
func RegisterAndGetMTLSCert(ctx context.Context, lcmClients *LCMClients, clientID, sharedSecret, tempDir string) (*ClientCredentials, error) {
	// Generate key pair
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("failed to generate ECDSA key: %w", err)
	}

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal public key: %w", err)
	}

	publicKeyPEM := string(pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	}))

	// Register client
	hostname := clientID // Use clientID as hostname for CN matching
	resp, err := lcmClients.LcmClient.RegisterLcmClient(ctx, &lcmV1.CreateLcmClientRequest{
		ClientId:     clientID,
		Hostname:     hostname,
		SharedSecret: &sharedSecret,
		PublicKey:    publicKeyPEM,
		DnsNames:     []string{hostname},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to register client: %w", err)
	}

	requestID := resp.Certificate.GetRequestId()
	if requestID == "" {
		return nil, fmt.Errorf("no request ID returned from registration")
	}

	// Poll for certificate issuance
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
			statusResp, err := lcmClients.LcmClient.GetRequestStatus(ctx, &lcmV1.GetRequestStatusRequest{
				RequestId: requestID,
				ClientId:  clientID,
			})
			if err != nil {
				return nil, fmt.Errorf("failed to get request status: %w", err)
			}

			if statusResp.Status == lcmV1.ClientCertificateStatus_CLIENT_CERT_STATUS_ISSUED {
				// Download the certificate
				downloadResp, err := lcmClients.LcmClient.DownloadClientCertificate(ctx, &lcmV1.DownloadClientCertificateRequest{
					RequestId: requestID,
					ClientId:  clientID,
					PublicKey: publicKeyPEM,
				})
				if err != nil {
					return nil, fmt.Errorf("failed to download certificate: %w", err)
				}

				if downloadResp.CertificatePem == nil || *downloadResp.CertificatePem == "" {
					return nil, fmt.Errorf("downloaded certificate is empty")
				}

				// Save certificate to temp file
				certFile := filepath.Join(tempDir, "client.crt")
				if err := os.WriteFile(certFile, []byte(*downloadResp.CertificatePem), 0644); err != nil {
					return nil, fmt.Errorf("failed to save certificate: %w", err)
				}

				// Save private key to temp file
				keyFile := filepath.Join(tempDir, "client.key")
				privateKeyBytes, err := x509.MarshalECPrivateKey(privateKey)
				if err != nil {
					return nil, fmt.Errorf("failed to marshal private key: %w", err)
				}
				privateKeyPEM := pem.EncodeToMemory(&pem.Block{
					Type:  "EC PRIVATE KEY",
					Bytes: privateKeyBytes,
				})
				if err := os.WriteFile(keyFile, privateKeyPEM, 0600); err != nil {
					return nil, fmt.Errorf("failed to save private key: %w", err)
				}

				return &ClientCredentials{
					CertFile:   certFile,
					KeyFile:    keyFile,
					PrivateKey: privateKey,
				}, nil
			}

			if statusResp.Status == lcmV1.ClientCertificateStatus_CLIENT_CERT_STATUS_REVOKED {
				return nil, fmt.Errorf("certificate request was revoked")
			}
		}
	}
}

// ParseCertificatePEM parses a PEM-encoded certificate
func ParseCertificatePEM(certPEM string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, fmt.Errorf("failed to decode PEM block containing certificate")
	}
	return x509.ParseCertificate(block.Bytes)
}
