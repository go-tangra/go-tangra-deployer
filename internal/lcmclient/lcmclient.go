// Package lcmclient fetches an issued certificate (cert + chain + optional
// retained private key) from the lcm service at deploy time over the module
// gRPC channel (SPIFFE mTLS, policed by lcm's policy.yaml). The deployer never
// persists certificate bytes (spec FR-012, SR-002): it fetches them per
// deployment, hands them to a provider, and drops them. The gRPC client is
// injected so this package is testable with a fake.
package lcmclient

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lcmv1 "github.com/go-tangra/go-tangra-lcm/sdk/v4/api/proto/lcm/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// ErrNotFound is returned when lcm has no such certificate.
var ErrNotFound = errors.New("lcmclient: certificate not found")

// Client fetches certificates from lcm over the module gRPC channel.
type Client struct {
	cc lcmv1.CertificatesClient
}

// New builds a client from a connected lcm.v1.Certificates client.
func New(cc lcmv1.CertificatesClient) *Client { return &Client{cc: cc} }

// FetchCertificate returns the certificate material for certID in tenantID.
// includeKey adds the private key (only present for certs whose key lcm retains).
func (c *Client) FetchCertificate(ctx context.Context, tenantID, certID string, includeKey bool) (provider.CertificateData, error) {
	b, err := c.cc.Download(ctx, &lcmv1.DownloadRequest{TenantId: tenantID, CertificateId: certID, IncludeKey: includeKey})
	if err != nil {
		switch st, _ := status.FromError(err); {
		case st.Code() == codes.NotFound:
			return provider.CertificateData{}, ErrNotFound
		case (st.Code() == codes.InvalidArgument || st.Code() == codes.FailedPrecondition) &&
			strings.Contains(strings.ToLower(st.Message()), "no stored private key"):
			// The certificate was issued from a CSR or its key was already
			// handed out: a retry cannot succeed.
			return provider.CertificateData{}, fmt.Errorf("lcmclient: download: %w", provider.ErrKeyUnavailable)
		}
		return provider.CertificateData{}, fmt.Errorf("lcmclient: download: %w", err)
	}
	cert := b.GetCertificate()
	cn := cert.GetSubject()
	if cn == "" {
		cn = cert.GetSpiffeId()
	}
	out := provider.CertificateData{
		ID:               certID,
		SerialNumber:     cert.GetSerial(),
		CommonName:       cn,
		SANs:             cert.GetSans(),
		CertificatePEM:   b.GetCertPem(),
		CertificateChain: b.GetChainPem(),
		PrivateKeyPEM:    b.GetKeyPem(),
	}
	if ts := cert.GetNotAfter(); ts != nil {
		out.ExpiresAt = ts.AsTime()
	}
	return out, nil
}
