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
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	lcmv1 "github.com/go-tangra/go-tangra-lcm/sdk/v4/api/proto/lcm/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// ErrNotFound is returned when lcm has no such certificate.
var ErrNotFound = provider.ErrCertificateNotFound

// ErrBadName is returned when lcm rejects the host name searched for.
var ErrBadName = errors.New("lcmclient: not a host name")

// Match is a live host certificate lcm found for a host name (metadata only).
type Match struct {
	ID       string    `json:"id"`
	Subject  string    `json:"subject"`
	SANs     []string  `json:"sans"`
	Serial   string    `json:"serial"`
	IssuerID string    `json:"issuer_id"`
	Status   string    `json:"status"` // active | expiring
	NotAfter time.Time `json:"not_after"`
	HasKey   bool      `json:"has_key"`
}

// FindCertificates asks lcm for the tenant's live host certificates matching
// name (exact names first, then the wildcard covering it, then names
// containing it).
func (c *Client) FindCertificates(ctx context.Context, tenantID, name string, limit int) ([]Match, error) {
	r, err := c.cc.Find(ctx, &lcmv1.FindCertificatesRequest{TenantId: tenantID, Name: name, Limit: int32(min(max(limit, 0), 50))}) // #nosec G115 -- bounded above
	if err != nil {
		if st, _ := status.FromError(err); st.Code() == codes.InvalidArgument {
			return nil, ErrBadName
		}
		return nil, fmt.Errorf("lcmclient: find: %w", err)
	}
	out := make([]Match, 0, len(r.GetCertificates()))
	for _, m := range r.GetCertificates() {
		v := Match{ID: m.GetId(), Subject: m.GetSubject(), SANs: m.GetSans(), Serial: m.GetSerial(), IssuerID: m.GetIssuerId(), HasKey: m.GetHasKey(), Status: "active"}
		if m.GetStatus() == lcmv1.CertificateStatus_CERTIFICATE_STATUS_EXPIRING {
			v.Status = "expiring"
		}
		if ts := m.GetNotAfter(); ts != nil {
			v.NotAfter = ts.AsTime()
		}
		out = append(out, v)
	}
	return out, nil
}

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
