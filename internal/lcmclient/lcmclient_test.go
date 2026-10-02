package lcmclient_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	lcmv1 "github.com/go-tangra/go-tangra-lcm/sdk/v4/api/proto/lcm/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/lcmclient"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// fakeCerts is a fake lcm.v1.Certificates client.
type fakeCerts struct {
	lastReq *lcmv1.DownloadRequest
	bundle  *lcmv1.CertificateBundle
	err     error
}

func (f *fakeCerts) Download(_ context.Context, in *lcmv1.DownloadRequest, _ ...grpc.CallOption) (*lcmv1.CertificateBundle, error) {
	f.lastReq = in
	return f.bundle, f.err
}

func TestFetchCertificateMapsBundleAndTenant(t *testing.T) {
	exp := time.Now().Add(24 * time.Hour).Truncate(time.Second)
	fc := &fakeCerts{bundle: &lcmv1.CertificateBundle{
		Certificate: &lcmv1.Certificate{Serial: "0A", Subject: "api.example.com", Sans: []string{"api.example.com"}, NotAfter: timestamppb.New(exp)},
		CertPem:     "CERT", ChainPem: "CHAIN", KeyPem: "KEY",
	}}
	c := lcmclient.New(fc)

	got, err := c.FetchCertificate(context.Background(), "tenant-1", "c1", true)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if fc.lastReq.GetTenantId() != "tenant-1" || fc.lastReq.GetCertificateId() != "c1" || !fc.lastReq.GetIncludeKey() {
		t.Fatalf("request not threaded: %+v", fc.lastReq)
	}
	if got.SerialNumber != "0A" || got.CommonName != "api.example.com" || got.CertificatePEM != "CERT" ||
		got.CertificateChain != "CHAIN" || got.PrivateKeyPEM != "KEY" || !got.ExpiresAt.Equal(exp) {
		t.Fatalf("bundle not mapped: %+v", got)
	}
	if len(got.SANs) != 1 || got.SANs[0] != "api.example.com" {
		t.Fatalf("sans: %v", got.SANs)
	}
}

func TestFetchCertificateOmitsKeyWhenNotRequested(t *testing.T) {
	fc := &fakeCerts{bundle: &lcmv1.CertificateBundle{
		Certificate: &lcmv1.Certificate{Serial: "0B", SpiffeId: "spiffe://example.org/svc/x"},
		CertPem:     "C",
	}}
	c := lcmclient.New(fc)
	got, err := c.FetchCertificate(context.Background(), "t", "c2", false)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if fc.lastReq.GetIncludeKey() {
		t.Fatal("include_key should be false")
	}
	// CommonName falls back to the SPIFFE id when the subject is empty.
	if got.CommonName != "spiffe://example.org/svc/x" || got.PrivateKeyPEM != "" {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestFetchCertificateNotFound(t *testing.T) {
	fc := &fakeCerts{err: status.Error(codes.NotFound, "not_found")}
	c := lcmclient.New(fc)
	if _, err := c.FetchCertificate(context.Background(), "t", "nope", false); err != lcmclient.ErrNotFound {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

// lcm answers InvalidArgument "no stored private key" for a certificate whose
// key it does not hold: mapped to provider.ErrKeyUnavailable so the job fails
// at once instead of retrying.
func TestFetchCertificateNoStoredKey(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition} {
		fc := &fakeCerts{err: status.Error(code, "no stored private key for this certificate")}
		_, err := lcmclient.New(fc).FetchCertificate(context.Background(), "t", "c", true)
		if !errors.Is(err, provider.ErrKeyUnavailable) {
			t.Fatalf("%v: err = %v, want ErrKeyUnavailable", code, err)
		}
	}
	// Other InvalidArgument errors are not mistaken for it.
	fc := &fakeCerts{err: status.Error(codes.InvalidArgument, "certificate_id is required")}
	if _, err := lcmclient.New(fc).FetchCertificate(context.Background(), "t", "", true); errors.Is(err, provider.ErrKeyUnavailable) || err == nil {
		t.Fatalf("err = %v", err)
	}
}
