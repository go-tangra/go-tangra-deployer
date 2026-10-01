package awsacm

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// recorder answers every request in-process and records the target host, so
// the tests never touch the network.
type recorder struct{ hosts []string }

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.hosts = append(r.hosts, req.URL.Host)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/x-amz-json-1.1"}},
		Body:       io.NopCloser(strings.NewReader(`{"CertificateArn":"arn:x","Certificate":{"Status":"ISSUED"}}`)),
		Request:    req,
	}, nil
}

func testCreds() map[string]any {
	return map[string]any{"access_key_id": "AKIDEXAMPLE", "secret_access_key": "s3cr3t", "session_token": "sess"}
}

// TestStoredEndpointCannotRedirect: an "endpoint" in the stored configuration
// (or a target override) must not move the signed request. The attacker
// server must never be contacted; the request goes to the regional AWS host.
func TestStoredEndpointCannotRedirect(t *testing.T) {
	var hits atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer attacker.Close()

	rec := &recorder{}
	p := Provider{transport: rec}
	cfg := map[string]any{"region": "eu-west-1", "endpoint": attacker.URL, "certificate_arn": "arn:x"}
	cert := &provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K"}

	if _, err := p.Deploy(context.Background(), cert, cfg, testCreds(), nil); err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if _, err := p.Verify(context.Background(), cert, cfg, testCreds()); err != nil {
		t.Fatalf("verify: %v", err)
	}
	if hits.Load() != 0 {
		t.Fatalf("attacker endpoint from stored config was contacted %d times", hits.Load())
	}
	if len(rec.hosts) != 2 {
		t.Fatalf("requests = %v", rec.hosts)
	}
	for _, h := range rec.hosts {
		if h != "acm.eu-west-1.amazonaws.com" {
			t.Fatalf("request sent to %q, want the regional ACM host", h)
		}
	}
}

// TestRegisteredProviderHasNoOverride: the provider in the registry carries no
// endpoint override.
func TestRegisteredProviderHasNoOverride(t *testing.T) {
	p, err := provider.Get("aws_acm")
	if err != nil {
		t.Fatal(err)
	}
	ap, ok := p.(Provider)
	if !ok || ap.endpoint != "" || ap.transport != nil {
		t.Fatalf("registered provider has test hooks set: %#v", p)
	}
	if got := ap.endpointFor("us-east-1"); got != "https://acm.us-east-1.amazonaws.com/" {
		t.Fatalf("endpoint = %q", got)
	}
}

// TestRegionCannotRedirect: the region becomes part of the host; a value that
// is not a plain region is refused before any request is built.
func TestRegionCannotRedirect(t *testing.T) {
	cert := &provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K"}
	for _, region := range []string{
		"evil.example/x?", "x@evil.example#", "us-east-1.evil.example", "evil.example:443/", "US-EAST-1", "us east 1",
	} {
		rec := &recorder{}
		p := Provider{transport: rec}
		cfg := map[string]any{"region": region, "certificate_arn": "arn:x"}
		if _, err := p.Deploy(context.Background(), cert, cfg, testCreds(), nil); err == nil {
			t.Errorf("deploy accepted region %q", region)
		}
		if _, err := p.Verify(context.Background(), cert, cfg, testCreds()); err == nil {
			t.Errorf("verify accepted region %q", region)
		}
		if err := p.ValidateCredentials(context.Background(), testCreds(), cfg); err == nil {
			t.Errorf("validate accepted region %q", region)
		}
		var fe *provider.FieldError
		if err := p.ValidateConfig(cfg); !errors.As(err, &fe) || fe.Field != "config.region" {
			t.Errorf("ValidateConfig(%q) = %v", region, err)
		}
		if len(rec.hosts) != 0 {
			t.Errorf("region %q produced requests to %v", region, rec.hosts)
		}
	}
	for _, region := range []string{"us-east-1", "us-gov-west-1", "eu-isoe-west-1", "ap-southeast-4", "cn-north-1"} {
		if err := (Provider{}).ValidateConfig(map[string]any{"region": region}); err != nil {
			t.Errorf("valid region %q refused: %v", region, err)
		}
	}
	if err := (Provider{}).ValidateConfig(map[string]any{}); err != nil {
		t.Errorf("absent region refused at save: %v", err)
	}
}
