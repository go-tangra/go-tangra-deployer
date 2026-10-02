package cloudflare

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

type recorder struct{ hosts []string }

func (r *recorder) RoundTrip(req *http.Request) (*http.Response, error) {
	r.hosts = append(r.hosts, req.URL.Host)
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": {"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"success":true,"result":[]}`)),
		Request:    req,
	}, nil
}

// TestStoredAPIBaseCannotRedirect: an "api_base" in the stored configuration
// (or a target override) must not move the bearer token. The attacker server
// must never be contacted; requests go to api.cloudflare.com.
func TestStoredAPIBaseCannotRedirect(t *testing.T) {
	var hits atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits.Add(1) }))
	defer attacker.Close()

	rec := &recorder{}
	p := Provider{transport: rec}
	cfg := map[string]any{"zone_id": "0123456789abcdef0123456789abcdef", "api_base": attacker.URL}
	cr := map[string]any{"api_token": "cf-token"}
	cert := &provider.CertificateData{CommonName: "example.com", CertificatePEM: "C", PrivateKeyPEM: "K"}

	_, _ = p.Deploy(context.Background(), cert, cfg, cr, nil)
	_, _ = p.Verify(context.Background(), cert, cfg, cr)

	if hits.Load() != 0 {
		t.Fatalf("attacker api_base from stored config was contacted %d times", hits.Load())
	}
	if len(rec.hosts) == 0 {
		t.Fatal("no request was made")
	}
	for _, h := range rec.hosts {
		if h != "api.cloudflare.com" {
			t.Fatalf("request sent to %q, want api.cloudflare.com", h)
		}
	}
}

// TestRegisteredProviderHasNoOverride: the provider in the registry carries no
// base URL override.
func TestRegisteredProviderHasNoOverride(t *testing.T) {
	p, err := provider.Get("cloudflare")
	if err != nil {
		t.Fatal(err)
	}
	cp, ok := p.(Provider)
	if !ok || cp.apiBase != "" || cp.transport != nil || cp.base() != defaultAPIBase {
		t.Fatalf("registered provider has test hooks set: %#v", p)
	}
}

// TestMalformedZoneIDBuildsNoRequest: zone_id is part of the request path; a
// malformed one is refused before any request (with the token) is built.
func TestMalformedZoneIDBuildsNoRequest(t *testing.T) {
	cert := &provider.CertificateData{CommonName: "example.com", CertificatePEM: "C", PrivateKeyPEM: "K"}
	cr := map[string]any{"api_token": "cf-token"}
	for _, z := range []string{"z", "../../user/tokens", "0123456789abcdef0123456789abcdef?x=1", "0123456789abcdef0123456789abcdeg"} {
		rec := &recorder{}
		p := Provider{transport: rec}
		cfg := map[string]any{"zone_id": z}
		if _, err := p.Deploy(context.Background(), cert, cfg, cr, nil); err == nil {
			t.Errorf("deploy accepted zone %q", z)
		}
		if _, err := p.Verify(context.Background(), cert, cfg, cr); err == nil {
			t.Errorf("verify accepted zone %q", z)
		}
		if err := p.ValidateCredentials(context.Background(), cr, cfg); err == nil {
			t.Errorf("validate accepted zone %q", z)
		}
		if err := p.ValidateConfig(cfg); err == nil {
			t.Errorf("ValidateConfig accepted zone %q", z)
		}
		if len(rec.hosts) != 0 {
			t.Errorf("zone %q produced requests", z)
		}
	}
	for _, cfg := range []map[string]any{{}, {"zone_id": ""}, {"zone_id": "  "}, {"zone_id": nil}} {
		if err := (Provider{}).ValidateConfig(cfg); err != nil {
			t.Errorf("empty zone %v refused at save: %v", cfg, err)
		}
	}
}
