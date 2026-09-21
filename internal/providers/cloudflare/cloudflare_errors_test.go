package cloudflare_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"

	_ "github.com/go-freya/freya/services/deployer/internal/providers/cloudflare"
)

const errToken = "cf-secret-XYZ-token"

func errCert() *provider.CertificateData {
	return &provider.CertificateData{
		ID:             "c1",
		CommonName:     "example.com",
		CertificatePEM: "-----BEGIN CERTIFICATE-----\nLEAF\n-----END CERTIFICATE-----",
		PrivateKeyPEM:  "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
	}
}

// TestDeployValidationErrors covers Deploy's three guard clauses.
func TestDeployValidationErrors(t *testing.T) {
	p, _ := provider.Get("cloudflare")
	ctx := context.Background()

	if _, err := p.Deploy(ctx, errCert(), map[string]any{"zone_id": "z"}, map[string]any{}, nil); err == nil {
		t.Fatal("expected api_token error")
	}
	if _, err := p.Deploy(ctx, errCert(), map[string]any{}, map[string]any{"api_token": errToken}, nil); err == nil {
		t.Fatal("expected zone_id error")
	}
	if _, err := p.Deploy(ctx, &provider.CertificateData{}, map[string]any{"zone_id": "z"}, map[string]any{"api_token": errToken}, nil); err == nil {
		t.Fatal("expected certificate-material error")
	}
}

// TestDeployUpdatesExisting drives the "existing certificate found -> PATCH" path.
func TestDeployUpdatesExisting(t *testing.T) {
	var patched bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = io.WriteString(w, `{"success":true,"errors":[],"result":[{"id":"existing-id","hosts":["example.com"]}]}`)
		case http.MethodPatch:
			patched = true
			_, _ = io.WriteString(w, `{"success":true,"errors":[],"result":{"id":"existing-id"}}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	res, err := p.Deploy(context.Background(), errCert(),
		map[string]any{"zone_id": "z", "api_base": srv.URL}, map[string]any{"api_token": errToken}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success || !patched {
		t.Fatalf("expected PATCH update, res=%+v patched=%v", res, patched)
	}
	if res.Details["was_update"] != true {
		t.Fatalf("was_update should be true: %+v", res.Details)
	}
}

// TestDeployListError covers the findExistingCert error path and confirms the
// api_token is never leaked into the failure message.
func TestDeployListError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"success":false,"errors":[{"code":9109,"message":"Invalid access token"}],"result":null}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	res, err := p.Deploy(context.Background(), errCert(),
		map[string]any{"zone_id": "z", "api_base": srv.URL}, map[string]any{"api_token": errToken}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "Invalid access token") {
		t.Fatalf("expected failure with cloudflare message: %+v", res)
	}
	if strings.Contains(res.Message, errToken) {
		t.Fatal("api_token leaked into failure message")
	}
}

// TestDeployUnparseableResponse covers call's JSON-unmarshal error branch.
func TestDeployUnparseableResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `not json at all`)
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	res, err := p.Deploy(context.Background(), errCert(),
		map[string]any{"zone_id": "z", "api_base": srv.URL}, map[string]any{"api_token": errToken}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success {
		t.Fatalf("expected failure on unparseable response: %+v", res)
	}
}

// TestDeployUnsuccessfulNoErrors covers clientError's no-Errors branch.
func TestDeployUnsuccessfulNoErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"success":false,"errors":[],"result":null}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	res, err := p.Deploy(context.Background(), errCert(),
		map[string]any{"zone_id": "z", "api_base": srv.URL}, map[string]any{"api_token": errToken}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "unsuccessful") {
		t.Fatalf("expected generic unsuccessful message: %+v", res)
	}
}

// TestVerifyValidationErrors covers Verify's three guard clauses.
func TestVerifyValidationErrors(t *testing.T) {
	p, _ := provider.Get("cloudflare")
	ctx := context.Background()

	if _, err := p.Verify(ctx, errCert(), map[string]any{"zone_id": "z"}, map[string]any{}); err == nil {
		t.Fatal("expected api_token error")
	}
	if _, err := p.Verify(ctx, errCert(), map[string]any{}, map[string]any{"api_token": errToken}); err == nil {
		t.Fatal("expected zone_id error")
	}
	if _, err := p.Verify(ctx, nil, map[string]any{"zone_id": "z"}, map[string]any{"api_token": errToken}); err == nil {
		t.Fatal("expected nil-cert error")
	}
}

// TestVerifyNotFoundAndError covers the "not found" and transport-error Verify paths.
func TestVerifyNotFoundAndError(t *testing.T) {
	p, _ := provider.Get("cloudflare")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"success":true,"errors":[],"result":[]}`)
	}))
	defer srv.Close()
	res, err := p.Verify(context.Background(), errCert(),
		map[string]any{"zone_id": "z", "api_base": srv.URL}, map[string]any{"api_token": errToken})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "not found") {
		t.Fatalf("expected not-found failure: %+v", res)
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = io.WriteString(w, `{"success":false,"errors":[{"code":9109,"message":"bad token"}]}`)
	}))
	defer srv2.Close()
	res, err = p.Verify(context.Background(), errCert(),
		map[string]any{"zone_id": "z", "api_base": srv2.URL}, map[string]any{"api_token": errToken})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Success {
		t.Fatalf("expected transport failure: %+v", res)
	}
	if strings.Contains(res.Message, errToken) {
		t.Fatal("api_token leaked into failure message")
	}
}
