package webhook_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

// TestDeployWebhookMissingURL covers Deploy's url guard.
func TestDeployWebhookMissingURL(t *testing.T) {
	p, _ := provider.Get("webhook")
	if _, err := p.Deploy(context.Background(), &provider.CertificateData{}, map[string]any{}, nil, nil); err == nil {
		t.Fatal("expected url error")
	}
}

// TestDeployWebhookMetadataAndHeaders exercises metadata + expiry handling, the
// api_key/secret/custom-header code in addHeaders, and the non-JSON 2xx success
// branch in send.
func TestDeployWebhookMetadataAndHeaders(t *testing.T) {
	var gotAPIKey, gotSecret, gotCustom, gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAPIKey = r.Header.Get("X-API-Key")
		gotSecret = r.Header.Get("X-Webhook-Secret")
		gotCustom = r.Header.Get("X-Custom")
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, "OK plain text") // non-JSON 2xx body
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	cert := &provider.CertificateData{
		ID: "c1", CommonName: "api.example.com", CertificatePEM: "C", PrivateKeyPEM: "K",
		ExpiresAt: time.Now().Add(time.Hour),
	}
	config := map[string]any{
		"url":      srv.URL,
		"metadata": map[string]any{"env": "prod", "ignored": 5},
		"headers":  map[string]any{"X-Custom": "cv"},
	}
	res, err := p.Deploy(context.Background(), cert, config, map[string]any{"api_key": "KEY-123", "secret": "SIGN-1"}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success on 2xx non-JSON body: %+v", res)
	}
	if gotAPIKey != "KEY-123" || gotSecret != "SIGN-1" || gotCustom != "cv" {
		t.Fatalf("headers wrong: apikey=%q secret=%q custom=%q", gotAPIKey, gotSecret, gotCustom)
	}
	if gotAuth != "" {
		t.Fatalf("no bearer expected when only api_key set, got %q", gotAuth)
	}
}

// TestDeployWebhookDetailsMapped covers resource_id/details mapping and the raw
// authorization-header credential branch.
func TestDeployWebhookDetailsMapped(t *testing.T) {
	var gotAuth string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		_, _ = io.WriteString(w, `{"success":true,"message":"ok","resource_id":"r9","details":{"k":"v"}}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	res, err := p.Deploy(context.Background(), &provider.CertificateData{ID: "c1"},
		map[string]any{"url": srv.URL}, map[string]any{"authorization": "Custom xyz"}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success || res.Details["resource_id"] != "r9" || res.Details["k"] != "v" {
		t.Fatalf("details not mapped: %+v", res)
	}
	if gotAuth != "Custom xyz" {
		t.Fatalf("raw authorization not forwarded: %q", gotAuth)
	}
}

// TestDeployWebhookSuccessFalse covers the endpoint-reported-failure branch.
func TestDeployWebhookSuccessFalse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"success":false,"message":"validation failed"}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	res, err := p.Deploy(context.Background(), &provider.CertificateData{}, map[string]any{"url": srv.URL}, nil, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "validation failed") {
		t.Fatalf("expected endpoint failure: %+v", res)
	}
}

// TestDeployWebhookTLSSkip covers the skip_tls_verify and timeout_seconds config
// branches in client().
func TestDeployWebhookTLSSkip(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	res, err := p.Deploy(context.Background(), &provider.CertificateData{ID: "c1"},
		map[string]any{"url": srv.URL, "skip_tls_verify": true, "timeout_seconds": float64(30)}, nil, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success against TLS endpoint: %+v", res)
	}
}

// TestVerifyWebhookMissingURL covers probe's url guard.
func TestVerifyWebhookMissingURL(t *testing.T) {
	p, _ := provider.Get("webhook")
	if _, err := p.Verify(context.Background(), &provider.CertificateData{}, map[string]any{}, nil); err == nil {
		t.Fatal("expected url error")
	}
}

// TestVerifyWebhookFallsBackToURL covers probe falling back to the main url when
// verify_url is absent.
func TestVerifyWebhookFallsBackToURL(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	res, err := p.Verify(context.Background(), &provider.CertificateData{ID: "c1"}, map[string]any{"url": srv.URL}, nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !res.Success || path != "/" {
		t.Fatalf("expected fallback to main url, path=%q res=%+v", path, res)
	}
}

// TestRollbackWebhookHappy covers a successful rollback routed to rollback_url.
func TestRollbackWebhookHappy(t *testing.T) {
	var action, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		raw, _ := io.ReadAll(r.Body)
		var b map[string]any
		_ = json.Unmarshal(raw, &b)
		action, _ = b["action"].(string)
		_, _ = io.WriteString(w, `{"success":true,"message":"rolled back"}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	res, err := p.Rollback(context.Background(), &provider.CertificateData{ID: "c1"},
		map[string]any{"url": srv.URL, "rollback_url": srv.URL + "/rb"}, nil)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if !res.Success || path != "/rb" || action != "rollback" {
		t.Fatalf("rollback routing wrong: path=%q action=%q res=%+v", path, action, res)
	}
}

// TestRollbackWebhookError covers probe's transport-error path (unreachable
// endpoint -> clean Success=false Result).
func TestRollbackWebhookError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close() // now unreachable

	p, _ := provider.Get("webhook")
	res, err := p.Rollback(context.Background(), &provider.CertificateData{ID: "c1"}, map[string]any{"url": url}, nil)
	if err != nil {
		t.Fatalf("rollback should return a clean Result, got err=%v", err)
	}
	if res.Success {
		t.Fatalf("expected failure against unreachable endpoint: %+v", res)
	}
}

// TestValidateWebhookReachable covers ValidateCredentials' reachable-OK and
// 401-rejected probe paths.
func TestValidateWebhookReachable(t *testing.T) {
	p, _ := provider.Get("webhook")

	ok := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ok.Close()
	if err := p.ValidateCredentials(context.Background(), map[string]any{"token": "t"}, map[string]any{"url": ok.URL}); err != nil {
		t.Fatalf("reachable endpoint should validate, got %v", err)
	}

	deny := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer deny.Close()
	if err := p.ValidateCredentials(context.Background(), nil, map[string]any{"url": deny.URL}); err == nil {
		t.Fatal("expected error on HTTP 401")
	}
}

// TestValidateWebhookUnreachable covers the non-fatal unreachable-endpoint path.
func TestValidateWebhookUnreachable(t *testing.T) {
	p, _ := provider.Get("webhook")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := srv.URL
	srv.Close()
	if err := p.ValidateCredentials(context.Background(), nil, map[string]any{"url": url}); err != nil {
		t.Fatalf("unreachable endpoint should be non-fatal, got %v", err)
	}
}
