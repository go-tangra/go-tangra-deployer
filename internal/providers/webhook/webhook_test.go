package webhook_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/webhook"
)

func TestRegistrationAndCapabilities(t *testing.T) {
	p, err := provider.Get("webhook")
	if err != nil {
		t.Fatalf("webhook not registered: %v", err)
	}
	c := p.Capabilities()
	if c.Type != "webhook" || !c.SupportsVerify || !c.SupportsRollback {
		t.Fatalf("capabilities: %+v", c)
	}
}

func TestDeployPostsBundleWithAuth(t *testing.T) {
	var gotAuth, gotAction string
	var body map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &body)
		gotAction, _ = body["action"].(string)
		_, _ = io.WriteString(w, `{"success":true,"message":"installed","resource_id":"rid-1"}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("webhook")
	res, err := p.Deploy(context.Background(),
		&provider.CertificateData{ID: "c1", CommonName: "api.example.com", CertificatePEM: "CERT", PrivateKeyPEM: "KEY"},
		map[string]any{"url": srv.URL}, map[string]any{"token": "tok-123"}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success || res.Details["resource_id"] != "rid-1" {
		t.Fatalf("result: %+v", res)
	}
	if gotAuth != "Bearer tok-123" {
		t.Fatalf("auth header = %q", gotAuth)
	}
	if gotAction != "deploy" || body["certificate_pem"] != "CERT" {
		t.Fatalf("payload: %+v", body)
	}
}

func TestDeployFailureFromEndpoint(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = io.WriteString(w, `{"success":false,"message":"backend down"}`)
	}))
	defer srv.Close()
	p, _ := provider.Get("webhook")
	res, err := p.Deploy(context.Background(), &provider.CertificateData{}, map[string]any{"url": srv.URL}, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "backend down") {
		t.Fatalf("expected failure: %+v", res)
	}
}

func TestVerifyUsesVerifyURL(t *testing.T) {
	var path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path = r.URL.Path
		_, _ = io.WriteString(w, `{"success":true,"message":"present"}`)
	}))
	defer srv.Close()
	p, _ := provider.Get("webhook")
	res, err := p.Verify(context.Background(), &provider.CertificateData{ID: "c1"},
		map[string]any{"url": srv.URL, "verify_url": srv.URL + "/verify"}, nil)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !res.Success || path != "/verify" {
		t.Fatalf("verify routed to %q, result %+v", path, res)
	}
}

func TestValidateCredentials(t *testing.T) {
	p, _ := provider.Get("webhook")
	if err := p.ValidateCredentials(context.Background(), nil, map[string]any{}); err == nil {
		t.Fatal("expected missing-url error")
	}
	if err := p.ValidateCredentials(context.Background(), nil, map[string]any{"url": "ftp://x"}); err == nil {
		t.Fatal("expected scheme error")
	}
}
