package cloudflare

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

const testToken = "cf-secret-token-abcdefghijklmnop"

func testCert() *provider.CertificateData {
	return &provider.CertificateData{
		ID:               "cert-1",
		SerialNumber:     "01",
		CommonName:       "example.com",
		CertificatePEM:   "-----BEGIN CERTIFICATE-----\nLEAF\n-----END CERTIFICATE-----",
		PrivateKeyPEM:    "-----BEGIN PRIVATE KEY-----\nKEY\n-----END PRIVATE KEY-----",
		CertificateChain: "-----BEGIN CERTIFICATE-----\nCHAIN\n-----END CERTIFICATE-----",
	}
}

func TestRegistrationAndCapabilities(t *testing.T) {
	p, err := provider.Get("cloudflare")
	if err != nil {
		t.Fatalf("cloudflare not registered: %v", err)
	}
	c := p.Capabilities()
	if c.Type != "cloudflare" || c.DisplayName != "Cloudflare" {
		t.Fatalf("type/display: %+v", c)
	}
	if !c.SupportsVerify {
		t.Fatal("SupportsVerify must be true")
	}
	if c.SupportsRollback {
		t.Fatal("SupportsRollback must be false")
	}
	if len(c.ConfigFields) != 1 || c.ConfigFields[0].Key != "zone_id" || !c.ConfigFields[0].Required {
		t.Fatalf("config fields: %+v", c.ConfigFields)
	}
	if len(c.CredentialFields) != 1 {
		t.Fatalf("credential fields: %+v", c.CredentialFields)
	}
	cf := c.CredentialFields[0]
	if cf.Key != "api_token" || !cf.Secret || !cf.Required {
		t.Fatalf("credential field: %+v", cf)
	}
}

func TestDeployHappyPath(t *testing.T) {
	var (
		gotAuth  string
		postPath string
		postBody map[string]any
		sawPost  bool
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet: // list existing certs -> none
			io.WriteString(w, `{"success":true,"errors":[],"result":[]}`)
		case http.MethodPost: // upload new cert
			sawPost = true
			postPath = r.URL.Path
			raw, _ := io.ReadAll(r.Body)
			_ = json.Unmarshal(raw, &postBody)
			io.WriteString(w, `{"success":true,"errors":[],"result":{"id":"new-cert-id"}}`)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	config := map[string]any{"zone_id": "zone123", "api_base": srv.URL}
	creds := map[string]any{"api_token": testToken}

	var lastPct int
	res, err := p.Deploy(context.Background(), testCert(), config, creds, func(pct int, _ string) { lastPct = pct })
	if err != nil {
		t.Fatalf("deploy error: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success, got %+v", res)
	}
	if lastPct != 100 {
		t.Fatalf("expected final progress 100, got %d", lastPct)
	}
	if !sawPost {
		t.Fatal("provider never POSTed the certificate")
	}
	if gotAuth != "Bearer "+testToken {
		t.Fatalf("wrong Authorization header: %q", gotAuth)
	}
	if !strings.HasSuffix(postPath, "/zones/zone123/custom_certificates") {
		t.Fatalf("wrong upload path: %q", postPath)
	}
	if postBody["certificate"] == nil || !strings.Contains(postBody["certificate"].(string), "LEAF") {
		t.Fatalf("cert not in body: %+v", postBody["certificate"])
	}
	if !strings.Contains(postBody["certificate"].(string), "CHAIN") {
		t.Fatal("chain not appended to certificate bundle")
	}
	if postBody["private_key"] == nil || !strings.Contains(postBody["private_key"].(string), "KEY") {
		t.Fatalf("private key not in body")
	}
	if res.Details["certificate_id"] != "new-cert-id" {
		t.Fatalf("details: %+v", res.Details)
	}
}

func TestDeployFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodGet {
			io.WriteString(w, `{"success":true,"errors":[],"result":[]}`)
			return
		}
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"success":false,"errors":[{"code":1004,"message":"Certificate validation failed"}],"result":null}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	config := map[string]any{"zone_id": "zone123", "api_base": srv.URL}
	creds := map[string]any{"api_token": testToken}

	res, err := p.Deploy(context.Background(), testCert(), config, creds, nil)
	if err != nil {
		t.Fatalf("expected nil error with failed Result, got %v", err)
	}
	if res.Success {
		t.Fatal("expected Success=false")
	}
	if res.Message == "" {
		t.Fatal("expected non-empty Message")
	}
	if strings.Contains(res.Message, testToken) {
		t.Fatalf("api_token leaked into Message: %q", res.Message)
	}
}

func TestVerifyHappyPath(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"success":true,"errors":[],"result":[{"id":"found-id","hosts":["example.com"]}]}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("cloudflare")
	config := map[string]any{"zone_id": "zone123", "api_base": srv.URL}
	creds := map[string]any{"api_token": testToken}

	res, err := p.Verify(context.Background(), testCert(), config, creds)
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected verified, got %+v", res)
	}
	if res.Details["certificate_id"] != "found-id" {
		t.Fatalf("details: %+v", res.Details)
	}
}

func TestValidateCredentials(t *testing.T) {
	p, _ := provider.Get("cloudflare")
	ctx := context.Background()

	if err := p.ValidateCredentials(ctx, map[string]any{"api_token": ""}, map[string]any{"zone_id": "z"}); err == nil {
		t.Fatal("expected error for empty api_token")
	}
	if err := p.ValidateCredentials(ctx, map[string]any{"api_token": testToken}, map[string]any{"zone_id": ""}); err == nil {
		t.Fatal("expected error for empty zone_id")
	}
	if err := p.ValidateCredentials(ctx, map[string]any{"api_token": testToken}, map[string]any{"zone_id": "z"}); err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
}

func TestRollbackUnsupported(t *testing.T) {
	p, _ := provider.Get("cloudflare")
	res, err := p.Rollback(context.Background(), testCert(), map[string]any{"zone_id": "z"}, map[string]any{"api_token": testToken})
	if err != nil {
		t.Fatalf("rollback should return nil error, got %v", err)
	}
	if res.Success {
		t.Fatal("rollback must report Success=false")
	}
	if res.Message == "" {
		t.Fatal("rollback must explain it is unsupported")
	}
}
