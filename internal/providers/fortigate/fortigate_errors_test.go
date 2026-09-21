package fortigate_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"

	_ "github.com/go-freya/freya/services/deployer/internal/providers/fortigate"
)

const fgToken = "SUPERSECRETTOKEN-do-not-leak-123"

func fgHost(srv *httptest.Server) string { return strings.TrimPrefix(srv.URL, "https://") }

// fgCert uses non-PEM material on purpose: the provider only ships and
// base64-encodes it, and the object name falls back to the sanitized common name
// ("www_example_com").
func fgCert() *provider.CertificateData {
	return &provider.CertificateData{
		ID:             "abcdef1234567890",
		CommonName:     "www.example.com",
		CertificatePEM: "CERTDATA",
		PrivateKeyPEM:  "KEYDATA",
	}
}

// TestValidateFortigate covers the reachable-OK, 401-rejected, missing-field and
// unreachable (non-fatal) paths of ValidateCredentials.
func TestValidateFortigate(t *testing.T) {
	p, _ := provider.Get("fortigate")
	ctx := context.Background()
	cfg := map[string]any{"vdom": "root"}

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"success"}`)
	}))
	defer srv.Close()
	if err := p.ValidateCredentials(ctx, map[string]any{"host": fgHost(srv), "api_token": fgToken}, cfg); err != nil {
		t.Fatalf("expected nil for reachable device, got %v", err)
	}

	un := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer un.Close()
	if err := p.ValidateCredentials(ctx, map[string]any{"host": fgHost(un), "api_token": fgToken}, cfg); err == nil {
		t.Fatal("expected error on HTTP 401")
	}

	if err := p.ValidateCredentials(ctx, map[string]any{"api_token": fgToken}, cfg); err == nil {
		t.Fatal("expected host error")
	}
	if err := p.ValidateCredentials(ctx, map[string]any{"host": "h"}, cfg); err == nil {
		t.Fatal("expected api_token error")
	}
	if err := p.ValidateCredentials(ctx, map[string]any{"host": "127.0.0.1:1", "api_token": fgToken}, cfg); err != nil {
		t.Fatalf("unreachable device should be non-fatal, got %v", err)
	}
}

// TestDeployFortigateValidation covers the credential guard and the
// missing-material guard.
func TestDeployFortigateValidation(t *testing.T) {
	p, _ := provider.Get("fortigate")
	ctx := context.Background()
	cfg := map[string]any{"vdom": "root"}

	if _, err := p.Deploy(ctx, fgCert(), cfg, map[string]any{"api_token": fgToken}, nil); err == nil {
		t.Fatal("expected host error")
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"status":"success"}`)
	}))
	defer srv.Close()
	if _, err := p.Deploy(ctx, &provider.CertificateData{}, cfg,
		map[string]any{"host": fgHost(srv), "api_token": fgToken}, nil); err == nil {
		t.Fatal("expected certificate-material error")
	}
}

// TestDeployFortigateReplacesExisting drives the existing-cert delete-then-import
// path (was_update=true).
func TestDeployFortigateReplacesExisting(t *testing.T) {
	var deleted, imported bool
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			_, _ = io.WriteString(w, `{"status":"success"}`)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"status":"success","results":[{"name":"www_example_com"}]}`)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			deleted = true
			_, _ = io.WriteString(w, `{"status":"success"}`)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/import"):
			imported = true
			_, _ = io.WriteString(w, `{"status":"success"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p, _ := provider.Get("fortigate")
	res, err := p.Deploy(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv), "api_token": fgToken}, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success: %+v", res)
	}
	if !deleted || !imported {
		t.Fatalf("expected delete+import, deleted=%v imported=%v", deleted, imported)
	}
	if res.Details["was_update"] != true {
		t.Fatalf("was_update should be true: %+v", res.Details)
	}
}

// TestDeployFortigateCheckError covers the check-existing-certificate error path.
func TestDeployFortigateCheckError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cmdb/system/global") {
			_, _ = io.WriteString(w, `{"status":"success"}`)
			return
		}
		if r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"status":"error"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	p, _ := provider.Get("fortigate")
	res, err := p.Deploy(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv), "api_token": fgToken}, nil)
	if err == nil {
		t.Fatalf("expected error, got res=%+v", res)
	}
	if strings.Contains(err.Error(), fgToken) {
		t.Fatal("api_token leaked into error")
	}
}

// TestDeployFortigateVerifyFailure covers the post-import verification failure.
func TestDeployFortigateVerifyFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			_, _ = io.WriteString(w, `{"status":"success"}`)
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/import"):
			_, _ = io.WriteString(w, `{"status":"success"}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	p, _ := provider.Get("fortigate")
	res, err := p.Deploy(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv), "api_token": fgToken}, nil)
	if err == nil {
		t.Fatalf("expected verification error, got res=%+v", res)
	}
	if !strings.Contains(err.Error(), "verification failed") {
		t.Fatalf("expected verification-failed error, got %v", err)
	}
}

// TestVerifyFortigateFailures covers the not-found and transport-error Verify paths.
func TestVerifyFortigateFailures(t *testing.T) {
	p, _ := provider.Get("fortigate")

	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cmdb/system/global") {
			_, _ = io.WriteString(w, `{"status":"success"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	res, err := p.Verify(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv), "api_token": fgToken})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "not found") {
		t.Fatalf("expected not-found failure: %+v", res)
	}

	srv2 := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cmdb/system/global") {
			_, _ = io.WriteString(w, `{"status":"success"}`)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, `{"status":"error"}`)
	}))
	defer srv2.Close()
	res, err = p.Verify(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv2), "api_token": fgToken})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "verify failed") {
		t.Fatalf("expected verify-failed message: %+v", res)
	}
}

// TestVerifyFortigateValidationFail covers Verify's credential guard.
func TestVerifyFortigateValidationFail(t *testing.T) {
	p, _ := provider.Get("fortigate")
	if _, err := p.Verify(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"api_token": fgToken}); err == nil {
		t.Fatal("expected host error")
	}
}

// TestRollbackFortigateHappy covers a successful rollback delete.
func TestRollbackFortigateHappy(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cmdb/system/global") {
			_, _ = io.WriteString(w, `{"status":"success"}`)
			return
		}
		if r.Method == http.MethodDelete {
			_, _ = io.WriteString(w, `{"status":"success"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	p, _ := provider.Get("fortigate")
	res, err := p.Rollback(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv), "api_token": fgToken})
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected rollback success: %+v", res)
	}
}

// TestRollbackFortigateError covers the rollback failure path and confirms the
// api_token is never leaked.
func TestRollbackFortigateError(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/cmdb/system/global") {
			_, _ = io.WriteString(w, `{"status":"success"}`)
			return
		}
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"status":"error","cli_error":"boom"}`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	p, _ := provider.Get("fortigate")
	res, err := p.Rollback(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": fgHost(srv), "api_token": fgToken})
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "rollback failed") {
		t.Fatalf("expected rollback-failed message: %+v", res)
	}
	if strings.Contains(res.Message, fgToken) {
		t.Fatal("api_token leaked into failure message")
	}
}

// TestRollbackFortigateValidationFail covers Rollback's credential guard.
func TestRollbackFortigateValidationFail(t *testing.T) {
	p, _ := provider.Get("fortigate")
	if _, err := p.Rollback(context.Background(), fgCert(), map[string]any{"vdom": "root"},
		map[string]any{"host": "h"}); err == nil {
		t.Fatal("expected api_token error")
	}
}
