package bigip_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"

	_ "github.com/go-freya/freya/services/deployer/internal/providers/bigip"
)

const bigipPass = "p@ss-secret"

var bigipCert = &provider.CertificateData{
	ID:               "abcdef0123456789",
	CommonName:       "www.example.com",
	CertificatePEM:   "-----BEGIN CERTIFICATE-----\nA\n-----END CERTIFICATE-----\n",
	PrivateKeyPEM:    "-----BEGIN PRIVATE KEY-----\nB\n-----END PRIVATE KEY-----\n",
	CertificateChain: "-----BEGIN CERTIFICATE-----\nC\n-----END CERTIFICATE-----\n",
}

func bigipHost(srv *httptest.Server) string { return strings.TrimPrefix(srv.URL, "https://") }

func bigipCreds(srv *httptest.Server) map[string]any {
	return map[string]any{"host": bigipHost(srv), "username": "admin", "password": bigipPass}
}

// happyBigIP returns a mock iControl server where every operation succeeds:
// uploads, crypto installs, the client-ssl profile create, cert existence GETs
// and DELETEs.
func happyBigIP(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		switch {
		case strings.HasPrefix(p, "/mgmt/shared/file-transfer/uploads/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"remainingByteCount":0}`)
		case p == "/mgmt/tm/sys/crypto/cert" || p == "/mgmt/tm/sys/crypto/key":
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"kind":"state"}`)
		case strings.HasPrefix(p, "/mgmt/tm/sys/crypto/cert/") || strings.HasPrefix(p, "/mgmt/tm/sys/crypto/key/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"name":"x"}`)
		case p == "/mgmt/tm/ltm/profile/client-ssl":
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"kind":"state"}`)
		case strings.HasPrefix(p, "/mgmt/tm/ltm/profile/client-ssl/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		}
	}))
}

// TestValidateCredentialsBigIP covers the reachable-OK, 401-rejected and
// unreachable (non-fatal) probe paths of ValidateCredentials.
func TestValidateCredentialsBigIP(t *testing.T) {
	p, _ := provider.Get("bigip")
	ctx := context.Background()
	cfg := map[string]any{"partition": "Common"}

	srv := happyBigIP(t)
	defer srv.Close()
	if err := p.ValidateCredentials(ctx, bigipCreds(srv), cfg); err != nil {
		t.Fatalf("expected nil for reachable appliance, got %v", err)
	}

	un := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer un.Close()
	if err := p.ValidateCredentials(ctx, bigipCreds(un), cfg); err == nil {
		t.Fatal("expected credentials error on HTTP 401")
	}

	if err := p.ValidateCredentials(ctx, map[string]any{"host": "127.0.0.1:1", "username": "a", "password": "b"}, cfg); err != nil {
		t.Fatalf("unreachable appliance should be non-fatal, got %v", err)
	}
}

// TestValidateCredentialsBigIPMissing covers the missing-field guards.
func TestValidateCredentialsBigIPMissing(t *testing.T) {
	p, _ := provider.Get("bigip")
	ctx := context.Background()
	cfg := map[string]any{"partition": "Common"}
	for _, c := range []map[string]any{
		{"username": "u", "password": "p"},
		{"host": "h", "password": "p"},
		{"host": "h", "username": "u"},
	} {
		if err := p.ValidateCredentials(ctx, c, cfg); err == nil {
			t.Fatalf("expected error for creds %+v", c)
		}
	}
}

// TestDeployBigIPValidation covers Deploy's missing-host, missing-partition and
// missing-material guards.
func TestDeployBigIPValidation(t *testing.T) {
	p, _ := provider.Get("bigip")
	ctx := context.Background()

	if _, err := p.Deploy(ctx, bigipCert, map[string]any{"partition": "Common"},
		map[string]any{"username": "u", "password": "p"}, nil); err == nil {
		t.Fatal("expected host error")
	}
	srv := happyBigIP(t)
	defer srv.Close()
	if _, err := p.Deploy(ctx, bigipCert, map[string]any{}, bigipCreds(srv), nil); err == nil {
		t.Fatal("expected partition error")
	}
	if _, err := p.Deploy(ctx, &provider.CertificateData{}, map[string]any{"partition": "Common"}, bigipCreds(srv), nil); err == nil {
		t.Fatal("expected certificate-material error")
	}
}

// TestDeployBigIPConflictRetry drives the "already exists -> overwrite" retry in
// installCrypto and the "already exists -> PATCH" path in bindClientSSLProfile.
func TestDeployBigIPConflictRetry(t *testing.T) {
	seen := map[string]int{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		key := r.Method + " " + p
		seen[key]++
		switch {
		case strings.HasPrefix(p, "/mgmt/shared/file-transfer/uploads/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		case p == "/mgmt/tm/sys/crypto/cert" || p == "/mgmt/tm/sys/crypto/key":
			if seen[key] == 1 {
				w.WriteHeader(http.StatusConflict)
				_, _ = io.WriteString(w, `{"message":"already exists"}`)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		case strings.HasPrefix(p, "/mgmt/tm/sys/crypto/cert/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"name":"x"}`)
		case p == "/mgmt/tm/ltm/profile/client-ssl":
			w.WriteHeader(http.StatusConflict)
			_, _ = io.WriteString(w, `{"message":"already exists"}`)
		case strings.HasPrefix(p, "/mgmt/tm/ltm/profile/client-ssl/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		}
	}))
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Deploy(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv), nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success through conflict-retry paths: %+v", res)
	}
}

// TestDeployBigIPCertInstallFailure covers the "certificate upload failed" path
// and confirms the password is never leaked.
func TestDeployBigIPCertInstallFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mgmt/tm/sys/crypto/cert" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"message":"install rejected"}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Deploy(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv), nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "certificate upload failed") {
		t.Fatalf("expected certificate upload failure: %+v", res)
	}
	if strings.Contains(res.Message, bigipPass) {
		t.Fatal("password leaked into failure message")
	}
}

// TestDeployBigIPBindFailure covers the client-SSL profile binding failure path.
func TestDeployBigIPBindFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/mgmt/tm/ltm/profile/client-ssl" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"message":"bind rejected"}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Deploy(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv), nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "binding failed") {
		t.Fatalf("expected profile binding failure: %+v", res)
	}
}

// TestDeployBigIPVerifyFailure covers the post-deploy verification failure path.
func TestDeployBigIPVerifyFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/mgmt/tm/sys/crypto/cert/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Deploy(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv), nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "post-deploy verification failed") {
		t.Fatalf("expected post-deploy verification failure: %+v", res)
	}
}

// TestVerifyBigIPFailure covers Verify when the cert object is absent (404).
func TestVerifyBigIPFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/mgmt/tm/sys/crypto/cert/") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Verify(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv))
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Success {
		t.Fatalf("expected verify failure for absent cert: %+v", res)
	}
}

// TestVerifyBigIPMissing covers Verify's credential and partition guards.
func TestVerifyBigIPMissing(t *testing.T) {
	p, _ := provider.Get("bigip")
	if _, err := p.Verify(context.Background(), bigipCert, map[string]any{"partition": "Common"},
		map[string]any{"username": "u", "password": "p"}); err == nil {
		t.Fatal("expected host error")
	}
	srv := happyBigIP(t)
	defer srv.Close()
	if _, err := p.Verify(context.Background(), bigipCert, map[string]any{}, bigipCreds(srv)); err == nil {
		t.Fatal("expected partition error")
	}
}

// TestRollbackBigIPHappy covers a fully successful rollback.
func TestRollbackBigIPHappy(t *testing.T) {
	srv := happyBigIP(t)
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Rollback(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv))
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected rollback success: %+v", res)
	}
}

// TestRollbackBigIPPartialFailure covers rollback when a delete fails.
func TestRollbackBigIPPartialFailure(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/mgmt/tm/sys/crypto/cert/") {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"message":"boom"}`)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("bigip")
	res, err := p.Rollback(context.Background(), bigipCert, map[string]any{"partition": "Common"}, bigipCreds(srv))
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "partially failed") {
		t.Fatalf("expected partial-failure rollback: %+v", res)
	}
	if strings.Contains(res.Message, bigipPass) {
		t.Fatal("password leaked into failure message")
	}
}

// TestRollbackBigIPMissing covers rollback's credential and partition guards.
func TestRollbackBigIPMissing(t *testing.T) {
	p, _ := provider.Get("bigip")
	if _, err := p.Rollback(context.Background(), bigipCert, map[string]any{"partition": "Common"},
		map[string]any{"username": "u", "password": "p"}); err == nil {
		t.Fatal("expected host error")
	}
	srv := happyBigIP(t)
	defer srv.Close()
	if _, err := p.Rollback(context.Background(), bigipCert, map[string]any{}, bigipCreds(srv)); err == nil {
		t.Fatal("expected partition error")
	}
}
