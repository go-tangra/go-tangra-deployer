package fortigate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

const testToken = "SUPERSECRETTOKEN-do-not-leak-123"

// makeCert returns a self-signed leaf cert + key PEM with the given common name.
func makeCert(t *testing.T, cn string) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(4242),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     []string{cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshalkey: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM
}

func testCert(t *testing.T) *provider.CertificateData {
	certPEM, keyPEM := makeCert(t, "www.example.com")
	return &provider.CertificateData{
		ID:             "abcdef1234567890",
		SerialNumber:   "4242",
		CommonName:     "www.example.com",
		CertificatePEM: certPEM,
		PrivateKeyPEM:  keyPEM,
	}
}

func hostOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	return strings.TrimPrefix(srv.URL, "https://")
}

// captured records what the mock FortiGate observed.
type captured struct {
	authHeader string
	vdom       string
	importSeen bool
}

func TestRegistrationAndCapabilities(t *testing.T) {
	if !provider.Exists("fortigate") {
		t.Fatal("fortigate provider not registered in init()")
	}
	got := Provider{}.Capabilities()
	want := provider.Capabilities{
		Type:             "fortigate",
		DisplayName:      "FortiGate",
		SupportsVerify:   true,
		SupportsRollback: true,
		ConfigFields: []provider.Field{
			{Key: "vdom", Label: "VDOM", Required: true},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "FortiGate host", Required: true},
			{Key: "api_token", Label: "API token", Secret: true, Required: true},
		},
	}
	if got.Type != want.Type || got.DisplayName != want.DisplayName ||
		got.SupportsVerify != want.SupportsVerify || got.SupportsRollback != want.SupportsRollback {
		t.Fatalf("capabilities scalar mismatch:\n got=%+v\nwant=%+v", got, want)
	}
	if len(got.ConfigFields) != 1 || got.ConfigFields[0] != want.ConfigFields[0] {
		t.Fatalf("config fields mismatch: %+v", got.ConfigFields)
	}
	if len(got.CredentialFields) != 2 ||
		got.CredentialFields[0] != want.CredentialFields[0] ||
		got.CredentialFields[1] != want.CredentialFields[1] {
		t.Fatalf("credential fields mismatch: %+v", got.CredentialFields)
	}
}

func TestDeployHappyPath(t *testing.T) {
	cap := &captured{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.authHeader = r.Header.Get("Authorization")
		cap.vdom = r.URL.Query().Get("vdom")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/monitor/vpn-certificate/local/import"):
			cap.importSeen = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			if cap.importSeen {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"success","results":[{"name":"www_example_com"}]}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	creds := map[string]any{"host": hostOf(t, srv), "api_token": testToken}
	config := map[string]any{"vdom": "root"}

	var lastPct int
	res, err := Provider{}.Deploy(context.Background(), testCert(t), config, creds, func(pct int, _ string) { lastPct = pct })
	if err != nil {
		t.Fatalf("Deploy error: %v", err)
	}
	if !res.Success {
		t.Fatalf("Deploy not successful: %+v", res)
	}
	if !cap.importSeen {
		t.Fatal("import endpoint was never hit")
	}
	if cap.authHeader != "Bearer "+testToken {
		t.Fatalf("token not presented; Authorization=%q", cap.authHeader)
	}
	if cap.vdom != "root" {
		t.Fatalf("vdom query param not set; got %q", cap.vdom)
	}
	if lastPct != 100 {
		t.Fatalf("progress did not reach 100: %d", lastPct)
	}
	if name, _ := res.Details["certificate_name"].(string); name != "www_example_com" {
		t.Fatalf("unexpected certificate_name: %v", res.Details["certificate_name"])
	}
}

func TestDeployFailureDoesNotLeakToken(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/import"):
			// FortiOS rejects the import: 424 Failed Dependency.
			w.WriteHeader(http.StatusFailedDependency)
			_, _ = w.Write([]byte(`{"status":"error","error":-23,"cli_error":"object in use"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	creds := map[string]any{"host": hostOf(t, srv), "api_token": testToken}
	config := map[string]any{"vdom": "root"}

	res, err := Provider{}.Deploy(context.Background(), testCert(t), config, creds, nil)

	// A device-side rejection surfaces as a non-nil error and/or a Result with
	// Success=false; either way the api_token must never appear in the message.
	if err == nil && (res == nil || res.Success) {
		t.Fatalf("expected failure, got res=%+v err=%v", res, err)
	}
	msg := ""
	if err != nil {
		msg += err.Error()
	}
	if res != nil {
		if res.Success {
			t.Fatalf("expected Success=false, got %+v", res)
		}
		msg += " " + res.Message
	}
	if strings.Contains(msg, testToken) {
		t.Fatalf("api_token leaked into failure message: %q", msg)
	}
}

func TestVerifyHappyPath(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			if r.URL.Query().Get("vdom") == "" {
				t.Errorf("vdom query param missing on verify GET")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","results":[{"name":"www_example_com"}]}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	creds := map[string]any{"host": hostOf(t, srv), "api_token": testToken}
	config := map[string]any{"vdom": "root"}

	res, err := Provider{}.Verify(context.Background(), testCert(t), config, creds)
	if err != nil {
		t.Fatalf("Verify error: %v", err)
	}
	if !res.Success {
		t.Fatalf("Verify not successful: %+v", res)
	}
	if name, _ := res.Details["certificate_name"].(string); name != "www_example_com" {
		t.Fatalf("unexpected certificate_name: %v", res.Details["certificate_name"])
	}
}

func TestValidateCredentialsMissingFields(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{"vdom": "root"}

	if err := (Provider{}).ValidateCredentials(ctx, map[string]any{"api_token": testToken}, config); err == nil {
		t.Fatal("expected error when host is missing")
	}
	if err := (Provider{}).ValidateCredentials(ctx, map[string]any{"host": "fw.example.com"}, config); err == nil {
		t.Fatal("expected error when api_token is missing")
	}
}
