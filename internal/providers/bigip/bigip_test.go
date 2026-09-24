package bigip

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// testCert is a small, self-consistent set of certificate material. The PEM
// bodies are placeholders; the provider never parses them, it only ships them.
var testCert = &provider.CertificateData{
	ID:               "abcdef0123456789",
	SerialNumber:     "01",
	CommonName:       "www.example.com",
	SANs:             []string{"www.example.com"},
	CertificatePEM:   "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----\n",
	PrivateKeyPEM:    "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n",
	CertificateChain: "-----BEGIN CERTIFICATE-----\nMIID\n-----END CERTIFICATE-----\n",
}

const (
	testUser = "admin"
	testPass = "s3cr3t-P@ss"
)

// capture records what the mock iControl server observed.
type capture struct {
	authUser     string
	authPass     string
	authSeen     bool
	installNames []string // "name" fields from crypto install payloads
	profileNames []string // "name" fields from client-ssl create payloads
	paths        []string
}

// mockBigIP returns an httptest TLS server that recognises the iControl upload,
// crypto-install, client-ssl and verify paths. fail401 makes every call return
// 401 so a failure path can be exercised.
func mockBigIP(t *testing.T, cap *capture, fail401 bool) *httptest.Server {
	t.Helper()
	return httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.paths = append(cap.paths, r.URL.Path)
		if u, p, ok := r.BasicAuth(); ok {
			cap.authSeen, cap.authUser, cap.authPass = true, u, p
		}
		if fail401 {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = io.WriteString(w, `{"code":401,"message":"authentication failed"}`)
			return
		}

		switch {
		// File-transfer upload endpoint.
		case strings.HasPrefix(r.URL.Path, "/mgmt/shared/file-transfer/uploads/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"remainingByteCount":0}`)

		// Crypto cert/key install (POST) or verify GET (path has a trailing segment).
		case r.URL.Path == "/mgmt/tm/sys/crypto/cert", r.URL.Path == "/mgmt/tm/sys/crypto/key":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if n, ok := body["name"].(string); ok {
				cap.installNames = append(cap.installNames, n)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"kind":"tm:sys:crypto:cert:certstate"}`)

		// Verify: GET a specific crypto/cert object.
		case strings.HasPrefix(r.URL.Path, "/mgmt/tm/sys/crypto/cert/"):
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"name":"www_example_com.crt"}`)

		// client-ssl profile create.
		case r.URL.Path == "/mgmt/tm/ltm/profile/client-ssl":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if n, ok := body["name"].(string); ok {
				cap.profileNames = append(cap.profileNames, n)
			}
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{"kind":"tm:ltm:profile:client-ssl:client-sslstate"}`)

		default:
			w.WriteHeader(http.StatusOK)
			_, _ = io.WriteString(w, `{}`)
		}
	}))
}

// hostOf strips the https:// scheme from a test server URL, as a real host cred
// would be stored.
func hostOf(srv *httptest.Server) string {
	return strings.TrimPrefix(srv.URL, "https://")
}

func credsFor(srv *httptest.Server) map[string]any {
	return map[string]any{"host": hostOf(srv), "username": testUser, "password": testPass}
}

func TestRegistrationAndCapabilities(t *testing.T) {
	p, err := provider.Get("bigip")
	if err != nil {
		t.Fatalf("bigip not registered: %v", err)
	}
	c := p.Capabilities()
	if c.Type != "bigip" || c.DisplayName != "F5 BIG-IP" {
		t.Fatalf("type/display: %+v", c)
	}
	if !c.SupportsVerify || !c.SupportsRollback {
		t.Fatalf("verify/rollback flags: %+v", c)
	}
	if len(c.ConfigFields) != 1 {
		t.Fatalf("config fields: %+v", c.ConfigFields)
	}
	pf := c.ConfigFields[0]
	if pf.Key != "partition" || !pf.Required {
		t.Fatalf("partition field: %+v", pf)
	}
	// Credential fields: host(req), username(req), password(secret,req).
	want := map[string]struct{ secret, required bool }{
		"host":     {false, true},
		"username": {false, true},
		"password": {true, true},
	}
	if len(c.CredentialFields) != len(want) {
		t.Fatalf("credential field count: %+v", c.CredentialFields)
	}
	for _, f := range c.CredentialFields {
		w, ok := want[f.Key]
		if !ok {
			t.Fatalf("unexpected credential field %q", f.Key)
		}
		if f.Secret != w.secret || f.Required != w.required {
			t.Fatalf("credential field %q = %+v, want secret=%v required=%v", f.Key, f, w.secret, w.required)
		}
	}
}

func TestDeployHappyPath(t *testing.T) {
	cap := &capture{}
	srv := mockBigIP(t, cap, false)
	defer srv.Close()

	p := Provider{}
	var lastPct int
	res, err := p.Deploy(context.Background(), testCert,
		map[string]any{"partition": "Prod"}, credsFor(srv),
		func(pct int, _ string) { lastPct = pct })
	if err != nil {
		t.Fatalf("deploy error: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success, got: %+v", res)
	}
	if lastPct != 100 {
		t.Fatalf("final progress = %d, want 100", lastPct)
	}

	// HTTP Basic auth: header must decode to username:password.
	if !cap.authSeen {
		t.Fatal("no Authorization header seen")
	}
	if cap.authUser != testUser || cap.authPass != testPass {
		t.Fatalf("basic auth = %q:%q, want %q:%q", cap.authUser, cap.authPass, testUser, testPass)
	}
	// Also confirm the wire header itself is a well-formed Basic credential.
	assertBasicHeaderDecodes(t, cap.authUser, cap.authPass)

	// The configured partition must appear in the installed object names.
	if len(cap.installNames) == 0 {
		t.Fatal("no crypto install captured")
	}
	for _, n := range cap.installNames {
		if !strings.HasPrefix(n, "/Prod/") {
			t.Fatalf("install name %q not in partition /Prod/", n)
		}
	}
	// The client-ssl profile must be created in the partition too.
	if len(cap.profileNames) == 0 {
		t.Fatal("no client-ssl profile created")
	}
	if !strings.HasPrefix(cap.profileNames[0], "/Prod/") {
		t.Fatalf("profile name %q not in partition /Prod/", cap.profileNames[0])
	}
	if res.Details["partition"] != "Prod" {
		t.Fatalf("details partition = %v", res.Details["partition"])
	}
}

func assertBasicHeaderDecodes(t *testing.T, gotUser, gotPass string) {
	t.Helper()
	raw := base64.StdEncoding.EncodeToString([]byte(testUser + ":" + testPass))
	decoded, err := base64.StdEncoding.DecodeString(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	parts := strings.SplitN(string(decoded), ":", 2)
	if len(parts) != 2 || parts[0] != gotUser || parts[1] != gotPass {
		t.Fatalf("decoded basic auth mismatch: %v", parts)
	}
}

func TestDeployFailureUnauthorized(t *testing.T) {
	cap := &capture{}
	srv := mockBigIP(t, cap, true) // every call returns 401
	defer srv.Close()

	p := Provider{}
	res, err := p.Deploy(context.Background(), testCert,
		map[string]any{"partition": "Common"}, credsFor(srv), nil)
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if res.Success {
		t.Fatalf("expected failure, got success: %+v", res)
	}
	if strings.Contains(res.Message, testPass) {
		t.Fatalf("password leaked into message: %q", res.Message)
	}
}

func TestVerifyHappyPath(t *testing.T) {
	cap := &capture{}
	srv := mockBigIP(t, cap, false)
	defer srv.Close()

	p := Provider{}
	res, err := p.Verify(context.Background(), testCert,
		map[string]any{"partition": "Common"}, credsFor(srv))
	if err != nil {
		t.Fatalf("verify error: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected verify success, got: %+v", res)
	}
	if res.Details["certificate_name"] != "/Common/www_example_com.crt" {
		t.Fatalf("cert name in details: %v", res.Details["certificate_name"])
	}
	// Verify must have hit the mock's GET-by-name path.
	hit := false
	for _, pth := range cap.paths {
		if strings.HasPrefix(pth, "/mgmt/tm/sys/crypto/cert/") {
			hit = true
		}
	}
	if !hit {
		t.Fatalf("verify did not query the cert object: %v", cap.paths)
	}
}

func TestValidateCredentialsMissingFields(t *testing.T) {
	p := Provider{}
	ctx := context.Background()
	cases := []struct {
		name  string
		creds map[string]any
	}{
		{"missing host", map[string]any{"username": testUser, "password": testPass}},
		{"missing username", map[string]any{"host": "bigip.local", "password": testPass}},
		{"missing password", map[string]any{"host": "bigip.local", "username": testUser}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := p.ValidateCredentials(ctx, tc.creds, map[string]any{"partition": "Common"}); err == nil {
				t.Fatalf("%s: expected error, got nil", tc.name)
			}
		})
	}
}
