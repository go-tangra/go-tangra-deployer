package bigip

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// keyBody is the base64 body of testCert's private key.
const keyBody = "MIIE"

// assertNoSecret fails when the password or the private key appears in a
// result's message or details.
func assertNoSecret(t *testing.T, res *provider.Result) {
	t.Helper()
	if res == nil {
		return
	}
	d, _ := json.Marshal(res.Details)
	for _, s := range []string{res.Message, string(d)} {
		if strings.Contains(s, testPass) || strings.Contains(s, keyBody) || strings.Contains(s, "PRIVATE KEY") {
			t.Fatalf("secret material in %q", s)
		}
	}
}

func assertNoSecretErr(t *testing.T, err error) {
	t.Helper()
	if err != nil && (strings.Contains(err.Error(), testPass) || strings.Contains(err.Error(), keyBody)) {
		t.Fatalf("secret material in error %q", err)
	}
}

// TestEchoedSecretsAreScrubbed: an appliance that echoes the password or the
// private key (as sent, JSON-escaped or otherwise encoded) in an error body
// never gets them into a message.
func TestEchoedSecretsAreScrubbed(t *testing.T) {
	escapedKey, _ := json.Marshal(strings.TrimSpace(testCert.PrivateKeyPEM))
	echoes := map[string]string{
		"password":     `{"message":"bad login for admin/` + testPass + `"}`,
		"key as sent":  testCert.PrivateKeyPEM,
		"key escaped":  `{"message":` + string(escapedKey) + `}`,
		"key reworded": `{"message":"-----BEGIN RSA PRIVATE KEY-----\nAAAA"}`,
	}
	paths := []struct{ method, path, prefix string }{
		{http.MethodPost, "/mgmt/shared/file-transfer/uploads/www_example_com.key", "failed to upload private key: "},
		{http.MethodPost, "/mgmt/tm/sys/crypto/key", "failed to upload private key: "},
		{http.MethodPost, "/mgmt/tm/ltm/profile/client-ssl", "failed to create/update SSL profile: "},
		{http.MethodGet, "/mgmt/tm/sys/crypto/cert/~Common~www_example_com.crt", "verification failed: "},
		{http.MethodGet, "/mgmt/tm/sys/version", "BIG-IP API error"},
	}
	for name, echo := range echoes {
		for _, p := range paths {
			t.Run(name+" "+p.path, func(t *testing.T) {
				f := newFake(t)
				f.failAlways(p.method, p.path, http.StatusBadRequest, echo)
				res, err := Provider{}.Deploy(context.Background(), testCert, map[string]any{"ssl_profile": "p"}, f.creds(), nil)
				if err != nil || res.Success || !strings.HasPrefix(res.Message, p.prefix) {
					t.Fatalf("result = %+v, %v", res, err)
				}
				assertNoSecret(t, res)
				if p.path != "/mgmt/tm/sys/version" && !strings.Contains(res.Message, redacted) {
					t.Fatalf("echo not redacted: %q", res.Message)
				}
			})
		}
	}
	// ValidateCredentials, Verify and Rollback scrub the password too.
	f := newFake(t)
	f.failAlways(http.MethodGet, "/mgmt/tm/sys/version", http.StatusBadGateway, testPass)
	err := Provider{}.ValidateCredentials(context.Background(), f.creds(), nil)
	if err == nil || !strings.Contains(err.Error(), redacted) {
		t.Fatalf("validate: %v", err)
	}
	assertNoSecretErr(t, err)
	g := newFake(t)
	g.failAlways(http.MethodGet, "/mgmt/tm/sys/crypto/cert/~Common~www_example_com.crt", 500, testPass)
	res, _ := Provider{}.Verify(context.Background(), testCert, nil, g.creds())
	assertNoSecret(t, res)
	g.failAlways(http.MethodDelete, "/mgmt/tm/sys/crypto/key/~Common~www_example_com.key", 500, testPass)
	res, _ = Provider{}.Rollback(context.Background(), testCert, nil, g.creds())
	if res.Success {
		t.Fatal(res)
	}
	assertNoSecret(t, res)
}

// TestNoSecretsAnywhere sweeps every outcome of every operation for the
// password and the private key.
func TestNoSecretsAnywhere(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	cfg := map[string]any{"partition": "Common", "ssl_profile": "p"}
	for i := 0; i < 2; i++ {
		res, err := Provider{}.Deploy(ctx, testCert, cfg, f.creds(), func(_ int, msg string) {
			if strings.Contains(msg, testPass) || strings.Contains(msg, keyBody) {
				t.Fatalf("secret in progress %q", msg)
			}
		})
		assertNoSecretErr(t, err)
		assertNoSecret(t, res)
	}
	res, err := Provider{}.Verify(ctx, testCert, cfg, f.creds())
	assertNoSecretErr(t, err)
	assertNoSecret(t, res)
	res, err = Provider{}.Rollback(ctx, testCert, cfg, f.creds())
	assertNoSecretErr(t, err)
	assertNoSecret(t, res)
	bad := f.creds()
	bad["host"] = "127.0.0.1:1"
	res, err = Provider{}.Deploy(ctx, testCert, cfg, bad, nil)
	assertNoSecretErr(t, err)
	assertNoSecret(t, res)
	assertNoSecretErr(t, Provider{}.ValidateCredentials(ctx, bad, cfg))
}
