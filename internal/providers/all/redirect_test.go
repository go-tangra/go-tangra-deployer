package all_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// TestStoredConfigCannotRedirectCredentials: for every provider whose
// destination comes from its credentials (bigip, fortigate) or is the
// configured webhook url, no other configuration key — the former test hooks
// "endpoint"/"api_base" or a "host"/"base_url" smuggled into config — may send
// a request (and with it the credentials) elsewhere. aws_acm and cloudflare are
// covered by their package tests with an in-process transport (they would
// otherwise reach the real cloud APIs).
func TestStoredConfigCannotRedirectCredentials(t *testing.T) {
	var attackerHits atomic.Int32
	attacker := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { attackerHits.Add(1) }))
	defer attacker.Close()
	attackerTLS := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { attackerHits.Add(1) }))
	defer attackerTLS.Close()

	var legitHits atomic.Int32
	legit := func(w http.ResponseWriter, _ *http.Request) {
		legitHits.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"success":true,"status":"success","results":[]}`)
	}
	legitPlain := httptest.NewServer(http.HandlerFunc(legit))
	defer legitPlain.Close()
	legitTLS := httptest.NewTLSServer(http.HandlerFunc(legit))
	defer legitTLS.Close()

	redirect := func(extra map[string]any) map[string]any {
		m := map[string]any{
			"endpoint": attacker.URL, "api_base": attacker.URL, "base_url": attacker.URL,
			"host": strings.TrimPrefix(attackerTLS.URL, "https://"),
		}
		for k, v := range extra {
			m[k] = v
		}
		return m
	}
	legitHost := strings.TrimPrefix(legitTLS.URL, "https://")

	cases := []struct {
		typ    string
		config map[string]any
		creds  map[string]any
	}{
		{"bigip", redirect(map[string]any{"partition": "Common"}), map[string]any{"host": legitHost, "username": "u", "password": "p"}},
		{"fortigate", redirect(nil), map[string]any{"host": legitHost, "api_token": "t"}},
		{"webhook", redirect(map[string]any{"url": legitPlain.URL}), map[string]any{"token": "t"}},
		{"dummy", redirect(nil), map[string]any{}},
	}
	cert := &provider.CertificateData{ID: "c1", CommonName: "example.com", CertificatePEM: "C", PrivateKeyPEM: "K"}
	for _, c := range cases {
		t.Run(c.typ, func(t *testing.T) {
			p, err := provider.Get(c.typ)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, _ = p.Deploy(ctx, cert, c.config, c.creds, nil)
			_, _ = p.Verify(ctx, cert, c.config, c.creds)
			_, _ = p.Rollback(ctx, cert, c.config, c.creds)
			_ = p.ValidateCredentials(ctx, c.creds, c.config)
			if n := attackerHits.Load(); n != 0 {
				t.Fatalf("%s: stored config redirected %d request(s) to the attacker", c.typ, n)
			}
		})
	}
	if legitHits.Load() == 0 {
		t.Fatal("no provider reached its legitimate destination; the test proves nothing")
	}
}
