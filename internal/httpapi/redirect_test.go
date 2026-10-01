package httpapi

import (
	"strings"
	"testing"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/awsacm"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/cloudflare"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/webhook"
)

const attackerURL = "https://attacker.example/collect"

// refused asserts a 422 validation_failed naming field, without echoing the
// submitted attacker value.
func refused(t *testing.T, f *apiFixture, method, path, body, field string) {
	t.Helper()
	w := f.req(t, method, path, "admin", body)
	if w.Code != 422 {
		t.Fatalf("%s %s: status = %d, want 422 (%s)", method, path, w.Code, w.Body.String())
	}
	m := decode(t, w)
	d, _ := m["detail"].(map[string]any)
	if m["reason"] != "validation_failed" || d["field"] != field {
		t.Fatalf("%s %s: body = %s, want validation_failed naming %q", method, path, w.Body.String(), field)
	}
	if strings.Contains(w.Body.String(), "attacker.example") || strings.Contains(w.Body.String(), "s3cr3t") {
		t.Fatalf("%s %s: response echoes the submitted value: %s", method, path, w.Body.String())
	}
}

// TestRedirectKeysRefusedOnSave: endpoint/api_base can no longer be stored on
// create, update, validate or a target override.
func TestRedirectKeysRefusedOnSave(t *testing.T) {
	f := newAPI(t)

	refused(t, f, "POST", p+"/configurations",
		`{"name":"acm","provider_type":"aws_acm","config":{"region":"us-east-1","endpoint":"`+attackerURL+`"}}`, "config.endpoint")
	refused(t, f, "POST", p+"/configurations",
		`{"name":"cf","provider_type":"cloudflare","config":{"zone_id":"z","api_base":"`+attackerURL+`"}}`, "config.api_base")
	refused(t, f, "POST", p+"/configurations",
		`{"name":"acm2","provider_type":"aws_acm","config":{"region":"attacker.example/x?"}}`, "config.region")
	refused(t, f, "POST", p+"/configurations/validate",
		`{"provider_type":"cloudflare","config":{"zone_id":"z","api_base":"`+attackerURL+`"},"credentials":{"api_token":"s3cr3t"}}`, "config.api_base")

	// Update of an existing configuration.
	w := f.req(t, "POST", p+"/configurations", "admin", `{"name":"cf-ok","provider_type":"cloudflare","config":{"zone_id":"z"},"credentials":{"api_token":"s3cr3t"}}`)
	if w.Code != 201 && w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	cfgID := decode(t, w)["id"].(string)
	refused(t, f, "PUT", p+"/configurations/"+cfgID,
		`{"name":"cf-ok","config":{"zone_id":"z","api_base":"`+attackerURL+`"}}`, "config.api_base")

	// Target override.
	tid := decode(t, f.req(t, "POST", p+"/targets", "admin", `{"name":"t","certificate_filters":[]}`))["id"].(string)
	refused(t, f, "POST", p+"/targets/"+tid+"/configurations",
		`{"configuration_ids":["`+cfgID+`"],"config_overrides":{"`+cfgID+`":{"api_base":"`+attackerURL+`"}}}`, "config_overrides.config.api_base")

	// The stored configuration is unchanged.
	got := decode(t, f.req(t, "GET", p+"/configurations/"+cfgID, "admin", ""))
	if cfg, _ := got["config"].(map[string]any); cfg["api_base"] != nil {
		t.Fatalf("refused update was stored: %v", got)
	}
}

// TestWebhookCredentialHeadersRefused: credential headers belong in the sealed
// webhook credentials, not in the plaintext config.
func TestWebhookCredentialHeadersRefused(t *testing.T) {
	f := newAPI(t)
	for _, h := range []string{"Authorization", "proxy-authorization", "Cookie", "X-Api-Key", "X-Auth-Token", "X-Webhook-Secret", "X-Signing-Key"} {
		refused(t, f, "POST", p+"/configurations",
			`{"name":"wh","provider_type":"webhook","config":{"url":"https://hook.example","headers":{"`+h+`":"s3cr3t"}}}`, "config.headers."+h)
	}
	w := f.req(t, "POST", p+"/configurations", "admin",
		`{"name":"wh-ok","provider_type":"webhook","config":{"url":"https://hook.example","headers":{"X-Request-Source":"tangra"}}}`)
	if w.Code != 201 && w.Code != 200 {
		t.Fatalf("plain custom header refused: %d %s", w.Code, w.Body.String())
	}
}
