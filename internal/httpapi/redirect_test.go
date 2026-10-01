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
		`{"name":"cf","provider_type":"cloudflare","config":{"zone_id":"0123456789abcdef0123456789abcdef","api_base":"`+attackerURL+`"}}`, "config.api_base")
	refused(t, f, "POST", p+"/configurations",
		`{"name":"acm2","provider_type":"aws_acm","config":{"region":"attacker.example/x?"}}`, "config.region")
	refused(t, f, "POST", p+"/configurations/validate",
		`{"provider_type":"cloudflare","config":{"zone_id":"0123456789abcdef0123456789abcdef","api_base":"`+attackerURL+`"},"credentials":{"api_token":"s3cr3t"}}`, "config.api_base")

	// Update of an existing configuration.
	w := f.req(t, "POST", p+"/configurations", "admin", `{"name":"cf-ok","provider_type":"cloudflare","config":{"zone_id":"0123456789abcdef0123456789abcdef"},"credentials":{"api_token":"s3cr3t"}}`)
	if w.Code != 201 && w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	cfgID := decode(t, w)["id"].(string)
	refused(t, f, "PUT", p+"/configurations/"+cfgID,
		`{"name":"cf-ok","config":{"zone_id":"0123456789abcdef0123456789abcdef","api_base":"`+attackerURL+`"}}`, "config.api_base")

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

func mustCreate(t *testing.T, f *apiFixture, body string) string {
	t.Helper()
	w := f.req(t, "POST", p+"/configurations", "admin", body)
	if w.Code != 201 && w.Code != 200 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	return decode(t, w)["id"].(string)
}

func ok(t *testing.T, f *apiFixture, method, path, body string) {
	t.Helper()
	w := f.req(t, method, path, "admin", body)
	if w.Code < 200 || w.Code > 299 {
		t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
	}
}

// TestWebhookDestinationChangeNeedsCredentials: moving a webhook that holds
// sealed credentials to another origin requires re-entering them in the same
// request; otherwise the stored token (and the private key) would follow the
// new URL.
func TestWebhookDestinationChangeNeedsCredentials(t *testing.T) {
	f := newAPI(t)
	id := mustCreate(t, f, `{"name":"wh","provider_type":"webhook","config":{"url":"https://hook.example/a"},"credentials":{"token":"s3cr3t"}}`)
	path := p + "/configurations/" + id

	refused(t, f, "PUT", path, `{"name":"wh","config":{"url":"https://attacker.example/a"}}`, "config.url")
	refused(t, f, "PUT", path, `{"name":"wh","config":{"url":"http://hook.example/a"}}`, "config.url")       // scheme
	refused(t, f, "PUT", path, `{"name":"wh","config":{"url":"https://hook.example:8443/a"}}`, "config.url") // port
	refused(t, f, "PUT", path, `{"name":"wh","config":{"url":"https://hook.example/a","verify_url":"https://attacker.example/v"}}`, "config.verify_url")
	refused(t, f, "PUT", path, `{"name":"wh","config":{"url":"https://hook.example/a","rollback_url":"https://attacker.example/r"}}`, "config.rollback_url")

	// Same origin (path change, explicit default port) needs no re-entry.
	ok(t, f, "PUT", path, `{"name":"wh","config":{"url":"https://HOOK.example:443/b","verify_url":"https://hook.example/v"}}`)
	// A new origin with the credentials re-entered is accepted.
	ok(t, f, "PUT", path, `{"name":"wh","config":{"url":"https://new.example/a"},"credentials":{"token":"n3w"}}`)

	// Without sealed credentials there is nothing to protect.
	open := mustCreate(t, f, `{"name":"wh-open","provider_type":"webhook","config":{"url":"https://hook.example/a"}}`)
	ok(t, f, "PUT", p+"/configurations/"+open, `{"name":"wh-open","config":{"url":"http://other.example/a"}}`)

	// URL shape: http(s) only, no user info; http stays allowed.
	refused(t, f, "POST", p+"/configurations", `{"name":"x1","provider_type":"webhook","config":{"url":"ftp://attacker.example/a"}}`, "config.url")
	refused(t, f, "POST", p+"/configurations", `{"name":"x2","provider_type":"webhook","config":{"url":"https://u:s3cr3t@attacker.example/a"}}`, "config.url")
	refused(t, f, "POST", p+"/configurations", `{"name":"x3","provider_type":"webhook","config":{"url":"/relative"}}`, "config.url")
	mustCreate(t, f, `{"name":"x4","provider_type":"webhook","config":{"url":"http://intranet.example/hook"},"credentials":{"token":"t"}}`)
}

// TestOverrideCannotMoveDestination: a target override may not set a
// destination key for a configuration that holds sealed credentials.
func TestOverrideCannotMoveDestination(t *testing.T) {
	f := newAPI(t)
	sealedID := mustCreate(t, f, `{"name":"wh","provider_type":"webhook","config":{"url":"https://hook.example/a"},"credentials":{"token":"s3cr3t"}}`)
	openID := mustCreate(t, f, `{"name":"wh-open","provider_type":"webhook","config":{"url":"https://hook.example/a"}}`)
	tid := decode(t, f.req(t, "POST", p+"/targets", "admin", `{"name":"t","certificate_filters":[]}`))["id"].(string)
	path := p + "/targets/" + tid + "/configurations"

	for _, k := range []string{"url", "verify_url", "rollback_url"} {
		refused(t, f, "POST", path, `{"configuration_ids":["`+sealedID+`"],"config_overrides":{"`+sealedID+`":{"`+k+`":"https://attacker.example/x"}}}`, "config_overrides.config."+k)
	}
	// Unknown configuration: fail closed.
	refused(t, f, "POST", path, `{"configuration_ids":["`+sealedID+`"],"config_overrides":{"`+absentID+`":{"url":"https://attacker.example/x"}}}`, "config_overrides.config.url")
	// Non-destination overrides still work; so does a destination override for a configuration without credentials.
	ok(t, f, "POST", path, `{"configuration_ids":["`+sealedID+`"],"config_overrides":{"`+sealedID+`":{"timeout_seconds":30}}}`)
	ok(t, f, "POST", path, `{"configuration_ids":["`+openID+`"],"config_overrides":{"`+openID+`":{"url":"https://other.example/x"}}}`)
}

// TestCloudflareZoneIDValidated: zone_id is interpolated into the request
// path, so only a 32-hex id is accepted on save.
func TestCloudflareZoneIDValidated(t *testing.T) {
	f := newAPI(t)
	for _, z := range []string{"z", "../../accounts", "0123456789abcdef0123456789abcdeg", "0123456789abcdef0123456789abcdef/x"} {
		refused(t, f, "POST", p+"/configurations", `{"name":"cf","provider_type":"cloudflare","config":{"zone_id":"`+z+`"}}`, "config.zone_id")
	}
	mustCreate(t, f, `{"name":"cf","provider_type":"cloudflare","config":{"zone_id":"0123456789ABCDEF0123456789abcdef"}}`)
}
