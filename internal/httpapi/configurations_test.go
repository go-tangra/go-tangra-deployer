package httpapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/bigip"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/cloudflare"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/fortigate"
)

// Submitted test secrets: none may appear in any response of this suite (SC-008).
var drawerSecrets = []string{"cf-token-s3cr3t", "bigip-pass-s3cr3t", "fg-token-s3cr3t", "n3w-bigip-pass"}

type secretScan struct{ bodies []string }

func (s *secretScan) req(t *testing.T, f *apiFixture, method, path, body string) (int, map[string]any) {
	t.Helper()
	w := f.req(t, method, path, "admin", body)
	s.bodies = append(s.bodies, w.Body.String())
	var m map[string]any
	if w.Body.Len() > 0 {
		m = decode(t, w)
	}
	return w.Code, m
}

func (s *secretScan) check(t *testing.T) {
	t.Helper()
	for _, b := range s.bodies {
		for _, sec := range drawerSecrets {
			if strings.Contains(b, sec) {
				t.Fatalf("response echoes a submitted secret %q: %s", sec, b)
			}
		}
	}
}

func fieldsOfBody(t *testing.T, m map[string]any) map[string]any {
	t.Helper()
	if m["reason"] != "validation_failed" {
		t.Fatalf("reason = %v (%v)", m["reason"], m)
	}
	d, _ := m["detail"].(map[string]any)
	f, _ := d["fields"].(map[string]any)
	if f == nil {
		t.Fatalf("no detail.fields: %v", m)
	}
	return f
}

// T083: 422 bodies name each field for create, update, validate and attach;
// required_by_targets lists the targets; reads never carry a secret.
func TestConfigurationDrawerAPI(t *testing.T) {
	f := newAPI(t)
	s := &secretScan{}
	defer s.check(t)

	// Catalogue with descriptors.
	code, cat := s.req(t, f, "GET", p+"/providers", "")
	if code != 200 || !strings.Contains(s.bodies[len(s.bodies)-1], `"overridable":true`) || cat["items"] == nil {
		t.Fatalf("providers: %d", code)
	}

	// Create: every missing/invalid field named.
	code, m := s.req(t, f, "POST", p+"/configurations", `{"name":"lb","provider_type":"bigip","config":{"partition":"bad partition!"},"credentials":{"password":"bigip-pass-s3cr3t"}}`)
	fl := fieldsOfBody(t, m)
	if code != 422 || fl["config.partition"] != "pattern" || fl["credentials.host"] != "required" || fl["credentials.username"] != "required" {
		t.Fatalf("create: %d %v", code, fl)
	}
	// A complete BIG-IP configuration; the read shows names and public values.
	code, m = s.req(t, f, "POST", p+"/configurations", `{"name":"lb","provider_type":"bigip","config":{"partition":"Common"},"credentials":{"host":"bigip.example.com","username":"deployer","password":"bigip-pass-s3cr3t"}}`)
	if code != 201 {
		t.Fatalf("create: %d %v", code, m)
	}
	lb := m["id"].(string)
	_, m = s.req(t, f, "GET", p+"/configurations/"+lb, "")
	if pub, _ := m["credentials_public"].(map[string]any); pub["host"] != "bigip.example.com" || pub["password"] != nil {
		t.Fatalf("credentials_public = %v", m["credentials_public"])
	}
	if set, _ := m["credentials_set"].([]any); len(set) != 3 {
		t.Fatalf("credentials_set = %v", m["credentials_set"])
	}
	// Update with a blank password keeps it; a new one replaces only it.
	if code, m = s.req(t, f, "PUT", p+"/configurations/"+lb, `{"name":"lb","config":{"partition":"Edge"},"credentials":{"password":""}}`); code != 200 {
		t.Fatalf("update: %d %v", code, m)
	}
	if code, m = s.req(t, f, "PUT", p+"/configurations/"+lb, `{"name":"lb","credentials":{"password":"n3w-bigip-pass"},"clear_credentials":[]}`); code != 200 {
		t.Fatalf("update: %d %v", code, m)
	}
	code, m = s.req(t, f, "PUT", p+"/configurations/"+lb, `{"name":"lb","clear_credentials":["password"]}`)
	if fl = fieldsOfBody(t, m); code != 422 || fl["credentials.password"] != "required" {
		t.Fatalf("clear required: %d %v", code, fl)
	}
	code, m = s.req(t, f, "PUT", p+"/configurations/"+lb, `{"name":"lb","provider_type":"fortigate"}`)
	if fl = fieldsOfBody(t, m); code != 422 || fl["provider_type"] == nil {
		t.Fatalf("provider change: %d %v", code, fl)
	}

	// Validate: descriptor errors first; partial for target-supplied fields.
	code, m = s.req(t, f, "POST", p+"/configurations/validate", `{"provider_type":"cloudflare","config":{"zone_id":"x"},"credentials":{"api_token":"cf-token-s3cr3t"}}`)
	if fl = fieldsOfBody(t, m); code != 422 || fl["config.zone_id"] != "pattern" {
		t.Fatalf("validate: %d %v", code, fl)
	}
	code, m = s.req(t, f, "POST", p+"/configurations/validate", `{"provider_type":"cloudflare","config":{},"credentials":{"api_token":"cf-token-s3cr3t"}}`)
	if code != 200 || m["checked"] != "partial" || m["deferred"].([]any)[0] != "zone_id" {
		t.Fatalf("validate partial: %d %v", code, m)
	}
	// Validate against a stored configuration of another provider type.
	code, m = s.req(t, f, "POST", p+"/configurations/validate", `{"provider_type":"cloudflare","configuration_id":"`+lb+`","config":{}}`)
	if fl = fieldsOfBody(t, m); code != 422 || fl["provider_type"] == nil {
		t.Fatalf("validate mismatch: %d %v", code, fl)
	}
	if code, _ = s.req(t, f, "POST", p+"/configurations/validate", `{"provider_type":"cloudflare","configuration_id":"`+absentID+`","config":{}}`); code != 404 {
		t.Fatalf("validate unknown id: %d", code)
	}

	// A shared Cloudflare configuration leaves zone_id to the targets.
	code, m = s.req(t, f, "POST", p+"/configurations", `{"name":"cf","provider_type":"cloudflare","config":{},"credentials":{"api_token":"cf-token-s3cr3t"}}`)
	if code != 201 || m["target_supplied"].([]any)[0] != "zone_id" {
		t.Fatalf("target-supplied create: %d %v", code, m)
	}
	cf := m["id"].(string)
	_, m = s.req(t, f, "POST", p+"/targets", `{"name":"edge-zone-a","certificate_filters":[]}`)
	tid := m["id"].(string)
	code, m = s.req(t, f, "POST", p+"/targets/"+tid+"/configurations", `{"configuration_ids":["`+cf+`"]}`)
	fl = fieldsOfBody(t, m)
	if d := m["detail"].(map[string]any); code != 422 || fl["config_overrides.zone_id"] != "required" || d["configuration_id"] != cf {
		t.Fatalf("attach without zone: %d %v", code, m)
	}
	code, m = s.req(t, f, "POST", p+"/targets/"+tid+"/configurations", `{"configuration_ids":["`+cf+`"],"overrides":{"`+cf+`":{"api_token":"cf-token-s3cr3t"}}}`)
	if fl = fieldsOfBody(t, m); code != 422 || fl["config_overrides.api_token"] != "not_overridable" {
		t.Fatalf("attach credential override: %d %v", code, fl)
	}
	if code, m = s.req(t, f, "POST", p+"/targets/"+tid+"/configurations", `{"configuration_ids":["`+cf+`"],"overrides":{"`+cf+`":{"zone_id":"023e105f4ecef8ad9ca31a8372d0c353"}}}`); code != 200 {
		t.Fatalf("attach: %d %v", code, m)
	}
	_, m = s.req(t, f, "GET", p+"/targets/"+tid, "")
	if ov, _ := m["config_overrides"].(map[string]any); ov[cf] == nil {
		t.Fatalf("target view overrides: %v", m)
	}

	// A configuration value a target relies on cannot be cleared.
	code, m = s.req(t, f, "POST", p+"/configurations", `{"name":"cf2","provider_type":"cloudflare","config":{"zone_id":"023e105f4ecef8ad9ca31a8372d0c353"},"credentials":{"api_token":"cf-token-s3cr3t"}}`)
	if code != 201 {
		t.Fatalf("create cf2: %d %v", code, m)
	}
	cf2 := m["id"].(string)
	if code, m = s.req(t, f, "POST", p+"/targets/"+tid+"/configurations", `{"configuration_ids":["`+cf2+`"]}`); code != 200 {
		t.Fatalf("attach cf2: %d %v", code, m)
	}
	code, m = s.req(t, f, "PUT", p+"/configurations/"+cf2, `{"name":"cf2","config":{}}`)
	fl = fieldsOfBody(t, m)
	tg, _ := m["detail"].(map[string]any)["targets"].([]any)
	if code != 422 || fl["config.zone_id"] != "required_by_targets" || len(tg) != 1 || tg[0].(map[string]any)["name"] != "edge-zone-a" {
		t.Fatalf("required_by_targets: %d %v", code, m)
	}
}

// credentials_rejected: a provider refusal is a fixed reason without detail
// (the provider's text may carry the submitted values).
func TestValidateCredentialsRejected(t *testing.T) {
	f := newAPI(t)
	s := &secretScan{}
	defer s.check(t)
	// A webhook endpoint answering 401 to the probe.
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }))
	defer hook.Close()
	code, m := s.req(t, f, "POST", p+"/configurations/validate", `{"provider_type":"webhook","config":{"url":"`+hook.URL+`"},"credentials":{"token":"fg-token-s3cr3t"}}`)
	if code != 422 || m["reason"] != "credentials_rejected" || m["detail"] != nil {
		t.Fatalf("validate: %d %v", code, m)
	}
}

// SR-013: the catalogue needs configurations:read, validate needs
// configurations:manage (enforced by the gateway from x-freya-permission).
func TestDrawerRoutePermissions(t *testing.T) {
	man, err := deployermanifest.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"GET /api/deployer/v1/providers":                           "configurations:read",
		"POST /api/deployer/v1/configurations/validate":            "configurations:manage",
		"POST /api/deployer/v1/configurations":                     "configurations:manage",
		"PUT /api/deployer/v1/configurations/{id}":                 "configurations:manage",
		"POST /api/deployer/v1/targets/{id}/configurations":        "targets:manage",
		"GET /api/deployer/v1/configurations/{id}":                 "configurations:read",
		"POST /api/deployer/v1/targets/{id}/configurations/remove": "targets:manage",
	}
	got := map[string]string{}
	for _, r := range man.Routes {
		got[r.Method+" "+r.Path] = r.Permission
	}
	for route, perm := range want {
		if got[route] != perm {
			t.Errorf("%s permission = %q, want %q", route, got[route], perm)
		}
	}
}
