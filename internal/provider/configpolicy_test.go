package provider

import (
	"strings"
	"testing"
)

func TestCheckConfigRedirectKeys(t *testing.T) {
	for _, k := range RedirectKeys {
		fe := CheckConfig(map[string]any{"zone_id": "z", k: "https://attacker.example"})
		if fe == nil || fe.Field != "config."+k {
			t.Fatalf("%s: got %v", k, fe)
		}
		if strings.Contains(fe.Error(), "attacker") {
			t.Fatalf("error echoes the value: %v", fe)
		}
	}
	if fe := CheckConfig(map[string]any{"zone_id": "z", "url": "https://hook.example"}); fe != nil {
		t.Fatalf("plain config refused: %v", fe)
	}
	if fe := CheckConfig(nil); fe != nil {
		t.Fatalf("nil config refused: %v", fe)
	}
}

func TestAuthHeaders(t *testing.T) {
	bad := []string{"Authorization", "authorization", "Proxy-Authorization", "Cookie", "X-Api-Key", "X-API-KEY",
		"X-Auth-Token", "X-Webhook-Secret", "Api-Key", "X-Access-Token", "X-Password", "X-Credential", " Authorization ",
		"X-Jwt", "X-Session-Id", "X-Bearer", "X-Hub-Signature"}
	for _, h := range bad {
		if !IsAuthHeader(h) {
			t.Errorf("%q not detected", h)
		}
		fe := CheckConfig(map[string]any{"headers": map[string]any{h: "v", "X-Source": "ok"}})
		if fe == nil || fe.Field != "config.headers."+h {
			t.Errorf("%q: got %v", h, fe)
		}
	}
	for _, h := range []string{"X-Request-Source", "Accept", "X-Tenant", "Content-Language"} {
		if IsAuthHeader(h) {
			t.Errorf("%q wrongly detected", h)
		}
	}
	// headers of another shape are not interpreted.
	if got := AuthHeaders(map[string]any{"headers": "Authorization: x"}); got != nil {
		t.Fatalf("non-map headers: %v", got)
	}
	got := AuthHeaders(map[string]any{"headers": map[string]any{"X-Api-Key": "a", "Authorization": "b", "X-Ok": "c"}})
	if len(got) != 2 || got[0] != "Authorization" || got[1] != "X-Api-Key" {
		t.Fatalf("AuthHeaders = %v", got)
	}
}

func TestIgnoredAndStrip(t *testing.T) {
	cfg := map[string]any{"endpoint": "e", "api_base": "a", "zone_id": "z"}
	if got := IgnoredKeys(cfg); len(got) != 2 || got[0] != "api_base" || got[1] != "endpoint" {
		t.Fatalf("IgnoredKeys = %v", got)
	}
	s := StripRedirectKeys(cfg)
	if len(s) != 1 || s["zone_id"] != "z" {
		t.Fatalf("Strip = %v", s)
	}
	if cfg["endpoint"] != "e" {
		t.Fatal("Strip must not mutate its input")
	}
	if IgnoredKeys(map[string]any{"zone_id": "z"}) != nil {
		t.Fatal("no ignored keys expected")
	}
}

func TestValidateConfigDelegates(t *testing.T) {
	reset()
	defer reset()
	Register(validatingProvider{})
	if err := ValidateConfig("validating", map[string]any{"bad": true}); err == nil {
		t.Fatal("provider ConfigValidator not called")
	}
	if err := ValidateConfig("validating", map[string]any{"endpoint": "x"}); err == nil {
		t.Fatal("policy not applied before the provider check")
	}
	if err := ValidateConfig("validating", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateConfig("unknown", map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if len(clip(string(make([]byte, 100)))) != 64 {
		t.Fatal("clip")
	}
}

type validatingProvider struct{ fake }

func (validatingProvider) Capabilities() Capabilities { return Capabilities{Type: "validating"} }
func (validatingProvider) ValidateConfig(c map[string]any) error {
	if c["bad"] != nil {
		return &FieldError{Field: "config.bad", Msg: "bad"}
	}
	return nil
}

func TestDestinationChange(t *testing.T) {
	base := map[string]any{"url": "https://hook.example/a"}
	same := []map[string]any{
		{"url": "https://hook.example/b?x=1"},
		{"url": "https://HOOK.example:443/a"},
		{"url": "https://hook.example/a", "verify_url": "https://hook.example/v", "rollback_url": ""},
	}
	for _, n := range same {
		if k, ch := DestinationChange(base, n); ch {
			t.Errorf("%v: reported change of %s", n, k)
		}
	}
	diff := map[string]map[string]any{
		"url":          {"url": "https://other.example/a"},
		"verify_url":   {"url": "https://hook.example/a", "verify_url": "https://other.example/v"},
		"rollback_url": {"url": "https://hook.example/a", "rollback_url": "http://hook.example/r"},
	}
	for want, n := range diff {
		if k, ch := DestinationChange(base, n); !ch || k != want {
			t.Errorf("%v: got %q %v, want %q", n, k, ch, want)
		}
	}
	if k, ch := DestinationChange(base, map[string]any{"url": "https://u:p@hook.example/a"}); !ch || k != "url" {
		t.Error("user info change not detected")
	}
	if k, ch := DestinationChange(base, map[string]any{}); !ch || k != "url" {
		t.Error("removal not detected")
	}
}

func TestCheckDestinationURL(t *testing.T) {
	for _, v := range []any{"https://h.example/x", "http://10.0.0.1:8080/hook"} {
		if fe := CheckDestinationURL("url", v); fe != nil {
			t.Errorf("%v refused: %v", v, fe)
		}
	}
	if fe := CheckDestinationURL("verify_url", ""); fe != nil {
		t.Errorf("empty optional url refused: %v", fe)
	}
	for _, v := range []any{"", "ftp://h.example", "/rel", "https://", "https://u:p@h.example", 42, "://bad"} {
		if fe := CheckDestinationURL("url", v); fe == nil || fe.Field != "config.url" {
			t.Errorf("%v accepted: %v", v, fe)
		}
	}
	ov := map[string]any{"url": "x", "rollback_url": "y", "timeout_seconds": 3}
	if got := OverrideDestinationKeys(ov); len(got) != 2 || got[0] != "rollback_url" || got[1] != "url" {
		t.Fatalf("OverrideDestinationKeys = %v", got)
	}
}

// TestCredentialDestinationChange (T110): the credential host names where
// the secret credentials go; case and surrounding spaces are no change.
func TestCredentialDestinationChange(t *testing.T) {
	stored := map[string]any{"host": "lb.example.com", "password": "p"}
	if k, ch := CredentialDestinationChange(stored, map[string]any{"host": " LB.example.com", "password": "q"}); ch {
		t.Fatalf("same host changed (%s)", k)
	}
	if k, ch := CredentialDestinationChange(stored, map[string]any{"host": "evil.example"}); !ch || k != "host" {
		t.Fatalf("new host: %s %v", k, ch)
	}
	if _, ch := CredentialDestinationChange(map[string]any{"token": "t"}, map[string]any{"token": "u"}); ch {
		t.Fatal("no host is no change")
	}
}
