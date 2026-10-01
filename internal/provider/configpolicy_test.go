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
		"X-Auth-Token", "X-Webhook-Secret", "Api-Key", "X-Access-Token", "X-Password", "X-Credential", " Authorization "}
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
