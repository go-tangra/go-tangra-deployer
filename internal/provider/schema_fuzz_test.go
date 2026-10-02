package provider

import (
	"encoding/json"
	"strings"
	"testing"
)

// FuzzValidateInput (T079, SR-012): the validator never panics and no error
// path or code ever contains a submitted string value.
func FuzzValidateInput(f *testing.F) {
	f.Add(`{"zone_id":"secret-zone-value","region":"eu"}`, `{"user":"u","token":"hunter2-secret"}`, `{"zone_id":"x"}`)
	f.Add(`{"tags":["Bearer-token-value"],"headers":{"X-Ok":"Authorization: secretvalue"}}`, `{}`, `{"tags":["zz"]}`)
	f.Add(`{"timeout_seconds":"not-a-number-value","mode":"another-value"}`, `{"token":["list"]}`, `{"region":"overridden"}`)
	v := loadVectors(f)
	caps := []Capabilities{v.Providers["zone"], v.Providers["hosts"], v.Providers["mixed_group"]}
	f.Fuzz(func(t *testing.T, cfgJSON, credJSON, ovJSON string) {
		var cfg, creds, ov map[string]any
		_ = json.Unmarshal([]byte(cfgJSON), &cfg)
		_ = json.Unmarshal([]byte(credJSON), &creds)
		_ = json.Unmarshal([]byte(ovJSON), &ov)
		values := collectStrings(cfg, creds, ov)
		for _, c := range caps {
			for _, mode := range []Mode{ModeConfiguration, ModeEffective} {
				errs, _ := ValidateInput(c, cfg, creds, mode)
				assertNoEcho(t, errs, values)
			}
			assertNoEcho(t, ValidateOverride(c, cfg, ov), values)
		}
	})
}

// collectStrings returns every string value (not map key) longer than 3
// characters found in the inputs.
func collectStrings(ms ...map[string]any) []string {
	var out []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			if len(x) > 3 {
				out = append(out, x)
			}
		case []any:
			for _, it := range x {
				walk(it)
			}
		case map[string]any:
			for _, it := range x {
				walk(it)
			}
		}
	}
	for _, m := range ms {
		walk(m)
	}
	return out
}

func assertNoEcho(t *testing.T, errs FieldErrors, values []string) {
	t.Helper()
	text := errs.Error()
	for _, v := range values {
		// The fixed vocabulary itself may coincide with a short value.
		if isVocabulary(v) {
			continue
		}
		if strings.Contains(text, v) {
			t.Fatalf("validation output echoes a submitted value %q: %s", v, text)
		}
	}
}

func isVocabulary(v string) bool {
	const vocab = "validation failed: config. credentials. config_overrides. required one_of_required wrong_type pattern too_long out_of_range too_many_items not_in_options invalid_url unknown_field forbidden_header not_overridable zone_id region note endpoint_url timeout_seconds verify mode tags headers metadata user token extra host_ids host_tags cert_name 1..300 :2 :8 :10 :16 :1024"
	return strings.Contains(vocab, v)
}
