package all_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

var update = flag.Bool("update", false, "rewrite testdata/capabilities.golden.json")

// catalogueJSON is the provider catalogue as served by GET /providers.
func catalogueJSON(t *testing.T) []byte {
	t.Helper()
	raw, err := json.MarshalIndent(map[string]any{"items": provider.List()}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, '\n')
}

// TestCatalogueGolden (T080): the declarations equal contracts/deployer-
// config-ui.md §7 — pinned as a golden file the UI and reviewers can read.
// Regenerate with: go test ./internal/providers/all -run TestCatalogueGolden -update
func TestCatalogueGolden(t *testing.T) {
	got := catalogueJSON(t)
	const golden = "testdata/capabilities.golden.json"
	if *update {
		if err := os.WriteFile(golden, got, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(golden)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("provider catalogue differs from %s (run with -update after reviewing the contract):\n%s", golden, got)
	}
}

func fieldsOf(c provider.Capabilities, pred func(provider.Field) bool) []string {
	var out []string
	for _, f := range append(append([]provider.Field{}, c.ConfigFields...), c.CredentialFields...) {
		if pred(f) {
			out = append(out, f.Key)
		}
	}
	sort.Strings(out)
	if out == nil {
		out = []string{}
	}
	return out
}

// TestRequiredParityWithV3 (FR-026): the required fields per provider at least
// match v3.
func TestRequiredParityWithV3(t *testing.T) {
	want := map[string][]string{
		"aws_acm":    {"access_key_id", "region", "secret_access_key"},
		"bigip":      {"host", "partition", "password", "username"},
		"cloudflare": {"api_token", "zone_id"},
		"fortigate":  {"api_token", "host", "vdom"},
		"webhook":    {"url"},
		"dummy":      {},
		// v3 "Tangra client": at least one of host_ids/host_tags (one_of_required).
		"inventory-agent": {},
	}
	for typ, req := range want {
		c, ok := provider.Info(typ)
		if !ok {
			t.Fatalf("%s not registered", typ)
		}
		if got := fieldsOf(c, func(f provider.Field) bool { return f.Required }); !reflect.DeepEqual(got, req) {
			t.Errorf("%s required = %v, want %v", typ, got, req)
		}
	}
}

// TestOverridableSet (SR-015, research D25): exactly the contract's O column;
// never a credential, a URL, the TLS switch or the custom headers.
func TestOverridableSet(t *testing.T) {
	want := map[string][]string{
		"aws_acm":         {"certificate_arn", "region"},
		"bigip":           {"partition"},
		"cloudflare":      {"zone_id"},
		"dummy":           {"fail"},
		"fortigate":       {"import_scope", "vdom"},
		"webhook":         {"metadata", "timeout_seconds"},
		"inventory-agent": {"cert_name", "host_ids", "host_tags", "key_policy", "require_all_success", "wait_seconds"},
	}
	for typ, ov := range want {
		c, _ := provider.Info(typ)
		if got := fieldsOf(c, func(f provider.Field) bool { return f.Overridable }); !reflect.DeepEqual(got, ov) {
			t.Errorf("%s overridable = %v, want %v", typ, got, ov)
		}
		for _, f := range c.CredentialFields {
			if f.Overridable {
				t.Errorf("%s credential %s is overridable", typ, f.Key)
			}
		}
	}
	for _, c := range provider.List() {
		for _, f := range c.ConfigFields {
			if f.Overridable && (f.Type == provider.TypeURL || f.Key == "skip_tls_verify" || f.Key == "headers") {
				t.Errorf("%s.%s decides where credentials go and must not be overridable", c.Type, f.Key)
			}
		}
	}
}

// TestEveryFieldDescribed: every shipped field has a type, a group and help,
// so the drawer can render it without hard-coded knowledge (SC-007).
func TestEveryFieldDescribed(t *testing.T) {
	for _, c := range provider.List() {
		if c.Description == "" || c.SchemaVersion != 1 {
			t.Errorf("%s: description/schema_version missing", c.Type)
		}
		for _, f := range append(append([]provider.Field{}, c.ConfigFields...), c.CredentialFields...) {
			if f.Type == "" || f.Group == "" || f.Help == "" {
				t.Errorf("%s.%s: type, group and help are required", c.Type, f.Key)
			}
		}
	}
}

// TestRedirectSettingsStayRefused (SR-014 regression guard for the hotfix
// fix/provider-endpoint-exfil): aws_acm "endpoint" and cloudflare "api_base"
// are undeclared, so the shared validator refuses them at save and in
// overrides; authentication header names stay refused in webhook headers.
func TestRedirectSettingsStayRefused(t *testing.T) {
	cases := []struct {
		typ, key string
		base     map[string]any
	}{
		{"aws_acm", "endpoint", map[string]any{"region": "eu-central-1"}},
		{"cloudflare", "api_base", map[string]any{"zone_id": "023e105f4ecef8ad9ca31a8372d0c353"}},
	}
	for _, c := range cases {
		caps, _ := provider.Info(c.typ)
		cfg := map[string]any{c.key: "https://attacker.example"}
		for k, v := range c.base {
			cfg[k] = v
		}
		errs, _ := provider.ValidateInput(caps, cfg, nil, provider.ModeConfiguration)
		if errs["config."+c.key] != provider.CodeUnknownField {
			t.Errorf("%s.%s at save: %v", c.typ, c.key, errs)
		}
		oerrs := provider.ValidateOverride(caps, c.base, map[string]any{c.key: "https://attacker.example"})
		if code := oerrs["config_overrides."+c.key]; code != provider.CodeUnknownField && code != provider.CodeNotOverridable {
			t.Errorf("%s.%s in override: %v", c.typ, c.key, oerrs)
		}
	}
	wh, _ := provider.Info("webhook")
	for _, h := range []string{"Authorization", "proxy-authorization", "Cookie", "X-API-Key", "X-Webhook-Secret"} {
		errs, _ := provider.ValidateInput(wh, map[string]any{"url": "https://hook.example", "headers": map[string]any{h: "v"}}, nil, provider.ModeConfiguration)
		if errs["config.headers"] != provider.CodeForbiddenHeader+":"+h {
			t.Errorf("header %s: %v", h, errs)
		}
	}
	// Destination-defining webhook settings cannot be overridden.
	for _, k := range []string{"url", "verify_url", "rollback_url", "skip_tls_verify", "headers", "token", "secret"} {
		errs := provider.ValidateOverride(wh, map[string]any{"url": "https://hook.example"}, map[string]any{k: "x"})
		if errs["config_overrides."+k] != provider.CodeNotOverridable {
			t.Errorf("webhook override %s: %v", k, errs)
		}
	}
}
