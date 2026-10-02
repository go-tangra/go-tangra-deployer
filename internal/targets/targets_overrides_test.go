package targets_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/cloudflare"
)

// hostsProvider mirrors inventory-agent: a one-of group that may be left to
// the targets, and a ConfigValidator for rules the descriptors cannot hold.
type hostsProvider struct{}

func init() { provider.Register(hostsProvider{}) }

func (hostsProvider) Deploy(context.Context, *provider.CertificateData, map[string]any, map[string]any, provider.ProgressFn) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (hostsProvider) Verify(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (hostsProvider) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, provider.ErrUnsupported
}
func (hostsProvider) ValidateCredentials(context.Context, map[string]any, map[string]any) error {
	return nil
}
func (hostsProvider) ValidateConfig(c map[string]any) error {
	if ids, _ := c["host_ids"].([]any); len(ids) > 0 && ids[0] == "not-a-uuid" {
		return &provider.FieldError{Field: "config.host_ids", Msg: "invalid_uuid"}
	}
	return nil
}
func (hostsProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Type: "test_hosts", DisplayName: "Hosts",
		ConfigFields: []provider.Field{
			{Key: "host_ids", Label: "Hosts", Type: provider.TypeHostSelector, Overridable: true, MaxItems: 3},
			{Key: "host_tags", Label: "Host tags", Type: provider.TypeStringList, Overridable: true},
		},
		OneOfRequired: [][]string{{"host_ids", "host_tags"}}}
}

func asTVE(t *testing.T, err error) *targets.ValidationError {
	t.Helper()
	var ve *targets.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *targets.ValidationError", err)
	}
	return ve
}

type overrideKit struct {
	ts       *targets.Service
	cs       *configs.Service
	m        *memstore.Mem
	targetID string
	cf       string // cloudflare configuration, zone_id target-supplied
	wh       string // webhook configuration
	hosts    string // test_hosts configuration, selection target-supplied
}

func newOverrideKit(t *testing.T) overrideKit {
	t.Helper()
	ts, cs, m := newServices(t)
	ctx := context.Background()
	subj := adminSubject()
	tgt, err := ts.Create(ctx, subj, targets.Input{Name: "edge"})
	if err != nil {
		t.Fatal(err)
	}
	cf, err := cs.Create(ctx, subj, configs.Input{Name: "cf-shared", ProviderType: "cloudflare", Credentials: map[string]any{"api_token": "cf-token-value"}})
	if err != nil || !reflect.DeepEqual(cf.TargetSupplied, []string{"zone_id"}) {
		t.Fatalf("shared cloudflare configuration: %+v, %v", cf, err)
	}
	hs, err := cs.Create(ctx, subj, configs.Input{Name: "hosts", ProviderType: "test_hosts"})
	if err != nil {
		t.Fatal(err)
	}
	return overrideKit{ts: ts, cs: cs, m: m, targetID: tgt.ID, cf: cf.ID, wh: makeConfig(t, cs, subj, "wh"), hosts: hs.ID}
}

func (k overrideKit) target(t *testing.T) store.DeploymentTarget {
	t.Helper()
	tg, err := k.m.GetTarget(context.Background(), adminSubject().TenantID, k.targetID)
	if err != nil {
		t.Fatal(err)
	}
	return tg
}

// T082: Attach validates every configuration before any write.
func TestAttachOverrideRules(t *testing.T) {
	k := newOverrideKit(t)
	ctx := context.Background()
	subj := adminSubject()
	const zone = "023e105f4ecef8ad9ca31a8372d0c353"

	cases := []struct {
		name  string
		ids   []string
		ov    map[string]map[string]any
		cid   string
		field string
		code  string
	}{
		{"undeclared key", []string{k.wh}, map[string]map[string]any{k.wh: {"nope": 1}}, k.wh, "config_overrides.nope", "unknown_field"},
		{"credential key", []string{k.wh}, map[string]map[string]any{k.wh: {"token": "leak-value"}}, k.wh, "config_overrides.token", "not_overridable"},
		{"webhook url", []string{k.wh}, map[string]map[string]any{k.wh: {"url": "https://attacker.example"}}, k.wh, "config_overrides.url", "not_overridable"},
		{"tls switch", []string{k.wh}, map[string]map[string]any{k.wh: {"skip_tls_verify": true}}, k.wh, "config_overrides.skip_tls_verify", "not_overridable"},
		{"headers", []string{k.wh}, map[string]map[string]any{k.wh: {"headers": map[string]any{"X-A": "b"}}}, k.wh, "config_overrides.headers", "not_overridable"},
		{"rule violation", []string{k.wh}, map[string]map[string]any{k.wh: {"timeout_seconds": 999}}, k.wh, "config_overrides.timeout_seconds", "out_of_range:1..300"},
		{"target-supplied missing", []string{k.cf}, nil, k.cf, "config_overrides.zone_id", "required"},
		{"target-supplied empty", []string{k.cf}, map[string]map[string]any{k.cf: {"zone_id": ""}}, k.cf, "config_overrides.zone_id", "required"},
		{"one-of group missing", []string{k.hosts}, map[string]map[string]any{k.hosts: {}}, k.hosts, "config_overrides.host_ids", "one_of_required:host_ids,host_tags"},
		{"provider validator on the merged config", []string{k.hosts}, map[string]map[string]any{k.hosts: {"host_ids": []any{"not-a-uuid"}}}, k.hosts, "config_overrides.host_ids", "invalid_uuid"},
		{"second configuration fails", []string{k.wh, k.cf}, map[string]map[string]any{k.wh: {"timeout_seconds": 30}}, k.cf, "config_overrides.zone_id", "required"},
	}
	for _, c := range cases {
		err := k.ts.Attach(ctx, subj, k.targetID, c.ids, c.ov)
		ve := asTVE(t, err)
		if ve.Fields[c.field] != c.code || ve.ConfigurationID != c.cid {
			t.Errorf("%s: fields=%v configuration=%s", c.name, ve.Fields, ve.ConfigurationID)
		}
		for _, v := range []string{"leak-value", "attacker.example"} {
			if strings.Contains(ve.Error(), v) {
				t.Errorf("%s: error echoes a value", c.name)
			}
		}
		// Nothing persisted on any error.
		if v, _ := k.ts.Get(ctx, subj, k.targetID); len(v.ConfigurationIDs) != 0 || len(k.target(t).ConfigOverrides) != 0 {
			t.Fatalf("%s: refused attach was persisted: %+v", c.name, v)
		}
	}

	// Valid overrides attach; a later override replaces the stored one.
	if err := k.ts.Attach(ctx, subj, k.targetID, []string{k.cf, k.hosts}, map[string]map[string]any{
		k.cf: {"zone_id": zone}, k.hosts: {"host_tags": []any{"role=web"}}}); err != nil {
		t.Fatal(err)
	}
	if err := k.ts.Attach(ctx, subj, k.targetID, nil, map[string]map[string]any{k.cf: {"zone_id": "123e105f4ecef8ad9ca31a8372d0c353"}}); err != nil {
		t.Fatal(err)
	}
	if got := k.target(t).ConfigOverrides[k.cf]["zone_id"]; got != "123e105f4ecef8ad9ca31a8372d0c353" {
		t.Fatalf("override not replaced: %v", got)
	}
	// Re-attaching a configuration without listing its override keeps (and
	// validates with) the stored one.
	if err := k.ts.Attach(ctx, subj, k.targetID, []string{k.cf}, nil); err != nil {
		t.Fatalf("stored override not used: %v", err)
	}
	// Defence in depth: credential-shaped names stay refused.
	if err := k.ts.Attach(ctx, subj, k.targetID, nil, map[string]map[string]any{k.hosts: {"host_tags": []any{"a"}, "password": "x"}}); err == nil {
		t.Fatal("credential-shaped override accepted")
	}
	// Unknown configuration: refused with its id.
	ve := asTVE(t, k.ts.Attach(ctx, subj, k.targetID, []string{store.NewID()}, nil))
	if ve.Fields["configuration_ids"] != "not_found" || ve.ConfigurationID == "" {
		t.Fatalf("unknown configuration: %+v", ve)
	}

	// The view carries the overrides and an empty missing_required.
	v, err := k.ts.Get(ctx, subj, k.targetID)
	if err != nil || v.ConfigOverrides[k.hosts] == nil || len(v.MissingRequired[k.cf]) != 0 {
		t.Fatalf("view = %+v, %v", v, err)
	}
	// Detaching removes the override.
	if err := k.ts.Detach(ctx, subj, k.targetID, []string{k.cf}); err != nil {
		t.Fatal(err)
	}
	if _, ok := k.target(t).ConfigOverrides[k.cf]; ok {
		t.Fatal("detached configuration kept its override")
	}
}

// A target saved before the rules (an override removed directly in the
// store) shows the missing required labels.
func TestTargetViewMissingRequired(t *testing.T) {
	k := newOverrideKit(t)
	ctx := context.Background()
	subj := adminSubject()
	if err := k.ts.Attach(ctx, subj, k.targetID, []string{k.cf}, map[string]map[string]any{k.cf: {"zone_id": "023e105f4ecef8ad9ca31a8372d0c353"}}); err != nil {
		t.Fatal(err)
	}
	tg := k.target(t)
	tg.ConfigOverrides = map[string]map[string]any{}
	if err := k.m.UpdateTarget(ctx, tg); err != nil {
		t.Fatal(err)
	}
	v, err := k.ts.Get(ctx, subj, k.targetID)
	if err != nil || !reflect.DeepEqual(v.MissingRequired[k.cf], []string{"Zone ID"}) {
		t.Fatalf("missing_required = %v, %v", v.MissingRequired, err)
	}
	// List rows do not compute it.
	rows, _ := k.ts.List(ctx, subj)
	if rows[0].MissingRequired != nil {
		t.Fatal("list rows must not compute missing_required")
	}
}
