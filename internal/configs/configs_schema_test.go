package configs_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/all"
)

// probeProvider is a test-connection provider recording what Validate hands it.
type probeProvider struct {
	mu     sync.Mutex
	calls  int
	creds  map[string]any
	config map[string]any
}

var probe = &probeProvider{}

func init() {
	provider.Register(probe)
	provider.Register(previewProvider{})
}

const wrongPassword = "wrong-password-value"

func (p *probeProvider) Deploy(context.Context, *provider.CertificateData, map[string]any, map[string]any, provider.ProgressFn) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (p *probeProvider) Verify(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (p *probeProvider) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, provider.ErrUnsupported
}
func (p *probeProvider) ValidateCredentials(_ context.Context, creds, config map[string]any) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	p.creds, p.config = creds, config
	if creds["password"] == wrongPassword {
		// A provider echoing its input: the service must not pass it on.
		return fmt.Errorf("login to %v refused for password %v", creds["host"], creds["password"])
	}
	if config["mode"] == "b" {
		return &provider.FieldError{Field: "config.mode", Msg: provider.CodeNotFoundOnEndpoint}
	}
	return nil
}

// ValidateConfig (provider.ConfigValidator, T027): "a" and site "forbidden"
// do not go together.
func (p *probeProvider) ValidateConfig(config map[string]any) error {
	if config["site"] == "forbidden" {
		return &provider.FieldError{Field: "config.site", Msg: "pattern"}
	}
	return nil
}
func (p *probeProvider) seen() (int, map[string]any, map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls, p.creds, p.config
}
func (p *probeProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Type: "test_probe", DisplayName: "Probe", TestConnection: true,
		ConfigFields: []provider.Field{
			{Key: "zone", Label: "Zone", Required: true, Overridable: true, Default: "root"},
			{Key: "site", Label: "Site", Required: true, Overridable: true},
			{Key: "mode", Label: "Mode", Type: provider.TypeEnum, Options: []provider.Option{{Value: "a", Label: "A"}, {Value: "b", Label: "B"}}},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "Host", Required: true},
			{Key: "password", Label: "Password", Secret: true, Required: true},
			{Key: "token", Label: "Token", Secret: true},
		}}
}

// previewProvider implements provider.Previewer.
type previewProvider struct{}

func (previewProvider) Deploy(context.Context, *provider.CertificateData, map[string]any, map[string]any, provider.ProgressFn) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (previewProvider) Verify(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (previewProvider) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, provider.ErrUnsupported
}
func (previewProvider) ValidateCredentials(context.Context, map[string]any, map[string]any) error {
	return errors.New("Preview must be used instead")
}
func (previewProvider) Preview(ctx context.Context, config map[string]any) (map[string]any, error) {
	m, _ := provider.JobFrom(ctx)
	return map[string]any{"tenant": m.TenantID, "matched_hosts": []any{config["hosts"]}}, nil
}
func (previewProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Type: "test_preview", DisplayName: "Preview",
		ConfigFields: []provider.Field{{Key: "hosts", Label: "Hosts", Type: provider.TypeStringList}}}
}

// complete, valid inputs per shipped provider (contracts/deployer-config-ui.md §7).
var samples = map[string]struct{ config, creds map[string]any }{
	"aws_acm":    {map[string]any{"region": "eu-central-1"}, map[string]any{"access_key_id": "AKIAIOSFODNN7EXAMPLE", "secret_access_key": "aws-secret-value"}},
	"bigip":      {map[string]any{"partition": "Common"}, map[string]any{"host": "bigip.example.com", "username": "deployer", "password": "bigip-secret-value"}},
	"cloudflare": {map[string]any{"zone_id": "023e105f4ecef8ad9ca31a8372d0c353"}, map[string]any{"api_token": "cf-secret-value"}},
	"fortigate":  {map[string]any{"vdom": "root"}, map[string]any{"host": "fg.example.com", "api_token": "fg-secret-value"}},
	"webhook":    {map[string]any{"url": "https://hook.example/certs"}, map[string]any{"token": "wh-secret-value"}},
	"dummy":      {map[string]any{}, map[string]any{}},
}

func without(m map[string]any, key string) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if k != key {
			out[k] = v
		}
	}
	return out
}

func asValidation(t *testing.T, err error) *configs.ValidationError {
	t.Helper()
	var ve *configs.ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *configs.ValidationError", err)
	}
	return ve
}

// T081 / SC-007: every required field of every provider — a missing
// non-overridable one is refused naming its path; a missing overridable one
// is saved as target-supplied.
func TestCreateRequiredFieldsPerProvider(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()
	n := 0
	for typ, in := range samples {
		caps, _ := provider.Info(typ)
		if _, err := cs.Create(ctx, subj, configs.Input{Name: typ + "-complete", ProviderType: typ, Config: in.config, Credentials: in.creds}); err != nil {
			t.Fatalf("%s complete sample refused: %v", typ, err)
		}
		for _, f := range caps.ConfigFields {
			if !f.Required {
				continue
			}
			n++
			v, err := cs.Create(ctx, subj, configs.Input{Name: fmt.Sprintf("%s-no-%s", typ, f.Key), ProviderType: typ, Config: without(in.config, f.Key), Credentials: in.creds})
			if f.Overridable {
				if err != nil || !reflect.DeepEqual(v.TargetSupplied, []string{f.Key}) {
					t.Errorf("%s without overridable %s: view=%v err=%v", typ, f.Key, v.TargetSupplied, err)
				}
				continue
			}
			if ve := asValidation(t, err); ve.Fields["config."+f.Key] != "required" {
				t.Errorf("%s without %s: %v", typ, f.Key, ve.Fields)
			}
		}
		for _, f := range caps.CredentialFields {
			if !f.Required {
				continue
			}
			n++
			_, err := cs.Create(ctx, subj, configs.Input{Name: fmt.Sprintf("%s-no-%s", typ, f.Key), ProviderType: typ, Config: in.config, Credentials: without(in.creds, f.Key)})
			if ve := asValidation(t, err); ve.Fields["credentials."+f.Key] != "required" {
				t.Errorf("%s without credential %s: %v", typ, f.Key, ve.Fields)
			}
		}
	}
	if n < 12 {
		t.Fatalf("only %d required fields exercised", n)
	}
}

func TestCreateUnknownKeyAndProviderValidator(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()
	_, err := cs.Create(ctx, subj, configs.Input{Name: "x", ProviderType: "webhook", Config: map[string]any{"url": "https://h.example", "endpoint": "https://evil.example"}})
	if ve := asValidation(t, err); ve.Fields["config.endpoint"] != "unknown_field" {
		t.Fatalf("fields = %v", ve.Fields)
	}
	// The provider's own ConfigValidator runs after the descriptors (T027).
	_, err = cs.Create(ctx, subj, configs.Input{Name: "p", ProviderType: "test_probe",
		Config: map[string]any{"zone": "z", "site": "forbidden"}, Credentials: map[string]any{"host": "h", "password": "p"}})
	if ve := asValidation(t, err); ve.Fields["config.site"] != "pattern" {
		t.Fatalf("fields = %v", ve.Fields)
	}
}

func bigipConfig(t *testing.T, cs *configs.Service) configs.View {
	t.Helper()
	in := samples["bigip"]
	v, err := cs.Create(context.Background(), adminSubject(), configs.Input{Name: "lb-" + store.NewID(), ProviderType: "bigip", Config: in.config, Credentials: in.creds})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func storedCreds(t *testing.T, cs *configs.Service, m interface {
	GetConfiguration(context.Context, string, string) (store.TargetConfiguration, error)
}, id string) map[string]any {
	t.Helper()
	row, err := m.GetConfiguration(context.Background(), adminSubject().TenantID, id)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := cs.OpenCredentials(row)
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

// FR-028 / D23: credentials merge per field on update.
func TestUpdateMergesCredentialsPerField(t *testing.T) {
	ctx := context.Background()
	cs, m := newService(t)
	subj := adminSubject()
	v := bigipConfig(t, cs)

	// A new partition with blank credentials keeps every stored credential.
	if _, err := cs.Update(ctx, subj, v.ID, configs.Input{Name: v.Name, Config: map[string]any{"partition": "Edge"},
		Credentials: map[string]any{"password": "", "host": ""}}); err != nil {
		t.Fatal(err)
	}
	if c := storedCreds(t, cs, m, v.ID); c["password"] != "bigip-secret-value" || c["host"] != "bigip.example.com" || c["username"] != "deployer" {
		t.Fatalf("blank credentials changed the stored ones: %v", c)
	}
	// A new password replaces only the password.
	if _, err := cs.Update(ctx, subj, v.ID, configs.Input{Name: v.Name, Credentials: map[string]any{"password": "n3w-password"}}); err != nil {
		t.Fatal(err)
	}
	if c := storedCreds(t, cs, m, v.ID); c["password"] != "n3w-password" || c["host"] != "bigip.example.com" || c["username"] != "deployer" {
		t.Fatalf("per-field merge: %v", c)
	}
	// Clearing a required credential is refused; nothing changes.
	_, err := cs.Update(ctx, subj, v.ID, configs.Input{Name: v.Name, ClearCredentials: []string{"password"}})
	if ve := asValidation(t, err); ve.Fields["credentials.password"] != "required" {
		t.Fatalf("fields = %v", ve.Fields)
	}
	// The provider cannot change.
	_, err = cs.Update(ctx, subj, v.ID, configs.Input{Name: v.Name, ProviderType: "fortigate"})
	if ve := asValidation(t, err); ve.Field != "provider_type" {
		t.Fatalf("provider change: %v", ve)
	}

	// An optional secret can be cleared explicitly; a legacy undeclared key too.
	wh, err := cs.Create(ctx, subj, configs.Input{Name: "wh", ProviderType: "webhook", Config: samples["webhook"].config,
		Credentials: map[string]any{"token": "t0ken-value", "secret": "s3cret-value"}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cs.Update(ctx, subj, wh.ID, configs.Input{Name: wh.Name, ClearCredentials: []string{"token", "legacy_key"}}); err != nil {
		t.Fatal(err)
	}
	if c := storedCreds(t, cs, m, wh.ID); c["token"] != nil || c["secret"] != "s3cret-value" {
		t.Fatalf("clear_credentials: %v", c)
	}
	// Unknown credential keys are refused on update as on create.
	_, err = cs.Update(ctx, subj, wh.ID, configs.Input{Name: wh.Name, Credentials: map[string]any{"password": "x"}})
	if ve := asValidation(t, err); ve.Fields["credentials.password"] != "unknown_field" {
		t.Fatalf("fields = %v", ve.Fields)
	}
}

// FR-028 / SR-011: names of stored credentials and non-secret values for the
// edit drawer; never a secret, never in list rows.
func TestViewCredentialProjection(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()
	v := bigipConfig(t, cs)
	got, err := cs.Get(ctx, subj, v.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.CredentialsSet, []string{"host", "password", "username"}) {
		t.Fatalf("credentials_set = %v", got.CredentialsSet)
	}
	if !reflect.DeepEqual(got.CredentialsPublic, map[string]any{"host": "bigip.example.com", "username": "deployer"}) {
		t.Fatalf("credentials_public = %v", got.CredentialsPublic)
	}
	raw, _ := json.Marshal(got)
	if strings.Contains(string(raw), "bigip-secret-value") {
		t.Fatalf("secret in view: %s", raw)
	}
	list, _ := cs.List(ctx, subj, "", "")
	for _, row := range list {
		if row.CredentialsSet != nil || row.CredentialsPublic != nil {
			t.Fatalf("list rows must not carry credential projections: %+v", row)
		}
	}
	// A configuration without credentials has no projection either.
	dv, _ := cs.Create(ctx, subj, configs.Input{Name: "d", ProviderType: "dummy"})
	if got, _ := cs.Get(ctx, subj, dv.ID); got.CredentialsSet != nil || !reflect.DeepEqual(got.TargetSupplied, []string{}) {
		t.Fatalf("dummy view: %+v", got)
	}
}

// Legacy rows (undeclared keys, missing required fields) stay readable.
func TestLegacyRowReadable(t *testing.T) {
	ctx := context.Background()
	cs, m := newService(t)
	subj := adminSubject()
	row := store.TargetConfiguration{ID: store.NewID(), TenantID: subj.TenantID, Name: "legacy", ProviderType: "cloudflare",
		Config: map[string]any{"api_base": "https://old.example"}, Status: store.ConfigActive}
	if err := m.InsertConfiguration(ctx, row); err != nil {
		t.Fatal(err)
	}
	v, err := cs.Get(ctx, subj, row.ID)
	if err != nil || !reflect.DeepEqual(v.TargetSupplied, []string{"zone_id"}) || v.IgnoredConfigKeys[0] != "api_base" {
		t.Fatalf("legacy view = %+v, %v", v, err)
	}
}

// D25 §5: an update that leaves an attached target without a required value
// it relied on is refused, naming the field and the targets; nothing saved.
func TestUpdateRequiredByTargets(t *testing.T) {
	ctx := context.Background()
	cs, m := newService(t)
	subj := adminSubject()
	in := samples["cloudflare"]
	v, err := cs.Create(ctx, subj, configs.Input{Name: "cf", ProviderType: "cloudflare", Config: in.config, Credentials: in.creds})
	if err != nil {
		t.Fatal(err)
	}
	relying := store.DeploymentTarget{ID: store.NewID(), TenantID: subj.TenantID, Name: "edge-zone-a"}
	supplying := store.DeploymentTarget{ID: store.NewID(), TenantID: subj.TenantID, Name: "edge-zone-b",
		ConfigOverrides: map[string]map[string]any{v.ID: {"zone_id": "123e105f4ecef8ad9ca31a8372d0c353"}}}
	for _, tg := range []store.DeploymentTarget{relying, supplying} {
		if err := m.InsertTarget(ctx, tg); err != nil {
			t.Fatal(err)
		}
		if err := m.AttachConfigurations(ctx, subj.TenantID, tg.ID, []string{v.ID}); err != nil {
			t.Fatal(err)
		}
	}
	_, err = cs.Update(ctx, subj, v.ID, configs.Input{Name: "cf", Config: map[string]any{}})
	ve := asValidation(t, err)
	if ve.Fields["config.zone_id"] != "required_by_targets" || len(ve.Targets) != 1 || ve.Targets[0].Name != "edge-zone-a" {
		t.Fatalf("refusal = %+v", ve)
	}
	if got, _ := cs.Get(ctx, subj, v.ID); got.Config["zone_id"] == nil {
		t.Fatal("refused update was saved")
	}
	// Once the relying target supplies the value, clearing it is allowed.
	relying.ConfigOverrides = map[string]map[string]any{v.ID: {"zone_id": "223e105f4ecef8ad9ca31a8372d0c353"}}
	if err := m.UpdateTarget(ctx, relying); err != nil {
		t.Fatal(err)
	}
	got, err := cs.Update(ctx, subj, v.ID, configs.Input{Name: "cf", Config: map[string]any{}})
	if err != nil || !reflect.DeepEqual(got.TargetSupplied, []string{"zone_id"}) {
		t.Fatalf("update = %+v, %v", got, err)
	}
}

type auditRecorder struct {
	mu     sync.Mutex
	events []audit.Event
}

func (a *auditRecorder) Record(_ context.Context, e audit.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err := audit.Validate(e); err != nil {
		return err
	}
	a.events = append(a.events, e)
	return nil
}

// FR-029 / §5: validate merges stored credentials, runs the descriptors
// first, answers credentials_rejected without the provider text and logs it
// redacted.
func TestValidateWithStoredCredentials(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()
	var logs bytes.Buffer
	cs.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
	aud := &auditRecorder{}
	cs.SetAuditor(aud)
	v, err := cs.Create(ctx, subj, configs.Input{Name: "probe", ProviderType: "test_probe",
		Config: map[string]any{"zone": "z", "site": "s"}, Credentials: map[string]any{"host": "h.example", "password": "stored-password-value"}})
	if err != nil {
		t.Fatal(err)
	}
	res, err := cs.Validate(ctx, subj, configs.ValidateRequest{ConfigurationID: v.ID, Config: map[string]any{"zone": "z", "site": "s"},
		Credentials: map[string]any{"password": ""}})
	if err != nil || res.Checked != configs.CheckedProbe || !res.Valid {
		t.Fatalf("validate = %+v, %v", res, err)
	}
	if _, creds, _ := probe.seen(); creds["password"] != "stored-password-value" || creds["host"] != "h.example" {
		t.Fatalf("stored credentials not merged: %v", creds)
	}
	// Unknown id → not found; provider mismatch → 422.
	if _, err := cs.Validate(ctx, subj, configs.ValidateRequest{ConfigurationID: store.NewID(), ProviderType: "test_probe"}); !errors.Is(err, configs.ErrNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
	_, err = cs.Validate(ctx, subj, configs.ValidateRequest{ConfigurationID: v.ID, ProviderType: "dummy"})
	if ve := asValidation(t, err); ve.Field != "provider_type" {
		t.Fatalf("mismatch: %v", ve)
	}
	// Descriptor errors come before the provider call.
	before, _, _ := probe.seen()
	_, err = cs.Validate(ctx, subj, configs.ValidateRequest{ProviderType: "test_probe", Config: map[string]any{"zone": "z", "site": "s"}})
	if ve := asValidation(t, err); ve.Fields["credentials.host"] != "required" || ve.Fields["credentials.password"] != "required" {
		t.Fatalf("fields = %v", ve.Fields)
	}
	if after, _, _ := probe.seen(); after != before {
		t.Fatal("provider called despite invalid input")
	}
	// A provider rejection is a fixed reason; its text is logged redacted.
	_, err = cs.Validate(ctx, subj, configs.ValidateRequest{ConfigurationID: v.ID, Config: map[string]any{"zone": "z", "site": "s"},
		Credentials: map[string]any{"password": wrongPassword}})
	if !errors.Is(err, configs.ErrCredentialsRejected) || strings.Contains(err.Error(), wrongPassword) {
		t.Fatalf("rejection = %v", err)
	}
	if out := logs.String(); !strings.Contains(out, "provider rejected") || strings.Contains(out, wrongPassword) || strings.Contains(out, "h.example") {
		t.Fatalf("log not redacted:\n%s", out)
	}
	// A named setting failing on the endpoint is reported on the field.
	_, err = cs.Validate(ctx, subj, configs.ValidateRequest{ConfigurationID: v.ID, Config: map[string]any{"zone": "z", "site": "s", "mode": "b"}})
	if ve := asValidation(t, err); ve.Fields["config.mode"] != "not_found_on_endpoint" {
		t.Fatalf("fields = %v", ve.Fields)
	}
	aud.mu.Lock()
	defer aud.mu.Unlock()
	if len(aud.events) < 4 {
		t.Fatalf("audit events = %d", len(aud.events))
	}
	for _, e := range aud.events {
		raw, _ := json.Marshal(e)
		if e.EventType != audit.ConfigurationValidated || strings.Contains(string(raw), "password-value") {
			t.Fatalf("audit event %s", raw)
		}
	}
}

// D25 §7: target-supplied fields use their default for the probe, or the
// probe is skipped (checked=partial, deferred).
func TestValidateTargetSuppliedFields(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()
	creds := map[string]any{"host": "h", "password": "p4ssword"}
	before, _, _ := probe.seen()
	res, err := cs.Validate(ctx, subj, configs.ValidateRequest{ProviderType: "test_probe", Config: map[string]any{"zone": "z"}, Credentials: creds})
	if err != nil || res.Checked != configs.CheckedPartial || !reflect.DeepEqual(res.Deferred, []string{"site"}) {
		t.Fatalf("partial = %+v, %v", res, err)
	}
	if after, _, _ := probe.seen(); after != before {
		t.Fatal("probe ran without the target-supplied value")
	}
	res, err = cs.Validate(ctx, subj, configs.ValidateRequest{ProviderType: "test_probe", Config: map[string]any{"site": "s"}, Credentials: creds})
	if err != nil || res.Checked != configs.CheckedProbe || len(res.Deferred) != 0 {
		t.Fatalf("default-filled = %+v, %v", res, err)
	}
	if _, _, cfg := probe.seen(); cfg["zone"] != "root" {
		t.Fatalf("descriptor default not used for the probe: %v", cfg)
	}
	// A provider without test_connection only checks the input.
	res, err = cs.Validate(ctx, subj, configs.ValidateRequest{ProviderType: "dummy", Config: map[string]any{}})
	if err != nil || res.Checked != configs.CheckedStatic {
		t.Fatalf("static = %+v, %v", res, err)
	}
}

// §5: Previewer providers return details; the tenant reaches the provider.
func TestValidatePreviewDetails(t *testing.T) {
	cs, _ := newService(t)
	res, err := cs.Validate(context.Background(), adminSubject(), configs.ValidateRequest{ProviderType: "test_preview", Config: map[string]any{"hosts": []any{"h1"}}})
	if err != nil || res.Details["tenant"] != adminSubject().TenantID || res.Details["matched_hosts"] == nil {
		t.Fatalf("preview = %+v, %v", res, err)
	}
}

func TestProvidersCatalogue(t *testing.T) {
	cs, _ := newService(t)
	items, err := cs.Providers(context.Background(), adminSubject())
	if err != nil || len(items) < 6 {
		t.Fatalf("providers = %d, %v", len(items), err)
	}
}
