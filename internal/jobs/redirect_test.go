package jobs_test

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// configRecorder is a test provider that records the effective config it is
// handed, so the worker's deploy-time sanitising can be observed.
type configRecorder struct {
	mu   sync.Mutex
	seen []map[string]any
}

var recorderProvider = &configRecorder{}

func init() { provider.Register(recorderProvider) }

func (r *configRecorder) record(cfg map[string]any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, cfg)
}

func (r *configRecorder) last() map[string]any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.seen[len(r.seen)-1]
}

func (r *configRecorder) Deploy(_ context.Context, _ *provider.CertificateData, cfg, _ map[string]any, _ provider.ProgressFn) (*provider.Result, error) {
	r.record(cfg)
	return &provider.Result{Success: true, Message: "ok"}, nil
}
func (r *configRecorder) Verify(_ context.Context, _ *provider.CertificateData, cfg, _ map[string]any) (*provider.Result, error) {
	r.record(cfg)
	return &provider.Result{Success: true}, nil
}
func (r *configRecorder) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return &provider.Result{Success: true}, nil
}
func (r *configRecorder) ValidateCredentials(context.Context, map[string]any, map[string]any) error {
	return nil
}
func (r *configRecorder) Capabilities() provider.Capabilities {
	return provider.Capabilities{Type: "test_recorder", DisplayName: "recorder", SupportsVerify: true}
}

const attacker = "https://attacker.example/collect"

// TestLegacyRedirectKeysIgnoredAtDeploy: rows stored before the save-time
// refusal may still carry endpoint/api_base (in the configuration or in a
// target override). The worker must not hand them to the provider, must warn
// once per configuration (names only), and must not rewrite the stored row.
func TestLegacyRedirectKeysIgnoredAtDeploy(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	var logs bytes.Buffer
	js.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))

	// A legacy row written straight to the store (bypassing save validation).
	legacy := store.TargetConfiguration{
		ID: store.NewID(), TenantID: subj.TenantID, Name: "legacy", ProviderType: "test_recorder",
		Config: map[string]any{
			"zone_id": "z", "endpoint": attacker, "api_base": attacker,
			"headers": map[string]any{"Authorization": "Bearer legacy-header-value", "X-Source": "tangra"},
		},
		Status: store.ConfigActive,
	}
	if err := m.InsertConfiguration(ctx, legacy); err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 2; i++ {
		if _, err := ds.Deploy(ctx, subj, "cert-1", legacy.ID, ""); err != nil {
			t.Fatalf("deploy: %v", err)
		}
		js.Once(ctx, nil)
		got := recorderProvider.last()
		if got["endpoint"] != nil || got["api_base"] != nil {
			t.Fatalf("redirect keys reached the provider: %v", got)
		}
		if got["zone_id"] != "z" || got["headers"] == nil {
			t.Fatalf("other settings must still be passed: %v", got)
		}
	}

	out := logs.String()
	if n := strings.Count(out, "no longer allowed"); n != 1 {
		t.Fatalf("want exactly one warning for the configuration, got %d:\n%s", n, out)
	}
	if !strings.Contains(out, legacy.ID) || !strings.Contains(out, "endpoint") || !strings.Contains(out, "Authorization") {
		t.Fatalf("warning should name the configuration and the keys:\n%s", out)
	}
	if strings.Contains(out, "attacker.example") || strings.Contains(out, "legacy-header-value") {
		t.Fatalf("warning leaks values:\n%s", out)
	}

	// The stored row is untouched and the API flags the ignored keys.
	row, _ := m.GetConfiguration(ctx, subj.TenantID, legacy.ID)
	if row.Config["endpoint"] != attacker {
		t.Fatal("stored data must not be rewritten")
	}
	v, err := cs.Get(ctx, subj, legacy.ID)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(v.IgnoredConfigKeys, ",") != "api_base,endpoint" {
		t.Fatalf("ignored_config_keys = %v", v.IgnoredConfigKeys)
	}
}

// TestLegacyOverrideRedirectKeysIgnored: the same holds for a target override
// stored before the refusal.
func TestLegacyOverrideRedirectKeysIgnored(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()
	js.SetLogger(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))

	cfg, err := cs.Create(ctx, subj, configs.Input{Name: "rec", ProviderType: "test_recorder", Config: map[string]any{"zone_id": "z"}})
	if err != nil {
		t.Fatal(err)
	}
	tgtID := makeTarget(t, ctx, subj, m, cfg.ID, nil)
	tgt, _ := m.GetTarget(ctx, subj.TenantID, tgtID)
	tgt.ConfigOverrides = map[string]map[string]any{cfg.ID: {"api_base": attacker, "zone_id": "z2"}}
	if err := m.UpdateTarget(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	if _, err := ds.DeployToTarget(ctx, subj, "cert-1", tgtID, ""); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		js.Once(ctx, nil)
	}
	got := recorderProvider.last()
	if got["api_base"] != nil || got["zone_id"] != "z2" {
		t.Fatalf("effective config = %v", got)
	}
}

// TestLegacyOverrideDestinationIgnoredWithCredentials: an override stored
// before the refusal that moves the destination of a configuration holding
// sealed credentials is ignored at deploy time (one WARN, no values); for a
// configuration without credentials it still applies.
func TestLegacyOverrideDestinationIgnoredWithCredentials(t *testing.T) {
	subj := adminSubj()
	ctx := context.Background()
	var logs bytes.Buffer

	run := func(creds map[string]any) map[string]any {
		m, cs, ds, js := newKit(t)
		js.SetLogger(slog.New(slog.NewTextHandler(&logs, nil)))
		cfg, err := cs.Create(ctx, subj, configs.Input{Name: "rec-" + store.NewID(), ProviderType: "test_recorder",
			Config: map[string]any{"url": "https://hook.example/a"}, Credentials: creds})
		if err != nil {
			t.Fatal(err)
		}
		tgtID := makeTarget(t, ctx, subj, m, cfg.ID, nil)
		tgt, _ := m.GetTarget(ctx, subj.TenantID, tgtID)
		tgt.ConfigOverrides = map[string]map[string]any{cfg.ID: {"url": attacker, "verify_url": attacker, "timeout_seconds": 5.0}}
		if err := m.UpdateTarget(ctx, tgt); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if _, err := ds.DeployToTarget(ctx, subj, "cert-1", tgtID, ""); err != nil {
				t.Fatal(err)
			}
			for j := 0; j < 3; j++ {
				js.Once(ctx, nil)
			}
		}
		return recorderProvider.last()
	}

	got := run(map[string]any{"token": "s3cr3t"})
	if got["url"] != "https://hook.example/a" || got["verify_url"] != nil || got["timeout_seconds"] != 5.0 {
		t.Fatalf("with credentials: effective config = %v", got)
	}
	out := logs.String()
	if n := strings.Count(out, "tries to change the destination"); n != 1 {
		t.Fatalf("want one warning, got %d:\n%s", n, out)
	}
	if strings.Contains(out, "attacker.example") || strings.Contains(out, "s3cr3t") {
		t.Fatalf("warning leaks values:\n%s", out)
	}

	got = run(nil)
	if got["url"] != attacker {
		t.Fatalf("without credentials the override applies: %v", got)
	}
}
