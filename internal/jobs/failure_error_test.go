package jobs_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// recorder captures published realtime events.
type recorder struct {
	mu     sync.Mutex
	events []recorded
}

type recorded struct {
	tenant, typ string
	data        map[string]any
}

func (r *recorder) Publish(_ context.Context, tenantID, typ string, payload any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, _ := payload.(map[string]any)
	r.events = append(r.events, recorded{tenantID, typ, m})
}

func (r *recorder) last(typ string) (map[string]any, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i := len(r.events) - 1; i >= 0; i-- {
		if r.events[i].typ == typ {
			return r.events[i].data, true
		}
	}
	return nil, false
}

// echoing fails its deployment with an error that echoes its credential, the
// way an endpoint answering "invalid token <token>" would.
type echoing struct{}

func (echoing) Capabilities() provider.Capabilities {
	return provider.Capabilities{Type: "test-echoing", DisplayName: "Echoing", ConfigFields: []provider.Field{},
		CredentialFields: []provider.Field{{Key: "token", Label: "Token", Secret: true}}}
}
func (echoing) ValidateCredentials(context.Context, map[string]any, map[string]any) error { return nil }
func (echoing) Deploy(_ context.Context, _ *provider.CertificateData, _ map[string]any, creds map[string]any, _ provider.ProgressFn) (*provider.Result, error) {
	tok, _ := creds["token"].(string)
	return nil, errors.New("endpoint said 401: invalid token " + tok)
}
func (echoing) Verify(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, nil
}
func (echoing) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, nil
}

func init() { provider.Register(echoing{}) }

func recordingKit(t *testing.T, cf jobs.CertFetcher, cfg jobs.Config) (*memstore.Mem, *configs.Service, *jobs.Service, *recorder) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	az := authz.New(m)
	cs := configs.New(m, env, az)
	rec := &recorder{}
	return m, cs, jobs.New(m, az, cf, cs, rec, cfg), rec
}

// A failed deployment keeps the provider's error (credentials redacted) on
// the job, in its history and in the live failure event.
func TestFailureKeepsRedactedError(t *testing.T) {
	ctx := context.Background()
	_, cs, js, rec := recordingKit(t, fakeCerts{}, jobs.Config{Workers: 2, Lease: time.Minute})
	subj := adminSubj()
	const secret = "s3cr3t-token-value"
	cfg, err := cs.Create(ctx, subj, configs.Input{Name: "echo", ProviderType: "test-echoing", Credentials: map[string]any{"token": secret}})
	if err != nil {
		t.Fatal(err)
	}
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, TriggeredBy: store.TriggerManual})
	js.Once(ctx, nil)

	v, _ := js.Get(ctx, subj, jobID)
	if v.Status != store.JobFailed {
		t.Fatalf("status = %q, want failed", v.Status)
	}
	want := "endpoint said 401: invalid token [redacted]"
	if v.Error != want {
		t.Fatalf("error = %q, want %q", v.Error, want)
	}
	res, _ := js.GetResult(ctx, subj, jobID)
	if strings.Contains(string(res.Result), secret) || len(res.History) != 1 || !strings.Contains(res.History[0].Message, want) {
		t.Fatalf("result = %s history = %+v", res.Result, res.History)
	}
	ev, ok := rec.last("deployment.failed")
	if !ok || ev["job_id"] != jobID || ev["error"] != want || ev["status"] != store.JobFailed {
		t.Fatalf("deployment.failed event = %v", ev)
	}
}

// A failing certificate fetch keeps lcm's error while retrying and publishes
// the retrying state; the success that follows clears it.
func TestRetryKeepsErrorUntilSuccess(t *testing.T) {
	ctx := context.Background()
	certs := &flakyCerts{fail: true}
	_, cs, js, rec := recordingKit(t, certs, jobs.Config{Workers: 2, Lease: time.Minute, RetryDelay: time.Millisecond})
	subj := adminSubj()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "ep", ProviderType: "dummy"})
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, MaxRetries: 2, TriggeredBy: store.TriggerManual})
	clk := time.Now()
	js.SetClock(func() time.Time { return clk })

	js.Once(ctx, nil)
	v, _ := js.Get(ctx, subj, jobID)
	if v.Status != store.JobRetrying || v.Error != "lcm down" {
		t.Fatalf("after failed fetch: status=%q error=%q", v.Status, v.Error)
	}
	if ev, ok := rec.last("job.updated"); !ok || ev["status"] != store.JobRetrying || ev["error"] != "lcm down" {
		t.Fatalf("job.updated event = %v", ev)
	}

	certs.set(false)
	clk = clk.Add(time.Hour)
	js.Once(ctx, nil)
	v, _ = js.Get(ctx, subj, jobID)
	if v.Status != store.JobCompleted || v.Error != "" {
		t.Fatalf("after success: status=%q error=%q", v.Status, v.Error)
	}
	if ev, ok := rec.last("deployment.completed"); !ok || ev["error"] != "" || ev["progress"] != 100 {
		t.Fatalf("deployment.completed event = %v", ev)
	}
}

// Cancelling a job publishes its new state.
func TestCancelPublishes(t *testing.T) {
	ctx := context.Background()
	_, cs, js, rec := recordingKit(t, fakeCerts{}, jobs.Config{Workers: 2, Lease: time.Minute})
	subj := adminSubj()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "ep", ProviderType: "dummy"})
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, TriggeredBy: store.TriggerManual})
	if _, err := js.Cancel(ctx, subj, jobID, false); err != nil {
		t.Fatal(err)
	}
	if ev, ok := rec.last("job.updated"); !ok || ev["job_id"] != jobID || ev["status"] != store.JobCancelled {
		t.Fatalf("job.updated event = %v", ev)
	}
}

// flakyCerts fails the fetch while fail is set.
type flakyCerts struct {
	mu   sync.Mutex
	fail bool
}

func (f *flakyCerts) set(fail bool) { f.mu.Lock(); f.fail = fail; f.mu.Unlock() }

func (f *flakyCerts) FetchCertificate(ctx context.Context, tenant, id string, key bool) (provider.CertificateData, error) {
	f.mu.Lock()
	fail := f.fail
	f.mu.Unlock()
	if fail {
		return provider.CertificateData{}, errors.New("lcm down")
	}
	return fakeCerts{}.FetchCertificate(ctx, tenant, id, key)
}
