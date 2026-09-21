package jobs_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/jobs"
	"github.com/go-freya/freya/services/deployer/internal/memstore"
	"github.com/go-freya/freya/services/deployer/internal/provider"
	"github.com/go-freya/freya/services/deployer/internal/sealed"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

// erroringCerts always fails the certificate fetch (drives the retry path).
type erroringCerts struct{}

func (erroringCerts) FetchCertificate(context.Context, string, string, bool) (provider.CertificateData, error) {
	return provider.CertificateData{}, errors.New("lcm down")
}

func kitWith(t *testing.T, cf jobs.CertFetcher, cfg jobs.Config) (*memstore.Mem, *configs.Service, *jobs.Service) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	az := authz.New(m)
	cs := configs.New(m, env, az)
	js := jobs.New(m, az, cf, cs, nil, cfg)
	return m, cs, js
}

// A job whose configuration has vanished fails cleanly (no panic, no retry loop).
func TestProcessConfigurationUnavailable(t *testing.T) {
	ctx := context.Background()
	m, cs, js := kitWith(t, fakeCerts{}, jobs.Config{Workers: 2, Lease: time.Minute, MaxRetries: 2})
	subj := adminSubj()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "gone", ProviderType: "dummy"})
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, MaxRetries: 2, TriggeredBy: store.TriggerManual})
	// Remove the configuration out from under the job.
	if err := m.DeleteConfiguration(ctx, subj.TenantID, cfg.ID); err != nil {
		t.Fatal(err)
	}
	js.Once(ctx, nil)
	if v, _ := js.Get(ctx, subj, jobID); v.Status != store.JobFailed {
		t.Fatalf("status = %q, want failed", v.Status)
	}
}

// A failing certificate fetch retries, then fails after exhausting retries.
func TestProcessCertFetchRetriesThenFails(t *testing.T) {
	ctx := context.Background()
	_, cs, js := kitWith(t, erroringCerts{}, jobs.Config{Workers: 2, Lease: time.Minute, MaxRetries: 1, RetryDelay: time.Millisecond})
	subj := adminSubj()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "ep", ProviderType: "dummy"})
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, MaxRetries: 1, TriggeredBy: store.TriggerManual})

	clk := time.Now()
	js.SetClock(func() time.Time { return clk })
	for i := 0; i < 6; i++ {
		if js.Once(ctx, nil) == 0 && i > 0 {
			break
		}
		clk = clk.Add(time.Hour)
	}
	if v, _ := js.Get(ctx, subj, jobID); v.Status != store.JobFailed {
		t.Fatalf("status = %q, want failed after retries", v.Status)
	}
}

// The worker pool (Run) claims a due job, processes it in a goroutine, and stops
// cleanly on ctx cancel.
func TestRunClaimsProcessesAndStops(t *testing.T) {
	ctx := context.Background()
	_, cs, js := kitWith(t, fakeCerts{}, jobs.Config{Workers: 2, Interval: 5 * time.Millisecond, Lease: time.Minute})
	subj := adminSubj()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "ep", ProviderType: "dummy"})
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, TriggeredBy: store.TriggerManual})

	rctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { js.Run(rctx, nil); close(done) }()
	completed := false
	for i := 0; i < 100; i++ {
		if v, _ := js.Get(ctx, subj, jobID); v.Status == store.JobCompleted {
			completed = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	<-done
	if !completed {
		t.Fatal("Run did not process the pending job to completion")
	}
}
