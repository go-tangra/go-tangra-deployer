package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/deploy"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/dummy"
)

type fakeCerts struct{}

func (fakeCerts) FetchCertificate(_ context.Context, _ string, id string, _ bool) (provider.CertificateData, error) {
	return provider.CertificateData{ID: id, SerialNumber: "0A", CommonName: "api.example.com"}, nil
}

func newKit(t *testing.T) (*memstore.Mem, *configs.Service, *deploy.Service, *jobs.Service) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	az := authz.New(m)
	cs := configs.New(m, env, az)
	ds := deploy.New(m, az)
	js := jobs.New(m, az, fakeCerts{}, cs, nil, jobs.Config{Workers: 4, Lease: time.Minute, MaxRetries: 2})
	return m, cs, ds, js
}

func adminSubj() authz.Subjects {
	return authz.Subjects{TenantID: "11111111-1111-1111-1111-111111111111", UserID: "u1", Roles: []string{"admin"}, ActorKind: "user"}
}

func TestUS1_CreateDeployComplete(t *testing.T) {
	_, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	cfg, err := cs.Create(ctx, subj, configs.Input{Name: "dummy-ep", ProviderType: "dummy"})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	if cfg.HasCredentials {
		t.Fatal("dummy config should have no credentials")
	}

	jobID, err := ds.Deploy(ctx, subj, "cert-1", cfg.ID, "")
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	// Before the worker runs, the job is pending.
	if v, _ := js.Get(ctx, subj, jobID); v.Status != store.JobPending {
		t.Fatalf("status before worker = %q", v.Status)
	}

	if n := js.Once(ctx, nil); n != 1 {
		t.Fatalf("processed %d, want 1", n)
	}
	v, err := js.Get(ctx, subj, jobID)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if v.Status != store.JobCompleted || v.Progress != 100 || v.Type != "direct" {
		t.Fatalf("job after worker = %+v", v)
	}
}

func TestUS1_FailingProviderRetriesThenFails(t *testing.T) {
	_, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "fail-ep", ProviderType: "dummy", Config: map[string]any{"fail": true}})
	jobID, _ := ds.Deploy(ctx, subj, "cert-1", cfg.ID, "")

	// First pass: fails -> retrying (retries remain).
	js.Once(ctx, nil)
	if v, _ := js.Get(ctx, subj, jobID); v.Status != store.JobRetrying {
		t.Fatalf("after 1st pass = %q, want retrying", v.Status)
	}
}

func TestUS1_UnknownProviderRejected(t *testing.T) {
	_, cs, _, _ := newKit(t)
	if _, err := cs.Create(context.Background(), adminSubj(), configs.Input{Name: "x", ProviderType: "nope"}); err == nil {
		t.Fatal("expected unknown-provider rejection")
	}
}
