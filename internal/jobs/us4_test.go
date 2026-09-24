package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
)

// makeTarget creates an auto-deploy target on the given store and attaches the
// configurations with the supplied per-config overrides. Returns the target id.
func makeTarget(t *testing.T, ctx context.Context, subj authz.Subjects, m *memstore.Mem, cfgID string, overrides map[string]map[string]any) string {
	t.Helper()
	ts := targets.New(m, authz.New(m))
	v, err := ts.Create(ctx, subj, targets.Input{Name: "tgt-" + store.NewID()[:8], AutoDeploy: true})
	if err != nil {
		t.Fatalf("create target: %v", err)
	}
	if err := ts.Attach(ctx, subj, v.ID, []string{cfgID}, overrides); err != nil {
		t.Fatalf("attach: %v", err)
	}
	return v.ID
}

// US4: a target's per-config override is layered over the configuration's base
// config in the worker, so one shared config can be flipped per target. Here the
// override injects fail=true, which must make the child job fail even though the
// base configuration is healthy.
func TestUS4_OverrideAppliedInWorker(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "ov-ep", ProviderType: "dummy"})
	// Sanity: a direct deploy (no override) completes.
	direct, _ := ds.Deploy(ctx, subj, "cert-1", cfg.ID, "")
	js.Once(ctx, nil)
	if v, _ := js.Get(ctx, subj, direct); v.Status != store.JobCompleted {
		t.Fatalf("direct job = %q, want completed", v.Status)
	}

	// Same config under a target whose override forces failure.
	tgt := makeTarget(t, ctx, subj, m, cfg.ID, map[string]map[string]any{cfg.ID: {"fail": true}})
	parentID, err := ds.DeployToTarget(ctx, subj, "cert-2", tgt, "")
	if err != nil {
		t.Fatalf("deploy to target: %v", err)
	}
	// Drain the child (exhaust its retries) so it lands in a terminal state.
	// Advance the clock each pass so scheduled retries become due.
	clk := time.Now()
	js.SetClock(func() time.Time { return clk })
	for i := 0; i < 10; i++ {
		if js.Once(ctx, nil) == 0 && i > 0 {
			break
		}
		clk = clk.Add(time.Hour)
	}
	res, err := js.GetResult(ctx, subj, parentID)
	if err != nil {
		t.Fatalf("get parent result: %v", err)
	}
	if len(res.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(res.Children))
	}
	if res.Children[0].Status != store.JobFailed {
		t.Fatalf("child status = %q, want failed (override should force failure)", res.Children[0].Status)
	}
}

// US4: deploying to a target fans out one child per attached configuration.
func TestUS4_FanOutOneChildPerConfig(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	c1, _ := cs.Create(ctx, subj, configs.Input{Name: "fan-1", ProviderType: "dummy"})
	c2, _ := cs.Create(ctx, subj, configs.Input{Name: "fan-2", ProviderType: "dummy"})
	ts := targets.New(m, authz.New(m))
	tv, _ := ts.Create(ctx, subj, targets.Input{Name: "fan-tgt", AutoDeploy: true})
	if err := ts.Attach(ctx, subj, tv.ID, []string{c1.ID, c2.ID}, nil); err != nil {
		t.Fatalf("attach: %v", err)
	}
	parentID, err := ds.DeployToTarget(ctx, subj, "cert-1", tv.ID, "")
	if err != nil {
		t.Fatalf("deploy to target: %v", err)
	}
	for i := 0; i < 10; i++ {
		if js.Once(ctx, nil) == 0 {
			break
		}
	}
	res, _ := js.GetResult(ctx, subj, parentID)
	if len(res.Children) != 2 {
		t.Fatalf("children = %d, want 2", len(res.Children))
	}
	if res.Status != store.JobCompleted {
		t.Fatalf("parent status = %q, want completed", res.Status)
	}
}

// US4/US3: a parent aggregates mixed child outcomes to "partial" — one config
// clean, one forced to fail via its override.
func TestUS4_ParentAggregatesPartial(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	good, _ := cs.Create(ctx, subj, configs.Input{Name: "mix-good", ProviderType: "dummy"})
	bad, _ := cs.Create(ctx, subj, configs.Input{Name: "mix-bad", ProviderType: "dummy"})
	ts := targets.New(m, authz.New(m))
	tv, _ := ts.Create(ctx, subj, targets.Input{Name: "mix-tgt", AutoDeploy: true})
	if err := ts.Attach(ctx, subj, tv.ID, []string{good.ID, bad.ID},
		map[string]map[string]any{bad.ID: {"fail": true}}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	parentID, err := ds.DeployToTarget(ctx, subj, "cert-1", tv.ID, "")
	if err != nil {
		t.Fatalf("deploy to target: %v", err)
	}
	clk := time.Now()
	js.SetClock(func() time.Time { return clk })
	for i := 0; i < 20; i++ {
		if js.Once(ctx, nil) == 0 && i > 0 {
			break
		}
		clk = clk.Add(time.Hour)
	}
	res, _ := js.GetResult(ctx, subj, parentID)
	if res.Status != store.JobPartial {
		t.Fatalf("parent status = %q, want partial; children=%+v", res.Status, res.Children)
	}
}
