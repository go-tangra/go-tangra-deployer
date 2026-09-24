package jobs_test

import (
	"context"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// US3: a completed job exposes its result and per-action history.
func TestUS3_GetResultHasHistory(t *testing.T) {
	_, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "res-ep", ProviderType: "dummy"})
	jobID, _ := ds.Deploy(ctx, subj, "cert-1", cfg.ID, "")
	if n := js.Once(ctx, nil); n != 1 {
		t.Fatalf("processed %d, want 1", n)
	}
	res, err := js.GetResult(ctx, subj, jobID)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	if res.Status != store.JobCompleted {
		t.Fatalf("status = %q", res.Status)
	}
	if len(res.History) != 1 || res.History[0].Action != store.ActionDeploy || res.History[0].Result != store.ResultSuccess {
		t.Fatalf("history = %+v", res.History)
	}
}

// US3: verify calls the provider and records a verify-history entry.
func TestUS3_VerifySuccessAndMismatch(t *testing.T) {
	_, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	// Passing config: verify succeeds.
	ok, _ := cs.Create(ctx, subj, configs.Input{Name: "ok-ep", ProviderType: "dummy"})
	okJob, _ := ds.Deploy(ctx, subj, "cert-1", ok.ID, "")
	js.Once(ctx, nil)
	vr, err := js.Verify(ctx, subj, okJob)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !vr.Success || vr.Action != store.ActionVerify {
		t.Fatalf("verify result = %+v", vr)
	}

	// Failing config: verify reports mismatch (but no error).
	bad, _ := cs.Create(ctx, subj, configs.Input{Name: "bad-ep", ProviderType: "dummy", Config: map[string]any{"fail": true}})
	badJob := store.NewID()
	// Create a completed direct job by hand so verify has something to act on.
	if err := js.Create(ctx, store.DeploymentJob{ID: badJob, TenantID: subj.TenantID, TargetConfigurationID: &bad.ID, CertificateID: "cert-1", Status: store.JobCompleted, MaxRetries: 3, TriggeredBy: store.TriggerManual}); err != nil {
		t.Fatal(err)
	}
	vr2, err := js.Verify(ctx, subj, badJob)
	if err != nil {
		t.Fatalf("verify bad: %v", err)
	}
	if vr2.Success {
		t.Fatalf("verify of failing config should not succeed: %+v", vr2)
	}
	// History for the bad job records the verify failure.
	res, _ := js.GetResult(ctx, subj, badJob)
	if len(res.History) != 1 || res.History[0].Action != store.ActionVerify || res.History[0].Result != store.ResultFailure {
		t.Fatalf("bad verify history = %+v", res.History)
	}
}

// US3: rollback runs where the provider supports it.
func TestUS3_Rollback(t *testing.T) {
	_, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "rb-ep", ProviderType: "dummy"})
	jobID, _ := ds.Deploy(ctx, subj, "cert-1", cfg.ID, "")
	js.Once(ctx, nil)
	rr, err := js.Rollback(ctx, subj, jobID)
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if !rr.Success || rr.Action != store.ActionRollback {
		t.Fatalf("rollback result = %+v", rr)
	}
}

// US3: a parent job has no provider, so verify/rollback are unsupported.
func TestUS3_VerifyParentUnsupported(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "p-ep", ProviderType: "dummy"})
	tgt := makeTarget(t, ctx, subj, m, cfg.ID, nil)
	parentID, err := ds.DeployToTarget(ctx, subj, "cert-1", tgt, "")
	if err != nil {
		t.Fatalf("deploy to target: %v", err)
	}
	if _, err := js.Verify(ctx, subj, parentID); err != jobs.ErrUnsupported {
		t.Fatalf("verify parent err = %v, want ErrUnsupported", err)
	}
}
