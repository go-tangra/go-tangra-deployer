package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/deploy"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// A failed job says what it deployed and where, which step failed and who
// reported the error; its parent names the failed configuration and the
// cause, not just "all deployments failed".
func TestFailedJobIsTraceable(t *testing.T) {
	ctx := context.Background()
	m, cs, js, _ := recordingKit(t, fakeCerts{}, jobs.Config{Workers: 2, Lease: time.Minute, RetryDelay: time.Millisecond})
	ds := deploy.New(m, authz.New(m))
	subj := adminSubj()
	cfg, err := cs.Create(ctx, subj, configs.Input{Name: "echo", ProviderType: "test-echoing", Credentials: map[string]any{"token": "t0k3n-value"}})
	if err != nil {
		t.Fatal(err)
	}
	tgt := makeTarget(t, ctx, subj, m, cfg.ID, nil)
	parentID, err := ds.DeployToTarget(ctx, subj, "cert-1", tgt, "")
	if err != nil {
		t.Fatal(err)
	}
	clk := time.Now()
	js.SetClock(func() time.Time { return clk })
	for i := 0; i < 10; i++ {
		js.Once(ctx, nil)
		clk = clk.Add(time.Hour)
	}

	res, err := js.GetResult(ctx, subj, parentID)
	if err != nil || len(res.Children) != 1 {
		t.Fatalf("result = %+v, err = %v", res, err)
	}
	child := res.Children[0]
	if child.Status != store.JobFailed || child.ErrorStage != jobs.StageDeploy || child.ErrorSource != "provider" ||
		child.ConfigurationName != "echo" || child.ProviderType != "test-echoing" || child.TargetName == "" ||
		child.CertificateName != "api.example.com" || child.CertificateSerial != "0A" {
		t.Fatalf("child = %+v", child)
	}
	p := res.View
	if p.Status != store.JobFailed || p.StatusMessage != "all 1 deployments failed" || p.ErrorStage != jobs.StageChildren ||
		p.ErrorSource != "deployer" || p.TargetName != child.TargetName || p.CertificateName != "api.example.com" {
		t.Fatalf("parent = %+v", p)
	}
	if want := "echo (test-echoing) [deploy]: endpoint said 401: invalid token [redacted]"; p.Error != want {
		t.Fatalf("parent error = %q, want %q", p.Error, want)
	}
}

// A certificate lcm cannot hand out fails at the certificate step, reported
// by lcm; the job has no certificate name since it never got the certificate.
func TestCertificateFailureNamesLcm(t *testing.T) {
	ctx := context.Background()
	_, cs, js, rec := recordingKit(t, &flakyCerts{fail: true}, jobs.Config{Workers: 2, Lease: time.Minute})
	subj := adminSubj()
	cfg, _ := cs.Create(ctx, subj, configs.Input{Name: "ep", ProviderType: "dummy"})
	jobID := store.NewID()
	_ = js.Create(ctx, store.DeploymentJob{ID: jobID, TenantID: subj.TenantID, TargetConfigurationID: &cfg.ID, CertificateID: "c1", Status: store.JobPending, TriggeredBy: store.TriggerManual})
	js.Once(ctx, nil)

	v, _ := js.Get(ctx, subj, jobID)
	if v.ErrorStage != jobs.StageCertificate || v.ErrorSource != "lcm" || v.ConfigurationName != "ep" || v.ProviderType != "dummy" || v.CertificateName != "" {
		t.Fatalf("job = %+v", v)
	}
	if ev, ok := rec.last("deployment.failed"); !ok || ev["error_stage"] != jobs.StageCertificate || ev["error_source"] != "lcm" {
		t.Fatalf("deployment.failed event = %v", ev)
	}
}
