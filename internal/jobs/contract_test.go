package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// sp returns a pointer to a copy of s (for the optional *string job fields).
func sp(s string) *string { return &s }

// TestContract_ListFilters pins the List() filter contract: each JobFilter field
// narrows the tenant's jobs to the expected subset, and an empty filter returns
// every job. Jobs are seeded directly through the exported Create API so their
// status, trigger, type and certificate are fully controlled.
func TestContract_ListFilters(t *testing.T) {
	_, _, _, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	// direct #1: completed, manual, cert-A.
	direct1 := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: direct1, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-1"),
		CertificateID: "cert-A", Status: store.JobCompleted, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}
	// direct #2: failed, event, cert-B.
	direct2 := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: direct2, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-2"),
		CertificateID: "cert-B", Status: store.JobFailed, MaxRetries: 3, TriggeredBy: store.TriggerEvent,
	}); err != nil {
		t.Fatal(err)
	}
	// parent: pending, manual, cert-A.
	parent := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: parent, TenantID: subj.TenantID, DeploymentTargetID: sp("tgt-1"),
		CertificateID: "cert-A", Status: store.JobPending, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}
	// child of parent: processing, manual, cert-A.
	child := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: child, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-1"), ParentJobID: sp(parent),
		CertificateID: "cert-A", Status: store.JobProcessing, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}

	list := func(f repo.JobFilter) []string {
		t.Helper()
		vs, err := js.List(ctx, subj, f)
		if err != nil {
			t.Fatalf("list %+v: %v", f, err)
		}
		ids := make([]string, 0, len(vs))
		for _, v := range vs {
			ids = append(ids, v.ID)
		}
		return ids
	}
	has := func(ids []string, want string) bool {
		for _, id := range ids {
			if id == want {
				return true
			}
		}
		return false
	}

	// Empty filter → all four jobs.
	if got := list(repo.JobFilter{}); len(got) != 4 {
		t.Fatalf("empty filter = %d jobs, want 4", len(got))
	}

	// Status filter.
	if got := list(repo.JobFilter{Status: store.JobCompleted}); len(got) != 1 || got[0] != direct1 {
		t.Fatalf("status=completed = %v, want [%s]", got, direct1)
	}
	if got := list(repo.JobFilter{Status: store.JobPending}); len(got) != 1 || got[0] != parent {
		t.Fatalf("status=pending = %v, want [%s]", got, parent)
	}

	// TriggeredBy filter.
	if got := list(repo.JobFilter{TriggeredBy: store.TriggerEvent}); len(got) != 1 || got[0] != direct2 {
		t.Fatalf("trigger=event = %v, want [%s]", got, direct2)
	}
	if got := list(repo.JobFilter{TriggeredBy: store.TriggerManual}); len(got) != 3 {
		t.Fatalf("trigger=manual = %d, want 3", len(got))
	}

	// JobType filter (computed: parent | child | direct).
	if got := list(repo.JobFilter{JobType: store.JobTypeParent}); len(got) != 1 || got[0] != parent {
		t.Fatalf("type=parent = %v, want [%s]", got, parent)
	}
	if got := list(repo.JobFilter{JobType: store.JobTypeChild}); len(got) != 1 || got[0] != child {
		t.Fatalf("type=child = %v, want [%s]", got, child)
	}
	if got := list(repo.JobFilter{JobType: store.JobTypeDirect}); len(got) != 2 ||
		!has(got, direct1) || !has(got, direct2) {
		t.Fatalf("type=direct = %v, want direct1+direct2", got)
	}

	// CertificateID filter.
	if got := list(repo.JobFilter{CertificateID: "cert-A"}); len(got) != 3 ||
		!has(got, direct1) || !has(got, parent) || !has(got, child) {
		t.Fatalf("cert=cert-A = %v, want direct1+parent+child", got)
	}
	if got := list(repo.JobFilter{CertificateID: "cert-B"}); len(got) != 1 || got[0] != direct2 {
		t.Fatalf("cert=cert-B = %v, want [%s]", got, direct2)
	}
}

// TestContract_GetResultChildrenAndHistory pins the read contract for a real
// target deploy: after the worker drains the fan-out, GetResult(parent) carries
// the child views under Children, and GetResult(child) carries the per-action
// deploy History.
func TestContract_GetResultChildrenAndHistory(t *testing.T) {
	m, cs, ds, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	cfg, err := cs.Create(ctx, subj, configs.Input{Name: "contract-ep", ProviderType: "dummy"})
	if err != nil {
		t.Fatalf("create config: %v", err)
	}
	tgt := makeTarget(t, ctx, subj, m, cfg.ID, nil)
	parentID, err := ds.DeployToTarget(ctx, subj, "cert-1", tgt, "")
	if err != nil {
		t.Fatalf("deploy to target: %v", err)
	}

	// Drive the worker, advancing an injected clock so scheduled retries become
	// due and every child reaches a terminal state (same pattern as us4_test).
	clk := time.Now()
	js.SetClock(func() time.Time { return clk })
	for i := 0; i < 10; i++ {
		if js.Once(ctx, nil) == 0 && i > 0 {
			break
		}
		clk = clk.Add(time.Hour)
	}

	// Parent result exposes the fanned-out child.
	res, err := js.GetResult(ctx, subj, parentID)
	if err != nil {
		t.Fatalf("get parent result: %v", err)
	}
	if res.Type != store.JobTypeParent {
		t.Fatalf("parent type = %q, want parent", res.Type)
	}
	if len(res.Children) != 1 {
		t.Fatalf("children = %d, want 1", len(res.Children))
	}
	if res.Children[0].Status != store.JobCompleted {
		t.Fatalf("child status = %q, want completed", res.Children[0].Status)
	}

	// The child's own result carries at least one deploy history entry.
	childID := res.Children[0].ID
	cres, err := js.GetResult(ctx, subj, childID)
	if err != nil {
		t.Fatalf("get child result: %v", err)
	}
	if len(cres.History) < 1 {
		t.Fatalf("child history = %d entries, want >= 1", len(cres.History))
	}
	sawDeploy := false
	for _, h := range cres.History {
		if h.Action == store.ActionDeploy {
			sawDeploy = true
		}
	}
	if !sawDeploy {
		t.Fatalf("child history has no deploy entry: %+v", cres.History)
	}
}

// TestContract_CancelCascade pins the cancel contract: cancelling a parent with
// cascade flips the parent and every still-pending child to cancelled, while a
// child already in a terminal state is left untouched; and cancelling a job that
// has already completed is a no-op.
func TestContract_CancelCascade(t *testing.T) {
	_, _, _, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	parent := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: parent, TenantID: subj.TenantID, DeploymentTargetID: sp("tgt-1"),
		CertificateID: "cert-1", Status: store.JobPending, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}
	childA := store.NewID()
	childB := store.NewID()
	childDone := store.NewID()
	for _, id := range []string{childA, childB} {
		if err := js.Create(ctx, store.DeploymentJob{
			ID: id, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-1"), ParentJobID: sp(parent),
			CertificateID: "cert-1", Status: store.JobPending, MaxRetries: 3, TriggeredBy: store.TriggerManual,
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A child that already completed must survive the cascade unchanged.
	if err := js.Create(ctx, store.DeploymentJob{
		ID: childDone, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-2"), ParentJobID: sp(parent),
		CertificateID: "cert-1", Status: store.JobCompleted, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}

	pv, err := js.Cancel(ctx, subj, parent, true)
	if err != nil {
		t.Fatalf("cancel parent: %v", err)
	}
	if pv.Status != store.JobCancelled {
		t.Fatalf("parent status = %q, want cancelled", pv.Status)
	}

	status := func(id string) string {
		t.Helper()
		v, err := js.Get(ctx, subj, id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		return v.Status
	}
	if s := status(childA); s != store.JobCancelled {
		t.Fatalf("childA status = %q, want cancelled", s)
	}
	if s := status(childB); s != store.JobCancelled {
		t.Fatalf("childB status = %q, want cancelled", s)
	}
	if s := status(childDone); s != store.JobCompleted {
		t.Fatalf("completed child status = %q, want completed (cascade must not touch it)", s)
	}

	// Cancelling an already-completed job directly is a no-op.
	done := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: done, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-3"),
		CertificateID: "cert-1", Status: store.JobCompleted, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}
	dv, err := js.Cancel(ctx, subj, done, false)
	if err != nil {
		t.Fatalf("cancel completed: %v", err)
	}
	if dv.Status != store.JobCompleted {
		t.Fatalf("completed job after cancel = %q, want completed (unchanged)", dv.Status)
	}
}

// TestContract_RetryForce pins the retry contract: a failed job with retries
// remaining re-queues to pending (retry_count preserved); a job that exhausted
// its retries is a no-op under force=false, but force=true re-queues it and
// resets retry_count to zero.
func TestContract_RetryForce(t *testing.T) {
	_, _, _, js := newKit(t)
	subj := adminSubj()
	ctx := context.Background()

	// Failed with retries remaining → force=false re-queues to pending.
	retriable := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: retriable, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-1"),
		CertificateID: "cert-1", Status: store.JobFailed, RetryCount: 1, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}
	rv, err := js.Retry(ctx, subj, retriable, false)
	if err != nil {
		t.Fatalf("retry retriable: %v", err)
	}
	if rv.Status != store.JobPending {
		t.Fatalf("retriable status = %q, want pending", rv.Status)
	}
	if rv.RetryCount != 1 {
		t.Fatalf("retriable retry_count = %d, want 1 preserved (non-force)", rv.RetryCount)
	}

	// Retries exhausted → force=false is a no-op, force=true re-queues + resets.
	exhausted := store.NewID()
	if err := js.Create(ctx, store.DeploymentJob{
		ID: exhausted, TenantID: subj.TenantID, TargetConfigurationID: sp("cfg-2"),
		CertificateID: "cert-1", Status: store.JobFailed, RetryCount: 3, MaxRetries: 3, TriggeredBy: store.TriggerManual,
	}); err != nil {
		t.Fatal(err)
	}
	nv, err := js.Retry(ctx, subj, exhausted, false)
	if err != nil {
		t.Fatalf("retry exhausted (no force): %v", err)
	}
	if nv.Status != store.JobFailed {
		t.Fatalf("exhausted status (no force) = %q, want failed (no-op)", nv.Status)
	}
	if nv.RetryCount != 3 {
		t.Fatalf("exhausted retry_count (no force) = %d, want 3 unchanged", nv.RetryCount)
	}

	fv, err := js.Retry(ctx, subj, exhausted, true)
	if err != nil {
		t.Fatalf("retry exhausted (force): %v", err)
	}
	if fv.Status != store.JobPending {
		t.Fatalf("exhausted status (force) = %q, want pending", fv.Status)
	}
	if fv.RetryCount != 0 {
		t.Fatalf("exhausted retry_count (force) = %d, want 0 reset", fv.RetryCount)
	}
}
