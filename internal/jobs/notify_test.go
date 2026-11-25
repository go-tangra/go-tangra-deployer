package jobs_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/notify"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
)

// fakeNotifier records the outcomes handed to it.
type fakeNotifier struct {
	mu  sync.Mutex
	got []notify.Outcome
}

func (f *fakeNotifier) Notify(o notify.Outcome) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.got = append(f.got, o)
	return true
}

func (f *fakeNotifier) outcomes() []notify.Outcome {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]notify.Outcome(nil), f.got...)
}

func ptr[T any](v T) *T { return &v }

// notifyKit builds a target with two dummy configurations and the given
// notification settings, deploys a certificate to it and returns the parent
// job id.
func notifyKit(t *testing.T, enabled bool, on string) (*memstore.Mem, *jobs.Service, *fakeNotifier, authz.Subjects, string) {
	t.Helper()
	m, cs, ds, js := newKit(t)
	fn := &fakeNotifier{}
	js.SetNotifier(fn)
	subj := adminSubj()
	ctx := context.Background()
	ts := targets.New(m, authz.New(m))
	tv, err := ts.Create(ctx, subj, targets.Input{Name: "edge", NotifyEnabled: ptr(enabled), NotifyOn: ptr(on)})
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, n := range []string{"alpha", "beta"} {
		c, err := cs.Create(ctx, subj, configs.Input{Name: n, ProviderType: "dummy"})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, c.ID)
	}
	if err := ts.Attach(ctx, subj, tv.ID, ids, nil); err != nil {
		t.Fatal(err)
	}
	pid, err := ds.DeployToTarget(ctx, subj, "cert-1", tv.ID, store.TriggerEvent)
	if err != nil {
		t.Fatal(err)
	}
	return m, js, fn, subj, pid
}

// setChildren forces the children's statuses (in creation order) and
// re-aggregates the parent.
func setChildren(t *testing.T, m *memstore.Mem, js *jobs.Service, subj authz.Subjects, pid string, statuses ...string) {
	t.Helper()
	ctx := context.Background()
	kids, err := m.ListChildJobs(ctx, subj.TenantID, pid)
	if err != nil || len(kids) != len(statuses) {
		t.Fatalf("children: %d %v", len(kids), err)
	}
	for i, k := range kids {
		k.Status = statuses[i]
		if k.Status == store.JobFailed {
			k.StatusMessage = "endpoint refused\nthe certificate"
		}
		if err := m.UpdateJob(ctx, k); err != nil {
			t.Fatal(err)
		}
	}
	aggregate(t, m, js, subj, pid)
}

func aggregate(t *testing.T, m repo.Store, js *jobs.Service, subj authz.Subjects, pid string) {
	t.Helper()
	p, err := m.GetJob(context.Background(), subj.TenantID, pid)
	if err != nil {
		t.Fatal(err)
	}
	jobs.AggregateParent(js, context.Background(), p)
}

// A target run that finishes notifies once, with the run's outcome.
func TestNotify_FinishedRunNotifiesOnce(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, true, store.NotifyAlways)
	ctx := context.Background()
	for range 3 {
		js.Once(ctx, nil)
	}
	if p, _ := m.GetJob(ctx, subj.TenantID, pid); p.Status != store.JobCompleted {
		t.Fatalf("parent = %q", p.Status)
	}
	got := fn.outcomes()
	if len(got) != 1 {
		t.Fatalf("notified %d times, want 1", len(got))
	}
	o := got[0]
	if o.JobID != pid || o.TenantID != subj.TenantID || o.TargetName != "edge" || o.Status != store.JobCompleted ||
		o.TriggeredBy != store.TriggerEvent || o.Total != 2 || o.Succeeded != 2 || o.Failed != 0 || o.Certificate != "api.example.com" || o.FinishedAt.IsZero() {
		t.Fatalf("outcome = %+v", o)
	}
	if len(o.Results) != 2 || o.Results[0].Status != store.JobCompleted || (o.Results[0].Configuration != "alpha" && o.Results[0].Configuration != "beta") {
		t.Fatalf("results = %+v", o.Results)
	}
	// Aggregating the same outcome again — concurrently — sends nothing more.
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			aggregate(t, m, js, subj, pid)
		}()
	}
	wg.Wait()
	if n := len(fn.outcomes()); n != 1 {
		t.Fatalf("notified %d times after re-aggregation, want 1", n)
	}
}

// Children finishing together (their aggregations racing) notify once.
func TestNotify_ConcurrentFinishNotifiesOnce(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, true, store.NotifyAlways)
	ctx := context.Background()
	kids, _ := m.ListChildJobs(ctx, subj.TenantID, pid)
	for _, k := range kids {
		k.Status = store.JobCompleted
		_ = m.UpdateJob(ctx, k)
	}
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			aggregate(t, m, js, subj, pid)
		}()
	}
	wg.Wait()
	if n := len(fn.outcomes()); n != 1 {
		t.Fatalf("notified %d times, want 1", n)
	}
}

func TestNotify_DisabledTargetSendsNothing(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, false, store.NotifyAlways)
	setChildren(t, m, js, subj, pid, store.JobFailed, store.JobFailed)
	if n := len(fn.outcomes()); n != 0 {
		t.Fatalf("notified %d times, want 0", n)
	}
}

// notify_on failure skips a completed run and reports a partial one.
func TestNotify_FailureOnlyFiltersCompleted(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, true, store.NotifyFailure)
	setChildren(t, m, js, subj, pid, store.JobCompleted, store.JobCompleted)
	if n := len(fn.outcomes()); n != 0 {
		t.Fatalf("completed run notified %d times, want 0", n)
	}
	setChildren(t, m, js, subj, pid, store.JobCompleted, store.JobFailed)
	got := fn.outcomes()
	if len(got) != 1 || got[0].Status != store.JobPartial || got[0].Succeeded != 1 || got[0].Failed != 1 {
		t.Fatalf("outcomes = %+v", got)
	}
	var failed []notify.Result
	for _, r := range got[0].Results {
		if r.Status == store.JobFailed {
			failed = append(failed, r)
		}
	}
	if len(failed) != 1 || failed[0].Message == "" || failed[0].Configuration == "" {
		t.Fatalf("results = %+v", got[0].Results)
	}
	// Retried into success: filtered out again.
	setChildren(t, m, js, subj, pid, store.JobCompleted, store.JobCompleted)
	if n := len(fn.outcomes()); n != 1 {
		t.Fatalf("notified %d times, want 1", n)
	}
}

// A retry that changes the outcome sends one update; the same outcome again
// sends nothing.
func TestNotify_ChangedOutcomeNotifiesAgain(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, true, store.NotifyAlways)
	setChildren(t, m, js, subj, pid, store.JobFailed, store.JobCompleted)
	setChildren(t, m, js, subj, pid, store.JobFailed, store.JobCompleted)
	setChildren(t, m, js, subj, pid, store.JobCompleted, store.JobCompleted)
	aggregate(t, m, js, subj, pid)
	got := fn.outcomes()
	if len(got) != 2 || got[0].Status != store.JobPartial || got[1].Status != store.JobCompleted {
		t.Fatalf("outcomes = %+v", got)
	}
}

// A run still in progress sends nothing.
func TestNotify_UnfinishedRunSendsNothing(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, true, store.NotifyAlways)
	setChildren(t, m, js, subj, pid, store.JobCompleted, store.JobProcessing)
	if n := len(fn.outcomes()); n != 0 {
		t.Fatalf("notified %d times, want 0", n)
	}
}

// A run the user cancels is not reported, not even when a child that was
// still running finishes afterwards.
func TestNotify_CancelledRunSendsNothing(t *testing.T) {
	m, js, fn, subj, pid := notifyKit(t, true, store.NotifyAlways)
	ctx := context.Background()
	if _, err := js.Cancel(ctx, subj, pid, true); err != nil {
		t.Fatal(err)
	}
	if n := len(fn.outcomes()); n != 0 {
		t.Fatalf("cancel notified %d times, want 0", n)
	}
	kids, _ := m.ListChildJobs(ctx, subj.TenantID, pid)
	kids[0].Status = store.JobCompleted
	_ = m.UpdateJob(ctx, kids[0])
	aggregate(t, m, js, subj, pid)
	if n := len(fn.outcomes()); n != 0 {
		t.Fatalf("late child notified %d times, want 0", n)
	}
}

func TestNotifyWanted(t *testing.T) {
	on := func(enabled bool, when string) store.DeploymentTarget {
		return store.DeploymentTarget{NotifyEnabled: enabled, NotifyOn: when}
	}
	cases := []struct {
		tgt    store.DeploymentTarget
		status string
		want   bool
	}{
		{on(false, store.NotifyAlways), store.JobFailed, false},
		{on(true, store.NotifyAlways), store.JobCompleted, true},
		{on(true, store.NotifyAlways), store.JobPartial, true},
		{on(true, store.NotifyAlways), store.JobFailed, true},
		{on(true, store.NotifyAlways), store.JobCancelled, false},
		{on(true, store.NotifyFailure), store.JobCompleted, false},
		{on(true, store.NotifyFailure), store.JobPartial, true},
		{on(true, store.NotifyFailure), store.JobFailed, true},
		{on(true, ""), store.JobCompleted, true}, // legacy row: always
	}
	for _, c := range cases {
		if got := jobs.NotifyWanted(c.tgt, c.status); got != c.want {
			t.Errorf("NotifyWanted(%+v, %s) = %v, want %v", c.tgt, c.status, got, c.want)
		}
	}
}
