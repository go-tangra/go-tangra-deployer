package memstore_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

const tenant = "11111111-1111-1111-1111-111111111111"

// The notification claim wins once per distinct (job, status), like the
// conditional UPDATE of the database store, and survives job updates.
func TestClaimJobNotification(t *testing.T) {
	m := memstore.New()
	ctx := context.Background()
	j := store.DeploymentJob{ID: store.NewID(), TenantID: tenant, Status: store.JobPartial}
	if err := m.InsertJob(ctx, j); err != nil {
		t.Fatal(err)
	}
	claim := func(tid, status string) bool {
		t.Helper()
		ok, err := m.ClaimJobNotification(ctx, tid, j.ID, status)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !claim(tenant, store.JobPartial) {
		t.Fatal("first claim lost")
	}
	if claim(tenant, store.JobPartial) {
		t.Fatal("second claim of the same status won")
	}
	if err := m.UpdateJob(ctx, j); err != nil { // UpdateJob does not reset the claim
		t.Fatal(err)
	}
	if claim(tenant, store.JobPartial) {
		t.Fatal("claim after UpdateJob won")
	}
	if !claim(tenant, store.JobCompleted) {
		t.Fatal("claim of a changed status lost")
	}
	if claim("22222222-2222-2222-2222-222222222222", store.JobFailed) {
		t.Fatal("another tenant claimed the job")
	}
	if ok, _ := m.ClaimJobNotification(ctx, tenant, store.NewID(), store.JobFailed); ok {
		t.Fatal("claimed an unknown job")
	}
	// Concurrent claims of one status: exactly one winner.
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _ := m.ClaimJobNotification(ctx, tenant, j.ID, store.JobFailed); ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d concurrent winners, want 1", wins)
	}
}

// Targets default notify_on to always, like the column default.
func TestTargetNotifyDefaults(t *testing.T) {
	m := memstore.New()
	ctx := context.Background()
	tg := store.DeploymentTarget{ID: store.NewID(), TenantID: tenant, Name: "t"}
	if err := m.InsertTarget(ctx, tg); err != nil {
		t.Fatal(err)
	}
	got, _ := m.GetTarget(ctx, tenant, tg.ID)
	if got.NotifyEnabled || got.NotifyOn != store.NotifyAlways {
		t.Fatalf("defaults = %v %q", got.NotifyEnabled, got.NotifyOn)
	}
}
