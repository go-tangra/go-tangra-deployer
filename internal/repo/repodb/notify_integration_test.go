//go:build integration

package repodb_test

import (
	"context"
	"sync"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// Target notification columns round-trip (defaults included) and the parent-job notification claim wins once per
// distinct status, under RLS and against concurrent claimers.
func TestTargetNotificationsAndClaim(t *testing.T) {
	db := openRepo(t)
	ctx := context.Background()

	plain := store.NewID()
	if err := db.InsertTarget(ctx, store.DeploymentTarget{ID: plain, TenantID: tenantA, Name: "plain"}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if got, err := db.GetTarget(ctx, tenantA, plain); err != nil || got.NotifyEnabled || got.NotifyOn != store.NotifyAlways {
		t.Fatalf("defaults: %+v %v", got, err)
	}
	tid := store.NewID()
	if err := db.InsertTarget(ctx, store.DeploymentTarget{ID: tid, TenantID: tenantA, Name: "edge", NotifyEnabled: true, NotifyOn: store.NotifyFailure}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	got, err := db.GetTarget(ctx, tenantA, tid)
	if err != nil || !got.NotifyEnabled || got.NotifyOn != store.NotifyFailure {
		t.Fatalf("get: %+v %v", got, err)
	}
	got.NotifyEnabled, got.NotifyOn = false, store.NotifyAlways
	if err := db.UpdateTarget(ctx, got); err != nil {
		t.Fatalf("update: %v", err)
	}
	if got, _ := db.GetTarget(ctx, tenantA, tid); got.NotifyEnabled || got.NotifyOn != store.NotifyAlways {
		t.Fatalf("updated: %+v", got)
	}
	if list, err := db.ListTargets(ctx, tenantA); err != nil || len(list) != 2 {
		t.Fatalf("list: %d %v", len(list), err)
	}

	// Claim.
	pid := store.NewID()
	if err := db.InsertJob(ctx, store.DeploymentJob{ID: pid, TenantID: tenantA, DeploymentTargetID: &tid, CertificateID: "c", Status: store.JobPending}); err != nil {
		t.Fatalf("insert job: %v", err)
	}
	claim := func(tenant, status string) bool {
		t.Helper()
		ok, err := db.ClaimJobNotification(ctx, tenant, pid, status)
		if err != nil {
			t.Fatalf("claim: %v", err)
		}
		return ok
	}
	if !claim(tenantA, store.JobPartial) {
		t.Fatal("first claim lost")
	}
	if claim(tenantA, store.JobPartial) {
		t.Fatal("second claim of the same status won")
	}
	j, _ := db.GetJob(ctx, tenantA, pid)
	j.Status = store.JobPartial
	if err := db.UpdateJob(ctx, j); err != nil { // UpdateJob leaves the claim alone
		t.Fatal(err)
	}
	if claim(tenantA, store.JobPartial) {
		t.Fatal("claim after UpdateJob won")
	}
	if claim(tenantB, store.JobCompleted) {
		t.Fatal("RLS breach: tenant B claimed tenant A's job")
	}
	if !claim(tenantA, store.JobCompleted) {
		t.Fatal("claim of a changed status lost")
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, err := db.ClaimJobNotification(ctx, tenantA, pid, store.JobFailed); err == nil && ok {
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
