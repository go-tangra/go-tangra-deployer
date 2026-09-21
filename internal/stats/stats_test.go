package stats_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/memstore"
	"github.com/go-freya/freya/services/deployer/internal/stats"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

const tenantA = "11111111-1111-1111-1111-111111111111"
const tenantB = "22222222-2222-2222-2222-222222222222"

func seed(t *testing.T, m *memstore.Mem, now time.Time) {
	t.Helper()
	ctx := context.Background()
	// Two configs (one active, one error), two providers.
	_ = m.InsertConfiguration(ctx, store.TargetConfiguration{ID: store.NewID(), TenantID: tenantA, Name: "acm", ProviderType: "aws_acm", Status: store.ConfigActive})
	_ = m.InsertConfiguration(ctx, store.TargetConfiguration{ID: store.NewID(), TenantID: tenantA, Name: "cf", ProviderType: "cloudflare", Status: "error"})
	// One auto-deploy target, one manual.
	_ = m.InsertTarget(ctx, store.DeploymentTarget{ID: store.NewID(), TenantID: tenantA, Name: "prod", AutoDeploy: true})
	_ = m.InsertTarget(ctx, store.DeploymentTarget{ID: store.NewID(), TenantID: tenantA, Name: "stage", AutoDeploy: false})
	// Jobs: 2 completed + 1 failed in the last hour; trigger mix.
	mk := func(status, trig string, age time.Duration) {
		comp := now.Add(-age)
		_ = m.InsertJob(ctx, store.DeploymentJob{ID: store.NewID(), TenantID: tenantA, CertificateID: "c1", Status: status, TriggeredBy: trig, CreatedAt: comp, CompletedAt: &comp, StatusMessage: "msg"})
	}
	mk(store.JobCompleted, store.TriggerManual, time.Hour)
	mk(store.JobCompleted, "event", 2*time.Hour)
	mk(store.JobFailed, "event", 30*time.Minute)
}

func TestTenantSnapshot(t *testing.T) {
	m := memstore.New()
	now := time.Now()
	seed(t, m, now)
	svc := stats.New(m)
	svc.SetClock(func() time.Time { return now })

	snap, err := svc.Tenant(context.Background(), authz.Subjects{TenantID: tenantA})
	if err != nil {
		t.Fatalf("tenant: %v", err)
	}
	if snap.JobsTotal != 3 || snap.JobsByStatus[store.JobCompleted] != 2 || snap.JobsByStatus[store.JobFailed] != 1 {
		t.Fatalf("jobs by status: %+v", snap.JobsByStatus)
	}
	if snap.JobsByTrigger["event"] != 2 || snap.JobsByTrigger[store.TriggerManual] != 1 {
		t.Fatalf("jobs by trigger: %+v", snap.JobsByTrigger)
	}
	if snap.TargetsTotal != 2 || snap.AutoDeployTargets != 1 {
		t.Fatalf("targets: total=%d auto=%d", snap.TargetsTotal, snap.AutoDeployTargets)
	}
	if snap.ConfigurationsTotal != 2 || snap.ConfigurationsByProvider["aws_acm"] != 1 || snap.ConfigurationsByStatus["error"] != 1 {
		t.Fatalf("configs: %+v %+v", snap.ConfigurationsByProvider, snap.ConfigurationsByStatus)
	}
	// 3 terminal jobs in 24h, 2 completed -> 2/3.
	if got := snap.SuccessRate24h; got < 0.66 || got > 0.67 {
		t.Fatalf("success rate 24h = %v, want ~0.667", got)
	}
	if len(snap.RecentErrors) != 1 || snap.RecentErrors[0].Message != "msg" {
		t.Fatalf("recent errors: %+v", snap.RecentErrors)
	}
}

func TestSystemWideRequiresAdmin(t *testing.T) {
	m := memstore.New()
	svc := stats.New(m)
	if _, err := svc.SystemWide(context.Background(), authz.Subjects{TenantID: tenantA}); err != authz.ErrForbidden {
		t.Fatalf("non-admin err = %v, want ErrForbidden", err)
	}
}

func TestSystemWideAggregatesTenants(t *testing.T) {
	m := memstore.New()
	now := time.Now()
	seed(t, m, now)
	// A second tenant with one config.
	_ = m.InsertConfiguration(context.Background(), store.TargetConfiguration{ID: store.NewID(), TenantID: tenantB, Name: "b-cf", ProviderType: "cloudflare", Status: store.ConfigActive})
	svc := stats.New(m)
	svc.SetClock(func() time.Time { return now })

	sys, err := svc.SystemWide(context.Background(), authz.Subjects{TenantID: tenantA, Roles: []string{"admin"}})
	if err != nil {
		t.Fatalf("system: %v", err)
	}
	if len(sys.PerTenant) != 2 {
		t.Fatalf("per-tenant breakdown = %d, want 2", len(sys.PerTenant))
	}
	if sys.Snapshot.ConfigurationsTotal != 3 {
		t.Fatalf("system configs total = %d, want 3", sys.Snapshot.ConfigurationsTotal)
	}
	if sys.Snapshot.ConfigurationsByProvider["cloudflare"] != 2 {
		t.Fatalf("system cloudflare count = %d, want 2", sys.Snapshot.ConfigurationsByProvider["cloudflare"])
	}
}
