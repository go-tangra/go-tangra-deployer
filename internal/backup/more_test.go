package backup_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/backup"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

func TestExportIncludesJobsAndClock(t *testing.T) {
	ctx := context.Background()
	m := memstore.New()
	cfgID, _ := seed(t, m)
	// A job to appear in the export's job metadata.
	_ = m.InsertJob(ctx, store.DeploymentJob{
		ID: store.NewID(), TenantID: tenantA, TargetConfigurationID: &cfgID,
		CertificateID: "cert-1", Status: store.JobCompleted, TriggeredBy: store.TriggerManual,
	})
	svc := backup.New(m)
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	svc.SetClock(func() time.Time { return fixed })

	b, err := svc.Export(ctx, subjA(), false)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if !b.ExportedAt.Equal(fixed) {
		t.Fatalf("clock not applied: %v", b.ExportedAt)
	}
	if len(b.Jobs) != 1 || b.Jobs[0].CertificateID != "cert-1" {
		t.Fatalf("jobs export: %+v", b.Jobs)
	}
}

func TestImportOverwriteReplacesTarget(t *testing.T) {
	ctx := context.Background()
	m := memstore.New()
	seed(t, m)
	svc := backup.New(m)
	b, _ := svc.Export(ctx, subjA(), true)

	// Tenant B already has a target named "prod" (different id).
	_ = m.InsertTarget(ctx, store.DeploymentTarget{
		ID: store.NewID(), TenantID: tenantB, Name: "prod", AutoDeploy: false,
		CertificateFilters: []store.CertificateFilter{}, ConfigOverrides: map[string]map[string]any{},
	})
	res, err := svc.Import(ctx, subjB(), b, backup.ModeOverwrite)
	if err != nil {
		t.Fatalf("overwrite import: %v", err)
	}
	if res.TargetsImported != 1 {
		t.Fatalf("overwrite result: %+v", res)
	}
	// The imported "prod" is auto-deploy (from tenant A), replacing the manual one.
	targets, _ := m.ListTargets(ctx, tenantB)
	if len(targets) != 1 || !targets[0].AutoDeploy {
		t.Fatalf("target not replaced: %+v", targets)
	}
}
