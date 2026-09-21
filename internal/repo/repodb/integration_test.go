//go:build integration

// Package repodb integration test: exercises the TimescaleDB-backed store and
// its repo wrappers against a real database (testcontainers), covering the SQL
// layer (store.Open/Migrate/Tx, the per-entity SQL funcs, and the repodb
// tenant/system scoping) and per-tenant row-level security. Run with:
//
//	go test -tags integration ./internal/repo/repodb/
//
// It skips cleanly when Docker/testcontainers is unavailable.
package repodb_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/repo/repodb"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

const (
	tenantA = "0190f7c2-6a3e-7c1a-9b2e-2f6f9d1b4c55"
	tenantB = "0190f7c2-6a3e-7c1a-9b2e-2f6f9d1b4c66"
)

func startDB(t *testing.T) (adminDSN, appDSN string) {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "timescale/timescaledb:latest-pg16", ExposedPorts: []string{"5432/tcp"},
			Env:        map[string]string{"POSTGRES_PASSWORD": "test", "POSTGRES_DB": "deployer"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
		}, Started: true,
	})
	if err != nil {
		t.Skipf("testcontainers unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432/tcp")
	adminDSN = "postgres://postgres:test@" + host + ":" + port.Port() + "/deployer?sslmode=disable"
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Exec(ctx, "CREATE ROLE deployer_app LOGIN PASSWORD 'app' NOBYPASSRLS")
	_ = conn.Close(ctx)
	appDSN = "postgres://deployer_app:app@" + host + ":" + port.Port() + "/deployer?sslmode=disable"
	return
}

func openRepo(t *testing.T) repo.Store {
	t.Helper()
	adminDSN, appDSN := startDB(t)
	ctx := context.Background()
	if err := store.Migrate(ctx, adminDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	if err := store.Migrate(ctx, adminDSN); err != nil { // idempotent
		t.Fatalf("migrate idempotent: %v", err)
	}
	st, err := store.Open(ctx, appDSN, 4)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(st.Close)
	return repodb.New(st)
}

func TestRepoConfigurationsTargetsJobs(t *testing.T) {
	db := openRepo(t)
	ctx := context.Background()

	// Configuration CRUD.
	cfgID := store.NewID()
	if err := db.InsertConfiguration(ctx, store.TargetConfiguration{
		ID: cfgID, TenantID: tenantA, Name: "acm", ProviderType: "aws_acm",
		Config: map[string]any{"region": "eu"}, CredentialsSealed: []byte("sealed"), Status: store.ConfigActive,
	}); err != nil {
		t.Fatalf("insert config: %v", err)
	}
	got, err := db.GetConfiguration(ctx, tenantA, cfgID)
	if err != nil || got.Name != "acm" || string(got.CredentialsSealed) != "sealed" {
		t.Fatalf("get config: %+v %v", got, err)
	}
	got.Description = "updated"
	if err := db.UpdateConfiguration(ctx, got); err != nil {
		t.Fatalf("update config: %v", err)
	}
	if list, err := db.ListConfigurations(ctx, tenantA, repo.ConfigFilter{ProviderType: "aws_acm"}); err != nil || len(list) != 1 {
		t.Fatalf("list config: %d %v", len(list), err)
	}
	now := time.Now()
	if err := db.SetConfigurationStatus(ctx, tenantA, cfgID, store.ConfigActive, "", &now); err != nil {
		t.Fatalf("set status: %v", err)
	}

	// RLS: tenant B cannot see tenant A's configuration.
	if _, err := db.GetConfiguration(ctx, tenantB, cfgID); err == nil {
		t.Fatal("RLS breach: tenant B read tenant A config")
	}

	// Target + attach.
	tgtID := store.NewID()
	if err := db.InsertTarget(ctx, store.DeploymentTarget{
		ID: tgtID, TenantID: tenantA, Name: "prod", AutoDeploy: true,
		CertificateFilters: []store.CertificateFilter{}, ConfigOverrides: map[string]map[string]any{},
	}); err != nil {
		t.Fatalf("insert target: %v", err)
	}
	if err := db.AttachConfigurations(ctx, tenantA, tgtID, []string{cfgID}); err != nil {
		t.Fatalf("attach: %v", err)
	}
	if ids, err := db.ListTargetConfigurationIDs(ctx, tenantA, tgtID); err != nil || len(ids) != 1 || ids[0] != cfgID {
		t.Fatalf("target config ids: %v %v", ids, err)
	}
	if auto, err := db.ListAutoDeployTargets(ctx, tenantA); err != nil || len(auto) != 1 {
		t.Fatalf("auto-deploy targets: %d %v", len(auto), err)
	}

	// Job hierarchy: parent + child.
	parentID, childID := store.NewID(), store.NewID()
	if err := db.Atomic(ctx, tenantA, func(tx repo.Store) error {
		if e := tx.InsertJob(ctx, store.DeploymentJob{ID: parentID, TenantID: tenantA, DeploymentTargetID: &tgtID, CertificateID: "c1", Status: store.JobPending, MaxRetries: 3, TriggeredBy: store.TriggerManual}); e != nil {
			return e
		}
		return tx.InsertJob(ctx, store.DeploymentJob{ID: childID, TenantID: tenantA, TargetConfigurationID: &cfgID, ParentJobID: &parentID, CertificateID: "c1", Status: store.JobPending, MaxRetries: 3, TriggeredBy: store.TriggerManual})
	}); err != nil {
		t.Fatalf("atomic insert jobs: %v", err)
	}
	if kids, err := db.ListChildJobs(ctx, tenantA, parentID); err != nil || len(kids) != 1 {
		t.Fatalf("child jobs: %d %v", len(kids), err)
	}
	if jl, err := db.ListJobs(ctx, tenantA, repo.JobFilter{JobType: "parent"}); err != nil || len(jl) != 1 {
		t.Fatalf("list parent jobs: %d %v", len(jl), err)
	}

	// History.
	if err := db.InsertHistory(ctx, store.DeploymentHistory{ID: store.NewID(), TenantID: tenantA, JobID: childID, Action: store.ActionDeploy, Result: store.ResultSuccess, DurationMS: 5}); err != nil {
		t.Fatalf("insert history: %v", err)
	}
	if h, err := db.ListHistory(ctx, tenantA, childID); err != nil || len(h) != 1 {
		t.Fatalf("history: %d %v", len(h), err)
	}

	// Exists + TenantIDs (system scope).
	if ok, err := db.Exists(ctx, tenantA, "deployer_configuration", cfgID); err != nil || !ok {
		t.Fatalf("exists: %v %v", ok, err)
	}
	if ids, err := db.TenantIDs(ctx); err != nil || len(ids) == 0 {
		t.Fatalf("tenant ids: %v %v", ids, err)
	}

	// Claim due jobs (system scope, single-winner lease).
	if due, err := db.ClaimDueJobs(ctx, time.Now(), time.Minute, 10); err != nil || len(due) == 0 {
		t.Fatalf("claim due: %d %v", len(due), err)
	}

	// Audit batch insert (system scope).
	if err := db.InsertAuditRows(ctx, []store.AuditRow{{TS: time.Now(), TenantID: tenantA, EventType: "configuration_created", ActorKind: "user", Outcome: "ok"}}); err != nil {
		t.Fatalf("audit insert: %v", err)
	}

	// Detach + delete.
	if err := db.DetachConfigurations(ctx, tenantA, tgtID, []string{cfgID}); err != nil {
		t.Fatalf("detach: %v", err)
	}
	if err := db.DeleteTarget(ctx, tenantA, tgtID); err != nil {
		t.Fatalf("delete target: %v", err)
	}
	if err := db.DeleteConfiguration(ctx, tenantA, cfgID); err != nil {
		t.Fatalf("delete config: %v", err)
	}
	if _, err := db.GetConfiguration(ctx, tenantA, cfgID); err == nil {
		t.Fatal("config still present after delete")
	}
}
