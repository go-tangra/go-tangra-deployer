// Package repodb binds repo.Store to the database: tenant-scoped calls run in a
// tenant transaction (RLS), system-scoped calls (job claim, cleanup, audit) in
// a system transaction.
package repodb

import (
	"context"
	"time"

	"github.com/go-tangra/go-tangra/v4/listquery"
	"github.com/jackc/pgx/v5"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// DB implements repo.Store over *store.Store.
type DB struct {
	St *store.Store
	tx pgx.Tx // set inside Atomic
}

// New wraps the store.
func New(st *store.Store) *DB { return &DB{St: st} }

func (d *DB) run(ctx context.Context, scope store.Scope, fn func(tx pgx.Tx) error) error {
	if d.tx != nil {
		return fn(d.tx)
	}
	return d.St.Tx(ctx, scope, fn)
}
func (d *DB) tenant(ctx context.Context, tid string, fn func(tx pgx.Tx) error) error {
	return d.run(ctx, store.Scope{TenantID: tid}, fn)
}
func (d *DB) system(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return d.run(ctx, store.Scope{System: true}, fn)
}

// Atomic runs fn in one tenant transaction.
func (d *DB) Atomic(ctx context.Context, tenantID string, fn func(repo.Store) error) error {
	if d.tx != nil {
		return fn(d)
	}
	return d.St.Tx(ctx, store.Scope{TenantID: tenantID}, func(tx pgx.Tx) error { return fn(&DB{St: d.St, tx: tx}) })
}

func (d *DB) Close() { d.St.Close() }

// --- configurations ---

func (d *DB) InsertConfiguration(ctx context.Context, c store.TargetConfiguration) error {
	return d.tenant(ctx, c.TenantID, func(tx pgx.Tx) error { return store.InsertConfiguration(ctx, tx, c) })
}
func (d *DB) GetConfiguration(ctx context.Context, tid, id string) (out store.TargetConfiguration, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.GetConfiguration(ctx, tx, tid, id); return err })
	return
}
func (d *DB) ListConfigurations(ctx context.Context, tid string, f repo.ConfigFilter) (out []store.TargetConfiguration, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error {
		out, err = store.ListConfigurations(ctx, tx, tid, f.ProviderType, f.Status)
		return err
	})
	return
}
func (d *DB) UpdateConfiguration(ctx context.Context, c store.TargetConfiguration) error {
	return d.tenant(ctx, c.TenantID, func(tx pgx.Tx) error { return store.UpdateConfiguration(ctx, tx, c) })
}
func (d *DB) DeleteConfiguration(ctx context.Context, tid, id string) error {
	return d.tenant(ctx, tid, func(tx pgx.Tx) error { return store.DeleteConfiguration(ctx, tx, tid, id) })
}
func (d *DB) SetConfigurationStatus(ctx context.Context, tid, id, status, message string, last *time.Time) error {
	return d.tenant(ctx, tid, func(tx pgx.Tx) error { return store.SetConfigurationStatus(ctx, tx, tid, id, status, message, last) })
}

// --- targets ---

func (d *DB) InsertTarget(ctx context.Context, t store.DeploymentTarget) error {
	return d.tenant(ctx, t.TenantID, func(tx pgx.Tx) error { return store.InsertTarget(ctx, tx, t) })
}
func (d *DB) GetTarget(ctx context.Context, tid, id string) (out store.DeploymentTarget, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.GetTarget(ctx, tx, tid, id); return err })
	return
}
func (d *DB) ListTargets(ctx context.Context, tid string) (out []store.DeploymentTarget, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.ListTargets(ctx, tx, tid, false); return err })
	return
}
func (d *DB) ListAutoDeployTargets(ctx context.Context, tid string) (out []store.DeploymentTarget, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.ListTargets(ctx, tx, tid, true); return err })
	return
}
func (d *DB) UpdateTarget(ctx context.Context, t store.DeploymentTarget) error {
	return d.tenant(ctx, t.TenantID, func(tx pgx.Tx) error { return store.UpdateTarget(ctx, tx, t) })
}
func (d *DB) DeleteTarget(ctx context.Context, tid, id string) error {
	return d.tenant(ctx, tid, func(tx pgx.Tx) error { return store.DeleteTarget(ctx, tx, tid, id) })
}
func (d *DB) AttachConfigurations(ctx context.Context, tid, targetID string, ids []string) error {
	return d.tenant(ctx, tid, func(tx pgx.Tx) error { return store.AttachConfigurations(ctx, tx, tid, targetID, ids) })
}
func (d *DB) DetachConfigurations(ctx context.Context, tid, targetID string, ids []string) error {
	return d.tenant(ctx, tid, func(tx pgx.Tx) error { return store.DetachConfigurations(ctx, tx, tid, targetID, ids) })
}
func (d *DB) ListTargetConfigurationIDs(ctx context.Context, tid, targetID string) (out []string, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.ListTargetConfigurationIDs(ctx, tx, tid, targetID); return err })
	return
}

// --- jobs ---

func (d *DB) InsertJob(ctx context.Context, j store.DeploymentJob) error {
	return d.tenant(ctx, j.TenantID, func(tx pgx.Tx) error { return store.InsertJob(ctx, tx, j) })
}
func (d *DB) GetJob(ctx context.Context, tid, id string) (out store.DeploymentJob, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.GetJob(ctx, tx, tid, id); return err })
	return
}
func (d *DB) ListJobs(ctx context.Context, tid string, f repo.JobFilter) (out []store.DeploymentJob, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.ListJobs(ctx, tx, tid, f.Conds(), f.Limit); return err })
	return
}
func (d *DB) ListChildJobs(ctx context.Context, tid, parentID string) (out []store.DeploymentJob, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.ListChildJobs(ctx, tx, tid, parentID); return err })
	return
}
func (d *DB) UpdateJob(ctx context.Context, j store.DeploymentJob) error {
	return d.tenant(ctx, j.TenantID, func(tx pgx.Tx) error { return store.UpdateJob(ctx, tx, j) })
}
func (d *DB) DeleteJobsOlderThan(ctx context.Context, cutoff time.Time) (n int, err error) {
	err = d.system(ctx, func(tx pgx.Tx) error { n, err = store.DeleteJobsOlderThan(ctx, tx, cutoff); return err })
	return
}
func (d *DB) ClaimDueJobs(ctx context.Context, now time.Time, lease time.Duration, limit int) (out []store.DeploymentJob, err error) {
	err = d.system(ctx, func(tx pgx.Tx) error { out, err = store.ClaimDueJobs(ctx, tx, now, lease, limit); return err })
	return
}

// --- history & audit ---

func (d *DB) InsertHistory(ctx context.Context, h store.DeploymentHistory) error {
	return d.tenant(ctx, h.TenantID, func(tx pgx.Tx) error { return store.InsertHistory(ctx, tx, h) })
}
func (d *DB) ListHistory(ctx context.Context, tid, jobID string) (out []store.DeploymentHistory, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.ListHistory(ctx, tx, tid, jobID); return err })
	return
}
func (d *DB) InsertAuditRows(ctx context.Context, rows []store.AuditRow) error {
	return d.system(ctx, func(tx pgx.Tx) error { return store.InsertAuditRows(ctx, tx, rows) })
}

// --- existence (authz) ---

func (d *DB) Exists(ctx context.Context, tid, resourceType, id string) (ok bool, err error) {
	table := map[string]string{
		"deployer_target": "deployer_targets", "deployer_configuration": "deployer_configs", "deployer_job": "deployer_jobs",
	}[resourceType]
	if table == "" {
		return false, nil
	}
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { ok, err = store.Exists(ctx, tx, tid, table, id); return err })
	return
}

// TenantIDs lists every tenant with deployer data (system scope).
func (d *DB) TenantIDs(ctx context.Context) (out []string, err error) {
	err = d.system(ctx, func(tx pgx.Tx) error { out, err = store.TenantIDs(ctx, tx); return err })
	return
}

// --- paged lists ---

func (d *DB) PageConfigurations(ctx context.Context, tid string, f repo.ConfigFilter, req listquery.Request) (out []store.TargetConfiguration, total int, applied listquery.Request, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error {
		out, total, applied, err = store.PageConfigurations(ctx, tx, tid, f.ProviderType, f.Status, req)
		return err
	})
	return
}
func (d *DB) PageTargets(ctx context.Context, tid string, req listquery.Request) (out []store.DeploymentTarget, total int, applied listquery.Request, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, total, applied, err = store.PageTargets(ctx, tx, tid, req); return err })
	return
}
func (d *DB) PageJobs(ctx context.Context, tid string, f repo.JobFilter, req listquery.Request) (out []store.DeploymentJob, total int, applied listquery.Request, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error {
		out, total, applied, err = store.PageJobs(ctx, tx, tid, f.Conds(), req)
		return err
	})
	return
}
func (d *DB) PageChildJobs(ctx context.Context, tid, parentID string, req listquery.Request) (out []store.DeploymentJob, total int, applied listquery.Request, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error {
		out, total, applied, err = store.PageChildJobs(ctx, tx, tid, parentID, req)
		return err
	})
	return
}
func (d *DB) PageHistory(ctx context.Context, tid, jobID string, req listquery.Request) (out []store.DeploymentHistory, total int, applied listquery.Request, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error {
		out, total, applied, err = store.PageHistory(ctx, tx, tid, jobID, req)
		return err
	})
	return
}
func (d *DB) TargetConfigurationIDsFor(ctx context.Context, tid string, targetIDs []string) (out map[string][]string, err error) {
	err = d.tenant(ctx, tid, func(tx pgx.Tx) error { out, err = store.TargetConfigurationIDsFor(ctx, tx, tid, targetIDs); return err })
	return
}

var _ repo.Store = (*DB)(nil)
