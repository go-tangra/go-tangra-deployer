package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
)

// ---- helpers ----

func jsonOrEmpty(b []byte, empty string) []byte {
	if len(b) == 0 {
		return []byte(empty)
	}
	return b
}

func marshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil || len(b) == 0 {
		return []byte("null")
	}
	return b
}

// ---- target configurations ----

const configCols = "id, tenant_id, name, description, provider_type, config, credentials_sealed, status, status_message, last_deployment_at, created_by, updated_by, created_at, updated_at"

func scanConfig(row pgx.Row) (TargetConfiguration, error) {
	var c TargetConfiguration
	var cfg []byte
	err := row.Scan(&c.ID, &c.TenantID, &c.Name, &c.Description, &c.ProviderType, &cfg, &c.CredentialsSealed,
		&c.Status, &c.StatusMessage, &c.LastDeploymentAt, &c.CreatedBy, &c.UpdatedBy, &c.CreatedAt, &c.UpdatedAt)
	if err != nil {
		return TargetConfiguration{}, notFound(err)
	}
	if len(cfg) > 0 {
		_ = json.Unmarshal(cfg, &c.Config)
	}
	return c, nil
}

// InsertConfiguration stores a configuration.
func InsertConfiguration(ctx context.Context, tx pgx.Tx, c TargetConfiguration) error {
	_, err := tx.Exec(ctx, `INSERT INTO deployer_configs
		(id, tenant_id, name, description, provider_type, config, credentials_sealed, status, status_message, created_by, updated_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$10)`,
		c.ID, c.TenantID, c.Name, c.Description, c.ProviderType, marshalJSON(c.Config), c.CredentialsSealed,
		defaultStr(c.Status, "active"), c.StatusMessage, c.CreatedBy)
	return conflict(err)
}

// GetConfiguration by tenant + id.
func GetConfiguration(ctx context.Context, tx pgx.Tx, tenantID, id string) (TargetConfiguration, error) {
	return scanConfig(tx.QueryRow(ctx, "SELECT "+configCols+" FROM deployer_configs WHERE tenant_id=$1 AND id=$2", tenantID, id))
}

// ListConfigurations with optional provider_type/status filter.
func ListConfigurations(ctx context.Context, tx pgx.Tx, tenantID, providerType, status string) ([]TargetConfiguration, error) {
	rows, err := tx.Query(ctx, "SELECT "+configCols+` FROM deployer_configs
		WHERE tenant_id=$1 AND ($2='' OR provider_type=$2) AND ($3='' OR status=$3) ORDER BY created_at DESC`,
		tenantID, providerType, status)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TargetConfiguration
	for rows.Next() {
		c, err := scanConfig(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// UpdateConfiguration rewrites the mutable columns.
func UpdateConfiguration(ctx context.Context, tx pgx.Tx, c TargetConfiguration) error {
	ct, err := tx.Exec(ctx, `UPDATE deployer_configs SET name=$3, description=$4, provider_type=$5, config=$6,
		credentials_sealed=$7, status=$8, status_message=$9, updated_by=$10, updated_at=now()
		WHERE tenant_id=$1 AND id=$2`,
		c.TenantID, c.ID, c.Name, c.Description, c.ProviderType, marshalJSON(c.Config), c.CredentialsSealed,
		c.Status, c.StatusMessage, c.UpdatedBy)
	if err != nil {
		return conflict(err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// SetConfigurationStatus updates status/message and optionally last_deployment_at.
func SetConfigurationStatus(ctx context.Context, tx pgx.Tx, tenantID, id, status, message string, lastDeploy *time.Time) error {
	ct, err := tx.Exec(ctx, `UPDATE deployer_configs SET status=$3, status_message=$4,
		last_deployment_at=COALESCE($5, last_deployment_at), updated_at=now() WHERE tenant_id=$1 AND id=$2`,
		tenantID, id, status, message, lastDeploy)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteConfiguration removes a configuration.
func DeleteConfiguration(ctx context.Context, tx pgx.Tx, tenantID, id string) error {
	ct, err := tx.Exec(ctx, "DELETE FROM deployer_configs WHERE tenant_id=$1 AND id=$2", tenantID, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ---- targets ----

const targetCols = "id, tenant_id, name, description, auto_deploy, certificate_filters, config_overrides, created_by, updated_by, created_at, updated_at"

func scanTarget(row pgx.Row) (DeploymentTarget, error) {
	var t DeploymentTarget
	var filters, overrides []byte
	err := row.Scan(&t.ID, &t.TenantID, &t.Name, &t.Description, &t.AutoDeploy, &filters, &overrides,
		&t.CreatedBy, &t.UpdatedBy, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return DeploymentTarget{}, notFound(err)
	}
	if len(filters) > 0 {
		_ = json.Unmarshal(filters, &t.CertificateFilters)
	}
	if len(overrides) > 0 {
		_ = json.Unmarshal(overrides, &t.ConfigOverrides)
	}
	return t, nil
}

// InsertTarget stores a target group.
func InsertTarget(ctx context.Context, tx pgx.Tx, t DeploymentTarget) error {
	_, err := tx.Exec(ctx, `INSERT INTO deployer_targets
		(id, tenant_id, name, description, auto_deploy, certificate_filters, config_overrides, created_by, updated_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$8)`,
		t.ID, t.TenantID, t.Name, t.Description, t.AutoDeploy,
		jsonOrEmpty(marshalJSON(t.CertificateFilters), "[]"), jsonOrEmpty(marshalJSON(t.ConfigOverrides), "{}"), t.CreatedBy)
	return conflict(err)
}

// GetTarget by tenant + id.
func GetTarget(ctx context.Context, tx pgx.Tx, tenantID, id string) (DeploymentTarget, error) {
	return scanTarget(tx.QueryRow(ctx, "SELECT "+targetCols+" FROM deployer_targets WHERE tenant_id=$1 AND id=$2", tenantID, id))
}

// ListTargets returns a tenant's targets; onlyAuto restricts to auto-deploy ones.
func ListTargets(ctx context.Context, tx pgx.Tx, tenantID string, onlyAuto bool) ([]DeploymentTarget, error) {
	rows, err := tx.Query(ctx, "SELECT "+targetCols+` FROM deployer_targets
		WHERE tenant_id=$1 AND ($2=false OR auto_deploy=true) ORDER BY created_at DESC`, tenantID, onlyAuto)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentTarget
	for rows.Next() {
		t, err := scanTarget(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// UpdateTarget rewrites the mutable columns.
func UpdateTarget(ctx context.Context, tx pgx.Tx, t DeploymentTarget) error {
	ct, err := tx.Exec(ctx, `UPDATE deployer_targets SET name=$3, description=$4, auto_deploy=$5,
		certificate_filters=$6, config_overrides=$7, updated_by=$8, updated_at=now() WHERE tenant_id=$1 AND id=$2`,
		t.TenantID, t.ID, t.Name, t.Description, t.AutoDeploy,
		jsonOrEmpty(marshalJSON(t.CertificateFilters), "[]"), jsonOrEmpty(marshalJSON(t.ConfigOverrides), "{}"), t.UpdatedBy)
	if err != nil {
		return conflict(err)
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// DeleteTarget removes a target.
func DeleteTarget(ctx context.Context, tx pgx.Tx, tenantID, id string) error {
	ct, err := tx.Exec(ctx, "DELETE FROM deployer_targets WHERE tenant_id=$1 AND id=$2", tenantID, id)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// AttachConfigurations links configurations to a target (idempotent).
func AttachConfigurations(ctx context.Context, tx pgx.Tx, tenantID, targetID string, configIDs []string) error {
	for _, cid := range configIDs {
		if _, err := tx.Exec(ctx, `INSERT INTO deployer_target_configs_link (tenant_id, target_id, configuration_id)
			VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, tenantID, targetID, cid); err != nil {
			return err
		}
	}
	return nil
}

// DetachConfigurations unlinks configurations from a target.
func DetachConfigurations(ctx context.Context, tx pgx.Tx, tenantID, targetID string, configIDs []string) error {
	for _, cid := range configIDs {
		if _, err := tx.Exec(ctx, `DELETE FROM deployer_target_configs_link
			WHERE tenant_id=$1 AND target_id=$2 AND configuration_id=$3`, tenantID, targetID, cid); err != nil {
			return err
		}
	}
	return nil
}

// ListTargetConfigurationIDs returns a target's attached configuration ids.
func ListTargetConfigurationIDs(ctx context.Context, tx pgx.Tx, tenantID, targetID string) ([]string, error) {
	rows, err := tx.Query(ctx, `SELECT configuration_id FROM deployer_target_configs_link
		WHERE tenant_id=$1 AND target_id=$2 ORDER BY configuration_id`, tenantID, targetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// ---- jobs ----

const jobCols = "id, tenant_id, deployment_target_id, target_configuration_id, parent_job_id, certificate_id, certificate_serial, status, status_message, progress, retry_count, max_retries, triggered_by, result, lease_until, started_at, completed_at, next_retry_at, created_at, updated_at"

func scanJob(row pgx.Row) (DeploymentJob, error) {
	var j DeploymentJob
	err := row.Scan(&j.ID, &j.TenantID, &j.DeploymentTargetID, &j.TargetConfigurationID, &j.ParentJobID,
		&j.CertificateID, &j.CertificateSerial, &j.Status, &j.StatusMessage, &j.Progress, &j.RetryCount, &j.MaxRetries,
		&j.TriggeredBy, &j.Result, &j.LeaseUntil, &j.StartedAt, &j.CompletedAt, &j.NextRetryAt, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		return DeploymentJob{}, notFound(err)
	}
	return j, nil
}

// InsertJob stores a job.
func InsertJob(ctx context.Context, tx pgx.Tx, j DeploymentJob) error {
	_, err := tx.Exec(ctx, `INSERT INTO deployer_jobs
		(id, tenant_id, deployment_target_id, target_configuration_id, parent_job_id, certificate_id, certificate_serial,
		 status, status_message, progress, retry_count, max_retries, triggered_by, result, next_retry_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		j.ID, j.TenantID, j.DeploymentTargetID, j.TargetConfigurationID, j.ParentJobID, j.CertificateID, j.CertificateSerial,
		defaultStr(j.Status, "pending"), j.StatusMessage, j.Progress, j.RetryCount, defaultInt(j.MaxRetries, 3),
		defaultStr(j.TriggeredBy, "manual"), j.Result, j.NextRetryAt)
	return err
}

// GetJob by tenant + id.
func GetJob(ctx context.Context, tx pgx.Tx, tenantID, id string) (DeploymentJob, error) {
	return scanJob(tx.QueryRow(ctx, "SELECT "+jobCols+" FROM deployer_jobs WHERE tenant_id=$1 AND id=$2", tenantID, id))
}

// ListChildJobs returns a parent's child jobs.
func ListChildJobs(ctx context.Context, tx pgx.Tx, tenantID, parentID string) ([]DeploymentJob, error) {
	rows, err := tx.Query(ctx, "SELECT "+jobCols+" FROM deployer_jobs WHERE tenant_id=$1 AND parent_job_id=$2 ORDER BY created_at", tenantID, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

func scanJobs(rows pgx.Rows) ([]DeploymentJob, error) {
	var out []DeploymentJob
	for rows.Next() {
		j, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// ListJobs applies the common filters (nil/empty ignored) newest-first.
func ListJobs(ctx context.Context, tx pgx.Tx, tenantID, status, trigger, certID, parentID string, limit int) ([]DeploymentJob, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	rows, err := tx.Query(ctx, "SELECT "+jobCols+` FROM deployer_jobs
		WHERE tenant_id=$1 AND ($2='' OR status=$2) AND ($3='' OR triggered_by=$3) AND ($4='' OR certificate_id=$4)
		AND ($5='' OR parent_job_id=$5::uuid) ORDER BY created_at DESC LIMIT $6`,
		tenantID, status, trigger, certID, parentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// UpdateJob rewrites the mutable columns.
func UpdateJob(ctx context.Context, tx pgx.Tx, j DeploymentJob) error {
	ct, err := tx.Exec(ctx, `UPDATE deployer_jobs SET status=$3, status_message=$4, progress=$5, retry_count=$6,
		triggered_by=$7, result=$8, lease_until=$9, started_at=$10, completed_at=$11, next_retry_at=$12, updated_at=now()
		WHERE tenant_id=$1 AND id=$2`,
		j.TenantID, j.ID, j.Status, j.StatusMessage, j.Progress, j.RetryCount, j.TriggeredBy, j.Result,
		j.LeaseUntil, j.StartedAt, j.CompletedAt, j.NextRetryAt)
	if err != nil {
		return err
	}
	if ct.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}

// ClaimDueJobs leases due pending/retrying jobs (system scope), one winner each.
func ClaimDueJobs(ctx context.Context, tx pgx.Tx, now time.Time, lease time.Duration, limit int) ([]DeploymentJob, error) {
	rows, err := tx.Query(ctx, `WITH due AS (
			SELECT id FROM deployer_jobs
			WHERE status IN ('pending','retrying') AND (next_retry_at IS NULL OR next_retry_at <= $1)
			AND (lease_until IS NULL OR lease_until < $1)
			ORDER BY created_at LIMIT $3 FOR UPDATE SKIP LOCKED)
		UPDATE deployer_jobs j SET status='processing', lease_until=$2, started_at=COALESCE(j.started_at,$1), updated_at=now()
		FROM due WHERE j.id=due.id RETURNING `+jobColsPrefixed("j"), now, now.Add(lease), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanJobs(rows)
}

// DeleteJobsOlderThan prunes jobs created before cutoff (system scope).
func DeleteJobsOlderThan(ctx context.Context, tx pgx.Tx, cutoff time.Time) (int, error) {
	ct, err := tx.Exec(ctx, "DELETE FROM deployer_jobs WHERE created_at < $1", cutoff)
	if err != nil {
		return 0, err
	}
	return int(ct.RowsAffected()), nil
}

// ---- history ----

// InsertHistory records a per-job action.
func InsertHistory(ctx context.Context, tx pgx.Tx, h DeploymentHistory) error {
	_, err := tx.Exec(ctx, `INSERT INTO deployer_history (id, tenant_id, job_id, action, result, message, duration_ms, details)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8)`,
		h.ID, h.TenantID, h.JobID, h.Action, h.Result, h.Message, h.DurationMS, h.Details)
	return err
}

// ListHistory returns a job's action history.
func ListHistory(ctx context.Context, tx pgx.Tx, tenantID, jobID string) ([]DeploymentHistory, error) {
	rows, err := tx.Query(ctx, `SELECT id, tenant_id, job_id, action, result, message, duration_ms, details, created_at
		FROM deployer_history WHERE tenant_id=$1 AND job_id=$2 ORDER BY created_at`, tenantID, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DeploymentHistory
	for rows.Next() {
		var h DeploymentHistory
		if err := rows.Scan(&h.ID, &h.TenantID, &h.JobID, &h.Action, &h.Result, &h.Message, &h.DurationMS, &h.Details, &h.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

// ---- audit ----

// InsertAuditRows batch-inserts audit entries (system scope).
func InsertAuditRows(ctx context.Context, tx pgx.Tx, rows []AuditRow) error {
	for _, r := range rows {
		if _, err := tx.Exec(ctx, `INSERT INTO deployer_audit_events
			(ts, tenant_id, event_type, actor_kind, actor_id, subject_kind, subject_id, outcome, reason, correlation_id, details)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`,
			r.TS, r.TenantID, r.EventType, r.ActorKind, r.ActorID, r.SubjectKind, r.SubjectID, r.Outcome, r.Reason, r.CorrelationID, r.Details); err != nil {
			return err
		}
	}
	return nil
}

// ---- existence (authz) ----

// Exists reports whether a resource exists in the tenant.
// TenantIDs returns every distinct tenant across configs, targets and jobs.
// Runs under a system-scoped transaction so RLS admits the cross-tenant read.
func TenantIDs(ctx context.Context, tx pgx.Tx) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT tenant_id FROM deployer_configs
		UNION SELECT tenant_id FROM deployer_targets
		UNION SELECT tenant_id FROM deployer_jobs`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func Exists(ctx context.Context, tx pgx.Tx, tenantID, table, id string) (bool, error) {
	var one int
	err := tx.QueryRow(ctx, "SELECT 1 FROM "+table+" WHERE tenant_id=$1 AND id=$2", tenantID, id).Scan(&one)
	if err != nil {
		if err == pgx.ErrNoRows {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func defaultStr(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
func defaultInt(n, d int) int {
	if n == 0 {
		return d
	}
	return n
}

func jobColsPrefixed(alias string) string {
	// jobCols with an alias prefix for the UPDATE ... RETURNING.
	cols := []string{"id", "tenant_id", "deployment_target_id", "target_configuration_id", "parent_job_id", "certificate_id", "certificate_serial", "status", "status_message", "progress", "retry_count", "max_retries", "triggered_by", "result", "lease_until", "started_at", "completed_at", "next_retry_at", "created_at", "updated_at"}
	out := ""
	for i, c := range cols {
		if i > 0 {
			out += ", "
		}
		out += alias + "." + c
	}
	return out
}
