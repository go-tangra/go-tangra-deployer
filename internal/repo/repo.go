// Package repo is the deployer's storage interface. Services depend on this
// interface; the TimescaleDB implementation (internal/store) and the in-memory
// fake (internal/memstore) both satisfy it. Every method is tenant-scoped —
// the concrete store enforces per-tenant RLS, the fake filters by tenant.
package repo

import (
	"context"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/store"
)

// Store is the deployer persistence surface.
type Store interface {
	// Atomic runs fn in a transaction scoped to tenantID; on error it rolls back.
	Atomic(ctx context.Context, tenantID string, fn func(tx Store) error) error

	// Target configurations.
	InsertConfiguration(ctx context.Context, c store.TargetConfiguration) error
	GetConfiguration(ctx context.Context, tenantID, id string) (store.TargetConfiguration, error)
	ListConfigurations(ctx context.Context, tenantID string, f ConfigFilter) ([]store.TargetConfiguration, error)
	UpdateConfiguration(ctx context.Context, c store.TargetConfiguration) error
	DeleteConfiguration(ctx context.Context, tenantID, id string) error
	SetConfigurationStatus(ctx context.Context, tenantID, id, status, message string, lastDeploy *time.Time) error

	// Deployment targets.
	InsertTarget(ctx context.Context, t store.DeploymentTarget) error
	GetTarget(ctx context.Context, tenantID, id string) (store.DeploymentTarget, error)
	ListTargets(ctx context.Context, tenantID string) ([]store.DeploymentTarget, error)
	ListAutoDeployTargets(ctx context.Context, tenantID string) ([]store.DeploymentTarget, error)
	UpdateTarget(ctx context.Context, t store.DeploymentTarget) error
	DeleteTarget(ctx context.Context, tenantID, id string) error
	// Attach/detach configurations to a target (the M:N link).
	AttachConfigurations(ctx context.Context, tenantID, targetID string, configIDs []string) error
	DetachConfigurations(ctx context.Context, tenantID, targetID string, configIDs []string) error
	ListTargetConfigurationIDs(ctx context.Context, tenantID, targetID string) ([]string, error)

	// Deployment jobs.
	InsertJob(ctx context.Context, j store.DeploymentJob) error
	GetJob(ctx context.Context, tenantID, id string) (store.DeploymentJob, error)
	ListJobs(ctx context.Context, tenantID string, f JobFilter) ([]store.DeploymentJob, error)
	ListChildJobs(ctx context.Context, tenantID, parentID string) ([]store.DeploymentJob, error)
	UpdateJob(ctx context.Context, j store.DeploymentJob) error
	DeleteJobsOlderThan(ctx context.Context, cutoff time.Time) (int, error)
	// ClaimDueJobs leases due pending/retryable jobs (system scope) so exactly
	// one worker runs each (FOR UPDATE SKIP LOCKED in the DB impl).
	ClaimDueJobs(ctx context.Context, now time.Time, lease time.Duration, limit int) ([]store.DeploymentJob, error)

	// Deployment history.
	InsertHistory(ctx context.Context, h store.DeploymentHistory) error
	ListHistory(ctx context.Context, tenantID, jobID string) ([]store.DeploymentHistory, error)

	// Audit (batch insert).
	InsertAuditRows(ctx context.Context, rows []store.AuditRow) error

	// Exists reports whether a resource (deployer_target|deployer_configuration|
	// deployer_job) exists in the tenant (for authz existence-masking).
	Exists(ctx context.Context, tenantID, resourceType, id string) (bool, error)

	// TenantIDs returns every tenant that has deployer data (system scope). Used
	// by the admin system-wide statistics to iterate tenants.
	TenantIDs(ctx context.Context) ([]string, error)

	Close()
}

// ConfigFilter selects configurations.
type ConfigFilter struct {
	ProviderType string
	Status       string
}

// JobFilter selects jobs.
type JobFilter struct {
	TargetID        string
	ConfigurationID string
	CertificateID   string
	Status          string
	TriggeredBy     string
	JobType         string // parent | child | direct
	ParentJobID     string
	Since           time.Time
	Until           time.Time
	Limit           int
	CursorID        string
}
