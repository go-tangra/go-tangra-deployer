package data

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	entCrud "github.com/tx7do/go-crud/entgo"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenttarget"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/targetconfiguration"
)

// StatisticsRepo handles statistics-related database queries
type StatisticsRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper
}

// NewStatisticsRepo creates a new StatisticsRepo
func NewStatisticsRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *StatisticsRepo {
	return &StatisticsRepo{
		entClient: entClient,
		log:       ctx.NewLoggerHelper("statistics/repo/deployer-service"),
	}
}

// JobStats holds deployment job statistics
type JobStats struct {
	TotalCount      int64
	PendingCount    int64
	ProcessingCount int64
	CompletedCount  int64
	FailedCount     int64
	CancelledCount  int64
	RetryingCount   int64
	PartialCount    int64
	ByStatus        map[string]int64
	ByTriggerType   map[string]int64
}

// JobTimeStats holds job statistics for a time period
type JobTimeStats struct {
	Total     int64
	Succeeded int64
	Failed    int64
}

// TargetStats holds deployment target statistics
type TargetStats struct {
	TotalCount              int64
	AutoDeployEnabledCount  int64
	AutoDeployDisabledCount int64
}

// ConfigurationStats holds target configuration statistics
type ConfigurationStats struct {
	TotalCount     int64
	ActiveCount    int64
	InactiveCount  int64
	ErrorCount     int64
	ByStatus       map[string]int64
	ByProviderType map[string]int64
}

// RecentErrorInfo holds information about a recent error
type RecentErrorInfo struct {
	OccurredAt        time.Time
	JobID             string
	ConfigurationID   string
	ConfigurationName string
	CertificateID     string
	ErrorMessage      string
	TenantID          uint32
	ProviderType      string
}

// GetJobStats returns job statistics
func (r *StatisticsRepo) GetJobStats(ctx context.Context, tenantID *uint32) (*JobStats, error) {
	stats := &JobStats{
		ByStatus:      make(map[string]int64),
		ByTriggerType: make(map[string]int64),
	}

	client := r.entClient.Client()

	baseQuery := func() *ent.DeploymentJobQuery {
		q := client.DeploymentJob.Query()
		if tenantID != nil {
			q = q.Where(deploymentjob.TenantIDEQ(*tenantID))
		}
		return q
	}

	// Total count
	total, err := baseQuery().Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.TotalCount = int64(total)

	// Status counts
	pending, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_PENDING)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.PendingCount = int64(pending)
	stats.ByStatus["pending"] = int64(pending)

	processing, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_PROCESSING)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.ProcessingCount = int64(processing)
	stats.ByStatus["processing"] = int64(processing)

	completed, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_COMPLETED)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.CompletedCount = int64(completed)
	stats.ByStatus["completed"] = int64(completed)

	failed, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_FAILED)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.FailedCount = int64(failed)
	stats.ByStatus["failed"] = int64(failed)

	cancelled, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_CANCELLED)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.CancelledCount = int64(cancelled)
	stats.ByStatus["cancelled"] = int64(cancelled)

	retrying, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_RETRYING)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.RetryingCount = int64(retrying)
	stats.ByStatus["retrying"] = int64(retrying)

	partial, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_PARTIAL)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.PartialCount = int64(partial)
	stats.ByStatus["partial"] = int64(partial)

	// Unspecified count
	unspecified, _ := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_UNSPECIFIED)).Count(ctx)
	stats.ByStatus["unspecified"] = int64(unspecified)

	// Trigger type counts
	manual, _ := baseQuery().Where(deploymentjob.TriggeredByEQ(deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL)).Count(ctx)
	stats.ByTriggerType["manual"] = int64(manual)

	event, _ := baseQuery().Where(deploymentjob.TriggeredByEQ(deploymentjob.TriggeredByTRIGGER_TYPE_EVENT)).Count(ctx)
	stats.ByTriggerType["event"] = int64(event)

	autoRenewal, _ := baseQuery().Where(deploymentjob.TriggeredByEQ(deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL)).Count(ctx)
	stats.ByTriggerType["auto_renewal"] = int64(autoRenewal)

	triggerUnspecified, _ := baseQuery().Where(deploymentjob.TriggeredByEQ(deploymentjob.TriggeredByTRIGGER_TYPE_UNSPECIFIED)).Count(ctx)
	stats.ByTriggerType["unspecified"] = int64(triggerUnspecified)

	return stats, nil
}

// GetJobTimeStats returns job statistics for a time period
func (r *StatisticsRepo) GetJobTimeStats(ctx context.Context, tenantID *uint32, since time.Time) (*JobTimeStats, error) {
	stats := &JobTimeStats{}

	client := r.entClient.Client()

	baseQuery := func() *ent.DeploymentJobQuery {
		q := client.DeploymentJob.Query().Where(deploymentjob.CreateTimeGTE(since))
		if tenantID != nil {
			q = q.Where(deploymentjob.TenantIDEQ(*tenantID))
		}
		return q
	}

	// Total count
	total, err := baseQuery().Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.Total = int64(total)

	// Succeeded (completed)
	succeeded, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_COMPLETED)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.Succeeded = int64(succeeded)

	// Failed
	failed, err := baseQuery().Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_FAILED)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.Failed = int64(failed)

	return stats, nil
}

// GetTargetStats returns deployment target statistics
func (r *StatisticsRepo) GetTargetStats(ctx context.Context, tenantID *uint32) (*TargetStats, error) {
	stats := &TargetStats{}

	client := r.entClient.Client()

	baseQuery := func() *ent.DeploymentTargetQuery {
		q := client.DeploymentTarget.Query()
		if tenantID != nil {
			q = q.Where(deploymenttarget.TenantIDEQ(*tenantID))
		}
		return q
	}

	// Total count
	total, err := baseQuery().Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.TotalCount = int64(total)

	// Auto-deploy enabled count
	autoEnabled, err := baseQuery().Where(deploymenttarget.AutoDeployOnRenewalEQ(true)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.AutoDeployEnabledCount = int64(autoEnabled)

	// Auto-deploy disabled count
	autoDisabled, err := baseQuery().Where(deploymenttarget.AutoDeployOnRenewalEQ(false)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.AutoDeployDisabledCount = int64(autoDisabled)

	return stats, nil
}

// GetConfigurationStats returns target configuration statistics
func (r *StatisticsRepo) GetConfigurationStats(ctx context.Context, tenantID *uint32) (*ConfigurationStats, error) {
	stats := &ConfigurationStats{
		ByStatus:       make(map[string]int64),
		ByProviderType: make(map[string]int64),
	}

	client := r.entClient.Client()

	baseQuery := func() *ent.TargetConfigurationQuery {
		q := client.TargetConfiguration.Query()
		if tenantID != nil {
			q = q.Where(targetconfiguration.TenantIDEQ(*tenantID))
		}
		return q
	}

	// Total count
	total, err := baseQuery().Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.TotalCount = int64(total)

	// Status counts
	active, err := baseQuery().Where(targetconfiguration.StatusEQ(targetconfiguration.StatusCONFIG_STATUS_ACTIVE)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.ActiveCount = int64(active)
	stats.ByStatus["active"] = int64(active)

	inactive, err := baseQuery().Where(targetconfiguration.StatusEQ(targetconfiguration.StatusCONFIG_STATUS_INACTIVE)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.InactiveCount = int64(inactive)
	stats.ByStatus["inactive"] = int64(inactive)

	errorCount, err := baseQuery().Where(targetconfiguration.StatusEQ(targetconfiguration.StatusCONFIG_STATUS_ERROR)).Count(ctx)
	if err != nil {
		return nil, err
	}
	stats.ErrorCount = int64(errorCount)
	stats.ByStatus["error"] = int64(errorCount)

	// Unspecified count
	unspecified, _ := baseQuery().Where(targetconfiguration.StatusEQ(targetconfiguration.StatusCONFIG_STATUS_UNSPECIFIED)).Count(ctx)
	stats.ByStatus["unspecified"] = int64(unspecified)

	// Get configurations to count by provider type
	configs, err := baseQuery().All(ctx)
	if err != nil {
		return nil, err
	}

	for _, cfg := range configs {
		providerType := cfg.ProviderType
		if providerType == "" {
			providerType = "unknown"
		}
		stats.ByProviderType[providerType]++
	}

	return stats, nil
}

// GetRecentErrors returns recent deployment errors
func (r *StatisticsRepo) GetRecentErrors(ctx context.Context, tenantID *uint32, limit int) ([]*RecentErrorInfo, error) {
	client := r.entClient.Client()

	query := client.DeploymentJob.Query().
		Where(deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_FAILED)).
		Order(ent.Desc(deploymentjob.FieldUpdateTime)).
		WithTargetConfiguration().
		Limit(limit)

	if tenantID != nil {
		query = query.Where(deploymentjob.TenantIDEQ(*tenantID))
	}

	jobs, err := query.All(ctx)
	if err != nil {
		return nil, err
	}

	errors := make([]*RecentErrorInfo, 0, len(jobs))
	for _, job := range jobs {
		errorInfo := &RecentErrorInfo{
			JobID:         job.ID,
			CertificateID: job.CertificateID,
			ErrorMessage:  job.StatusMessage,
		}

		// Handle pointer types
		if job.UpdateTime != nil {
			errorInfo.OccurredAt = *job.UpdateTime
		}
		if job.TenantID != nil {
			errorInfo.TenantID = *job.TenantID
		}

		// Get configuration info if available
		if job.Edges.TargetConfiguration != nil {
			errorInfo.ConfigurationID = job.Edges.TargetConfiguration.ID
			errorInfo.ConfigurationName = job.Edges.TargetConfiguration.Name
			errorInfo.ProviderType = job.Edges.TargetConfiguration.ProviderType
		} else if job.TargetConfigurationID != nil {
			errorInfo.ConfigurationID = *job.TargetConfigurationID
		}

		errors = append(errors, errorInfo)
	}

	return errors, nil
}

// GetTenantIDs returns all distinct tenant IDs from the deployer data
func (r *StatisticsRepo) GetTenantIDs(ctx context.Context) ([]uint32, error) {
	client := r.entClient.Client()
	seen := make(map[uint32]bool)
	tenantIDs := make([]uint32, 0)

	// Get tenant IDs from DeploymentJob
	jobs, err := client.DeploymentJob.Query().All(ctx)
	if err == nil {
		for _, job := range jobs {
			if job.TenantID != nil && *job.TenantID > 0 && !seen[*job.TenantID] {
				seen[*job.TenantID] = true
				tenantIDs = append(tenantIDs, *job.TenantID)
			}
		}
	}

	// Get tenant IDs from DeploymentTarget
	targets, err := client.DeploymentTarget.Query().All(ctx)
	if err == nil {
		for _, target := range targets {
			if target.TenantID != nil && *target.TenantID > 0 && !seen[*target.TenantID] {
				seen[*target.TenantID] = true
				tenantIDs = append(tenantIDs, *target.TenantID)
			}
		}
	}

	// Get tenant IDs from TargetConfiguration
	configs, err := client.TargetConfiguration.Query().All(ctx)
	if err == nil {
		for _, cfg := range configs {
			if cfg.TenantID != nil && *cfg.TenantID > 0 && !seen[*cfg.TenantID] {
				seen[*cfg.TenantID] = true
				tenantIDs = append(tenantIDs, *cfg.TenantID)
			}
		}
	}

	return tenantIDs, nil
}
