package service

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

// StatisticsService implements the DeployerStatisticsService gRPC service
type StatisticsService struct {
	deployerV1.UnimplementedDeployerStatisticsServiceServer

	statsRepo *data.StatisticsRepo
	log       *log.Helper
}

// NewStatisticsService creates a new StatisticsService
func NewStatisticsService(ctx *bootstrap.Context, statsRepo *data.StatisticsRepo) *StatisticsService {
	return &StatisticsService{
		statsRepo: statsRepo,
		log:       ctx.NewLoggerHelper("deployer/service/statistics"),
	}
}

// GetStatistics returns comprehensive statistics about the Deployer system
func (s *StatisticsService) GetStatistics(ctx context.Context, req *deployerV1.GetStatisticsRequest) (*deployerV1.GetStatisticsResponse, error) {
	s.log.Info("GetStatistics called")

	// Set defaults
	recentErrorsLimit := int32(10)
	if req.RecentErrorsLimit != nil {
		recentErrorsLimit = *req.RecentErrorsLimit
	}

	// Optional tenant filter
	var tenantID *uint32
	if req.TenantId != nil {
		tenantID = req.TenantId
	}

	response := &deployerV1.GetStatisticsResponse{
		GeneratedAt: timestamppb.Now(),
	}

	// Get job statistics
	jobStats, err := s.statsRepo.GetJobStats(ctx, tenantID)
	if err != nil {
		s.log.Errorf("Failed to get job stats: %v", err)
	} else {
		// Get time-based stats
		last24Hours := time.Now().Add(-24 * time.Hour)
		last7Days := time.Now().Add(-7 * 24 * time.Hour)

		stats24h, _ := s.statsRepo.GetJobTimeStats(ctx, tenantID, last24Hours)
		stats7d, _ := s.statsRepo.GetJobTimeStats(ctx, tenantID, last7Days)

		response.Jobs = &deployerV1.JobStatistics{
			TotalCount:      jobStats.TotalCount,
			PendingCount:    jobStats.PendingCount,
			ProcessingCount: jobStats.ProcessingCount,
			CompletedCount:  jobStats.CompletedCount,
			FailedCount:     jobStats.FailedCount,
			CancelledCount:  jobStats.CancelledCount,
			RetryingCount:   jobStats.RetryingCount,
			PartialCount:    jobStats.PartialCount,
			ByStatus:        jobStats.ByStatus,
			ByTriggerType:   jobStats.ByTriggerType,
		}

		if stats24h != nil {
			successRate := float64(0)
			if stats24h.Total > 0 {
				successRate = float64(stats24h.Succeeded) / float64(stats24h.Total) * 100
			}
			response.Jobs.Last_24Hours = &deployerV1.JobTimeBreakdown{
				Total:       stats24h.Total,
				Succeeded:   stats24h.Succeeded,
				Failed:      stats24h.Failed,
				SuccessRate: successRate,
			}
		}

		if stats7d != nil {
			successRate := float64(0)
			if stats7d.Total > 0 {
				successRate = float64(stats7d.Succeeded) / float64(stats7d.Total) * 100
			}
			response.Jobs.Last_7Days = &deployerV1.JobTimeBreakdown{
				Total:       stats7d.Total,
				Succeeded:   stats7d.Succeeded,
				Failed:      stats7d.Failed,
				SuccessRate: successRate,
			}
		}
	}

	// Get target statistics
	targetStats, err := s.statsRepo.GetTargetStats(ctx, tenantID)
	if err != nil {
		s.log.Errorf("Failed to get target stats: %v", err)
	} else {
		response.Targets = &deployerV1.TargetStatistics{
			TotalCount:              targetStats.TotalCount,
			AutoDeployEnabledCount:  targetStats.AutoDeployEnabledCount,
			AutoDeployDisabledCount: targetStats.AutoDeployDisabledCount,
		}
	}

	// Get configuration statistics
	configStats, err := s.statsRepo.GetConfigurationStats(ctx, tenantID)
	if err != nil {
		s.log.Errorf("Failed to get configuration stats: %v", err)
	} else {
		response.Configurations = &deployerV1.ConfigurationStatistics{
			TotalCount:     configStats.TotalCount,
			ActiveCount:    configStats.ActiveCount,
			InactiveCount:  configStats.InactiveCount,
			ErrorCount:     configStats.ErrorCount,
			ByStatus:       configStats.ByStatus,
			ByProviderType: configStats.ByProviderType,
		}
	}

	// Get recent errors
	recentErrors, err := s.statsRepo.GetRecentErrors(ctx, tenantID, int(recentErrorsLimit))
	if err != nil {
		s.log.Errorf("Failed to get recent errors: %v", err)
	} else {
		response.RecentErrors = make([]*deployerV1.RecentError, 0, len(recentErrors))
		for _, e := range recentErrors {
			response.RecentErrors = append(response.RecentErrors, &deployerV1.RecentError{
				OccurredAt:        timestamppb.New(e.OccurredAt),
				JobId:             e.JobID,
				ConfigurationId:   e.ConfigurationID,
				ConfigurationName: e.ConfigurationName,
				CertificateId:     e.CertificateID,
				ErrorMessage:      e.ErrorMessage,
				TenantId:          e.TenantID,
				ProviderType:      e.ProviderType,
			})
		}
	}

	// Get tenant breakdown (only if no specific tenant filter is applied)
	if tenantID == nil {
		tenantIDs, err := s.statsRepo.GetTenantIDs(ctx)
		if err != nil {
			s.log.Errorf("Failed to get tenant IDs: %v", err)
		} else {
			response.TenantBreakdown = make([]*deployerV1.TenantStatistics, 0, len(tenantIDs))
			for _, tid := range tenantIDs {
				tenantStats, err := s.getTenantStatistics(ctx, tid)
				if err != nil {
					s.log.Errorf("Failed to get tenant stats for tenant %d: %v", tid, err)
					continue
				}
				response.TenantBreakdown = append(response.TenantBreakdown, tenantStats)
			}
		}
	}

	return response, nil
}

// GetTenantStatistics returns statistics for a specific tenant
func (s *StatisticsService) GetTenantStatistics(ctx context.Context, req *deployerV1.GetTenantStatisticsRequest) (*deployerV1.TenantStatistics, error) {
	s.log.Infof("GetTenantStatistics called for tenant %d", req.TenantId)

	return s.getTenantStatistics(ctx, req.TenantId)
}

// getTenantStatistics is a helper function to get statistics for a specific tenant
func (s *StatisticsService) getTenantStatistics(ctx context.Context, tenantID uint32) (*deployerV1.TenantStatistics, error) {
	tid := tenantID
	result := &deployerV1.TenantStatistics{
		TenantId: tenantID,
	}

	// Get job statistics
	jobStats, err := s.statsRepo.GetJobStats(ctx, &tid)
	if err == nil {
		result.Jobs = &deployerV1.JobStatistics{
			TotalCount:      jobStats.TotalCount,
			PendingCount:    jobStats.PendingCount,
			ProcessingCount: jobStats.ProcessingCount,
			CompletedCount:  jobStats.CompletedCount,
			FailedCount:     jobStats.FailedCount,
			CancelledCount:  jobStats.CancelledCount,
			RetryingCount:   jobStats.RetryingCount,
			PartialCount:    jobStats.PartialCount,
			ByStatus:        jobStats.ByStatus,
			ByTriggerType:   jobStats.ByTriggerType,
		}
	}

	// Get target statistics
	targetStats, err := s.statsRepo.GetTargetStats(ctx, &tid)
	if err == nil {
		result.Targets = &deployerV1.TargetStatistics{
			TotalCount:              targetStats.TotalCount,
			AutoDeployEnabledCount:  targetStats.AutoDeployEnabledCount,
			AutoDeployDisabledCount: targetStats.AutoDeployDisabledCount,
		}
	}

	// Get configuration statistics
	configStats, err := s.statsRepo.GetConfigurationStats(ctx, &tid)
	if err == nil {
		result.Configurations = &deployerV1.ConfigurationStatistics{
			TotalCount:     configStats.TotalCount,
			ActiveCount:    configStats.ActiveCount,
			InactiveCount:  configStats.InactiveCount,
			ErrorCount:     configStats.ErrorCount,
			ByStatus:       configStats.ByStatus,
			ByProviderType: configStats.ByProviderType,
		}
	}

	return result, nil
}
