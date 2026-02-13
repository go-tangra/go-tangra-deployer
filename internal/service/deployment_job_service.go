package service

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

// DeploymentJobService implements the DeploymentJobService gRPC service
// Supports both target group deployments (parent/child jobs) and direct configuration deployments
type DeploymentJobService struct {
	deployerV1.UnimplementedDeploymentJobServiceServer

	log         *log.Helper
	jobRepo     *data.DeploymentJobRepo
	targetRepo  *data.DeploymentTargetRepo
	configRepo  *data.TargetConfigurationRepo
	historyRepo *data.DeploymentHistoryRepo
}

// NewDeploymentJobService creates a new DeploymentJobService
func NewDeploymentJobService(
	ctx *bootstrap.Context,
	jobRepo *data.DeploymentJobRepo,
	targetRepo *data.DeploymentTargetRepo,
	configRepo *data.TargetConfigurationRepo,
	historyRepo *data.DeploymentHistoryRepo,
) *DeploymentJobService {
	return &DeploymentJobService{
		log:         ctx.NewLoggerHelper("deployer/service/job"),
		jobRepo:     jobRepo,
		targetRepo:  targetRepo,
		configRepo:  configRepo,
		historyRepo: historyRepo,
	}
}

// CreateJob creates a new deployment job
// Supports creating jobs for target groups (parent + child jobs) or direct configuration jobs
func (s *DeploymentJobService) CreateJob(ctx context.Context, req *deployerV1.CreateJobRequest) (*deployerV1.CreateJobResponse, error) {
	// Determine trigger type
	triggerType := deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL
	if req.TriggeredBy != nil {
		switch *req.TriggeredBy {
		case deployerV1.TriggerType_TRIGGER_TYPE_EVENT:
			triggerType = deploymentjob.TriggeredByTRIGGER_TYPE_EVENT
		case deployerV1.TriggerType_TRIGGER_TYPE_AUTO_RENEWAL:
			triggerType = deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL
		}
	}

	// Determine max retries
	maxRetries := int32(3)
	if req.MaxRetries != nil {
		maxRetries = *req.MaxRetries
	}

	// Handle deployment to target group (parent + child jobs)
	if req.DeploymentTargetId != nil && *req.DeploymentTargetId != "" {
		return s.createTargetGroupJob(ctx, *req.DeploymentTargetId, req.GetCertificateId(), nil, triggerType, maxRetries)
	}

	// Handle direct deployment to configuration
	if req.TargetConfigurationId != nil && *req.TargetConfigurationId != "" {
		return s.createDirectJob(ctx, *req.TargetConfigurationId, req.GetCertificateId(), nil, triggerType, maxRetries)
	}

	return nil, deployerV1.ErrorBadRequest("either deployment_target_id or target_configuration_id must be specified")
}

// createTargetGroupJob creates a parent job for a target group and child jobs for each configuration
func (s *DeploymentJobService) createTargetGroupJob(ctx context.Context, targetID, certID string, certSerial *string, triggerType deploymentjob.TriggeredBy, maxRetries int32) (*deployerV1.CreateJobResponse, error) {
	s.log.Infof("CreateJob: deployment_target_id=%s, certificate_id=%s", targetID, certID)

	// Validate target exists and get with configurations
	target, err := s.targetRepo.GetByIDWithConfigurations(ctx, targetID)
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
	}

	configs := target.Edges.Configurations
	if len(configs) == 0 {
		return nil, deployerV1.ErrorBadRequest("deployment target has no linked configurations")
	}

	// Get tenant ID from target
	targetTenantID := uint32(0)
	if target.TenantID != nil {
		targetTenantID = *target.TenantID
	}

	// Get serial number
	serial := ""
	if certSerial != nil {
		serial = *certSerial
	}

	// Create parent job
	parentJob, err := s.jobRepo.CreateParentJob(ctx, targetTenantID, targetID, certID, serial, triggerType, maxRetries)
	if err != nil {
		return nil, err
	}

	// Create child jobs for each configuration
	for _, config := range configs {
		_, err := s.jobRepo.CreateChildJob(ctx, targetTenantID, parentJob.ID, config.ID, certID, serial, triggerType, maxRetries)
		if err != nil {
			s.log.Errorf("Failed to create child job for configuration %s: %v", config.ID, err)
			// Continue creating other child jobs
		}
	}

	// Fetch the parent job with child jobs for response
	parentJob, err = s.jobRepo.GetByIDWithChildJobs(ctx, parentJob.ID)
	if err != nil {
		return nil, err
	}

	return &deployerV1.CreateJobResponse{
		Job: s.jobRepo.ToProto(parentJob),
	}, nil
}

// createDirectJob creates a direct job for a single configuration
func (s *DeploymentJobService) createDirectJob(ctx context.Context, configID, certID string, certSerial *string, triggerType deploymentjob.TriggeredBy, maxRetries int32) (*deployerV1.CreateJobResponse, error) {
	s.log.Infof("CreateJob: target_configuration_id=%s, certificate_id=%s", configID, certID)

	// Validate configuration exists
	config, err := s.configRepo.GetByID(ctx, configID)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
	}

	// Get tenant ID from config
	configTenantID := uint32(0)
	if config.TenantID != nil {
		configTenantID = *config.TenantID
	}

	// Get serial number
	serial := ""
	if certSerial != nil {
		serial = *certSerial
	}

	job, err := s.jobRepo.CreateDirectJob(ctx, configTenantID, configID, certID, serial, triggerType, maxRetries)
	if err != nil {
		return nil, err
	}

	return &deployerV1.CreateJobResponse{
		Job: s.jobRepo.ToProto(job),
	}, nil
}

// GetJobStatus gets the status of a deployment job
func (s *DeploymentJobService) GetJobStatus(ctx context.Context, req *deployerV1.GetJobStatusRequest) (*deployerV1.GetJobStatusResponse, error) {
	s.log.Infof("GetJobStatus: id=%s", req.GetId())

	var job *data.DeploymentJob
	var err error

	if req.GetIncludeChildJobs() {
		job, err = s.jobRepo.GetByIDWithChildJobs(ctx, req.GetId())
	} else {
		job, err = s.jobRepo.GetByID(ctx, req.GetId())
	}
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, deployerV1.ErrorJobNotFound("deployment job not found")
	}

	return &deployerV1.GetJobStatusResponse{
		Job: s.jobRepo.ToProto(job),
	}, nil
}

// GetJobResult gets the result of a deployment job with history
func (s *DeploymentJobService) GetJobResult(ctx context.Context, req *deployerV1.GetJobResultRequest) (*deployerV1.GetJobResultResponse, error) {
	s.log.Infof("GetJobResult: id=%s, include_children=%v", req.GetId(), req.GetIncludeChildJobs())

	var job *data.DeploymentJob
	var err error

	if req.GetIncludeChildJobs() {
		job, err = s.jobRepo.GetByIDWithChildJobs(ctx, req.GetId())
	} else {
		job, err = s.jobRepo.GetByID(ctx, req.GetId())
	}

	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, deployerV1.ErrorJobNotFound("deployment job not found")
	}

	history, err := s.historyRepo.ListByJobID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	return &deployerV1.GetJobResultResponse{
		Job:     s.jobRepo.ToProto(job),
		History: s.historyRepo.ToProtoList(history),
	}, nil
}

// ListJobs lists deployment jobs
func (s *DeploymentJobService) ListJobs(ctx context.Context, req *deployerV1.ListJobsRequest) (*deployerV1.ListJobsResponse, error) {
	s.log.Infof("ListJobs: tenant_id=%v, target_id=%v, config_id=%v",
		req.TenantId, req.DeploymentTargetId, req.TargetConfigurationId)

	// Convert status
	var status *deploymentjob.Status
	if req.Status != nil {
		var st deploymentjob.Status
		switch *req.Status {
		case deployerV1.JobStatus_JOB_STATUS_PENDING:
			st = deploymentjob.StatusJOB_STATUS_PENDING
		case deployerV1.JobStatus_JOB_STATUS_PROCESSING:
			st = deploymentjob.StatusJOB_STATUS_PROCESSING
		case deployerV1.JobStatus_JOB_STATUS_COMPLETED:
			st = deploymentjob.StatusJOB_STATUS_COMPLETED
		case deployerV1.JobStatus_JOB_STATUS_FAILED:
			st = deploymentjob.StatusJOB_STATUS_FAILED
		case deployerV1.JobStatus_JOB_STATUS_CANCELLED:
			st = deploymentjob.StatusJOB_STATUS_CANCELLED
		case deployerV1.JobStatus_JOB_STATUS_RETRYING:
			st = deploymentjob.StatusJOB_STATUS_RETRYING
		case deployerV1.JobStatus_JOB_STATUS_PARTIAL:
			st = deploymentjob.StatusJOB_STATUS_PARTIAL
		}
		status = &st
	}

	// Convert trigger type
	var triggeredBy *deploymentjob.TriggeredBy
	if req.TriggeredBy != nil {
		var t deploymentjob.TriggeredBy
		switch *req.TriggeredBy {
		case deployerV1.TriggerType_TRIGGER_TYPE_MANUAL:
			t = deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL
		case deployerV1.TriggerType_TRIGGER_TYPE_EVENT:
			t = deploymentjob.TriggeredByTRIGGER_TYPE_EVENT
		case deployerV1.TriggerType_TRIGGER_TYPE_AUTO_RENEWAL:
			t = deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL
		}
		triggeredBy = &t
	}

	// Convert timestamps
	var createdAfter, createdBefore *time.Time
	if req.CreatedAfter != nil {
		t := req.CreatedAfter.AsTime()
		createdAfter = &t
	}
	if req.CreatedBefore != nil {
		t := req.CreatedBefore.AsTime()
		createdBefore = &t
	}

	page := uint32(1)
	pageSize := uint32(20)
	if req.Page != nil && *req.Page > 0 {
		page = *req.Page
	}
	if req.PageSize != nil && *req.PageSize > 0 {
		pageSize = *req.PageSize
	}

	// Include child jobs for parent jobs when requested
	includeChildJobs := req.GetIncludeChildJobs()

	// Pass job type directly (repo expects proto type)
	jobs, total, err := s.jobRepo.List(ctx, req.TenantId, req.DeploymentTargetId, req.TargetConfigurationId,
		req.CertificateId, req.ParentJobId, status, triggeredBy, req.JobType, createdAfter, createdBefore,
		includeChildJobs, page, pageSize)
	if err != nil {
		return nil, err
	}

	items := make([]*deployerV1.DeploymentJob, 0, len(jobs))
	for _, job := range jobs {
		items = append(items, s.jobRepo.ToProto(job))
	}

	return &deployerV1.ListJobsResponse{
		Items: items,
		Total: uint64(total),
	}, nil
}

// CancelJob cancels a pending or processing job
// For parent jobs, optionally cancels all child jobs as well
func (s *DeploymentJobService) CancelJob(ctx context.Context, req *deployerV1.CancelJobRequest) (*deployerV1.CancelJobResponse, error) {
	s.log.Infof("CancelJob: id=%s, cascade=%v", req.GetId(), req.GetCancelChildJobs())

	job, err := s.jobRepo.Cancel(ctx, req.GetId(), req.GetCancelChildJobs())
	if err != nil {
		return nil, err
	}

	// Fetch with child jobs if this is a parent job
	if job.DeploymentTargetID != nil && *job.DeploymentTargetID != "" {
		job, err = s.jobRepo.GetByIDWithChildJobs(ctx, job.ID)
		if err != nil {
			return nil, err
		}
	}

	return &deployerV1.CancelJobResponse{
		Job: s.jobRepo.ToProto(job),
	}, nil
}

// RetryJob retries a failed job
// For parent jobs, optionally retries all failed child jobs as well
func (s *DeploymentJobService) RetryJob(ctx context.Context, req *deployerV1.RetryJobRequest) (*deployerV1.RetryJobResponse, error) {
	s.log.Infof("RetryJob: id=%s, retry_children=%v", req.GetId(), req.GetRetryFailedChildrenOnly())

	job, err := s.jobRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, deployerV1.ErrorJobNotFound("deployment job not found")
	}

	// Check if job can be retried (failed or partial)
	if job.Status != deploymentjob.StatusJOB_STATUS_FAILED && job.Status != deploymentjob.StatusJOB_STATUS_PARTIAL {
		return nil, deployerV1.ErrorConflict("only failed or partial jobs can be retried")
	}

	// For parent jobs, retry failed child jobs if requested
	if job.DeploymentTargetID != nil && *job.DeploymentTargetID != "" && req.GetRetryFailedChildrenOnly() {
		childJobs, err := s.jobRepo.ListChildJobs(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		for _, child := range childJobs {
			if child.Status == deploymentjob.StatusJOB_STATUS_FAILED {
				if _, err := s.jobRepo.UpdateStatus(ctx, child.ID, deploymentjob.StatusJOB_STATUS_PENDING, "Retry requested", 0); err != nil {
				s.log.Warnf("Failed to reset child job %s for retry: %v", child.ID, err)
			}
			}
		}
		// Reset parent status to processing
		job, err = s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_PROCESSING, "Retrying failed deployments", 0)
	} else {
		// Reset status to pending for direct/child job
		job, err = s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_PENDING, "Retry requested", 0)
	}
	if err != nil {
		return nil, err
	}

	// Fetch with child jobs if this is a parent job
	if job.DeploymentTargetID != nil && *job.DeploymentTargetID != "" {
		job, err = s.jobRepo.GetByIDWithChildJobs(ctx, job.ID)
		if err != nil {
			return nil, err
		}
	}

	return &deployerV1.RetryJobResponse{
		Job: s.jobRepo.ToProto(job),
	}, nil
}
