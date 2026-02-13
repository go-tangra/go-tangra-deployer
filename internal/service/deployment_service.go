package service

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenthistory"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

// DeploymentService implements the DeploymentService gRPC service
// Supports deployments to both target groups (parent/child jobs) and individual configurations
type DeploymentService struct {
	deployerV1.UnimplementedDeploymentServiceServer

	log           *log.Helper
	jobRepo       *data.DeploymentJobRepo
	targetRepo    *data.DeploymentTargetRepo
	configRepo    *data.TargetConfigurationRepo
	historyRepo   *data.DeploymentHistoryRepo
	configService *TargetConfigurationService
}

// NewDeploymentService creates a new DeploymentService
func NewDeploymentService(
	ctx *bootstrap.Context,
	jobRepo *data.DeploymentJobRepo,
	targetRepo *data.DeploymentTargetRepo,
	configRepo *data.TargetConfigurationRepo,
	historyRepo *data.DeploymentHistoryRepo,
	configService *TargetConfigurationService,
) *DeploymentService {
	return &DeploymentService{
		log:           ctx.NewLoggerHelper("deployer/service/deployment"),
		jobRepo:       jobRepo,
		targetRepo:    targetRepo,
		configRepo:    configRepo,
		historyRepo:   historyRepo,
		configService: configService,
	}
}

// Deploy deploys a certificate to a configuration (direct deployment)
func (s *DeploymentService) Deploy(ctx context.Context, req *deployerV1.DeployRequest) (*deployerV1.DeployResponse, error) {
	configID := req.GetTargetConfigurationId()
	if configID == "" {
		return nil, deployerV1.ErrorBadRequest("target_configuration_id is required")
	}

	s.log.Infof("Deploy: config_id=%s, certificate_id=%s, wait=%v",
		configID, req.GetCertificateId(), req.GetWaitForCompletion())

	// Get configuration
	config, err := s.configRepo.GetByID(ctx, configID)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
	}

	// Create job
	tenantID := uint32(0)
	if config.TenantID != nil {
		tenantID = *config.TenantID
	}
	job, err := s.jobRepo.CreateDirectJob(ctx, tenantID, configID, req.GetCertificateId(),
		"", deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL, 3)
	if err != nil {
		return nil, err
	}

	// If not waiting, return immediately
	if !req.GetWaitForCompletion() {
		return &deployerV1.DeployResponse{
			Job: s.jobRepo.ToProto(job),
		}, nil
	}

	// Execute deployment synchronously
	timeout := 300 * time.Second
	if req.TimeoutSeconds != nil && *req.TimeoutSeconds > 0 {
		timeout = time.Duration(*req.TimeoutSeconds) * time.Second
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	result, err := s.executeDeployment(ctx, job, config)
	if err != nil {
		return nil, err
	}

	// Reload job to get updated status
	job, err = s.jobRepo.GetByID(ctx, job.ID)
	if err != nil {
		return nil, err
	}

	return &deployerV1.DeployResponse{
		Job:    s.jobRepo.ToProto(job),
		Result: result,
	}, nil
}

// DeployToTargets deploys a certificate to multiple configurations (deprecated - use DeployToConfigurations)
func (s *DeploymentService) DeployToTargets(ctx context.Context, req *deployerV1.DeployToTargetsRequest) (*deployerV1.DeployToTargetsResponse, error) {
	s.log.Warnf("DeployToTargets is deprecated, use DeployToConfigurations instead")
	s.log.Infof("DeployToTargets: certificate_id=%s, config_count=%d, triggered_by=%s",
		req.GetCertificateId(), len(req.GetTargetIds()), req.GetTriggeredBy())

	results := make([]*deployerV1.TargetDeploymentResult, 0, len(req.GetTargetIds()))
	succeeded := int32(0)
	failed := int32(0)

	triggeredBy := deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL
	if req.TriggeredBy != nil {
		switch *req.TriggeredBy {
		case "event":
			triggeredBy = deploymentjob.TriggeredByTRIGGER_TYPE_EVENT
		case "auto_renewal":
			triggeredBy = deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL
		}
	}

	for _, configID := range req.GetTargetIds() {
		result := &deployerV1.TargetDeploymentResult{
			TargetId: configID,
		}

		// Get configuration
		config, err := s.configRepo.GetByID(ctx, configID)
		if err != nil {
			errMsg := err.Error()
			result.Error = &errMsg
			failed++
			results = append(results, result)
			continue
		}
		if config == nil {
			errMsg := "target configuration not found"
			result.Error = &errMsg
			failed++
			results = append(results, result)
			continue
		}

		result.TargetName = config.Name

		// Create job
		tid := uint32(0)
		if config.TenantID != nil {
			tid = *config.TenantID
		}
		job, err := s.jobRepo.CreateDirectJob(ctx, tid, configID, req.GetCertificateId(),
			"", triggeredBy, 3)
		if err != nil {
			errMsg := err.Error()
			result.Error = &errMsg
			failed++
			results = append(results, result)
			continue
		}

		result.Job = s.jobRepo.ToProto(job)
		succeeded++
		results = append(results, result)
	}

	return &deployerV1.DeployToTargetsResponse{
		Total:     int32(len(req.GetTargetIds())),
		Succeeded: succeeded,
		Failed:    failed,
		Results:   results,
	}, nil
}

// DeployToTarget deploys a certificate to a target group (creates parent + child jobs)
func (s *DeploymentService) DeployToTarget(ctx context.Context, req *deployerV1.DeployToTargetRequest) (*deployerV1.DeployToTargetResponse, error) {
	s.log.Infof("DeployToTarget: target_id=%s, certificate_id=%s", req.GetDeploymentTargetId(), req.GetCertificateId())

	// Get target with configurations
	target, err := s.targetRepo.GetByIDWithConfigurations(ctx, req.GetDeploymentTargetId())
	if err != nil {
		return nil, err
	}
	if target == nil {
		return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
	}

	configs := target.Edges.Configurations
	if len(configs) == 0 {
		return nil, deployerV1.ErrorTargetNotFound("deployment target has no linked configurations")
	}

	// Get tenant ID
	tenantID := uint32(0)
	if target.TenantID != nil {
		tenantID = *target.TenantID
	}

	// Determine trigger type
	triggeredBy := deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL
	if req.TriggeredBy != nil {
		switch *req.TriggeredBy {
		case deployerV1.TriggerType_TRIGGER_TYPE_EVENT:
			triggeredBy = deploymentjob.TriggeredByTRIGGER_TYPE_EVENT
		case deployerV1.TriggerType_TRIGGER_TYPE_AUTO_RENEWAL:
			triggeredBy = deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL
		}
	}

	// Create parent job
	parentJob, err := s.jobRepo.CreateParentJob(ctx, tenantID, req.GetDeploymentTargetId(),
		req.GetCertificateId(), "", triggeredBy, 3)
	if err != nil {
		return nil, err
	}

	// Create child jobs for each configuration
	for _, config := range configs {
		_, err := s.jobRepo.CreateChildJob(ctx, tenantID, parentJob.ID, config.ID,
			req.GetCertificateId(), "", triggeredBy, 3)
		if err != nil {
			s.log.Errorf("Failed to create child job for configuration %s: %v", config.ID, err)
		}
	}

	// Fetch parent job with child jobs for response
	parentJob, err = s.jobRepo.GetByIDWithChildJobs(ctx, parentJob.ID)
	if err != nil {
		return nil, err
	}

	return &deployerV1.DeployToTargetResponse{
		Job: s.jobRepo.ToProto(parentJob),
	}, nil
}

// DeployToConfigurations deploys a certificate to multiple configurations
func (s *DeploymentService) DeployToConfigurations(ctx context.Context, req *deployerV1.DeployToConfigurationsRequest) (*deployerV1.DeployToConfigurationsResponse, error) {
	s.log.Infof("DeployToConfigurations: certificate_id=%s, config_count=%d",
		req.GetCertificateId(), len(req.GetConfigurationIds()))

	results := make([]*deployerV1.ConfigurationDeploymentResult, 0, len(req.GetConfigurationIds()))
	succeeded := int32(0)
	failed := int32(0)

	triggeredBy := deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL
	if req.TriggeredBy != nil {
		switch *req.TriggeredBy {
		case deployerV1.TriggerType_TRIGGER_TYPE_EVENT:
			triggeredBy = deploymentjob.TriggeredByTRIGGER_TYPE_EVENT
		case deployerV1.TriggerType_TRIGGER_TYPE_AUTO_RENEWAL:
			triggeredBy = deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL
		}
	}

	for _, configID := range req.GetConfigurationIds() {
		result := &deployerV1.ConfigurationDeploymentResult{
			ConfigurationId: configID,
		}

		// Get configuration
		config, err := s.configRepo.GetByID(ctx, configID)
		if err != nil {
			errMsg := err.Error()
			result.Error = &errMsg
			failed++
			results = append(results, result)
			continue
		}
		if config == nil {
			errMsg := "target configuration not found"
			result.Error = &errMsg
			failed++
			results = append(results, result)
			continue
		}

		result.ConfigurationName = config.Name

		// Create job
		tid := uint32(0)
		if config.TenantID != nil {
			tid = *config.TenantID
		}
		job, err := s.jobRepo.CreateDirectJob(ctx, tid, configID, req.GetCertificateId(),
			"", triggeredBy, 3)
		if err != nil {
			errMsg := err.Error()
			result.Error = &errMsg
			failed++
			results = append(results, result)
			continue
		}

		result.Job = s.jobRepo.ToProto(job)
		succeeded++
		results = append(results, result)
	}

	return &deployerV1.DeployToConfigurationsResponse{
		Total:     int32(len(req.GetConfigurationIds())),
		Succeeded: succeeded,
		Failed:    failed,
		Results:   results,
	}, nil
}

// Verify verifies a deployment
func (s *DeploymentService) Verify(ctx context.Context, req *deployerV1.VerifyRequest) (*deployerV1.VerifyResponse, error) {
	configID := req.GetTargetConfigurationId()
	if configID == "" {
		return nil, deployerV1.ErrorBadRequest("target_configuration_id is required")
	}

	s.log.Infof("Verify: config_id=%s, certificate_id=%s", configID, req.GetCertificateId())

	// Get configuration
	config, err := s.configRepo.GetByID(ctx, configID)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
	}

	// Get provider
	provider, err := registry.Get(config.ProviderType)
	if err != nil {
		return nil, deployerV1.ErrorProviderNotFound("provider not found")
	}

	// Check if provider supports verification
	caps := provider.GetCapabilities()
	if !caps.SupportsVerification {
		return nil, deployerV1.ErrorUnprocessableEntity("provider does not support verification")
	}

	// Get credentials
	credentials, err := s.configService.GetDecryptedCredentials(ctx, config.ID)
	if err != nil {
		return nil, err
	}

	// Get certificate data (placeholder - in real implementation, fetch from LCM)
	certData := &registry.CertificateData{
		ID:           req.GetCertificateId(),
		SerialNumber: "",
	}

	// Verify
	result, err := provider.Verify(ctx, certData, config.Config, credentials)
	if err != nil {
		return nil, err
	}

	return &deployerV1.VerifyResponse{
		Result: toProtoResult(result),
	}, nil
}

// Rollback rolls back a deployment
func (s *DeploymentService) Rollback(ctx context.Context, req *deployerV1.RollbackRequest) (*deployerV1.RollbackResponse, error) {
	configID := req.GetTargetConfigurationId()
	if configID == "" {
		return nil, deployerV1.ErrorBadRequest("target_configuration_id is required")
	}

	s.log.Infof("Rollback: config_id=%s, certificate_id=%s", configID, req.GetCertificateId())

	// Get configuration
	config, err := s.configRepo.GetByID(ctx, configID)
	if err != nil {
		return nil, err
	}
	if config == nil {
		return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
	}

	// Get provider
	provider, err := registry.Get(config.ProviderType)
	if err != nil {
		return nil, deployerV1.ErrorProviderNotFound("provider not found")
	}

	// Check if provider supports rollback
	caps := provider.GetCapabilities()
	if !caps.SupportsRollback {
		return nil, deployerV1.ErrorUnprocessableEntity("provider does not support rollback")
	}

	// Create job for rollback
	rollbackTenantID := uint32(0)
	if config.TenantID != nil {
		rollbackTenantID = *config.TenantID
	}
	job, err := s.jobRepo.CreateDirectJob(ctx, rollbackTenantID, configID, req.GetCertificateId(),
		"", deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL, 1)
	if err != nil {
		return nil, err
	}

	// Get credentials
	credentials, err := s.configService.GetDecryptedCredentials(ctx, config.ID)
	if err != nil {
		return nil, err
	}

	// Get certificate data (placeholder)
	certData := &registry.CertificateData{
		ID:           req.GetCertificateId(),
		SerialNumber: "",
	}

	// Execute rollback
	startTime := time.Now()
	_, err = s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_PROCESSING, "Rolling back", 0)
	if err != nil {
		return nil, err
	}

	result, err := provider.Rollback(ctx, certData, config.Config, credentials)
	if err != nil {
		if _, statusErr := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, err.Error(), 0); statusErr != nil {
			s.log.Warnf("Failed to update job %s status after rollback error: %v", job.ID, statusErr)
		}
		return nil, err
	}

	// Record history
	historyResult := deploymenthistory.ResultRESULT_SUCCESS
	if !result.Success {
		historyResult = deploymenthistory.ResultRESULT_FAILURE
	}
	if _, err := s.historyRepo.Create(ctx, job.ID, deploymenthistory.ActionACTION_ROLLBACK,
		historyResult, result.Message, time.Since(startTime).Milliseconds(), result.Details); err != nil {
		s.log.Warnf("Failed to create rollback history for job %s: %v", job.ID, err)
	}

	// Update job status
	if result.Success {
		if _, err := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_COMPLETED, "Rollback complete", 100); err != nil {
			s.log.Warnf("Failed to update job %s status after rollback: %v", job.ID, err)
		}
	} else {
		if _, err := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, result.Message, 0); err != nil {
			s.log.Warnf("Failed to update job %s status after rollback failure: %v", job.ID, err)
		}
	}

	// Reload job
	job, err = s.jobRepo.GetByID(ctx, job.ID)
	if err != nil {
		s.log.Warnf("Failed to reload job %s after rollback: %v", job.ID, err)
	}

	return &deployerV1.RollbackResponse{
		Job:    s.jobRepo.ToProto(job),
		Result: toProtoResult(result),
	}, nil
}

// executeDeployment executes a deployment and records the result
func (s *DeploymentService) executeDeployment(ctx context.Context, job *data.DeploymentJob, config *data.TargetConfiguration) (*deployerV1.DeploymentResult, error) {
	startTime := time.Now()

	// Update job to processing
	_, err := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_PROCESSING, "Starting deployment", 0)
	if err != nil {
		return nil, err
	}

	// Get provider
	provider, err := registry.Get(config.ProviderType)
	if err != nil {
		if _, statusErr := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, "Provider not found", 0); statusErr != nil {
			s.log.Warnf("Failed to update job %s status: %v", job.ID, statusErr)
		}
		return nil, err
	}

	// Get credentials
	credentials, err := s.configService.GetDecryptedCredentials(ctx, config.ID)
	if err != nil {
		if _, statusErr := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, "Failed to get credentials", 0); statusErr != nil {
			s.log.Warnf("Failed to update job %s status: %v", job.ID, statusErr)
		}
		return nil, err
	}

	// Get certificate data (placeholder - in real implementation, fetch from LCM)
	certData := &registry.CertificateData{
		ID:           job.CertificateID,
		SerialNumber: "",
	}

	// Progress callback
	progressCb := func(progress int32, message string) {
		if _, err := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_PROCESSING, message, progress); err != nil {
			s.log.Warnf("Failed to update job %s progress: %v", job.ID, err)
		}
	}

	// Execute deployment
	result, err := provider.Deploy(ctx, certData, config.Config, credentials, progressCb)
	if err != nil {
		if _, statusErr := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, err.Error(), 0); statusErr != nil {
			s.log.Warnf("Failed to update job %s status after deploy error: %v", job.ID, statusErr)
		}
		if _, histErr := s.historyRepo.Create(ctx, job.ID, deploymenthistory.ActionACTION_DEPLOY,
			deploymenthistory.ResultRESULT_FAILURE, err.Error(), time.Since(startTime).Milliseconds(), nil); histErr != nil {
			s.log.Warnf("Failed to create deploy failure history for job %s: %v", job.ID, histErr)
		}
		return nil, err
	}

	// Record history
	historyResult := deploymenthistory.ResultRESULT_SUCCESS
	if !result.Success {
		historyResult = deploymenthistory.ResultRESULT_FAILURE
	}
	if _, err := s.historyRepo.Create(ctx, job.ID, deploymenthistory.ActionACTION_DEPLOY,
		historyResult, result.Message, result.DurationMs, result.Details); err != nil {
		s.log.Warnf("Failed to create deployment history for job %s: %v", job.ID, err)
	}

	// Update job status
	if result.Success {
		if _, err := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_COMPLETED, "Deployment complete", 100); err != nil {
			s.log.Warnf("Failed to update job %s status after deployment: %v", job.ID, err)
		}
		if err := s.configRepo.UpdateLastDeployment(ctx, config.ID); err != nil {
			s.log.Warnf("Failed to update last deployment for config %s: %v", config.ID, err)
		}
	} else {
		if _, err := s.jobRepo.UpdateStatus(ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, result.Message, 0); err != nil {
			s.log.Warnf("Failed to update job %s status after deployment failure: %v", job.ID, err)
		}
	}

	return toProtoResult(result), nil
}

func toProtoResult(result *registry.DeploymentResult) *deployerV1.DeploymentResult {
	if result == nil {
		return nil
	}

	proto := &deployerV1.DeploymentResult{
		Success:    result.Success,
		DurationMs: &result.DurationMs,
	}

	if result.Message != "" {
		proto.Message = &result.Message
	}
	if result.ResourceID != "" {
		proto.ResourceId = &result.ResourceID
	}
	if result.Details != nil {
		if details, err := structpb.NewStruct(result.Details); err == nil {
			proto.Details = details
		}
	}

	return proto
}
