package service

import (
	"context"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/conf"
	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenthistory"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"

	appViewer "github.com/go-tangra/go-tangra-common/viewer"
)

// JobExecutor handles background job execution
// It processes child jobs and direct jobs (not parent jobs)
// Parent jobs aggregate status from their child jobs
type JobExecutor struct {
	log           *log.Helper
	jobRepo       *data.DeploymentJobRepo
	configRepo    *data.TargetConfigurationRepo
	historyRepo   *data.DeploymentHistoryRepo
	configService *TargetConfigurationService
	lcmClient     *data.LcmClient
	config        *conf.JobConfig

	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

// NewJobExecutor creates a new job executor
func NewJobExecutor(
	ctx *bootstrap.Context,
	jobRepo *data.DeploymentJobRepo,
	configRepo *data.TargetConfigurationRepo,
	historyRepo *data.DeploymentHistoryRepo,
	configService *TargetConfigurationService,
	lcmClient *data.LcmClient,
) *JobExecutor {
	// Get config
	var jobCfg *conf.JobConfig
	if cfg, ok := ctx.GetCustomConfig("deployer"); ok && cfg != nil {
		if deployerCfg, ok := cfg.(*conf.Deployer); ok && deployerCfg.Jobs != nil {
			jobCfg = deployerCfg.Jobs
		}
	}

	// Default config
	if jobCfg == nil {
		jobCfg = &conf.JobConfig{
			WorkerCount:            5,
			MaxRetries:             3,
			RetryDelaySeconds:      60,
			RetryBackoffMultiplier: 2.0,
			JobTimeoutSeconds:      300,
			CleanupDays:            30,
		}
	}

	return &JobExecutor{
		log:           ctx.NewLoggerHelper("deployer/job-executor"),
		jobRepo:       jobRepo,
		configRepo:    configRepo,
		historyRepo:   historyRepo,
		configService: configService,
		lcmClient:     lcmClient,
		config:        jobCfg,
	}
}

// Start starts the job executor
func (e *JobExecutor) Start() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.running {
		return nil
	}

	// Use system viewer context for background operations (bypasses tenant privacy checks)
	baseCtx := appViewer.NewSystemViewerContext(context.Background())
	e.ctx, e.cancel = context.WithCancel(baseCtx)
	e.running = true

	workerCount := e.config.WorkerCount
	if workerCount <= 0 {
		workerCount = 5
	}

	e.log.Infof("Starting job executor with %d workers", workerCount)

	// Start worker goroutines
	for i := int32(0); i < workerCount; i++ {
		e.wg.Add(1)
		go e.worker(i)
	}

	// Start cleanup goroutine
	e.wg.Add(1)
	go e.cleanupWorker()

	return nil
}

// Stop stops the job executor
func (e *JobExecutor) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if !e.running {
		return nil
	}

	e.log.Info("Stopping job executor")
	e.cancel()
	e.wg.Wait()
	e.running = false

	return nil
}

// worker is a background worker that processes jobs
func (e *JobExecutor) worker(id int32) {
	defer e.wg.Done()

	e.log.Infof("Worker %d started", id)

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-e.ctx.Done():
			e.log.Infof("Worker %d stopped", id)
			return
		case <-ticker.C:
			e.processJobs()
		}
	}
}

// processJobs processes pending and retryable jobs
func (e *JobExecutor) processJobs() {
	// Process pending jobs
	pendingJobs, err := e.jobRepo.ListPending(e.ctx, 10)
	if err != nil {
		e.log.Errorf("Failed to list pending jobs: %v", err)
		return
	}

	for _, job := range pendingJobs {
		// Try to claim the job atomically - only one worker will succeed
		claimed, err := e.jobRepo.ClaimJob(e.ctx, job.ID, deploymentjob.StatusJOB_STATUS_PENDING)
		if err != nil {
			e.log.Errorf("Failed to claim job %s: %v", job.ID, err)
			continue
		}
		if !claimed {
			// Job was already claimed by another worker, skip it
			continue
		}

		if err := e.processJob(job); err != nil {
			e.log.Errorf("Failed to process job %s: %v", job.ID, err)
		}
	}

	// Process retryable jobs
	retryJobs, err := e.jobRepo.ListRetryable(e.ctx, 10)
	if err != nil {
		e.log.Errorf("Failed to list retryable jobs: %v", err)
		return
	}

	for _, job := range retryJobs {
		// Try to claim the job atomically - only one worker will succeed
		claimed, err := e.jobRepo.ClaimJob(e.ctx, job.ID, deploymentjob.StatusJOB_STATUS_RETRYING)
		if err != nil {
			e.log.Errorf("Failed to claim retry job %s: %v", job.ID, err)
			continue
		}
		if !claimed {
			// Job was already claimed by another worker, skip it
			continue
		}

		if err := e.processJob(job); err != nil {
			e.log.Errorf("Failed to process retry job %s: %v", job.ID, err)
		}
	}
}

// processJob processes a single job
// Only processes child jobs and direct jobs (not parent jobs)
func (e *JobExecutor) processJob(job *ent.DeploymentJob) error {
	// Skip parent jobs - they aggregate child job status
	if job.TargetConfigurationID == nil || *job.TargetConfigurationID == "" {
		e.log.Debugf("Skipping parent job %s (no target configuration)", job.ID)
		return nil
	}

	configID := *job.TargetConfigurationID
	e.log.Infof("Processing job %s for configuration %s", job.ID, configID)

	// Get target configuration
	config, err := e.configRepo.GetByID(e.ctx, configID)
	if err != nil {
		return e.failJob(job, "Failed to get configuration: "+err.Error())
	}
	if config == nil {
		return e.failJob(job, "Configuration not found")
	}

	// Get provider
	provider, err := registry.Get(config.ProviderType)
	if err != nil {
		return e.failJob(job, "Provider not found: "+err.Error())
	}

	// Get credentials
	credentials, err := e.configService.GetDecryptedCredentials(e.ctx, config.ID)
	if err != nil {
		return e.failJob(job, "Failed to get credentials: "+err.Error())
	}

	// Fetch certificate data from LCM service
	var certData *registry.CertificateData
	if e.lcmClient != nil {
		lcmCert, err := e.lcmClient.GetCertificateByJobID(e.ctx, job.CertificateID, true)
		if err != nil {
			e.log.Warnf("Failed to fetch certificate from LCM: %v, using placeholder data", err)
			certData = &registry.CertificateData{
				ID:           job.CertificateID,
				SerialNumber: job.CertificateSerial,
			}
		} else {
			certData = &registry.CertificateData{
				ID:               lcmCert.JobID,
				SerialNumber:     lcmCert.SerialNumber,
				CommonName:       lcmCert.CommonName,
				SANs:             lcmCert.SANs,
				CertificatePEM:   lcmCert.CertificatePEM,
				CertificateChain: lcmCert.CACertificatePEM,
				PrivateKeyPEM:    lcmCert.PrivateKeyPEM,
				ExpiresAt:        lcmCert.ExpiresAt,
			}
			e.log.Infof("Fetched certificate from LCM: serial=%s, cn=%s, sans=%v", lcmCert.SerialNumber, lcmCert.CommonName, lcmCert.SANs)
		}
	} else {
		e.log.Warn("LCM client not available, using placeholder certificate data")
		certData = &registry.CertificateData{
			ID:           job.CertificateID,
			SerialNumber: job.CertificateSerial,
		}
	}

	// Job is already claimed with PROCESSING status by ClaimJob()

	// Create context with timeout
	timeout := time.Duration(e.config.JobTimeoutSeconds) * time.Second
	ctx, cancel := context.WithTimeout(e.ctx, timeout)
	defer cancel()

	// Progress callback
	progressCb := func(progress int32, message string) {
		_, _ = e.jobRepo.UpdateStatus(e.ctx, job.ID, deploymentjob.StatusJOB_STATUS_PROCESSING, message, progress)
	}

	// Execute deployment
	startTime := time.Now()
	result, err := provider.Deploy(ctx, certData, config.Config, credentials, progressCb)

	// Record history
	historyResult := deploymenthistory.ResultRESULT_SUCCESS
	historyMessage := ""
	if err != nil {
		historyResult = deploymenthistory.ResultRESULT_FAILURE
		historyMessage = err.Error()
	} else if !result.Success {
		historyResult = deploymenthistory.ResultRESULT_FAILURE
		historyMessage = result.Message
	} else {
		historyMessage = result.Message
	}

	_, _ = e.historyRepo.Create(e.ctx, job.ID, deploymenthistory.ActionACTION_DEPLOY,
		historyResult, historyMessage, time.Since(startTime).Milliseconds(), nil)

	// Handle result
	if err != nil || !result.Success {
		errMsg := historyMessage
		if err != nil {
			errMsg = err.Error()
		}

		// Check if we should retry
		if job.RetryCount < job.MaxRetries {
			return e.scheduleRetry(job, errMsg)
		}
		return e.failJobAndUpdateParent(job, errMsg)
	}

	// Success
	_, err = e.jobRepo.UpdateStatus(e.ctx, job.ID, deploymentjob.StatusJOB_STATUS_COMPLETED, "Deployment successful", 100)
	if err != nil {
		return err
	}

	// Update configuration last deployment
	_ = e.configRepo.UpdateLastDeployment(e.ctx, config.ID)

	// Update parent job status if this is a child job
	if job.ParentJobID != nil && *job.ParentJobID != "" {
		e.updateParentJobStatus(*job.ParentJobID)
	}

	e.log.Infof("Job %s completed successfully", job.ID)
	return nil
}

// failJob marks a job as failed (for non-child jobs or during claim)
func (e *JobExecutor) failJob(job *ent.DeploymentJob, message string) error {
	e.log.Warnf("Job %s failed: %s", job.ID, message)
	_, err := e.jobRepo.UpdateStatus(e.ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, message, 0)
	return err
}

// failJobAndUpdateParent marks a job as failed and updates parent job status
func (e *JobExecutor) failJobAndUpdateParent(job *ent.DeploymentJob, message string) error {
	e.log.Warnf("Job %s failed: %s", job.ID, message)
	_, err := e.jobRepo.UpdateStatus(e.ctx, job.ID, deploymentjob.StatusJOB_STATUS_FAILED, message, 0)
	if err != nil {
		return err
	}

	// Update parent job status if this is a child job
	if job.ParentJobID != nil && *job.ParentJobID != "" {
		e.updateParentJobStatus(*job.ParentJobID)
	}

	return nil
}

// updateParentJobStatus updates the parent job status based on child job results
func (e *JobExecutor) updateParentJobStatus(parentJobID string) {
	// Get child job counts
	completed, failed, total, err := e.jobRepo.GetChildJobCounts(e.ctx, parentJobID)
	if err != nil {
		e.log.Errorf("Failed to get child job counts for parent %s: %v", parentJobID, err)
		return
	}

	// Determine parent status
	var status deploymentjob.Status
	var message string
	var progress int32

	finishedCount := completed + failed

	if finishedCount < total {
		// Still processing
		status = deploymentjob.StatusJOB_STATUS_PROCESSING
		progress = int32((finishedCount * 100) / total)
		message = "Deploying to targets"
	} else if failed == 0 {
		// All completed successfully
		status = deploymentjob.StatusJOB_STATUS_COMPLETED
		progress = 100
		message = "All deployments completed successfully"
	} else if completed == 0 {
		// All failed
		status = deploymentjob.StatusJOB_STATUS_FAILED
		progress = 0
		message = "All deployments failed"
	} else {
		// Partial success
		status = deploymentjob.StatusJOB_STATUS_PARTIAL
		progress = int32((completed * 100) / total)
		message = "Some deployments failed"
	}

	_, err = e.jobRepo.UpdateStatus(e.ctx, parentJobID, status, message, progress)
	if err != nil {
		e.log.Errorf("Failed to update parent job %s status: %v", parentJobID, err)
	}
}

// scheduleRetry schedules a job for retry
func (e *JobExecutor) scheduleRetry(job *ent.DeploymentJob, message string) error {
	// Calculate next retry time with exponential backoff
	delay := float64(e.config.RetryDelaySeconds) * float64(time.Second)
	multiplier := e.config.RetryBackoffMultiplier
	if multiplier <= 0 {
		multiplier = 2.0
	}

	for i := int32(0); i < job.RetryCount; i++ {
		delay *= float64(multiplier)
	}

	nextRetry := time.Now().Add(time.Duration(delay))

	e.log.Infof("Scheduling job %s for retry at %v (attempt %d/%d)",
		job.ID, nextRetry, job.RetryCount+1, job.MaxRetries)

	_, err := e.jobRepo.MarkForRetry(e.ctx, job.ID, nextRetry)
	return err
}

// cleanupWorker periodically cleans up old jobs
func (e *JobExecutor) cleanupWorker() {
	defer e.wg.Done()

	// Run cleanup once per day
	ticker := time.NewTicker(24 * time.Hour)
	defer ticker.Stop()

	// Run initial cleanup
	e.runCleanup()

	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.runCleanup()
		}
	}
}

// runCleanup removes old completed/failed jobs
func (e *JobExecutor) runCleanup() {
	days := e.config.CleanupDays
	if days <= 0 {
		days = 30
	}

	deleted, err := e.jobRepo.CleanupOld(e.ctx, int(days))
	if err != nil {
		e.log.Errorf("Failed to cleanup old jobs: %v", err)
		return
	}

	if deleted > 0 {
		e.log.Infof("Cleaned up %d old jobs", deleted)
	}
}
