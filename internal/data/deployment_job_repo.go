package data

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	entCrud "github.com/tx7do/go-crud/entgo"

	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

type DeploymentJobRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper
}

func NewDeploymentJobRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *DeploymentJobRepo {
	return &DeploymentJobRepo{
		log:       ctx.NewLoggerHelper("deployment_job/repo"),
		entClient: entClient,
	}
}

// CreateParentJob creates a new parent job for deploying to a target group
func (r *DeploymentJobRepo) CreateParentJob(ctx context.Context, tenantID uint32, deploymentTargetID, certificateID, certificateSerial string,
	triggeredBy deploymentjob.TriggeredBy, maxRetries int32) (*ent.DeploymentJob, error) {

	id := uuid.New().String()

	builder := r.entClient.Client().DeploymentJob.Create().
		SetID(id).
		SetTenantID(tenantID).
		SetDeploymentTargetID(deploymentTargetID).
		SetCertificateID(certificateID).
		SetStatus(deploymentjob.StatusJOB_STATUS_PENDING).
		SetTriggeredBy(triggeredBy).
		SetMaxRetries(maxRetries).
		SetProgress(0).
		SetRetryCount(0).
		SetCreateTime(time.Now())

	if certificateSerial != "" {
		builder.SetCertificateSerial(certificateSerial)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create parent job failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("create parent job failed")
	}

	return entity, nil
}

// CreateChildJob creates a child job for a parent job
func (r *DeploymentJobRepo) CreateChildJob(ctx context.Context, tenantID uint32, parentJobID, targetConfigurationID, certificateID, certificateSerial string,
	triggeredBy deploymentjob.TriggeredBy, maxRetries int32) (*ent.DeploymentJob, error) {

	id := uuid.New().String()

	builder := r.entClient.Client().DeploymentJob.Create().
		SetID(id).
		SetTenantID(tenantID).
		SetParentJobID(parentJobID).
		SetTargetConfigurationID(targetConfigurationID).
		SetCertificateID(certificateID).
		SetStatus(deploymentjob.StatusJOB_STATUS_PENDING).
		SetTriggeredBy(triggeredBy).
		SetMaxRetries(maxRetries).
		SetProgress(0).
		SetRetryCount(0).
		SetCreateTime(time.Now())

	if certificateSerial != "" {
		builder.SetCertificateSerial(certificateSerial)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create child job failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("create child job failed")
	}

	return entity, nil
}

// CreateDirectJob creates a direct job to a single target configuration (legacy/manual)
func (r *DeploymentJobRepo) CreateDirectJob(ctx context.Context, tenantID uint32, targetConfigurationID, certificateID, certificateSerial string,
	triggeredBy deploymentjob.TriggeredBy, maxRetries int32) (*ent.DeploymentJob, error) {

	id := uuid.New().String()

	builder := r.entClient.Client().DeploymentJob.Create().
		SetID(id).
		SetTenantID(tenantID).
		SetTargetConfigurationID(targetConfigurationID).
		SetCertificateID(certificateID).
		SetStatus(deploymentjob.StatusJOB_STATUS_PENDING).
		SetTriggeredBy(triggeredBy).
		SetMaxRetries(maxRetries).
		SetProgress(0).
		SetRetryCount(0).
		SetCreateTime(time.Now())

	if certificateSerial != "" {
		builder.SetCertificateSerial(certificateSerial)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create direct job failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("create direct job failed")
	}

	return entity, nil
}

// GetByID retrieves a deployment job by ID
func (r *DeploymentJobRepo) GetByID(ctx context.Context, id string) (*ent.DeploymentJob, error) {
	entity, err := r.entClient.Client().DeploymentJob.Query().
		Where(deploymentjob.IDEQ(id)).
		WithDeploymentTarget().
		WithTargetConfiguration().
		WithParentJob().
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get deployment job failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get deployment job failed")
	}
	return entity, nil
}

// GetByIDWithChildJobs retrieves a deployment job by ID with child jobs
func (r *DeploymentJobRepo) GetByIDWithChildJobs(ctx context.Context, id string) (*ent.DeploymentJob, error) {
	entity, err := r.entClient.Client().DeploymentJob.Query().
		Where(deploymentjob.IDEQ(id)).
		WithDeploymentTarget().
		WithTargetConfiguration().
		WithChildJobs(func(q *ent.DeploymentJobQuery) {
			q.WithTargetConfiguration()
		}).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get deployment job with children failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get deployment job failed")
	}
	return entity, nil
}

// List lists deployment jobs with optional filters
func (r *DeploymentJobRepo) List(ctx context.Context, tenantID *uint32, deploymentTargetID, targetConfigurationID, certificateID, parentJobID *string,
	status *deploymentjob.Status, triggeredBy *deploymentjob.TriggeredBy, jobType *deployerV1.JobType,
	createdAfter, createdBefore *time.Time, includeChildJobs bool, page, pageSize uint32) ([]*ent.DeploymentJob, int, error) {

	query := r.entClient.Client().DeploymentJob.Query()

	if tenantID != nil {
		query = query.Where(deploymentjob.TenantIDEQ(*tenantID))
	}
	if deploymentTargetID != nil {
		query = query.Where(deploymentjob.DeploymentTargetIDEQ(*deploymentTargetID))
	}
	if targetConfigurationID != nil {
		query = query.Where(deploymentjob.TargetConfigurationIDEQ(*targetConfigurationID))
	}
	if certificateID != nil {
		query = query.Where(deploymentjob.CertificateIDEQ(*certificateID))
	}
	if parentJobID != nil {
		query = query.Where(deploymentjob.ParentJobIDEQ(*parentJobID))
	}
	if status != nil {
		query = query.Where(deploymentjob.StatusEQ(*status))
	}
	if triggeredBy != nil {
		query = query.Where(deploymentjob.TriggeredByEQ(*triggeredBy))
	}
	if createdAfter != nil {
		query = query.Where(deploymentjob.CreateTimeGTE(*createdAfter))
	}
	if createdBefore != nil {
		query = query.Where(deploymentjob.CreateTimeLTE(*createdBefore))
	}

	// Filter by job type
	if jobType != nil {
		switch *jobType {
		case deployerV1.JobType_JOB_TYPE_PARENT:
			query = query.Where(
				deploymentjob.DeploymentTargetIDNotNil(),
				deploymentjob.ParentJobIDIsNil(),
			)
		case deployerV1.JobType_JOB_TYPE_CHILD:
			query = query.Where(deploymentjob.ParentJobIDNotNil())
		case deployerV1.JobType_JOB_TYPE_DIRECT:
			query = query.Where(
				deploymentjob.TargetConfigurationIDNotNil(),
				deploymentjob.DeploymentTargetIDIsNil(),
				deploymentjob.ParentJobIDIsNil(),
			)
		}
	}

	// Count total
	total, err := query.Clone().Count(ctx)
	if err != nil {
		r.log.Errorf("count deployment jobs failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("count deployment jobs failed")
	}

	// Include edges
	query = query.WithDeploymentTarget().WithTargetConfiguration()
	if includeChildJobs {
		query = query.WithChildJobs(func(q *ent.DeploymentJobQuery) {
			q.WithTargetConfiguration()
		})
	}

	// Apply pagination
	if page > 0 && pageSize > 0 {
		offset := int((page - 1) * pageSize)
		query = query.Offset(offset).Limit(int(pageSize))
	}

	entities, err := query.Order(ent.Desc(deploymentjob.FieldCreateTime)).All(ctx)
	if err != nil {
		r.log.Errorf("list deployment jobs failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("list deployment jobs failed")
	}

	return entities, total, nil
}

// ListPending lists pending jobs ordered by creation time (excludes parent jobs)
func (r *DeploymentJobRepo) ListPending(ctx context.Context, limit int) ([]*ent.DeploymentJob, error) {
	entities, err := r.entClient.Client().DeploymentJob.Query().
		Where(
			deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_PENDING),
			// Only child/direct jobs - parent jobs don't execute directly
			deploymentjob.TargetConfigurationIDNotNil(),
		).
		Order(ent.Asc(deploymentjob.FieldCreateTime)).
		Limit(limit).
		WithTargetConfiguration().
		All(ctx)
	if err != nil {
		r.log.Errorf("list pending jobs failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("list pending jobs failed")
	}
	return entities, nil
}

// ListRetryable lists jobs that are due for retry
func (r *DeploymentJobRepo) ListRetryable(ctx context.Context, limit int) ([]*ent.DeploymentJob, error) {
	now := time.Now()
	entities, err := r.entClient.Client().DeploymentJob.Query().
		Where(
			deploymentjob.StatusEQ(deploymentjob.StatusJOB_STATUS_RETRYING),
			deploymentjob.NextRetryAtLTE(now),
			// Only child/direct jobs
			deploymentjob.TargetConfigurationIDNotNil(),
		).
		Order(ent.Asc(deploymentjob.FieldNextRetryAt)).
		Limit(limit).
		WithTargetConfiguration().
		All(ctx)
	if err != nil {
		r.log.Errorf("list retryable jobs failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("list retryable jobs failed")
	}
	return entities, nil
}

// ListChildJobs lists child jobs for a parent job
func (r *DeploymentJobRepo) ListChildJobs(ctx context.Context, parentJobID string) ([]*ent.DeploymentJob, error) {
	entities, err := r.entClient.Client().DeploymentJob.Query().
		Where(deploymentjob.ParentJobIDEQ(parentJobID)).
		WithTargetConfiguration().
		Order(ent.Asc(deploymentjob.FieldCreateTime)).
		All(ctx)
	if err != nil {
		r.log.Errorf("list child jobs failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("list child jobs failed")
	}
	return entities, nil
}

// GetChildJobCounts returns the count of total, completed, and failed child jobs
func (r *DeploymentJobRepo) GetChildJobCounts(ctx context.Context, parentJobID string) (total, completed, failed int, err error) {
	childJobs, err := r.ListChildJobs(ctx, parentJobID)
	if err != nil {
		return 0, 0, 0, err
	}

	for _, job := range childJobs {
		total++
		switch job.Status {
		case deploymentjob.StatusJOB_STATUS_COMPLETED:
			completed++
		case deploymentjob.StatusJOB_STATUS_FAILED:
			failed++
		}
	}

	return total, completed, failed, nil
}

// UpdateStatus updates the status of a deployment job
func (r *DeploymentJobRepo) UpdateStatus(ctx context.Context, id string, status deploymentjob.Status, message string, progress int32) (*ent.DeploymentJob, error) {
	builder := r.entClient.Client().DeploymentJob.UpdateOneID(id).
		SetStatus(status).
		SetUpdateTime(time.Now())

	if message != "" {
		builder.SetStatusMessage(message)
	}
	if progress >= 0 && progress <= 100 {
		builder.SetProgress(progress)
	}

	// Set timestamps based on status
	now := time.Now()
	switch status {
	case deploymentjob.StatusJOB_STATUS_PROCESSING:
		builder.SetStartedAt(now)
	case deploymentjob.StatusJOB_STATUS_COMPLETED, deploymentjob.StatusJOB_STATUS_FAILED, deploymentjob.StatusJOB_STATUS_CANCELLED, deploymentjob.StatusJOB_STATUS_PARTIAL:
		builder.SetCompletedAt(now)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("update job status failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("update job status failed")
	}
	return entity, nil
}

// SetResult sets the result of a deployment job
func (r *DeploymentJobRepo) SetResult(ctx context.Context, id string, result map[string]any) error {
	err := r.entClient.Client().DeploymentJob.UpdateOneID(id).
		SetResult(result).
		SetUpdateTime(time.Now()).
		Exec(ctx)
	if err != nil {
		r.log.Errorf("set job result failed: %s", err.Error())
		return deployerV1.ErrorInternalServerError("set job result failed")
	}
	return nil
}

// MarkForRetry marks a job for retry
func (r *DeploymentJobRepo) MarkForRetry(ctx context.Context, id string, nextRetryAt time.Time) (*ent.DeploymentJob, error) {
	entity, err := r.entClient.Client().DeploymentJob.UpdateOneID(id).
		SetStatus(deploymentjob.StatusJOB_STATUS_RETRYING).
		SetNextRetryAt(nextRetryAt).
		AddRetryCount(1).
		SetUpdateTime(time.Now()).
		Save(ctx)
	if err != nil {
		r.log.Errorf("mark job for retry failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("mark job for retry failed")
	}
	return entity, nil
}

// ClaimJob atomically claims a job for processing
func (r *DeploymentJobRepo) ClaimJob(ctx context.Context, id string, expectedStatus deploymentjob.Status) (bool, error) {
	affected, err := r.entClient.Client().DeploymentJob.Update().
		Where(
			deploymentjob.IDEQ(id),
			deploymentjob.StatusEQ(expectedStatus),
		).
		SetStatus(deploymentjob.StatusJOB_STATUS_PROCESSING).
		SetStatusMessage("Processing").
		SetStartedAt(time.Now()).
		SetUpdateTime(time.Now()).
		Save(ctx)

	if err != nil {
		r.log.Errorf("claim job failed: %s", err.Error())
		return false, deployerV1.ErrorInternalServerError("claim job failed")
	}

	return affected > 0, nil
}

// Cancel cancels a pending or processing job
func (r *DeploymentJobRepo) Cancel(ctx context.Context, id string, cancelChildJobs bool) (*ent.DeploymentJob, error) {
	job, err := r.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, deployerV1.ErrorJobNotFound("deployment job not found")
	}

	if job.Status != deploymentjob.StatusJOB_STATUS_PENDING &&
		job.Status != deploymentjob.StatusJOB_STATUS_PROCESSING &&
		job.Status != deploymentjob.StatusJOB_STATUS_RETRYING {
		return nil, deployerV1.ErrorConflict("job cannot be cancelled in current state")
	}

	// Cancel child jobs if requested and this is a parent job
	if cancelChildJobs && job.DeploymentTargetID != nil {
		childJobs, err := r.ListChildJobs(ctx, job.ID)
		if err != nil {
			return nil, err
		}
		for _, childJob := range childJobs {
			if childJob.Status == deploymentjob.StatusJOB_STATUS_PENDING ||
				childJob.Status == deploymentjob.StatusJOB_STATUS_PROCESSING ||
				childJob.Status == deploymentjob.StatusJOB_STATUS_RETRYING {
				if _, err := r.UpdateStatus(ctx, childJob.ID, deploymentjob.StatusJOB_STATUS_CANCELLED, "Cancelled by parent job", childJob.Progress); err != nil {
				r.log.Warnf("Failed to cancel child job %s: %v", childJob.ID, err)
			}
			}
		}
	}

	return r.UpdateStatus(ctx, id, deploymentjob.StatusJOB_STATUS_CANCELLED, "Job cancelled by user", job.Progress)
}

// CleanupOld deletes jobs older than the specified number of days
func (r *DeploymentJobRepo) CleanupOld(ctx context.Context, days int) (int, error) {
	cutoff := time.Now().AddDate(0, 0, -days)
	affected, err := r.entClient.Client().DeploymentJob.Delete().
		Where(
			deploymentjob.CreateTimeLT(cutoff),
			deploymentjob.StatusIn(
				deploymentjob.StatusJOB_STATUS_COMPLETED,
				deploymentjob.StatusJOB_STATUS_FAILED,
				deploymentjob.StatusJOB_STATUS_CANCELLED,
			),
		).
		Exec(ctx)
	if err != nil {
		r.log.Errorf("cleanup old jobs failed: %s", err.Error())
		return 0, deployerV1.ErrorInternalServerError("cleanup old jobs failed")
	}
	return affected, nil
}

// GetJobType determines the job type based on field values
func (r *DeploymentJobRepo) GetJobType(job *ent.DeploymentJob) deployerV1.JobType {
	if job == nil {
		return deployerV1.JobType_JOB_TYPE_UNSPECIFIED
	}

	if job.ParentJobID != nil {
		return deployerV1.JobType_JOB_TYPE_CHILD
	}
	if job.DeploymentTargetID != nil {
		return deployerV1.JobType_JOB_TYPE_PARENT
	}
	if job.TargetConfigurationID != nil {
		return deployerV1.JobType_JOB_TYPE_DIRECT
	}
	return deployerV1.JobType_JOB_TYPE_UNSPECIFIED
}

// ToProto converts an ent.DeploymentJob to deployerV1.DeploymentJob
func (r *DeploymentJobRepo) ToProto(entity *ent.DeploymentJob) *deployerV1.DeploymentJob {
	if entity == nil {
		return nil
	}

	proto := &deployerV1.DeploymentJob{
		Id:            &entity.ID,
		TenantId:      entity.TenantID,
		CertificateId: &entity.CertificateID,
		Progress:      &entity.Progress,
		RetryCount:    &entity.RetryCount,
		MaxRetries:    &entity.MaxRetries,
	}

	// Set IDs
	if entity.DeploymentTargetID != nil {
		proto.DeploymentTargetId = entity.DeploymentTargetID
	}
	if entity.TargetConfigurationID != nil {
		proto.TargetConfigurationId = entity.TargetConfigurationID
	}
	if entity.ParentJobID != nil {
		proto.ParentJobId = entity.ParentJobID
	}

	// Job type
	jobType := r.GetJobType(entity)
	proto.JobType = &jobType

	if entity.CertificateSerial != "" {
		proto.CertificateSerial = &entity.CertificateSerial
	}
	if entity.StatusMessage != "" {
		proto.StatusMessage = &entity.StatusMessage
	}

	// Get names from edges
	if entity.Edges.DeploymentTarget != nil {
		proto.DeploymentTargetName = &entity.Edges.DeploymentTarget.Name
	}
	if entity.Edges.TargetConfiguration != nil {
		proto.TargetConfigurationName = &entity.Edges.TargetConfiguration.Name
	}

	// Map status
	switch entity.Status {
	case deploymentjob.StatusJOB_STATUS_PENDING:
		s := deployerV1.JobStatus_JOB_STATUS_PENDING
		proto.Status = &s
	case deploymentjob.StatusJOB_STATUS_PROCESSING:
		s := deployerV1.JobStatus_JOB_STATUS_PROCESSING
		proto.Status = &s
	case deploymentjob.StatusJOB_STATUS_COMPLETED:
		s := deployerV1.JobStatus_JOB_STATUS_COMPLETED
		proto.Status = &s
	case deploymentjob.StatusJOB_STATUS_FAILED:
		s := deployerV1.JobStatus_JOB_STATUS_FAILED
		proto.Status = &s
	case deploymentjob.StatusJOB_STATUS_CANCELLED:
		s := deployerV1.JobStatus_JOB_STATUS_CANCELLED
		proto.Status = &s
	case deploymentjob.StatusJOB_STATUS_RETRYING:
		s := deployerV1.JobStatus_JOB_STATUS_RETRYING
		proto.Status = &s
	case deploymentjob.StatusJOB_STATUS_PARTIAL:
		s := deployerV1.JobStatus_JOB_STATUS_PARTIAL
		proto.Status = &s
	default:
		s := deployerV1.JobStatus_JOB_STATUS_UNSPECIFIED
		proto.Status = &s
	}

	// Map triggered by
	switch entity.TriggeredBy {
	case deploymentjob.TriggeredByTRIGGER_TYPE_MANUAL:
		t := deployerV1.TriggerType_TRIGGER_TYPE_MANUAL
		proto.TriggeredBy = &t
	case deploymentjob.TriggeredByTRIGGER_TYPE_EVENT:
		t := deployerV1.TriggerType_TRIGGER_TYPE_EVENT
		proto.TriggeredBy = &t
	case deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL:
		t := deployerV1.TriggerType_TRIGGER_TYPE_AUTO_RENEWAL
		proto.TriggeredBy = &t
	default:
		t := deployerV1.TriggerType_TRIGGER_TYPE_UNSPECIFIED
		proto.TriggeredBy = &t
	}

	// Convert result
	if entity.Result != nil {
		resultStruct, err := structpb.NewStruct(entity.Result)
		if err == nil {
			proto.Result = resultStruct
		}
	}

	// Convert child jobs if loaded
	if entity.Edges.ChildJobs != nil {
		totalChild := int32(len(entity.Edges.ChildJobs))
		proto.TotalChildJobs = &totalChild

		var completedCount, failedCount int32
		for _, child := range entity.Edges.ChildJobs {
			proto.ChildJobs = append(proto.ChildJobs, r.ToProto(child))
			switch child.Status {
			case deploymentjob.StatusJOB_STATUS_COMPLETED:
				completedCount++
			case deploymentjob.StatusJOB_STATUS_FAILED:
				failedCount++
			}
		}
		proto.CompletedChildJobs = &completedCount
		proto.FailedChildJobs = &failedCount
	}

	// Convert timestamps
	if entity.StartedAt != nil {
		proto.StartedAt = timestamppb.New(*entity.StartedAt)
	}
	if entity.CompletedAt != nil {
		proto.CompletedAt = timestamppb.New(*entity.CompletedAt)
	}
	if entity.NextRetryAt != nil {
		proto.NextRetryAt = timestamppb.New(*entity.NextRetryAt)
	}
	if entity.CreateBy != nil {
		proto.CreatedBy = entity.CreateBy
	}
	if entity.CreateTime != nil && !entity.CreateTime.IsZero() {
		proto.CreateTime = timestamppb.New(*entity.CreateTime)
	}
	if entity.UpdateTime != nil && !entity.UpdateTime.IsZero() {
		proto.UpdateTime = timestamppb.New(*entity.UpdateTime)
	}

	return proto
}
