package data

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	entCrud "github.com/tx7do/go-crud/entgo"

	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenthistory"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

type DeploymentHistoryRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper
}

func NewDeploymentHistoryRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *DeploymentHistoryRepo {
	return &DeploymentHistoryRepo{
		log:       ctx.NewLoggerHelper("deployment_history/repo"),
		entClient: entClient,
	}
}

// Create creates a new deployment history entry
func (r *DeploymentHistoryRepo) Create(ctx context.Context, jobID string, action deploymenthistory.Action,
	result deploymenthistory.Result, message string, durationMs int64, details map[string]any) (*ent.DeploymentHistory, error) {

	builder := r.entClient.Client().DeploymentHistory.Create().
		SetJobID(jobID).
		SetAction(action).
		SetResult(result).
		SetDurationMs(durationMs).
		SetCreateTime(time.Now())

	if message != "" {
		builder.SetMessage(message)
	}
	if details != nil {
		builder.SetDetails(details)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create deployment history failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("create deployment history failed")
	}

	return entity, nil
}

// ListByJobID lists history entries for a job
func (r *DeploymentHistoryRepo) ListByJobID(ctx context.Context, jobID string) ([]*ent.DeploymentHistory, error) {
	entities, err := r.entClient.Client().DeploymentHistory.Query().
		Where(deploymenthistory.JobIDEQ(jobID)).
		Order(ent.Asc(deploymenthistory.FieldCreateTime)).
		All(ctx)
	if err != nil {
		r.log.Errorf("list deployment history failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("list deployment history failed")
	}
	return entities, nil
}

// GetLatestByJobID gets the latest history entry for a job
func (r *DeploymentHistoryRepo) GetLatestByJobID(ctx context.Context, jobID string) (*ent.DeploymentHistory, error) {
	entity, err := r.entClient.Client().DeploymentHistory.Query().
		Where(deploymenthistory.JobIDEQ(jobID)).
		Order(ent.Desc(deploymenthistory.FieldCreateTime)).
		First(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get latest deployment history failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get latest deployment history failed")
	}
	return entity, nil
}

// DeleteByJobID deletes all history entries for a job
func (r *DeploymentHistoryRepo) DeleteByJobID(ctx context.Context, jobID string) (int, error) {
	affected, err := r.entClient.Client().DeploymentHistory.Delete().
		Where(deploymenthistory.JobIDEQ(jobID)).
		Exec(ctx)
	if err != nil {
		r.log.Errorf("delete deployment history failed: %s", err.Error())
		return 0, deployerV1.ErrorInternalServerError("delete deployment history failed")
	}
	return affected, nil
}

// ToProto converts an ent.DeploymentHistory to deployerV1.JobHistoryEntry
func (r *DeploymentHistoryRepo) ToProto(entity *ent.DeploymentHistory) *deployerV1.JobHistoryEntry {
	if entity == nil {
		return nil
	}

	id := int32(entity.ID)
	proto := &deployerV1.JobHistoryEntry{
		Id:         &id,
		JobId:      &entity.JobID,
		DurationMs: &entity.DurationMs,
	}

	if entity.Message != "" {
		proto.Message = &entity.Message
	}

	// Map action
	var actionStr string
	switch entity.Action {
	case deploymenthistory.ActionACTION_DEPLOY:
		actionStr = "deploy"
	case deploymenthistory.ActionACTION_VERIFY:
		actionStr = "verify"
	case deploymenthistory.ActionACTION_ROLLBACK:
		actionStr = "rollback"
	}
	proto.Action = &actionStr

	// Map result
	var resultStr string
	switch entity.Result {
	case deploymenthistory.ResultRESULT_SUCCESS:
		resultStr = "success"
	case deploymenthistory.ResultRESULT_FAILURE:
		resultStr = "failure"
	case deploymenthistory.ResultRESULT_PARTIAL:
		resultStr = "partial"
	}
	proto.Result = &resultStr

	// Convert details
	if entity.Details != nil {
		detailsStruct, err := structpb.NewStruct(entity.Details)
		if err == nil {
			proto.Details = detailsStruct
		}
	}

	// Convert timestamps
	if entity.CreateTime != nil && !entity.CreateTime.IsZero() {
		proto.CreateTime = timestamppb.New(*entity.CreateTime)
	}

	return proto
}

// ToProtoList converts a list of ent.DeploymentHistory to deployerV1.JobHistoryEntry list
func (r *DeploymentHistoryRepo) ToProtoList(entities []*ent.DeploymentHistory) []*deployerV1.JobHistoryEntry {
	protos := make([]*deployerV1.JobHistoryEntry, len(entities))
	for i, entity := range entities {
		protos[i] = r.ToProto(entity)
	}
	return protos
}
