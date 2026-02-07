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
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/targetconfiguration"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

type TargetConfigurationRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper
}

func NewTargetConfigurationRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *TargetConfigurationRepo {
	return &TargetConfigurationRepo{
		log:       ctx.NewLoggerHelper("target_configuration/repo"),
		entClient: entClient,
	}
}

// Create creates a new target configuration
func (r *TargetConfigurationRepo) Create(ctx context.Context, tenantID uint32, name, description, providerType string,
	credentialsEncrypted []byte, config map[string]any) (*ent.TargetConfiguration, error) {

	id := uuid.New().String()

	builder := r.entClient.Client().TargetConfiguration.Create().
		SetID(id).
		SetTenantID(tenantID).
		SetName(name).
		SetProviderType(providerType).
		SetCredentialsEncrypted(credentialsEncrypted).
		SetStatus(targetconfiguration.StatusCONFIG_STATUS_ACTIVE).
		SetCreateTime(time.Now())

	if description != "" {
		builder.SetDescription(description)
	}
	if config != nil {
		builder.SetConfig(config)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create target configuration failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("create target configuration failed")
	}

	return entity, nil
}

// GetByID retrieves a target configuration by ID
func (r *TargetConfigurationRepo) GetByID(ctx context.Context, id string) (*ent.TargetConfiguration, error) {
	entity, err := r.entClient.Client().TargetConfiguration.Get(ctx, id)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get target configuration failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get target configuration failed")
	}
	return entity, nil
}

// GetByTenantAndName retrieves a target configuration by tenant ID and name
func (r *TargetConfigurationRepo) GetByTenantAndName(ctx context.Context, tenantID uint32, name string) (*ent.TargetConfiguration, error) {
	entity, err := r.entClient.Client().TargetConfiguration.Query().
		Where(
			targetconfiguration.TenantIDEQ(tenantID),
			targetconfiguration.NameEQ(name),
		).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get target configuration by name failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get target configuration failed")
	}
	return entity, nil
}

// List lists target configurations with optional filters
func (r *TargetConfigurationRepo) List(ctx context.Context, tenantID *uint32, providerType *string, status *targetconfiguration.Status,
	page, pageSize uint32) ([]*ent.TargetConfiguration, int, error) {

	query := r.entClient.Client().TargetConfiguration.Query()

	if tenantID != nil {
		query = query.Where(targetconfiguration.TenantIDEQ(*tenantID))
	}
	if providerType != nil {
		query = query.Where(targetconfiguration.ProviderTypeEQ(*providerType))
	}
	if status != nil {
		query = query.Where(targetconfiguration.StatusEQ(*status))
	}

	// Count total
	total, err := query.Clone().Count(ctx)
	if err != nil {
		r.log.Errorf("count target configurations failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("count target configurations failed")
	}

	// Apply pagination
	if page > 0 && pageSize > 0 {
		offset := int((page - 1) * pageSize)
		query = query.Offset(offset).Limit(int(pageSize))
	}

	entities, err := query.Order(ent.Desc(targetconfiguration.FieldCreateTime)).All(ctx)
	if err != nil {
		r.log.Errorf("list target configurations failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("list target configurations failed")
	}

	return entities, total, nil
}

// ListByIDs retrieves multiple target configurations by IDs
func (r *TargetConfigurationRepo) ListByIDs(ctx context.Context, ids []string) ([]*ent.TargetConfiguration, error) {
	entities, err := r.entClient.Client().TargetConfiguration.Query().
		Where(targetconfiguration.IDIn(ids...)).
		All(ctx)
	if err != nil {
		r.log.Errorf("list target configurations by IDs failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("list target configurations failed")
	}
	return entities, nil
}

// Update updates a target configuration
func (r *TargetConfigurationRepo) Update(ctx context.Context, id string, name, description *string,
	credentialsEncrypted []byte, config map[string]any, status *targetconfiguration.Status) (*ent.TargetConfiguration, error) {

	builder := r.entClient.Client().TargetConfiguration.UpdateOneID(id).
		SetUpdateTime(time.Now())

	if name != nil {
		builder.SetName(*name)
	}
	if description != nil {
		builder.SetDescription(*description)
	}
	if credentialsEncrypted != nil {
		builder.SetCredentialsEncrypted(credentialsEncrypted)
	}
	if config != nil {
		builder.SetConfig(config)
	}
	if status != nil {
		builder.SetStatus(*status)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
		}
		r.log.Errorf("update target configuration failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("update target configuration failed")
	}

	return entity, nil
}

// UpdateStatus updates the status of a target configuration
func (r *TargetConfigurationRepo) UpdateStatus(ctx context.Context, id string, status targetconfiguration.Status, message string) (*ent.TargetConfiguration, error) {
	builder := r.entClient.Client().TargetConfiguration.UpdateOneID(id).
		SetStatus(status).
		SetUpdateTime(time.Now())

	if message != "" {
		builder.SetStatusMessage(message)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("update target configuration status failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("update target configuration status failed")
	}
	return entity, nil
}

// UpdateLastDeployment updates the last deployment timestamp
func (r *TargetConfigurationRepo) UpdateLastDeployment(ctx context.Context, id string) error {
	now := time.Now()
	err := r.entClient.Client().TargetConfiguration.UpdateOneID(id).
		SetLastDeploymentAt(now).
		SetUpdateTime(now).
		Exec(ctx)
	if err != nil {
		r.log.Errorf("update last deployment failed: %s", err.Error())
		return deployerV1.ErrorInternalServerError("update last deployment failed")
	}
	return nil
}

// Delete deletes a target configuration
func (r *TargetConfigurationRepo) Delete(ctx context.Context, id string) error {
	err := r.entClient.Client().TargetConfiguration.DeleteOneID(id).Exec(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return deployerV1.ErrorConfigurationNotFound("target configuration not found")
		}
		r.log.Errorf("delete target configuration failed: %s", err.Error())
		return deployerV1.ErrorInternalServerError("delete target configuration failed")
	}
	return nil
}

// GetCredentialsEncrypted retrieves the encrypted credentials for a configuration
func (r *TargetConfigurationRepo) GetCredentialsEncrypted(ctx context.Context, id string) ([]byte, error) {
	entity, err := r.entClient.Client().TargetConfiguration.Query().
		Where(targetconfiguration.IDEQ(id)).
		Select(targetconfiguration.FieldCredentialsEncrypted).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, deployerV1.ErrorConfigurationNotFound("target configuration not found")
		}
		r.log.Errorf("get credentials failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get credentials failed")
	}
	return entity.CredentialsEncrypted, nil
}

// ToProto converts an ent.TargetConfiguration to deployerV1.TargetConfiguration
func (r *TargetConfigurationRepo) ToProto(entity *ent.TargetConfiguration) *deployerV1.TargetConfiguration {
	if entity == nil {
		return nil
	}

	proto := &deployerV1.TargetConfiguration{
		Id:           &entity.ID,
		TenantId:     entity.TenantID,
		Name:         &entity.Name,
		ProviderType: &entity.ProviderType,
	}

	if entity.Description != "" {
		proto.Description = &entity.Description
	}
	if entity.StatusMessage != "" {
		proto.StatusMessage = &entity.StatusMessage
	}

	// Map status
	switch entity.Status {
	case targetconfiguration.StatusCONFIG_STATUS_ACTIVE:
		s := deployerV1.ConfigurationStatus_CONFIG_STATUS_ACTIVE
		proto.Status = &s
	case targetconfiguration.StatusCONFIG_STATUS_INACTIVE:
		s := deployerV1.ConfigurationStatus_CONFIG_STATUS_INACTIVE
		proto.Status = &s
	case targetconfiguration.StatusCONFIG_STATUS_ERROR:
		s := deployerV1.ConfigurationStatus_CONFIG_STATUS_ERROR
		proto.Status = &s
	default:
		s := deployerV1.ConfigurationStatus_CONFIG_STATUS_UNSPECIFIED
		proto.Status = &s
	}

	// Convert config
	if entity.Config != nil {
		configStruct, err := structpb.NewStruct(entity.Config)
		if err == nil {
			proto.Config = configStruct
		}
	}

	// Convert timestamps
	if entity.LastDeploymentAt != nil {
		proto.LastDeploymentAt = timestamppb.New(*entity.LastDeploymentAt)
	}
	if entity.CreateBy != nil {
		proto.CreatedBy = entity.CreateBy
	}
	if entity.UpdateBy != nil {
		proto.UpdatedBy = entity.UpdateBy
	}
	if entity.CreateTime != nil && !entity.CreateTime.IsZero() {
		proto.CreateTime = timestamppb.New(*entity.CreateTime)
	}
	if entity.UpdateTime != nil && !entity.UpdateTime.IsZero() {
		proto.UpdateTime = timestamppb.New(*entity.UpdateTime)
	}

	return proto
}
