package data

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/timestamppb"

	entCrud "github.com/tx7do/go-crud/entgo"

	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenttarget"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/schema"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

type DeploymentTargetRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper
}

func NewDeploymentTargetRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *DeploymentTargetRepo {
	return &DeploymentTargetRepo{
		log:       ctx.NewLoggerHelper("deployment_target/repo"),
		entClient: entClient,
	}
}

// Create creates a new deployment target (group)
func (r *DeploymentTargetRepo) Create(ctx context.Context, tenantID uint32, name, description string,
	autoDeployOnRenewal bool, filters []schema.CertificateFilter, configIDs []string) (*ent.DeploymentTarget, error) {

	id := uuid.New().String()

	builder := r.entClient.Client().DeploymentTarget.Create().
		SetID(id).
		SetTenantID(tenantID).
		SetName(name).
		SetAutoDeployOnRenewal(autoDeployOnRenewal).
		SetCreateTime(time.Now())

	if description != "" {
		builder.SetDescription(description)
	}
	if filters != nil {
		builder.SetCertificateFilters(filters)
	}

	// Link configurations if provided
	if len(configIDs) > 0 {
		builder.AddConfigurationIDs(configIDs...)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create deployment target failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("create deployment target failed")
	}

	return entity, nil
}

// GetByID retrieves a deployment target by ID
func (r *DeploymentTargetRepo) GetByID(ctx context.Context, id string) (*ent.DeploymentTarget, error) {
	entity, err := r.entClient.Client().DeploymentTarget.Get(ctx, id)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get deployment target failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get deployment target failed")
	}
	return entity, nil
}

// GetByIDWithConfigurations retrieves a deployment target by ID with linked configurations
func (r *DeploymentTargetRepo) GetByIDWithConfigurations(ctx context.Context, id string) (*ent.DeploymentTarget, error) {
	entity, err := r.entClient.Client().DeploymentTarget.Query().
		Where(deploymenttarget.IDEQ(id)).
		WithConfigurations().
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get deployment target with configurations failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get deployment target failed")
	}
	return entity, nil
}

// GetByTenantAndName retrieves a deployment target by tenant ID and name
func (r *DeploymentTargetRepo) GetByTenantAndName(ctx context.Context, tenantID uint32, name string) (*ent.DeploymentTarget, error) {
	entity, err := r.entClient.Client().DeploymentTarget.Query().
		Where(
			deploymenttarget.TenantIDEQ(tenantID),
			deploymenttarget.NameEQ(name),
		).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get deployment target by name failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get deployment target failed")
	}
	return entity, nil
}

// List lists deployment targets with optional filters
func (r *DeploymentTargetRepo) List(ctx context.Context, tenantID *uint32, autoDeployOnRenewal *bool,
	includeConfigurations bool, page, pageSize uint32) ([]*ent.DeploymentTarget, int, error) {

	query := r.entClient.Client().DeploymentTarget.Query()

	if tenantID != nil {
		query = query.Where(deploymenttarget.TenantIDEQ(*tenantID))
	}
	if autoDeployOnRenewal != nil {
		query = query.Where(deploymenttarget.AutoDeployOnRenewalEQ(*autoDeployOnRenewal))
	}

	// Count total
	total, err := query.Clone().Count(ctx)
	if err != nil {
		r.log.Errorf("count deployment targets failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("count deployment targets failed")
	}

	// Include configurations if requested
	if includeConfigurations {
		query = query.WithConfigurations()
	}

	// Apply pagination
	if page > 0 && pageSize > 0 {
		offset := int((page - 1) * pageSize)
		query = query.Offset(offset).Limit(int(pageSize))
	}

	entities, err := query.Order(ent.Desc(deploymenttarget.FieldCreateTime)).All(ctx)
	if err != nil {
		r.log.Errorf("list deployment targets failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("list deployment targets failed")
	}

	return entities, total, nil
}

// ListByAutoDeployEnabled lists targets that have auto-deploy enabled
func (r *DeploymentTargetRepo) ListByAutoDeployEnabled(ctx context.Context) ([]*ent.DeploymentTarget, error) {
	entities, err := r.entClient.Client().DeploymentTarget.Query().
		Where(deploymenttarget.AutoDeployOnRenewalEQ(true)).
		WithConfigurations().
		All(ctx)
	if err != nil {
		r.log.Errorf("list auto-deploy targets failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("list auto-deploy targets failed")
	}
	return entities, nil
}

// Update updates a deployment target
func (r *DeploymentTargetRepo) Update(ctx context.Context, id string, name, description *string,
	autoDeployOnRenewal *bool, filters []schema.CertificateFilter) (*ent.DeploymentTarget, error) {

	builder := r.entClient.Client().DeploymentTarget.UpdateOneID(id).
		SetUpdateTime(time.Now())

	if name != nil {
		builder.SetName(*name)
	}
	if description != nil {
		builder.SetDescription(*description)
	}
	if autoDeployOnRenewal != nil {
		builder.SetAutoDeployOnRenewal(*autoDeployOnRenewal)
	}
	if filters != nil {
		builder.SetCertificateFilters(filters)
	}

	entity, err := builder.Save(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
		}
		r.log.Errorf("update deployment target failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("update deployment target failed")
	}

	return entity, nil
}

// AddConfigurations adds configurations to a deployment target
func (r *DeploymentTargetRepo) AddConfigurations(ctx context.Context, id string, configIDs []string) (*ent.DeploymentTarget, error) {
	entity, err := r.entClient.Client().DeploymentTarget.UpdateOneID(id).
		AddConfigurationIDs(configIDs...).
		SetUpdateTime(time.Now()).
		Save(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
		}
		r.log.Errorf("add configurations failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("add configurations failed")
	}
	return entity, nil
}

// RemoveConfigurations removes configurations from a deployment target
func (r *DeploymentTargetRepo) RemoveConfigurations(ctx context.Context, id string, configIDs []string) (*ent.DeploymentTarget, error) {
	entity, err := r.entClient.Client().DeploymentTarget.UpdateOneID(id).
		RemoveConfigurationIDs(configIDs...).
		SetUpdateTime(time.Now()).
		Save(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
		}
		r.log.Errorf("remove configurations failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("remove configurations failed")
	}
	return entity, nil
}

// GetConfigurations retrieves configurations linked to a deployment target
func (r *DeploymentTargetRepo) GetConfigurations(ctx context.Context, id string, page, pageSize uint32) ([]*ent.TargetConfiguration, int, error) {
	target, err := r.entClient.Client().DeploymentTarget.Query().
		Where(deploymenttarget.IDEQ(id)).
		WithConfigurations().
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, 0, deployerV1.ErrorTargetNotFound("deployment target not found")
		}
		r.log.Errorf("get configurations failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("get configurations failed")
	}

	configs := target.Edges.Configurations
	total := len(configs)

	// Apply pagination
	if page > 0 && pageSize > 0 {
		offset := int((page - 1) * pageSize)
		end := offset + int(pageSize)
		if offset > len(configs) {
			return []*ent.TargetConfiguration{}, total, nil
		}
		if end > len(configs) {
			end = len(configs)
		}
		configs = configs[offset:end]
	}

	return configs, total, nil
}

// GetConfigurationCount returns the count of linked configurations
func (r *DeploymentTargetRepo) GetConfigurationCount(ctx context.Context, id string) (int, error) {
	count, err := r.entClient.Client().DeploymentTarget.Query().
		Where(deploymenttarget.IDEQ(id)).
		QueryConfigurations().
		Count(ctx)
	if err != nil {
		r.log.Errorf("count configurations failed: %s", err.Error())
		return 0, deployerV1.ErrorInternalServerError("count configurations failed")
	}
	return count, nil
}

// Delete deletes a deployment target
func (r *DeploymentTargetRepo) Delete(ctx context.Context, id string) error {
	err := r.entClient.Client().DeploymentTarget.DeleteOneID(id).Exec(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return deployerV1.ErrorTargetNotFound("deployment target not found")
		}
		r.log.Errorf("delete deployment target failed: %s", err.Error())
		return deployerV1.ErrorInternalServerError("delete deployment target failed")
	}
	return nil
}

// ToProto converts an ent.DeploymentTarget to deployerV1.DeploymentTarget
func (r *DeploymentTargetRepo) ToProto(entity *ent.DeploymentTarget, configRepo *TargetConfigurationRepo) *deployerV1.DeploymentTarget {
	if entity == nil {
		return nil
	}

	proto := &deployerV1.DeploymentTarget{
		Id:                  &entity.ID,
		TenantId:            entity.TenantID,
		Name:                &entity.Name,
		AutoDeployOnRenewal: &entity.AutoDeployOnRenewal,
	}

	if entity.Description != "" {
		proto.Description = &entity.Description
	}

	// Convert certificate filters
	if entity.CertificateFilters != nil {
		for _, f := range entity.CertificateFilters {
			filter := &deployerV1.CertificateFilter{}
			if f.IssuerName != "" {
				filter.IssuerName = &f.IssuerName
			}
			if f.CommonNamePattern != "" {
				filter.CommonNamePattern = &f.CommonNamePattern
			}
			if f.SANPattern != "" {
				filter.SanPattern = &f.SANPattern
			}
			if f.SubjectOrganization != "" {
				filter.SubjectOrganization = &f.SubjectOrganization
			}
			if f.SubjectOrgUnit != "" {
				filter.SubjectOrgUnit = &f.SubjectOrgUnit
			}
			if f.SubjectCountry != "" {
				filter.SubjectCountry = &f.SubjectCountry
			}
			if f.DomainPattern != "" {
				filter.DomainPattern = &f.DomainPattern
			}
			if len(f.Labels) > 0 {
				filter.Labels = f.Labels
			}
			proto.CertificateFilters = append(proto.CertificateFilters, filter)
		}
	}

	// Include configurations if loaded
	if entity.Edges.Configurations != nil {
		configCount := int32(len(entity.Edges.Configurations))
		proto.ConfigurationCount = &configCount
		for _, config := range entity.Edges.Configurations {
			if configRepo != nil {
				proto.Configurations = append(proto.Configurations, configRepo.ToProto(config))
			}
		}
	}

	// Convert timestamps
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
