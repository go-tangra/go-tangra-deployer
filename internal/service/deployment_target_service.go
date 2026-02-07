package service

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/schema"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

// DeploymentTargetService implements the DeploymentTargetService gRPC service
// This service manages deployment target GROUPS (collections of target configurations)
type DeploymentTargetService struct {
	deployerV1.UnimplementedDeploymentTargetServiceServer

	log        *log.Helper
	targetRepo *data.DeploymentTargetRepo
	configRepo *data.TargetConfigurationRepo
}

// NewDeploymentTargetService creates a new DeploymentTargetService
func NewDeploymentTargetService(
	ctx *bootstrap.Context,
	targetRepo *data.DeploymentTargetRepo,
	configRepo *data.TargetConfigurationRepo,
) *DeploymentTargetService {
	return &DeploymentTargetService{
		log:        ctx.NewLoggerHelper("deployer/service/target"),
		targetRepo: targetRepo,
		configRepo: configRepo,
	}
}

// CreateTarget creates a new deployment target (group)
func (s *DeploymentTargetService) CreateTarget(ctx context.Context, req *deployerV1.CreateTargetRequest) (*deployerV1.CreateTargetResponse, error) {
	s.log.Infof("CreateTarget: tenant_id=%d, name=%s", req.GetTenantId(), req.GetName())

	// Check for duplicate name
	existing, err := s.targetRepo.GetByTenantAndName(ctx, req.GetTenantId(), req.GetName())
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return nil, deployerV1.ErrorTargetNameExists("target with name '%s' already exists", req.GetName())
	}

	// Convert certificate filters
	var filters []schema.CertificateFilter
	for _, f := range req.GetCertificateFilters() {
		filters = append(filters, schema.CertificateFilter{
			IssuerName:          f.GetIssuerName(),
			CommonNamePattern:   f.GetCommonNamePattern(),
			SANPattern:          f.GetSanPattern(),
			SubjectOrganization: f.GetSubjectOrganization(),
			SubjectOrgUnit:      f.GetSubjectOrgUnit(),
			SubjectCountry:      f.GetSubjectCountry(),
			DomainPattern:       f.GetDomainPattern(),
			Labels:              f.GetLabels(),
		})
	}

	// Get description
	var description string
	if req.Description != nil {
		description = *req.Description
	}

	// Get auto-deploy setting
	autoDeployOnRenewal := false
	if req.AutoDeployOnRenewal != nil {
		autoDeployOnRenewal = *req.AutoDeployOnRenewal
	}

	// Validate configuration IDs exist
	if len(req.ConfigurationIds) > 0 {
		configs, err := s.configRepo.ListByIDs(ctx, req.ConfigurationIds)
		if err != nil {
			return nil, err
		}
		if len(configs) != len(req.ConfigurationIds) {
			return nil, deployerV1.ErrorConfigurationNotFound("one or more configuration IDs not found")
		}
	}

	entity, err := s.targetRepo.Create(ctx, req.GetTenantId(), req.GetName(), description,
		autoDeployOnRenewal, filters, req.ConfigurationIds)
	if err != nil {
		return nil, err
	}

	// Fetch with configurations for response
	entity, err = s.targetRepo.GetByIDWithConfigurations(ctx, entity.ID)
	if err != nil {
		return nil, err
	}

	return &deployerV1.CreateTargetResponse{
		Target: s.targetRepo.ToProto(entity, s.configRepo),
	}, nil
}

// GetTarget gets a deployment target by ID
func (s *DeploymentTargetService) GetTarget(ctx context.Context, req *deployerV1.GetTargetRequest) (*deployerV1.GetTargetResponse, error) {
	s.log.Infof("GetTarget: id=%s, includeConfigs=%v", req.GetId(), req.GetIncludeConfigurations())

	var entity interface{}
	var err error

	if req.GetIncludeConfigurations() {
		entity, err = s.targetRepo.GetByIDWithConfigurations(ctx, req.GetId())
	} else {
		entity, err = s.targetRepo.GetByID(ctx, req.GetId())
	}

	if err != nil {
		return nil, err
	}
	if entity == nil {
		return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
	}

	return &deployerV1.GetTargetResponse{
		Target: s.targetRepo.ToProto(entity.(*data.DeploymentTarget), s.configRepo),
	}, nil
}

// ListTargets lists deployment targets
func (s *DeploymentTargetService) ListTargets(ctx context.Context, req *deployerV1.ListTargetsRequest) (*deployerV1.ListTargetsResponse, error) {
	s.log.Infof("ListTargets: tenant_id=%v", req.TenantId)

	page := uint32(1)
	pageSize := uint32(20)
	if req.Page != nil && *req.Page > 0 {
		page = *req.Page
	}
	if req.PageSize != nil && *req.PageSize > 0 {
		pageSize = *req.PageSize
	}

	includeConfigs := req.GetIncludeConfigurations()

	entities, total, err := s.targetRepo.List(ctx, req.TenantId, req.AutoDeployOnRenewal, includeConfigs, page, pageSize)
	if err != nil {
		return nil, err
	}

	items := make([]*deployerV1.DeploymentTarget, 0, len(entities))
	for _, entity := range entities {
		items = append(items, s.targetRepo.ToProto(entity, s.configRepo))
	}

	return &deployerV1.ListTargetsResponse{
		Items: items,
		Total: uint64(total),
	}, nil
}

// UpdateTarget updates a deployment target
func (s *DeploymentTargetService) UpdateTarget(ctx context.Context, req *deployerV1.UpdateTargetRequest) (*deployerV1.UpdateTargetResponse, error) {
	s.log.Infof("UpdateTarget: id=%s", req.GetId())

	// Validate target exists
	existing, err := s.targetRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
	}

	// Convert certificate filters if provided
	var filters []schema.CertificateFilter
	if req.CertificateFilters != nil {
		for _, f := range req.CertificateFilters {
			filters = append(filters, schema.CertificateFilter{
				IssuerName:          f.GetIssuerName(),
				CommonNamePattern:   f.GetCommonNamePattern(),
				SANPattern:          f.GetSanPattern(),
				SubjectOrganization: f.GetSubjectOrganization(),
				SubjectOrgUnit:      f.GetSubjectOrgUnit(),
				SubjectCountry:      f.GetSubjectCountry(),
				DomainPattern:       f.GetDomainPattern(),
				Labels:              f.GetLabels(),
			})
		}
	}

	entity, err := s.targetRepo.Update(ctx, req.GetId(), req.Name, req.Description,
		req.AutoDeployOnRenewal, filters)
	if err != nil {
		return nil, err
	}

	// Fetch with configurations for response
	entity, err = s.targetRepo.GetByIDWithConfigurations(ctx, entity.ID)
	if err != nil {
		return nil, err
	}

	return &deployerV1.UpdateTargetResponse{
		Target: s.targetRepo.ToProto(entity, s.configRepo),
	}, nil
}

// DeleteTarget deletes a deployment target
func (s *DeploymentTargetService) DeleteTarget(ctx context.Context, req *deployerV1.DeleteTargetRequest) (*emptypb.Empty, error) {
	s.log.Infof("DeleteTarget: id=%s", req.GetId())

	if err := s.targetRepo.Delete(ctx, req.GetId()); err != nil {
		return nil, err
	}

	return &emptypb.Empty{}, nil
}

// AddConfigurations adds configurations to a deployment target
func (s *DeploymentTargetService) AddConfigurations(ctx context.Context, req *deployerV1.AddConfigurationsRequest) (*deployerV1.AddConfigurationsResponse, error) {
	s.log.Infof("AddConfigurations: target=%s, configs=%v", req.GetId(), req.ConfigurationIds)

	// Validate target exists
	existing, err := s.targetRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
	}

	// Validate configuration IDs exist
	configs, err := s.configRepo.ListByIDs(ctx, req.ConfigurationIds)
	if err != nil {
		return nil, err
	}
	if len(configs) != len(req.ConfigurationIds) {
		return nil, deployerV1.ErrorConfigurationNotFound("one or more configuration IDs not found")
	}

	_, err = s.targetRepo.AddConfigurations(ctx, req.GetId(), req.ConfigurationIds)
	if err != nil {
		return nil, err
	}

	// Fetch with configurations for response
	entity, err := s.targetRepo.GetByIDWithConfigurations(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	return &deployerV1.AddConfigurationsResponse{
		Target: s.targetRepo.ToProto(entity, s.configRepo),
	}, nil
}

// RemoveConfigurations removes configurations from a deployment target
func (s *DeploymentTargetService) RemoveConfigurations(ctx context.Context, req *deployerV1.RemoveConfigurationsRequest) (*deployerV1.RemoveConfigurationsResponse, error) {
	s.log.Infof("RemoveConfigurations: target=%s, configs=%v", req.GetId(), req.ConfigurationIds)

	// Validate target exists
	existing, err := s.targetRepo.GetByID(ctx, req.GetId())
	if err != nil {
		return nil, err
	}
	if existing == nil {
		return nil, deployerV1.ErrorTargetNotFound("deployment target not found")
	}

	_, err = s.targetRepo.RemoveConfigurations(ctx, req.GetId(), req.ConfigurationIds)
	if err != nil {
		return nil, err
	}

	// Fetch with configurations for response
	entity, err := s.targetRepo.GetByIDWithConfigurations(ctx, req.GetId())
	if err != nil {
		return nil, err
	}

	return &deployerV1.RemoveConfigurationsResponse{
		Target: s.targetRepo.ToProto(entity, s.configRepo),
	}, nil
}

// ListTargetConfigurations lists configurations linked to a deployment target
func (s *DeploymentTargetService) ListTargetConfigurations(ctx context.Context, req *deployerV1.ListTargetConfigurationsRequest) (*deployerV1.ListTargetConfigurationsResponse, error) {
	s.log.Infof("ListTargetConfigurations: target=%s", req.GetId())

	page := uint32(1)
	pageSize := uint32(20)
	if req.Page != nil && *req.Page > 0 {
		page = *req.Page
	}
	if req.PageSize != nil && *req.PageSize > 0 {
		pageSize = *req.PageSize
	}

	configs, total, err := s.targetRepo.GetConfigurations(ctx, req.GetId(), page, pageSize)
	if err != nil {
		return nil, err
	}

	items := make([]*deployerV1.TargetConfiguration, 0, len(configs))
	for _, config := range configs {
		items = append(items, s.configRepo.ToProto(config))
	}

	return &deployerV1.ListTargetConfigurationsResponse{
		Items: items,
		Total: uint64(total),
	}, nil
}
