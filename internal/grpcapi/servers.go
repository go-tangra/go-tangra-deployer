package grpcapi

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	deployerv1 "github.com/go-freya/freya/services/deployer/api/proto/deployer/v1"
	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/deploy"
)

// ---- TargetConfigurationService

// ConfigurationServer implements deployer.v1.TargetConfigurationService.
type ConfigurationServer struct {
	deployerv1.UnimplementedTargetConfigurationServiceServer
	Svc *configs.Service
}

func (s *ConfigurationServer) Create(ctx context.Context, req *deployerv1.CreateConfigurationRequest) (*deployerv1.Configuration, error) {
	subj, err := caller(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	cfg, err := parseObject(req.GetConfigJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "config_json must be a JSON object")
	}
	creds, err := parseObject(req.GetCredentialsJson())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "credentials_json must be a JSON object")
	}
	v, err := s.Svc.Create(ctx, subj, configs.Input{
		Name: req.GetName(), Description: req.GetDescription(), ProviderType: req.GetProviderType(),
		Config: cfg, Credentials: creds,
	})
	if err != nil {
		return nil, grpcError(err)
	}
	return toConfigPB(v), nil
}

func (s *ConfigurationServer) Get(ctx context.Context, req *deployerv1.GetConfigurationRequest) (*deployerv1.Configuration, error) {
	subj, err := caller(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	v, err := s.Svc.Get(ctx, subj, req.GetId())
	if err != nil {
		return nil, grpcError(err)
	}
	return toConfigPB(v), nil
}

func (s *ConfigurationServer) List(ctx context.Context, req *deployerv1.ListConfigurationsRequest) (*deployerv1.ListConfigurationsResponse, error) {
	subj, err := caller(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	items, err := s.Svc.List(ctx, subj, req.GetProviderType(), req.GetStatus())
	if err != nil {
		return nil, grpcError(err)
	}
	out := &deployerv1.ListConfigurationsResponse{Configurations: make([]*deployerv1.Configuration, 0, len(items))}
	for _, v := range items {
		out.Configurations = append(out.Configurations, toConfigPB(v))
	}
	return out, nil
}

// ---- DeploymentService

// DeploymentServer implements deployer.v1.DeploymentService.
type DeploymentServer struct {
	deployerv1.UnimplementedDeploymentServiceServer
	Svc *deploy.Service
}

func (s *DeploymentServer) Deploy(ctx context.Context, req *deployerv1.DeployRequest) (*deployerv1.DeployResponse, error) {
	subj, err := caller(ctx, req.GetTenantId())
	if err != nil {
		return nil, err
	}
	jobID, err := s.Svc.Deploy(ctx, subj, req.GetCertificateId(), req.GetConfigurationId(), req.GetTrigger())
	if err != nil {
		return nil, grpcError(err)
	}
	return &deployerv1.DeployResponse{JobId: jobID, Status: "processing"}, nil
}

func parseObject(s string) (map[string]any, error) {
	if s == "" {
		return nil, nil
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return nil, err
	}
	return m, nil
}

func toConfigPB(v configs.View) *deployerv1.Configuration {
	c := &deployerv1.Configuration{
		Id: v.ID, Name: v.Name, Description: v.Description, ProviderType: v.ProviderType,
		Status: v.Status, HasCredentials: v.HasCredentials,
	}
	if v.LastDeploymentAt != nil {
		c.LastDeploymentAt = timestamppb.New(*v.LastDeploymentAt)
	}
	return c
}
