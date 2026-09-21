// Package deployerclient is a thin, typed Go client for the deployer.v1
// service-to-service gRPC API, for other Freya modules that manage deployment
// configurations or trigger deployments. It wraps the generated gRPC stubs so
// callers deal in ordinary Go values, not protobuf messages. The caller supplies
// a connected, SPIFFE-mTLS *grpc.ClientConn (e.g. from freya.App.Client(ctx,
// "deployer")); this package does not dial or manage the connection.
package deployerclient

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"google.golang.org/grpc"

	deployerv1 "github.com/go-freya/freya/services/deployer/api/proto/deployer/v1"
)

// Client calls the deployer.v1 API over a caller-provided gRPC connection.
type Client struct {
	configs deployerv1.TargetConfigurationServiceClient
	deploy  deployerv1.DeploymentServiceClient
}

// New builds a client from a connected (SPIFFE-mTLS) gRPC connection to the
// deployer service.
func New(conn grpc.ClientConnInterface) *Client {
	return &Client{
		configs: deployerv1.NewTargetConfigurationServiceClient(conn),
		deploy:  deployerv1.NewDeploymentServiceClient(conn),
	}
}

// Configuration is a deployment endpoint as returned by the deployer. Provider
// credentials are never returned; HasCredentials reports whether any are set.
type Configuration struct {
	ID               string
	Name             string
	Description      string
	ProviderType     string
	Status           string
	HasCredentials   bool
	LastDeploymentAt *time.Time
}

// CreateConfigurationInput describes a new deployment endpoint. Config and
// Credentials are arbitrary JSON objects; Credentials are sealed at rest and
// never returned.
type CreateConfigurationInput struct {
	TenantID     string
	Name         string
	Description  string
	ProviderType string
	Config       map[string]any
	Credentials  map[string]any
}

// CreateConfiguration creates a deployment endpoint.
func (c *Client) CreateConfiguration(ctx context.Context, in CreateConfigurationInput) (Configuration, error) {
	cfgJSON, err := marshalObject(in.Config)
	if err != nil {
		return Configuration{}, fmt.Errorf("config: %w", err)
	}
	credJSON, err := marshalObject(in.Credentials)
	if err != nil {
		return Configuration{}, fmt.Errorf("credentials: %w", err)
	}
	pb, err := c.configs.Create(ctx, &deployerv1.CreateConfigurationRequest{
		TenantId: in.TenantID, Name: in.Name, Description: in.Description,
		ProviderType: in.ProviderType, ConfigJson: cfgJSON, CredentialsJson: credJSON,
	})
	if err != nil {
		return Configuration{}, err
	}
	return toConfiguration(pb), nil
}

// GetConfiguration returns one configuration by id.
func (c *Client) GetConfiguration(ctx context.Context, tenantID, id string) (Configuration, error) {
	pb, err := c.configs.Get(ctx, &deployerv1.GetConfigurationRequest{TenantId: tenantID, Id: id})
	if err != nil {
		return Configuration{}, err
	}
	return toConfiguration(pb), nil
}

// ListConfigurations lists a tenant's configurations (optionally filtered by
// provider type and/or status; empty filters match all).
func (c *Client) ListConfigurations(ctx context.Context, tenantID, providerType, status string) ([]Configuration, error) {
	resp, err := c.configs.List(ctx, &deployerv1.ListConfigurationsRequest{TenantId: tenantID, ProviderType: providerType, Status: status})
	if err != nil {
		return nil, err
	}
	out := make([]Configuration, 0, len(resp.GetConfigurations()))
	for _, pb := range resp.GetConfigurations() {
		out = append(out, toConfiguration(pb))
	}
	return out, nil
}

// Deploy triggers a deployment of certificateID to configurationID and returns
// the created job id. trigger is optional (defaults to "manual" server-side).
func (c *Client) Deploy(ctx context.Context, tenantID, certificateID, configurationID, trigger string) (jobID string, err error) {
	resp, err := c.deploy.Deploy(ctx, &deployerv1.DeployRequest{
		TenantId: tenantID, CertificateId: certificateID, ConfigurationId: configurationID, Trigger: trigger,
	})
	if err != nil {
		return "", err
	}
	return resp.GetJobId(), nil
}

func marshalObject(m map[string]any) (string, error) {
	if len(m) == 0 {
		return "", nil
	}
	b, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func toConfiguration(pb *deployerv1.Configuration) Configuration {
	c := Configuration{
		ID: pb.GetId(), Name: pb.GetName(), Description: pb.GetDescription(),
		ProviderType: pb.GetProviderType(), Status: pb.GetStatus(), HasCredentials: pb.GetHasCredentials(),
	}
	if ts := pb.GetLastDeploymentAt(); ts != nil {
		t := ts.AsTime()
		c.LastDeploymentAt = &t
	}
	return c
}
