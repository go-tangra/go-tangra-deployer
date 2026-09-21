// Package grpcapi serves deployer.v1 for other platform services on the Freya
// SPIFFE mTLS channel: the caller is an authenticated service (policed by
// deploy/policy.yaml, SR-003) acting for the tenant named in the request.
// Nothing here is proxied by the gateway. The tenant comes from the request and
// the actor identity from the verified SPIFFE peer.
package grpcapi

import (
	"context"
	"errors"
	"regexp"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-freya/freya/authn"
	deployerv1 "github.com/go-freya/freya/services/deployer/api/proto/deployer/v1"
	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/deploy"
)

var uuidRE = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// callerFunc resolves the SPIFFE identity of a call (overridable in tests).
var callerFunc = func(ctx context.Context) (string, bool) {
	p, ok := authn.FromContext(ctx)
	if !ok {
		return "", false
	}
	return p.ID.String(), true
}

// caller returns the service subjects for the tenant named in the request; the
// tenant must be a uuid and the peer must present a SPIFFE identity.
func caller(ctx context.Context, tenantID string) (authz.Subjects, error) {
	id, ok := callerFunc(ctx)
	if !ok {
		return authz.Subjects{}, status.Error(codes.Unauthenticated, "service identity required")
	}
	if !uuidRE.MatchString(tenantID) {
		return authz.Subjects{}, status.Error(codes.InvalidArgument, "tenant_id must be a uuid")
	}
	return authz.Subjects{TenantID: tenantID, UserID: id, ActorKind: "service"}, nil
}

// grpcError maps a service error to a gRPC status.
func grpcError(err error) error {
	var ve *configs.ValidationError
	switch {
	case errors.As(err, &ve):
		return status.Error(codes.InvalidArgument, "validation_failed")
	case errors.Is(err, authz.ErrForbidden):
		return status.Error(codes.PermissionDenied, "forbidden")
	case errors.Is(err, configs.ErrNotFound), errors.Is(err, deploy.ErrNotFound):
		return status.Error(codes.NotFound, "not_found")
	case errors.Is(err, deploy.ErrInactive), errors.Is(err, deploy.ErrNoConfigs):
		return status.Error(codes.FailedPrecondition, "not_deployable")
	}
	return status.Error(codes.Unavailable, "temporarily_unavailable")
}

// Deps carries the services the deployer.v1 servers use.
type Deps struct {
	Configs *configs.Service
	Deploy  *deploy.Service
}

// Register registers the deployer.v1 servers on the gRPC server. Callers are
// authenticated services policed by policy.yaml; nothing here is gateway-proxied.
func Register(gs grpc.ServiceRegistrar, d Deps) {
	if d.Configs != nil {
		deployerv1.RegisterTargetConfigurationServiceServer(gs, &ConfigurationServer{Svc: d.Configs})
	}
	if d.Deploy != nil {
		deployerv1.RegisterDeploymentServiceServer(gs, &DeploymentServer{Svc: d.Deploy})
	}
}
