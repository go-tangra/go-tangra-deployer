package server

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/middleware/logging"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/validate"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/cert"
	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/service"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"

	appViewer "github.com/go-tangra/go-tangra-deployer/pkg/viewer"
	"github.com/go-tangra/go-tangra-common/middleware/audit"
	"github.com/go-tangra/go-tangra-common/middleware/mtls"
)

// Public endpoints that don't require mTLS authentication
var publicEndpoints = []string{
	"/deployer.service.v1.DeployerStatisticsService/HealthCheck",
}

// systemViewerMiddleware injects system viewer context for all requests
// This allows the deployer service to bypass tenant privacy checks
func systemViewerMiddleware() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			ctx = appViewer.NewSystemViewerContext(ctx)
			return handler(ctx, req)
		}
	}
}

// newGrpcMiddleware creates gRPC middleware stack with mTLS and audit logging
func newGrpcMiddleware(
	logger log.Logger,
	auditLogRepo *data.AuditLogRepo,
) []middleware.Middleware {
	var ms []middleware.Middleware

	ms = append(ms, recovery.Recovery())
	ms = append(ms, systemViewerMiddleware()) // Inject system viewer for ENT privacy

	// Add mTLS middleware for client certificate authentication
	// This must run before audit logging to populate client context
	ms = append(ms, mtls.MTLSMiddleware(logger,
		mtls.WithPublicEndpoints(publicEndpoints...),
	))

	ms = append(ms, logging.Server(logger))

	// Add audit logging middleware with cryptographic signing
	if auditLogRepo != nil {
		ms = append(ms, audit.Server(logger,
			audit.WithServiceName("deployer-service"),
			audit.WithWriteAuditLogFunc(audit.NewDatabaseWriter(auditLogRepo)),
			audit.WithSkipOperations(
				"/deployer.service.v1.DeployerStatisticsService/HealthCheck",
			),
		))
	}

	ms = append(ms, validate.Validator())

	return ms
}

// NewGRPCServer creates a new gRPC server with mTLS support
func NewGRPCServer(
	ctx *bootstrap.Context,
	certManager *cert.CertManager,
	auditLogRepo *data.AuditLogRepo,
	targetSvc *service.DeploymentTargetService,
	configSvc *service.TargetConfigurationService,
	jobSvc *service.DeploymentJobService,
	deploymentSvc *service.DeploymentService,
	statisticsSvc *service.StatisticsService,
) *grpc.Server {
	cfg := ctx.GetConfig()
	logger := ctx.GetLogger()

	l := log.NewHelper(log.With(logger, "module", "server/grpc"))

	// Create gRPC server options
	opts := []grpc.ServerOption{
		grpc.Middleware(newGrpcMiddleware(logger, auditLogRepo)...),
	}

	// Add TLS configuration if certificate manager is available
	if certManager != nil && certManager.IsTLSEnabled() {
		tlsConfig, err := certManager.GetServerTLSConfig()
		if err != nil {
			l.Warnf("Failed to get TLS config, running without mTLS: %v", err)
		} else {
			opts = append(opts, grpc.TLSConfig(tlsConfig))
			l.Info("mTLS enabled for gRPC server")
		}
	} else {
		l.Warn("Running gRPC server without mTLS - certificates not available")
	}

	// Add server configuration from bootstrap config
	if cfg.Server != nil && cfg.Server.Grpc != nil {
		if cfg.Server.Grpc.Addr != "" {
			opts = append(opts, grpc.Address(cfg.Server.Grpc.Addr))
		}
		if cfg.Server.Grpc.Timeout != nil {
			opts = append(opts, grpc.Timeout(cfg.Server.Grpc.Timeout.AsDuration()))
		}
	}

	// Create the gRPC server
	srv := grpc.NewServer(opts...)

	// Register services
	deployerV1.RegisterDeploymentTargetServiceServer(srv, targetSvc)
	deployerV1.RegisterTargetConfigurationServiceServer(srv, configSvc)
	deployerV1.RegisterDeploymentJobServiceServer(srv, jobSvc)
	deployerV1.RegisterDeploymentServiceServer(srv, deploymentSvc)
	deployerV1.RegisterDeployerStatisticsServiceServer(srv, statisticsSvc)

	l.Info("gRPC server configured with all Deployer services")

	return srv
}
