package main

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/transport/grpc"

	conf "github.com/tx7do/kratos-bootstrap/api/gen/go/conf/v1"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-common/registration"
	pkgService "github.com/go-tangra/go-tangra-common/service"
	"github.com/go-tangra/go-tangra-deployer/cmd/server/assets"
	deployerCnf "github.com/go-tangra/go-tangra-deployer/internal/conf"
	"github.com/go-tangra/go-tangra-deployer/internal/event"
	"github.com/go-tangra/go-tangra-deployer/internal/service"

	// Import providers to register them via init()
	_ "github.com/go-tangra/go-tangra-deployer/pkg/deploy/providers"
)

var (
	// Module info
	moduleID    = "deployer"
	moduleName  = "Deployer"
	version     = "1.0.0"
	description = "Certificate deployment service for deploying certificates to various targets"
)

// Global references for cleanup
var globalEventSubscriber *event.Subscriber
var globalJobExecutor *service.JobExecutor
var globalRegHelper *registration.RegistrationHelper

func newApp(
	ctx *bootstrap.Context,
	gs *grpc.Server,
	eventSubscriber *event.Subscriber,
	jobExecutor *service.JobExecutor,
) *kratos.App {
	// Start the event subscriber and store reference for cleanup
	globalEventSubscriber = eventSubscriber
	if eventSubscriber != nil {
		if err := eventSubscriber.Start(); err != nil {
			log.Warnf("Failed to start event subscriber: %v", err)
		}
	}

	// Start the job executor and store reference for cleanup
	globalJobExecutor = jobExecutor
	if jobExecutor != nil {
		if err := jobExecutor.Start(); err != nil {
			log.Warnf("Failed to start job executor: %v", err)
		}
	}

	globalRegHelper = registration.StartRegistration(ctx, ctx.GetLogger(), &registration.Config{
		ModuleID:          moduleID,
		ModuleName:        moduleName,
		Version:           version,
		Description:       description,
		GRPCEndpoint:      registration.GetGRPCAdvertiseAddr(ctx, "0.0.0.0:9200"),
		AdminEndpoint:     registration.GetEnvOrDefault("ADMIN_GRPC_ENDPOINT", ""),
		OpenapiSpec:       assets.OpenApiData,
		ProtoDescriptor:   assets.DescriptorData,
		MenusYaml:         assets.MenusData,
		HeartbeatInterval: 30 * time.Second,
		RetryInterval:     5 * time.Second,
		MaxRetries:        60,
	})

	return bootstrap.NewApp(ctx, gs)
}

// stopServices stops background services (called from wire cleanup)
func stopServices() {
	if globalEventSubscriber != nil {
		if err := globalEventSubscriber.Stop(); err != nil {
			log.Warnf("Failed to stop event subscriber: %v", err)
		}
	}
	if globalJobExecutor != nil {
		if err := globalJobExecutor.Stop(); err != nil {
			log.Warnf("Failed to stop job executor: %v", err)
		}
	}
}

func runApp() error {
	ctx := bootstrap.NewContext(
		context.Background(),
		&conf.AppInfo{
			Project: pkgService.Project,
			AppId:   "deployer-service",
			Version: version,
		},
	)
	ctx.RegisterCustomConfig("deployer", &deployerCnf.Deployer{})

	// Ensure services are stopped on exit
	defer stopServices()
	defer globalRegHelper.Stop()

	return bootstrap.RunApp(ctx, initApp)
}

func main() {
	if err := runApp(); err != nil {
		panic(err)
	}
}

