//go:build wireinject
// +build wireinject

//go:generate go run github.com/google/wire/cmd/wire

// This file defines the dependency injection ProviderSet for the service layer.

package providers

import (
	"github.com/google/wire"

	"github.com/go-tangra/go-tangra-deployer/internal/event"
	"github.com/go-tangra/go-tangra-deployer/internal/service"
)

// ProviderSet is the Wire provider set for service layer.
var ProviderSet = wire.NewSet(
	service.NewTargetConfigurationService,
	service.NewDeploymentTargetService,
	service.NewDeploymentJobService,
	service.NewDeploymentService,
	service.NewJobExecutor,
	service.NewStatisticsService,
	event.NewHandler,
	event.NewSubscriber,
)
