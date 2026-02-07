//go:build wireinject
// +build wireinject

//go:generate go run github.com/google/wire/cmd/wire

// This file defines the dependency injection ProviderSet for the data layer.

package providers

import (
	"github.com/google/wire"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
)

// ProviderSet is the Wire provider set for data layer.
var ProviderSet = wire.NewSet(
	data.NewRedisClient,
	data.NewEntClient,
	data.NewTargetConfigurationRepo,
	data.NewDeploymentTargetRepo,
	data.NewDeploymentJobRepo,
	data.NewDeploymentHistoryRepo,
	data.NewLcmClient,
	data.NewStatisticsRepo,
	data.NewAuditLogRepo,
)
