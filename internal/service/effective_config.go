package service

import (
	"context"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// resolveEffectiveConfig returns the provider config to deploy with: the
// configuration's own config, with the deployment target's override for that
// configuration layered on top.
//
// It is what makes one shared configuration usable by several targets — a
// single Cloudflare API token deploying to a different zone_id per target —
// instead of duplicating the configuration and its credentials per zone.
//
// Every provider call site must go through this. Reading config.Config
// directly silently ignores the override and deploys to the wrong zone, which
// is a far worse failure than an error.
//
// A missing or unreadable target is not fatal: the base config is still a valid
// deployment, and failing the job because the override could not be read would
// turn a cosmetic problem into an outage. The lookup is logged when it fails.
func resolveEffectiveConfig(
	ctx context.Context,
	targetRepo *data.DeploymentTargetRepo,
	l *log.Helper,
	deploymentTargetID *string,
	config *ent.TargetConfiguration,
) map[string]any {
	if config == nil {
		return nil
	}
	if targetRepo == nil || deploymentTargetID == nil || *deploymentTargetID == "" {
		return registry.MergeConfig(config.Config, nil)
	}
	targetID := *deploymentTargetID

	target, err := targetRepo.GetByID(ctx, targetID)
	if err != nil || target == nil {
		if err != nil {
			l.Warnf("config overrides unavailable for target %s: %v; deploying with the configuration's own config",
				targetID, err)
		}
		return registry.MergeConfig(config.Config, nil)
	}

	override := target.ConfigOverrides[config.ID]
	if len(override) > 0 {
		l.Infof("applying %d config override(s) from target %s to configuration %s",
			len(override), targetID, config.ID)
	}
	return registry.MergeConfig(config.Config, override)
}
