package metrics

import (
	"context"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
)

// Seed loads initial gauge values from the database.
// Called once at startup so Prometheus has accurate values from the start.
func (c *Collector) Seed(ctx context.Context, statsRepo *data.StatisticsRepo) {
	c.log.Info("Seeding Prometheus metrics from database...")

	jobStats, err := statsRepo.GetJobStats(ctx, nil)
	if err != nil {
		c.log.Errorf("Failed to seed job stats: %v", err)
	} else {
		for status, count := range jobStats.ByStatus {
			c.JobsByStatus.WithLabelValues(status).Set(float64(count))
		}
		for triggerType, count := range jobStats.ByTriggerType {
			c.JobsByTrigger.WithLabelValues(triggerType).Set(float64(count))
		}
	}

	targetStats, err := statsRepo.GetTargetStats(ctx, nil)
	if err != nil {
		c.log.Errorf("Failed to seed target stats: %v", err)
	} else {
		c.TargetsTotal.Set(float64(targetStats.TotalCount))
		c.TargetsAutoDeployEnabled.Set(float64(targetStats.AutoDeployEnabledCount))
	}

	configStats, err := statsRepo.GetConfigurationStats(ctx, nil)
	if err != nil {
		c.log.Errorf("Failed to seed configuration stats: %v", err)
	} else {
		for status, count := range configStats.ByStatus {
			c.ConfigurationsByStatus.WithLabelValues(status).Set(float64(count))
		}
	}

	c.log.Info("Prometheus metrics seeded successfully")
}
