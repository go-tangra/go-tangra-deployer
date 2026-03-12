package metrics

import (
	"context"
	"os"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	commonMetrics "github.com/go-tangra/go-tangra-common/metrics"
)

const namespace = "tangra"
const subsystem = "deployer"

// Collector holds all Prometheus metrics for the deployer module.
type Collector struct {
	log    *log.Helper
	server *commonMetrics.MetricsServer

	// Job metrics
	JobsByStatus  *prometheus.GaugeVec
	JobsByTrigger *prometheus.GaugeVec

	// Target metrics
	TargetsTotal             prometheus.Gauge
	TargetsAutoDeployEnabled prometheus.Gauge

	// Configuration metrics
	ConfigurationsByStatus *prometheus.GaugeVec

	// gRPC request metrics
	RequestDuration *prometheus.HistogramVec
	RequestsTotal   *prometheus.CounterVec
}

// NewCollector creates and registers all deployer Prometheus metrics.
func NewCollector(ctx *bootstrap.Context) *Collector {
	c := &Collector{
		log: ctx.NewLoggerHelper("deployer/metrics"),

		JobsByStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "jobs_by_status",
			Help:      "Number of deployment jobs by status.",
		}, []string{"status"}),

		JobsByTrigger: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "jobs_by_trigger",
			Help:      "Number of deployment jobs by trigger type.",
		}, []string{"trigger_type"}),

		TargetsTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "targets_total",
			Help:      "Total number of deployment targets.",
		}),

		TargetsAutoDeployEnabled: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "targets_auto_deploy_enabled",
			Help:      "Number of deployment targets with auto-deploy enabled.",
		}),

		ConfigurationsByStatus: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "configurations_by_status",
			Help:      "Number of target configurations by status.",
		}, []string{"status"}),

		RequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "grpc_request_duration_seconds",
			Help:      "Histogram of gRPC request durations in seconds.",
			Buckets:   prometheus.DefBuckets,
		}, []string{"method"}),

		RequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Subsystem: subsystem,
			Name:      "grpc_requests_total",
			Help:      "Total number of gRPC requests by method and status.",
		}, []string{"method", "status"}),
	}

	prometheus.MustRegister(
		c.JobsByStatus,
		c.JobsByTrigger,
		c.TargetsTotal,
		c.TargetsAutoDeployEnabled,
		c.ConfigurationsByStatus,
		c.RequestDuration,
		c.RequestsTotal,
	)

	addr := os.Getenv("METRICS_ADDR")
	if addr == "" {
		addr = ":9210"
	}
	c.server = commonMetrics.NewMetricsServer(addr, nil, ctx.GetLogger())

	go func() {
		if err := c.server.Start(); err != nil {
			c.log.Errorf("Metrics server failed: %v", err)
		}
	}()

	return c
}

// Stop shuts down the metrics HTTP server.
func (c *Collector) Stop(ctx context.Context) {
	if c.server != nil {
		c.server.Stop(ctx)
	}
}

// Middleware returns a Kratos middleware that records gRPC request metrics.
func (c *Collector) Middleware() middleware.Middleware {
	return commonMetrics.NewServerMiddleware(c.RequestDuration, c.RequestsTotal)
}

// --- Job helpers ---

// JobCreated increments counters for a newly created job.
func (c *Collector) JobCreated(status, triggerType string) {
	c.JobsByStatus.WithLabelValues(status).Inc()
	c.JobsByTrigger.WithLabelValues(triggerType).Inc()
}

// JobDeleted decrements counters for a deleted job.
func (c *Collector) JobDeleted(status, triggerType string) {
	c.JobsByStatus.WithLabelValues(status).Dec()
	c.JobsByTrigger.WithLabelValues(triggerType).Dec()
}

// JobStatusChanged adjusts the status gauge when a job's status changes.
func (c *Collector) JobStatusChanged(oldStatus, newStatus string) {
	c.JobsByStatus.WithLabelValues(oldStatus).Dec()
	c.JobsByStatus.WithLabelValues(newStatus).Inc()
}

// --- Target helpers ---

// TargetCreated increments the target counters.
func (c *Collector) TargetCreated(autoDeploy bool) {
	c.TargetsTotal.Inc()
	if autoDeploy {
		c.TargetsAutoDeployEnabled.Inc()
	}
}

// TargetDeleted decrements the target counters.
func (c *Collector) TargetDeleted(autoDeploy bool) {
	c.TargetsTotal.Dec()
	if autoDeploy {
		c.TargetsAutoDeployEnabled.Dec()
	}
}

// --- Configuration helpers ---

// ConfigCreated increments the configuration status counter.
func (c *Collector) ConfigCreated(status string) {
	c.ConfigurationsByStatus.WithLabelValues(status).Inc()
}

// ConfigDeleted decrements the configuration status counter.
func (c *Collector) ConfigDeleted(status string) {
	c.ConfigurationsByStatus.WithLabelValues(status).Dec()
}

// ConfigStatusChanged adjusts the configuration status gauge.
func (c *Collector) ConfigStatusChanged(oldStatus, newStatus string) {
	c.ConfigurationsByStatus.WithLabelValues(oldStatus).Dec()
	c.ConfigurationsByStatus.WithLabelValues(newStatus).Inc()
}
