// Package config loads and validates the deployer service configuration: the
// Freya framework config plus the module's own sections. Every value is
// explicit; insecure opt-outs are named and logged at start (Constitution I/VII).
package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	fconfig "github.com/go-tangra/go-tangra/v4/config"
	"gopkg.in/yaml.v3"
)

// Config is the deployer service configuration.
type Config struct {
	fconfig.Config `yaml:",inline"`

	DB      DB      `yaml:"db"`
	Valkey  Valkey  `yaml:"valkey"`
	KEK     KEK     `yaml:"kek"`
	Jobs    Jobs    `yaml:"jobs"`
	Events  Events  `yaml:"events"`
	Gateway Gateway `yaml:"gateway"`
	LCM     LCM     `yaml:"lcm"`
	Enroll  Enroll  `yaml:"enroll"`
	Limits  Limits  `yaml:"limits_deployer"`
}

// DB configures TimescaleDB.
type DB struct {
	DSN        string `yaml:"dsn"`
	MigrateDSN string `yaml:"migrate_dsn"`
	MaxConns   int32  `yaml:"max_conns"`
}

// Valkey configures the platform event bus (Valkey Streams).
type Valkey struct {
	Addresses      []string `yaml:"addresses"`
	Username       string   `yaml:"username"`
	Password       string   `yaml:"password"`
	AllowPlaintext bool     `yaml:"allow_plaintext"`
	CAFile         string   `yaml:"ca_file"`
}

// KEK names where the 32-byte key-encryption key comes from.
type KEK struct {
	Source string `yaml:"source"` // file | env
	Path   string `yaml:"path"`
	Env    string `yaml:"env"`
}

// Jobs configures the distributed deployment worker pool.
type Jobs struct {
	Workers           int     `yaml:"workers"`
	IntervalSeconds   int     `yaml:"interval_seconds"`
	LeaseSeconds      int     `yaml:"lease_seconds"`
	MaxRetries        int     `yaml:"max_retries"`
	RetryDelaySeconds int     `yaml:"retry_delay_seconds"`
	BackoffMultiplier float64 `yaml:"backoff_multiplier"`
	JobTimeoutSeconds int     `yaml:"job_timeout_seconds"`
	CleanupDays       int     `yaml:"cleanup_days"`
}

// Events configures the auto-deploy consumer + realtime publisher.
type Events struct {
	Enabled bool `yaml:"enabled"`
}

// Gateway names the application gateway and the platform token issuer.
type Gateway struct {
	Service string `yaml:"service"`
	Issuer  string `yaml:"issuer"`
}

// LCM names the lcm service the deployer fetches certificates from.
type LCM struct {
	Service string `yaml:"service"`
}

// Enroll makes the service obtain its SVID by enrolling with lcm over the
// network (the multi-host path); app.Build injects the enroll identity provider.
type Enroll struct {
	Enabled       bool   `yaml:"enabled"`
	EnrollURL     string `yaml:"enroll_url"`
	LCMGRPCTarget string `yaml:"lcm_grpc"`
	TenantID      string `yaml:"tenant_id"`
	TokenFile     string `yaml:"token_file"`
	StateFile     string `yaml:"state_file"`
	Insecure      bool   `yaml:"insecure"`
}

// Limits bound the module's request shapes.
type Limits struct {
	BackupMaxBytes  int64 `yaml:"backup_max_bytes"`
	ConfigMaxBytes  int64 `yaml:"config_max_bytes"`
	FilterMaxLength int   `yaml:"filter_max_length"`
}

// Default returns secure defaults on top of the Freya defaults.
func Default() Config {
	return Config{
		Config:  fconfig.Default(),
		DB:      DB{MaxConns: 16},
		KEK:     KEK{Source: "file"},
		Jobs:    Jobs{Workers: 5, IntervalSeconds: 5, LeaseSeconds: 300, MaxRetries: 3, RetryDelaySeconds: 60, BackoffMultiplier: 2.0, JobTimeoutSeconds: 300, CleanupDays: 30},
		Events:  Events{Enabled: true},
		Gateway: Gateway{Service: "gateway"},
		LCM:     LCM{Service: "lcm"},
		Limits:  Limits{BackupMaxBytes: 16 << 20, ConfigMaxBytes: 16 << 10, FilterMaxLength: 256},
	}
}

// Load reads YAML over Default(); unknown fields are rejected.
func Load(path string) (Config, error) {
	cfg := Default()
	raw, err := os.ReadFile(path) // #nosec G304 -- operator-supplied config path
	if err != nil {
		return cfg, fmt.Errorf("config: %w", err)
	}
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("config: %s: %w", path, err)
	}
	return cfg, nil
}

// Validate checks the Freya config and every module section.
func (c Config) Validate() error {
	if err := c.Config.Validate(); err != nil {
		return err
	}
	prod := c.IsProduction()
	if c.DB.DSN == "" {
		return errors.New("config: db.dsn is required")
	}
	if prod && !strings.Contains(c.DB.DSN, "sslmode=verify-full") && !strings.Contains(c.DB.DSN, "sslmode=verify-ca") {
		return errors.New("config: db.dsn must use sslmode=verify-full (or verify-ca) in production")
	}
	if len(c.Valkey.Addresses) == 0 {
		return errors.New("config: valkey.addresses is required")
	}
	if prod && c.Valkey.AllowPlaintext {
		return errors.New("config: valkey.allow_plaintext is not permitted in production")
	}
	switch c.KEK.Source {
	case "file":
		if c.KEK.Path == "" {
			return errors.New("config: kek.path is required for kek.source file")
		}
	case "env":
		if c.KEK.Env == "" {
			return errors.New("config: kek.env is required for kek.source env")
		}
	default:
		return errors.New("config: kek.source must be file or env")
	}
	if c.Jobs.Workers < 1 || c.Jobs.Workers > 64 {
		return errors.New("config: jobs.workers must be within [1, 64]")
	}
	if c.Jobs.IntervalSeconds < 1 || c.Jobs.IntervalSeconds > 60 {
		return errors.New("config: jobs.interval_seconds must be within [1, 60]")
	}
	if c.Jobs.LeaseSeconds < c.Jobs.IntervalSeconds || c.Jobs.LeaseSeconds > 3600 {
		return errors.New("config: jobs.lease_seconds must be within [interval, 3600]")
	}
	if c.Jobs.MaxRetries < 0 || c.Jobs.MaxRetries > 20 {
		return errors.New("config: jobs.max_retries must be within [0, 20]")
	}
	if c.Jobs.RetryDelaySeconds < 1 || c.Jobs.RetryDelaySeconds > 3600 {
		return errors.New("config: jobs.retry_delay_seconds must be within [1, 3600]")
	}
	if c.Jobs.BackoffMultiplier < 1 || c.Jobs.BackoffMultiplier > 10 {
		return errors.New("config: jobs.backoff_multiplier must be within [1, 10]")
	}
	if c.Jobs.JobTimeoutSeconds < 10 || c.Jobs.JobTimeoutSeconds > 3600 {
		return errors.New("config: jobs.job_timeout_seconds must be within [10, 3600]")
	}
	if c.Jobs.CleanupDays < 1 || c.Jobs.CleanupDays > 3650 {
		return errors.New("config: jobs.cleanup_days must be within [1, 3650]")
	}
	if c.Gateway.Service == "" {
		return errors.New("config: gateway.service is required")
	}
	if iu, err := url.Parse(c.Gateway.Issuer); err != nil || iu.Scheme != "https" || iu.Host == "" {
		return errors.New("config: gateway.issuer must be an https origin")
	}
	if c.LCM.Service == "" {
		return errors.New("config: lcm.service is required")
	}
	if c.Limits.BackupMaxBytes < 4<<20 || c.Limits.BackupMaxBytes > 64<<20 {
		return errors.New("config: limits_deployer.backup_max_bytes must be within [4 MiB, 64 MiB]")
	}
	if c.Limits.ConfigMaxBytes < 1<<10 || c.Limits.ConfigMaxBytes > 1<<20 {
		return errors.New("config: limits_deployer.config_max_bytes must be within [1 KiB, 1 MiB]")
	}
	if c.Limits.FilterMaxLength < 16 || c.Limits.FilterMaxLength > 1024 {
		return errors.New("config: limits_deployer.filter_max_length must be within [16, 1024]")
	}
	return nil
}

// Warnings lists accepted insecure opt-outs (logged at start).
func (c Config) Warnings() []string {
	w := c.Config.Warnings()
	if c.Valkey.AllowPlaintext {
		w = append(w, "valkey.allow_plaintext: event-bus traffic without TLS (development only)")
	}
	return w
}

// Interval is the worker tick.
func (c Config) Interval() time.Duration { return time.Duration(c.Jobs.IntervalSeconds) * time.Second }

// Lease is how long a claimed job stays claimed.
func (c Config) Lease() time.Duration { return time.Duration(c.Jobs.LeaseSeconds) * time.Second }

// JobTimeout bounds a single deployment attempt.
func (c Config) JobTimeout() time.Duration {
	return time.Duration(c.Jobs.JobTimeoutSeconds) * time.Second
}

// CleanupWindow is the job retention window.
func (c Config) CleanupWindow() time.Duration {
	return time.Duration(c.Jobs.CleanupDays) * 24 * time.Hour
}

// RetryDelay is the base retry delay.
func (c Config) RetryDelay() time.Duration {
	return time.Duration(c.Jobs.RetryDelaySeconds) * time.Second
}
