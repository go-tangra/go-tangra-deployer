package config_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/config"
)

// valid returns a fully populated deployer configuration that passes Validate in
// a development environment. Individual tests mutate a copy to exercise failures.
func valid() config.Config {
	c := config.Default()
	c.ServiceName, c.TrustDomain, c.Env = "deployer", "example.org", "dev"
	c.Identity.Provider = "file"
	c.Identity.File.Cert, c.Identity.File.Key, c.Identity.File.Bundle = "c", "k", "b"
	c.Authz.Source, c.Authz.Path = "file", "p.yaml"
	c.DB.DSN = "postgres://deployer_app:x@db/deployer?sslmode=disable"
	c.Valkey.Addresses = []string{"127.0.0.1:6379"}
	c.Valkey.AllowPlaintext = true
	c.KEK.Path = "deploy/kek.dev"
	c.Gateway.Issuer = "https://localhost:8443"
	return c
}

// production flips a valid config into production mode with production-safe
// transport settings.
func production(c *config.Config) {
	c.Env = "production"
	c.DB.DSN = "postgres://u:p@db/deployer?sslmode=verify-full"
	c.Valkey.AllowPlaintext = false
}

// TestDefaultsAreSane asserts Default() returns the documented secure defaults.
func TestDefaultsAreSane(t *testing.T) {
	c := config.Default()
	if c.Valkey.AllowPlaintext {
		t.Error("Default: valkey.allow_plaintext must be opt-in (false)")
	}
	if c.KEK.Source != "file" {
		t.Errorf("Default: KEK.Source = %q, want %q", c.KEK.Source, "file")
	}
	if c.DB.MaxConns != 16 {
		t.Errorf("Default: DB.MaxConns = %d, want 16", c.DB.MaxConns)
	}
	if c.Jobs.Workers != 5 || c.Jobs.IntervalSeconds != 5 || c.Jobs.LeaseSeconds != 300 ||
		c.Jobs.MaxRetries != 3 || c.Jobs.RetryDelaySeconds != 60 || c.Jobs.BackoffMultiplier != 2.0 ||
		c.Jobs.JobTimeoutSeconds != 300 || c.Jobs.CleanupDays != 30 {
		t.Errorf("Default: unexpected Jobs defaults %+v", c.Jobs)
	}
	if !c.Events.Enabled {
		t.Error("Default: Events.Enabled must default to true")
	}
	if c.Gateway.Service != "gateway" || c.LCM.Service != "lcm" {
		t.Errorf("Default: Gateway.Service=%q LCM.Service=%q, want gateway/lcm", c.Gateway.Service, c.LCM.Service)
	}
	if c.Limits.BackupMaxBytes != 16<<20 || c.Limits.ConfigMaxBytes != 16<<10 || c.Limits.FilterMaxLength != 256 {
		t.Errorf("Default: unexpected Limits defaults %+v", c.Limits)
	}
}

// TestValidateAcceptsDevAndProduction covers the happy paths and the duration
// helper methods, plus the KEK env source variant.
func TestValidateAcceptsDevAndProduction(t *testing.T) {
	c := valid()
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(dev): unexpected error %v", err)
	}

	// Duration helpers derive from the defaults.
	if c.Interval() != 5*time.Second {
		t.Errorf("Interval() = %v, want 5s", c.Interval())
	}
	if c.Lease() != 300*time.Second {
		t.Errorf("Lease() = %v, want 300s", c.Lease())
	}
	if c.JobTimeout() != 300*time.Second {
		t.Errorf("JobTimeout() = %v, want 300s", c.JobTimeout())
	}
	if c.RetryDelay() != 60*time.Second {
		t.Errorf("RetryDelay() = %v, want 60s", c.RetryDelay())
	}
	if c.CleanupWindow() != 30*24*time.Hour {
		t.Errorf("CleanupWindow() = %v, want 30d", c.CleanupWindow())
	}

	// A dev config with allow_plaintext produces the documented warning.
	if w := c.Warnings(); !strings.Contains(strings.Join(w, "\n"), "valkey.allow_plaintext") {
		t.Errorf("Warnings() = %v, want a valkey.allow_plaintext entry", w)
	}

	// Production with verify-full passes.
	production(&c)
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(production): unexpected error %v", err)
	}
	// verify-ca is also accepted.
	c.DB.DSN = "postgres://u:p@db/deployer?sslmode=verify-ca"
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(verify-ca): unexpected error %v", err)
	}
	// The env KEK source variant is accepted.
	c.KEK = config.KEK{Source: "env", Env: "DEPLOYER_KEK"}
	if err := c.Validate(); err != nil {
		t.Fatalf("Validate(kek env): unexpected error %v", err)
	}
}

// TestValidateRejects covers each required field / bound the validator enforces.
func TestValidateRejects(t *testing.T) {
	cases := map[string]func(*config.Config){
		"service_name":      func(c *config.Config) { c.ServiceName = "" },
		"db.dsn required":   func(c *config.Config) { c.DB.DSN = "" },
		"db.dsn prod ssl":   func(c *config.Config) { production(c); c.DB.DSN = "postgres://u:p@db/deployer?sslmode=disable" },
		"valkey addresses":  func(c *config.Config) { c.Valkey.Addresses = nil },
		"valkey plaintext":  func(c *config.Config) { production(c); c.Valkey.AllowPlaintext = true },
		"kek file path":     func(c *config.Config) { c.KEK = config.KEK{Source: "file"} },
		"kek env":           func(c *config.Config) { c.KEK = config.KEK{Source: "env"} },
		"kek source":        func(c *config.Config) { c.KEK = config.KEK{Source: "vault"} },
		"jobs workers":      func(c *config.Config) { c.Jobs.Workers = 0 },
		"jobs interval":     func(c *config.Config) { c.Jobs.IntervalSeconds = 0 },
		"jobs lease":        func(c *config.Config) { c.Jobs.LeaseSeconds = 1 },
		"jobs max retries":  func(c *config.Config) { c.Jobs.MaxRetries = -1 },
		"jobs retry delay":  func(c *config.Config) { c.Jobs.RetryDelaySeconds = 0 },
		"jobs backoff":      func(c *config.Config) { c.Jobs.BackoffMultiplier = 0 },
		"jobs job timeout":  func(c *config.Config) { c.Jobs.JobTimeoutSeconds = 1 },
		"jobs cleanup days": func(c *config.Config) { c.Jobs.CleanupDays = 0 },
		"gateway service":   func(c *config.Config) { c.Gateway.Service = "" },
		"gateway issuer":    func(c *config.Config) { c.Gateway.Issuer = "http://insecure" },
		"lcm service":       func(c *config.Config) { c.LCM.Service = "" },
		"backup max bytes":  func(c *config.Config) { c.Limits.BackupMaxBytes = 1 << 20 },
		"config max bytes":  func(c *config.Config) { c.Limits.ConfigMaxBytes = 1 },
		"filter max length": func(c *config.Config) { c.Limits.FilterMaxLength = 1 },
	}
	for name, mut := range cases {
		c := valid()
		mut(&c)
		if err := c.Validate(); err == nil {
			t.Errorf("%s: expected a validation error, got nil", name)
		}
	}
}

// TestLoad round-trips a written YAML file over the defaults and checks the
// error paths (missing file, unknown field).
func TestLoad(t *testing.T) {
	dir := t.TempDir()

	// A YAML overlay changes selected values; the rest keep their defaults.
	path := filepath.Join(dir, "cfg.yaml")
	if err := os.WriteFile(path, []byte("db:\n  max_conns: 32\njobs:\n  workers: 8\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.DB.MaxConns != 32 {
		t.Errorf("Load: DB.MaxConns = %d, want 32", c.DB.MaxConns)
	}
	if c.Jobs.Workers != 8 {
		t.Errorf("Load: Jobs.Workers = %d, want 8", c.Jobs.Workers)
	}
	// Unmentioned fields retain their defaults.
	if c.Jobs.IntervalSeconds != 5 {
		t.Errorf("Load: Jobs.IntervalSeconds = %d, want default 5", c.Jobs.IntervalSeconds)
	}
	if c.Gateway.Service != "gateway" {
		t.Errorf("Load: Gateway.Service = %q, want default gateway", c.Gateway.Service)
	}

	// A missing file is an error.
	if _, err := config.Load(filepath.Join(dir, "nope.yaml")); err == nil {
		t.Error("Load(missing file): expected an error, got nil")
	}

	// An unknown field is rejected (KnownFields).
	bad := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(bad, []byte("not_a_field: 1\n"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	if _, err := config.Load(bad); err == nil {
		t.Error("Load(unknown field): expected an error, got nil")
	}
}
