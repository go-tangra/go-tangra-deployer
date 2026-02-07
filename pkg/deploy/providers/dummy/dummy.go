package dummy

import (
	"context"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

const ProviderType = "dummy"

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{
			log: log.NewHelper(log.DefaultLogger),
		}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "Dummy (Test)",
		Description: "Dummy provider for testing - simulates deployments without actual external calls",
		Caps: &registry.ProviderCapabilities{
			SupportsVerification: true,
			SupportsRollback:     true,
			RequiredConfigFields: []string{},
			RequiredCredFields:   []string{},
		},
	})
}

// Config holds dummy provider configuration
type Config struct {
	// SimulateDelayMs is the delay in milliseconds to simulate deployment time
	SimulateDelayMs int64 `json:"simulate_delay_ms"`
	// ShouldFail if true, the deployment will fail
	ShouldFail bool `json:"should_fail"`
	// FailMessage is the error message when ShouldFail is true
	FailMessage string `json:"fail_message"`
	// SimulateProgressSteps is the number of progress updates to send
	SimulateProgressSteps int `json:"simulate_progress_steps"`
}

// Provider implements a dummy deployment provider for testing
type Provider struct {
	log *log.Helper
}

// Deploy simulates deploying a certificate
func (p *Provider) Deploy(
	ctx context.Context,
	cert *registry.CertificateData,
	config map[string]any,
	credentials map[string]any,
	progressCb registry.ProgressCallback,
) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	cfg := p.parseConfig(config)

	p.log.Infof("[DUMMY] Starting deployment for certificate: %s", cert.ID)
	p.log.Infof("[DUMMY] Config: delay=%dms, shouldFail=%v, progressSteps=%d",
		cfg.SimulateDelayMs, cfg.ShouldFail, cfg.SimulateProgressSteps)

	// Log certificate details
	p.log.Infof("[DUMMY] Certificate details:")
	p.log.Infof("[DUMMY]   - ID: %s", cert.ID)
	p.log.Infof("[DUMMY]   - Serial: %s", cert.SerialNumber)
	p.log.Infof("[DUMMY]   - CommonName: %s", cert.CommonName)
	p.log.Infof("[DUMMY]   - SANs: %v", cert.SANs)
	if cert.ExpiresAt > 0 {
		p.log.Infof("[DUMMY]   - ExpiresAt: %s", time.Unix(cert.ExpiresAt, 0).Format(time.RFC3339))
	}

	// Log credentials (keys only, not values for security)
	p.log.Infof("[DUMMY] Credentials provided: %v", getKeys(credentials))

	// Simulate progress
	steps := cfg.SimulateProgressSteps
	if steps <= 0 {
		steps = 5
	}

	delayPerStep := time.Duration(cfg.SimulateDelayMs) * time.Millisecond / time.Duration(steps)
	if delayPerStep < 10*time.Millisecond {
		delayPerStep = 10 * time.Millisecond
	}

	progressMessages := []string{
		"Validating certificate...",
		"Preparing deployment package...",
		"Connecting to target...",
		"Uploading certificate...",
		"Activating certificate...",
		"Verifying deployment...",
		"Cleaning up...",
		"Finalizing...",
	}

	for i := 0; i < steps; i++ {
		select {
		case <-ctx.Done():
			return &registry.DeploymentResult{
				Success:    false,
				Message:    "Deployment cancelled",
				DurationMs: time.Since(startTime).Milliseconds(),
			}, ctx.Err()
		default:
		}

		progress := int32((i + 1) * 100 / steps)
		if progress > 100 {
			progress = 100
		}

		msgIdx := i % len(progressMessages)
		if progressCb != nil {
			progressCb(progress, progressMessages[msgIdx])
		}

		p.log.Infof("[DUMMY] Progress: %d%% - %s", progress, progressMessages[msgIdx])

		time.Sleep(delayPerStep)
	}

	// Check if we should simulate a failure
	if cfg.ShouldFail {
		failMsg := cfg.FailMessage
		if failMsg == "" {
			failMsg = "Simulated deployment failure"
		}
		p.log.Warnf("[DUMMY] Simulating failure: %s", failMsg)

		return &registry.DeploymentResult{
			Success:    false,
			Message:    failMsg,
			DurationMs: time.Since(startTime).Milliseconds(),
			Details: map[string]any{
				"provider":       ProviderType,
				"certificate_id": cert.ID,
				"simulated":      true,
				"error":          failMsg,
			},
		}, nil
	}

	// Success
	resourceID := fmt.Sprintf("dummy-%s-%d", cert.ID, time.Now().Unix())
	p.log.Infof("[DUMMY] Deployment successful! Resource ID: %s", resourceID)

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate deployed successfully (dummy)",
		ResourceID: resourceID,
		DurationMs: time.Since(startTime).Milliseconds(),
		Details: map[string]any{
			"provider":       ProviderType,
			"certificate_id": cert.ID,
			"resource_id":    resourceID,
			"simulated":      true,
			"common_name":    cert.CommonName,
			"sans":           cert.SANs,
		},
	}, nil
}

// Verify simulates verifying a deployment
func (p *Provider) Verify(
	ctx context.Context,
	cert *registry.CertificateData,
	config map[string]any,
	credentials map[string]any,
) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	cfg := p.parseConfig(config)

	p.log.Infof("[DUMMY] Verifying deployment for certificate: %s", cert.ID)

	// Simulate delay
	delay := time.Duration(cfg.SimulateDelayMs) * time.Millisecond
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(delay / 2):
	}

	if cfg.ShouldFail {
		failMsg := cfg.FailMessage
		if failMsg == "" {
			failMsg = "Simulated verification failure"
		}
		return &registry.DeploymentResult{
			Success:    false,
			Message:    failMsg,
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	p.log.Infof("[DUMMY] Verification successful for certificate: %s", cert.ID)

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate verified successfully (dummy)",
		DurationMs: time.Since(startTime).Milliseconds(),
		Details: map[string]any{
			"provider":       ProviderType,
			"certificate_id": cert.ID,
			"verified":       true,
			"simulated":      true,
		},
	}, nil
}

// Rollback simulates rolling back a deployment
func (p *Provider) Rollback(
	ctx context.Context,
	cert *registry.CertificateData,
	config map[string]any,
	credentials map[string]any,
) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	cfg := p.parseConfig(config)

	p.log.Infof("[DUMMY] Rolling back deployment for certificate: %s", cert.ID)

	// Simulate delay
	delay := time.Duration(cfg.SimulateDelayMs) * time.Millisecond
	if delay < 100*time.Millisecond {
		delay = 100 * time.Millisecond
	}

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(delay / 2):
	}

	if cfg.ShouldFail {
		failMsg := cfg.FailMessage
		if failMsg == "" {
			failMsg = "Simulated rollback failure"
		}
		return &registry.DeploymentResult{
			Success:    false,
			Message:    failMsg,
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	p.log.Infof("[DUMMY] Rollback successful for certificate: %s", cert.ID)

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate rolled back successfully (dummy)",
		DurationMs: time.Since(startTime).Milliseconds(),
		Details: map[string]any{
			"provider":       ProviderType,
			"certificate_id": cert.ID,
			"rolled_back":    true,
			"simulated":      true,
		},
	}, nil
}

// ValidateCredentials validates the provided credentials
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	p.log.Infof("[DUMMY] Validating credentials: %v", getKeys(credentials))

	// Check if should_fail is set in credentials for testing
	if shouldFail, ok := credentials["should_fail"].(bool); ok && shouldFail {
		return fmt.Errorf("simulated credential validation failure")
	}

	// Always succeed otherwise
	p.log.Infof("[DUMMY] Credentials validation successful")
	return nil
}

// GetCapabilities returns the provider's capabilities
func (p *Provider) GetCapabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     true,
		RequiredConfigFields: []string{},
		RequiredCredFields:   []string{},
	}
}

// parseConfig parses the config map into a Config struct
func (p *Provider) parseConfig(config map[string]any) *Config {
	cfg := &Config{
		SimulateDelayMs:       1000, // Default 1 second
		ShouldFail:            false,
		FailMessage:           "",
		SimulateProgressSteps: 5,
	}

	if config == nil {
		return cfg
	}

	if v, ok := config["simulate_delay_ms"].(float64); ok {
		cfg.SimulateDelayMs = int64(v)
	} else if v, ok := config["simulate_delay_ms"].(int64); ok {
		cfg.SimulateDelayMs = v
	} else if v, ok := config["simulate_delay_ms"].(int); ok {
		cfg.SimulateDelayMs = int64(v)
	}

	if v, ok := config["should_fail"].(bool); ok {
		cfg.ShouldFail = v
	}

	if v, ok := config["fail_message"].(string); ok {
		cfg.FailMessage = v
	}

	if v, ok := config["simulate_progress_steps"].(float64); ok {
		cfg.SimulateProgressSteps = int(v)
	} else if v, ok := config["simulate_progress_steps"].(int); ok {
		cfg.SimulateProgressSteps = v
	}

	return cfg
}

// getKeys returns the keys from a map (for logging without exposing values)
func getKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
