package webhook

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

const (
	ProviderType = "webhook"
)

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "Webhook",
		Description: "Deploy certificates via HTTP webhook to custom endpoints",
		Caps: &registry.ProviderCapabilities{
			SupportsVerification: true,
			SupportsRollback:     true,
			RequiredConfigFields: []string{"url"},
			RequiredCredFields:   []string{},
		},
	})
}

// Provider implements the webhook deployment provider
type Provider struct{}

// WebhookPayload is the payload sent to the webhook endpoint
type WebhookPayload struct {
	Action           string            `json:"action"` // deploy, verify, rollback
	CertificateID    string            `json:"certificate_id"`
	SerialNumber     string            `json:"serial_number"`
	CommonName       string            `json:"common_name"`
	SANs             []string          `json:"sans,omitempty"`
	CertificatePEM   string            `json:"certificate_pem,omitempty"`
	CertificateChain string            `json:"certificate_chain,omitempty"`
	PrivateKeyPEM    string            `json:"private_key_pem,omitempty"`
	ExpiresAt        int64             `json:"expires_at,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// WebhookResponse is the expected response from the webhook endpoint
type WebhookResponse struct {
	Success    bool           `json:"success"`
	Message    string         `json:"message,omitempty"`
	ResourceID string         `json:"resource_id,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

// GetCapabilities returns the provider's capabilities
func (p *Provider) GetCapabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     true,
		RequiredConfigFields: []string{"url"},
		RequiredCredFields:   []string{},
	}
}

// ValidateCredentials validates webhook configuration
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	url, ok := config["url"].(string)
	if !ok || url == "" {
		return fmt.Errorf("url is required")
	}

	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return fmt.Errorf("url must start with http:// or https://")
	}

	// Optionally test the endpoint with a HEAD request
	client := p.createHTTPClient(config)
	req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
	if err != nil {
		return fmt.Errorf("invalid URL: %w", err)
	}

	p.addHeaders(req, credentials, config)

	resp, err := client.Do(req)
	if err != nil {
		// Don't fail validation if endpoint is unreachable, just warn
		return nil
	}
	defer resp.Body.Close()

	// Accept any 2xx or 405 (Method Not Allowed - endpoint exists but doesn't support HEAD)
	if resp.StatusCode >= 200 && resp.StatusCode < 300 || resp.StatusCode == 405 {
		return nil
	}

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return fmt.Errorf("authentication failed (HTTP %d)", resp.StatusCode)
	}

	return nil
}

// Deploy sends certificate data to the webhook endpoint
func (p *Provider) Deploy(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	url, ok := config["url"].(string)
	if !ok || url == "" {
		return nil, fmt.Errorf("url is required in config")
	}

	progressCb(10, "Preparing webhook payload")

	// Build payload
	payload := WebhookPayload{
		Action:           "deploy",
		CertificateID:    cert.ID,
		SerialNumber:     cert.SerialNumber,
		CommonName:       cert.CommonName,
		SANs:             cert.SANs,
		CertificatePEM:   cert.CertificatePEM,
		CertificateChain: cert.CertificateChain,
		PrivateKeyPEM:    cert.PrivateKeyPEM,
		ExpiresAt:        cert.ExpiresAt,
	}

	// Add custom metadata if configured
	if metadata, ok := config["metadata"].(map[string]any); ok {
		payload.Metadata = make(map[string]string)
		for k, v := range metadata {
			if s, ok := v.(string); ok {
				payload.Metadata[k] = s
			}
		}
	}

	progressCb(30, "Sending webhook request")

	// Send the request
	result, err := p.sendWebhook(ctx, url, payload, config, credentials)
	if err != nil {
		return nil, fmt.Errorf("webhook request failed: %w", err)
	}

	progressCb(90, "Processing response")

	if !result.Success {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    result.Message,
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	progressCb(100, "Deployment complete")

	return &registry.DeploymentResult{
		Success:    true,
		Message:    result.Message,
		ResourceID: result.ResourceID,
		Details:    result.Details,
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// Verify sends a verify request to the webhook endpoint
func (p *Provider) Verify(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	// Use verify_url if configured, otherwise use main url
	url := ""
	if verifyURL, ok := config["verify_url"].(string); ok && verifyURL != "" {
		url = verifyURL
	} else if mainURL, ok := config["url"].(string); ok {
		url = mainURL
	} else {
		return nil, fmt.Errorf("url is required in config")
	}

	payload := WebhookPayload{
		Action:        "verify",
		CertificateID: cert.ID,
		SerialNumber:  cert.SerialNumber,
		CommonName:    cert.CommonName,
		SANs:          cert.SANs,
	}

	result, err := p.sendWebhook(ctx, url, payload, config, credentials)
	if err != nil {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Verification failed: %v", err),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    result.Success,
		Message:    result.Message,
		ResourceID: result.ResourceID,
		Details:    result.Details,
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// Rollback sends a rollback request to the webhook endpoint
func (p *Provider) Rollback(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	// Use rollback_url if configured, otherwise use main url
	url := ""
	if rollbackURL, ok := config["rollback_url"].(string); ok && rollbackURL != "" {
		url = rollbackURL
	} else if mainURL, ok := config["url"].(string); ok {
		url = mainURL
	} else {
		return nil, fmt.Errorf("url is required in config")
	}

	payload := WebhookPayload{
		Action:        "rollback",
		CertificateID: cert.ID,
		SerialNumber:  cert.SerialNumber,
		CommonName:    cert.CommonName,
		SANs:          cert.SANs,
	}

	result, err := p.sendWebhook(ctx, url, payload, config, credentials)
	if err != nil {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Rollback failed: %v", err),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    result.Success,
		Message:    result.Message,
		ResourceID: result.ResourceID,
		Details:    result.Details,
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// sendWebhook sends a webhook request and parses the response
func (p *Provider) sendWebhook(ctx context.Context, url string, payload WebhookPayload, config, credentials map[string]any) (*WebhookResponse, error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "Deployer-Webhook/1.0")
	p.addHeaders(req, credentials, config)

	client := p.createHTTPClient(config)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Try to parse as WebhookResponse
	var result WebhookResponse
	if err := json.Unmarshal(respBody, &result); err != nil {
		// If response is not JSON, check HTTP status
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return &WebhookResponse{
				Success: true,
				Message: fmt.Sprintf("Webhook returned HTTP %d", resp.StatusCode),
			}, nil
		}
		return &WebhookResponse{
			Success: false,
			Message: fmt.Sprintf("Webhook returned HTTP %d: %s", resp.StatusCode, string(respBody)),
		}, nil
	}

	// If success field is not set, infer from HTTP status
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		if result.Message == "" {
			result.Message = "Webhook request successful"
		}
		// Only override success if response didn't explicitly set it to false
		if !result.Success && result.Message == "Webhook request successful" {
			result.Success = true
		}
	} else if resp.StatusCode >= 400 {
		result.Success = false
		if result.Message == "" {
			result.Message = fmt.Sprintf("Webhook returned HTTP %d", resp.StatusCode)
		}
	}

	return &result, nil
}

// createHTTPClient creates an HTTP client with optional TLS configuration
func (p *Provider) createHTTPClient(config map[string]any) *http.Client {
	timeout := 60 * time.Second
	if t, ok := config["timeout_seconds"].(float64); ok && t > 0 {
		timeout = time.Duration(t) * time.Second
	}

	skipVerify := false
	if skip, ok := config["skip_tls_verify"].(bool); ok {
		skipVerify = skip
	}

	return &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: skipVerify,
			},
		},
	}
}

// addHeaders adds authentication and custom headers to the request
func (p *Provider) addHeaders(req *http.Request, credentials, config map[string]any) {
	// Add Authorization header if configured
	if authHeader, ok := credentials["authorization"].(string); ok && authHeader != "" {
		req.Header.Set("Authorization", authHeader)
	} else if apiKey, ok := credentials["api_key"].(string); ok && apiKey != "" {
		// Support X-API-Key header
		req.Header.Set("X-API-Key", apiKey)
	} else if token, ok := credentials["token"].(string); ok && token != "" {
		// Support Bearer token
		req.Header.Set("Authorization", "Bearer "+token)
	}

	// Add secret header if configured
	if secret, ok := credentials["secret"].(string); ok && secret != "" {
		req.Header.Set("X-Webhook-Secret", secret)
	}

	// Add custom headers from config
	if headers, ok := config["headers"].(map[string]any); ok {
		for k, v := range headers {
			if s, ok := v.(string); ok {
				req.Header.Set(k, s)
			}
		}
	}
}
