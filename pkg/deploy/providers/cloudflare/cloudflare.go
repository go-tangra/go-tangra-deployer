package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

const (
	ProviderType      = "cloudflare"
	cloudflareAPIBase = "https://api.cloudflare.com/client/v4"
)

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "Cloudflare",
		Description: "Deploy SSL/TLS certificates to Cloudflare zones using Custom SSL",
		Caps: &registry.ProviderCapabilities{
			SupportsVerification: true,
			SupportsRollback:     false,
			RequiredConfigFields: []string{"zone_id"},
			RequiredCredFields:   []string{"api_token"},
		},
	})
}

// Provider implements the Cloudflare deployment provider
type Provider struct{}

// GetCapabilities returns the provider's capabilities
func (p *Provider) GetCapabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     false,
		RequiredConfigFields: []string{"zone_id"},
		RequiredCredFields:   []string{"api_token"},
	}
}

// ValidateCredentials validates Cloudflare API credentials
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	apiToken, ok := credentials["api_token"].(string)
	if !ok || apiToken == "" {
		return fmt.Errorf("api_token is required")
	}

	// Basic token format validation
	if len(apiToken) < 20 {
		return fmt.Errorf("api_token appears to be invalid (too short)")
	}

	client := &http.Client{Timeout: 30 * time.Second}

	// If zone_id is provided, validate by accessing the specific zone
	// This is the most reliable method for zone-scoped tokens
	if zoneID, ok := config["zone_id"].(string); ok && zoneID != "" {
		url := fmt.Sprintf("%s/zones/%s", cloudflareAPIBase, zoneID)
		req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
		if err != nil {
			return fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+apiToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return fmt.Errorf("failed to validate credentials: %w", err)
		}
		defer resp.Body.Close()

		body, _ := io.ReadAll(resp.Body)

		var result struct {
			Success bool `json:"success"`
			Errors  []struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"errors"`
		}

		if err := json.Unmarshal(body, &result); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}

		if result.Success {
			return nil
		}

		if len(result.Errors) > 0 {
			return fmt.Errorf("credentials validation failed: %s (code: %d)", result.Errors[0].Message, result.Errors[0].Code)
		}
		return fmt.Errorf("credentials validation failed: could not access zone")
	}

	// Fallback: try generic endpoints when no zone_id provided
	endpoints := []string{
		"/user/tokens/verify", // User API tokens
		"/zones",              // Account-level zone access
		"/user",               // User-scoped tokens
	}

	var lastErr error
	for _, endpoint := range endpoints {
		req, err := http.NewRequestWithContext(ctx, "GET", cloudflareAPIBase+endpoint, nil)
		if err != nil {
			continue
		}
		req.Header.Set("Authorization", "Bearer "+apiToken)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		var result struct {
			Success bool `json:"success"`
			Errors  []struct {
				Code    int    `json:"code"`
				Message string `json:"message"`
			} `json:"errors"`
		}

		if err := json.Unmarshal(body, &result); err != nil {
			continue
		}

		if result.Success {
			return nil
		}

		for _, e := range result.Errors {
			if e.Code == 10000 || e.Code == 9109 || e.Code == 1000 {
				lastErr = fmt.Errorf("%s (code: %d)", e.Message, e.Code)
			}
		}
	}

	if lastErr != nil {
		return fmt.Errorf("credentials validation failed: %w", lastErr)
	}
	return fmt.Errorf("credentials validation failed: could not verify API token")
}

// Deploy deploys a certificate to Cloudflare
func (p *Provider) Deploy(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	apiToken, ok := credentials["api_token"].(string)
	if !ok || apiToken == "" {
		return nil, fmt.Errorf("api_token is required")
	}

	zoneID, ok := config["zone_id"].(string)
	if !ok || zoneID == "" {
		return nil, fmt.Errorf("zone_id is required in config")
	}

	progressCb(10, "Validating certificate data")

	if cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}

	progressCb(20, "Preparing certificate upload")

	// Prepare the certificate bundle
	certBundle := cert.CertificatePEM
	if cert.CertificateChain != "" {
		certBundle = certBundle + "\n" + cert.CertificateChain
	}

	// Check if there's an existing custom certificate
	progressCb(30, "Checking for existing certificates")
	existingCertID, err := p.findExistingCert(ctx, apiToken, zoneID, cert.CommonName)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing certificates: %w", err)
	}

	var resourceID string
	if existingCertID != "" {
		// Update existing certificate
		progressCb(50, "Updating existing certificate")
		resourceID, err = p.updateCert(ctx, apiToken, zoneID, existingCertID, certBundle, cert.PrivateKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed to update certificate: %w", err)
		}
	} else {
		// Upload new certificate
		progressCb(50, "Uploading new certificate")
		resourceID, err = p.uploadCert(ctx, apiToken, zoneID, certBundle, cert.PrivateKeyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed to upload certificate: %w", err)
		}
	}

	progressCb(90, "Verifying deployment")

	// Brief wait for propagation
	time.Sleep(2 * time.Second)

	progressCb(100, "Deployment complete")

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate deployed successfully to Cloudflare",
		ResourceID: resourceID,
		Details: map[string]any{
			"zone_id":        zoneID,
			"certificate_id": resourceID,
			"was_update":     existingCertID != "",
		},
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// Verify verifies that a certificate is deployed correctly
func (p *Provider) Verify(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	apiToken, ok := credentials["api_token"].(string)
	if !ok || apiToken == "" {
		return nil, fmt.Errorf("api_token is required")
	}

	zoneID, ok := config["zone_id"].(string)
	if !ok || zoneID == "" {
		return nil, fmt.Errorf("zone_id is required in config")
	}

	// Find the certificate
	certID, err := p.findExistingCert(ctx, apiToken, zoneID, cert.CommonName)
	if err != nil {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Failed to verify: %v", err),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	if certID == "" {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    "Certificate not found in Cloudflare zone",
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate verified in Cloudflare zone",
		ResourceID: certID,
		Details: map[string]any{
			"zone_id":        zoneID,
			"certificate_id": certID,
		},
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// Rollback is not supported for Cloudflare
func (p *Provider) Rollback(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	return &registry.DeploymentResult{
		Success:    false,
		Message:    "Rollback is not supported for Cloudflare provider",
		DurationMs: 0,
	}, nil
}

// findExistingCert finds an existing custom certificate by hostname
func (p *Provider) findExistingCert(ctx context.Context, apiToken, zoneID, hostname string) (string, error) {
	url := fmt.Sprintf("%s/zones/%s/custom_certificates", cloudflareAPIBase, zoneID)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Success bool `json:"success"`
		Result  []struct {
			ID    string   `json:"id"`
			Hosts []string `json:"hosts"`
		} `json:"result"`
		Errors []struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"errors"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if !result.Success {
		if len(result.Errors) > 0 {
			return "", fmt.Errorf("cloudflare API error: %s (code: %d)", result.Errors[0].Message, result.Errors[0].Code)
		}
		return "", fmt.Errorf("cloudflare API returned unsuccessful response (HTTP %d)", resp.StatusCode)
	}

	// Find certificate matching the hostname
	for _, cert := range result.Result {
		for _, host := range cert.Hosts {
			if host == hostname {
				return cert.ID, nil
			}
		}
	}

	return "", nil
}

// uploadCert uploads a new custom certificate
func (p *Provider) uploadCert(ctx context.Context, apiToken, zoneID, certBundle, privateKey string) (string, error) {
	url := fmt.Sprintf("%s/zones/%s/custom_certificates", cloudflareAPIBase, zoneID)

	payload := map[string]any{
		"certificate":   certBundle,
		"private_key":   privateKey,
		"bundle_method": "ubiquitous",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Success bool `json:"success"`
		Result  struct {
			ID string `json:"id"`
		} `json:"result"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if !result.Success {
		if len(result.Errors) > 0 {
			return "", fmt.Errorf("cloudflare API error: %s", result.Errors[0].Message)
		}
		return "", fmt.Errorf("cloudflare API returned unsuccessful response")
	}

	return result.Result.ID, nil
}

// updateCert updates an existing custom certificate
func (p *Provider) updateCert(ctx context.Context, apiToken, zoneID, certID, certBundle, privateKey string) (string, error) {
	url := fmt.Sprintf("%s/zones/%s/custom_certificates/%s", cloudflareAPIBase, zoneID, certID)

	payload := map[string]any{
		"certificate":   certBundle,
		"private_key":   privateKey,
		"bundle_method": "ubiquitous",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var result struct {
		Success bool `json:"success"`
		Result  struct {
			ID string `json:"id"`
		} `json:"result"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", err
	}

	if !result.Success {
		if len(result.Errors) > 0 {
			return "", fmt.Errorf("cloudflare API error: %s", result.Errors[0].Message)
		}
		return "", fmt.Errorf("cloudflare API returned unsuccessful response")
	}

	return result.Result.ID, nil
}
