package fortigate

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

const (
	ProviderType = "fortigate"
)

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "FortiGate",
		Description: "Deploy SSL/TLS certificates to FortiGate firewalls via REST API",
		Caps: &registry.ProviderCapabilities{
			SupportsVerification: true,
			SupportsRollback:     true,
			RequiredConfigFields: []string{"vdom"},
			RequiredCredFields:   []string{"host", "api_token"},
		},
	})
}

// Provider implements the FortiGate deployment provider
type Provider struct{}

// GetCapabilities returns the provider's capabilities
func (p *Provider) GetCapabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     true,
		RequiredConfigFields: []string{"vdom"},
		RequiredCredFields:   []string{"host", "api_token"},
	}
}

// ValidateCredentials validates FortiGate credentials by checking connectivity
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	host, ok := credentials["host"].(string)
	if !ok || host == "" {
		return fmt.Errorf("host is required")
	}

	apiToken, ok := credentials["api_token"].(string)
	if !ok || apiToken == "" {
		return fmt.Errorf("api_token is required")
	}

	vdom := "root"
	if v, ok := config["vdom"].(string); ok && v != "" {
		vdom = v
	}

	client := p.createHTTPClient()

	// Test connectivity by getting system status
	url := fmt.Sprintf("https://%s/api/v2/cmdb/system/global?vdom=%s", host, vdom)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to FortiGate: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 || resp.StatusCode == 403 {
		return fmt.Errorf("authentication failed: invalid API token")
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("FortiGate API error (HTTP %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// Deploy deploys a certificate to FortiGate
func (p *Provider) Deploy(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	// Validate credentials
	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}

	host := credentials["host"].(string)
	apiToken := credentials["api_token"].(string)

	vdom := "root"
	if v, ok := config["vdom"].(string); ok && v != "" {
		vdom = v
	}

	// Generate certificate name from common name
	certName := sanitizeName(cert.CommonName)
	if certName == "" {
		certName = fmt.Sprintf("cert-%s", cert.ID[:8])
	}

	progressCb(10, "Validating certificate data")

	if cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}

	client := p.createHTTPClient()

	// Check if certificate already exists
	progressCb(20, "Checking for existing certificate")
	exists, err := p.certExists(ctx, client, host, apiToken, vdom, certName)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing certificate: %w", err)
	}

	// Prepare full certificate chain
	fullCert := cert.CertificatePEM
	if cert.CertificateChain != "" {
		fullCert = fullCert + "\n" + cert.CertificateChain
	}

	if exists {
		// Update existing certificate
		progressCb(50, "Updating existing certificate")
		if err := p.updateCertificate(ctx, client, host, apiToken, vdom, certName, fullCert, cert.PrivateKeyPEM); err != nil {
			return nil, fmt.Errorf("failed to update certificate: %w", err)
		}
	} else {
		// Upload new certificate
		progressCb(50, "Uploading new certificate")
		if err := p.uploadCertificate(ctx, client, host, apiToken, vdom, certName, fullCert, cert.PrivateKeyPEM); err != nil {
			return nil, fmt.Errorf("failed to upload certificate: %w", err)
		}
	}

	progressCb(90, "Verifying deployment")

	// Verify the certificate was uploaded
	exists, err = p.certExists(ctx, client, host, apiToken, vdom, certName)
	if err != nil || !exists {
		return nil, fmt.Errorf("verification failed: certificate not found after upload")
	}

	progressCb(100, "Deployment complete")

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate deployed successfully to FortiGate",
		ResourceID: certName,
		Details: map[string]any{
			"host":             host,
			"vdom":             vdom,
			"certificate_name": certName,
			"was_update":       exists,
		},
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// Verify verifies that a certificate is deployed correctly
func (p *Provider) Verify(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}

	host := credentials["host"].(string)
	apiToken := credentials["api_token"].(string)

	vdom := "root"
	if v, ok := config["vdom"].(string); ok && v != "" {
		vdom = v
	}

	certName := sanitizeName(cert.CommonName)
	if certName == "" {
		certName = fmt.Sprintf("cert-%s", cert.ID[:8])
	}

	client := p.createHTTPClient()

	exists, err := p.certExists(ctx, client, host, apiToken, vdom, certName)
	if err != nil {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Failed to verify: %v", err),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	if !exists {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    "Certificate not found on FortiGate",
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate verified on FortiGate",
		ResourceID: certName,
		Details: map[string]any{
			"host":             host,
			"vdom":             vdom,
			"certificate_name": certName,
		},
		DurationMs: time.Since(startTime).Milliseconds(),
	}, nil
}

// Rollback removes a deployed certificate
func (p *Provider) Rollback(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}

	host := credentials["host"].(string)
	apiToken := credentials["api_token"].(string)

	vdom := "root"
	if v, ok := config["vdom"].(string); ok && v != "" {
		vdom = v
	}

	certName := sanitizeName(cert.CommonName)
	if certName == "" {
		certName = fmt.Sprintf("cert-%s", cert.ID[:8])
	}

	client := p.createHTTPClient()

	if err := p.deleteCertificate(ctx, client, host, apiToken, vdom, certName); err != nil {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Rollback failed: %v", err),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate removed from FortiGate",
		DurationMs: time.Since(startTime).Milliseconds(),
		Details: map[string]any{
			"deleted_cert": certName,
		},
	}, nil
}

// createHTTPClient creates an HTTP client for FortiGate API calls
func (p *Provider) createHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // FortiGate often uses self-signed certs
			},
		},
	}
}

// certExists checks if a certificate exists on FortiGate
func (p *Provider) certExists(ctx context.Context, client *http.Client, host, apiToken, vdom, name string) (bool, error) {
	url := fmt.Sprintf("https://%s/api/v2/cmdb/certificate/local/%s?vdom=%s", host, name, vdom)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return false, nil
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(body))
	}

	return true, nil
}

// uploadCertificate uploads a new certificate to FortiGate
func (p *Provider) uploadCertificate(ctx context.Context, client *http.Client, host, apiToken, vdom, name, certPEM, keyPEM string) error {
	url := fmt.Sprintf("https://%s/api/v2/monitor/vpn-certificate/local/import?vdom=%s", host, vdom)

	// FortiGate expects base64 encoded certificate and key in specific format
	payload := map[string]any{
		"type":             "regular",
		"certname":         name,
		"file_content":     base64.StdEncoding.EncodeToString([]byte(certPEM)),
		"key_file_content": base64.StdEncoding.EncodeToString([]byte(keyPEM)),
		"scope":            "global",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// updateCertificate updates an existing certificate on FortiGate
// FortiGate doesn't have a direct update - we delete and re-upload
func (p *Provider) updateCertificate(ctx context.Context, client *http.Client, host, apiToken, vdom, name, certPEM, keyPEM string) error {
	// Delete existing certificate first
	if err := p.deleteCertificate(ctx, client, host, apiToken, vdom, name); err != nil {
		// Log but continue - might be in use or already deleted
	}

	// Small delay to ensure deletion is processed
	time.Sleep(1 * time.Second)

	// Upload new certificate
	return p.uploadCertificate(ctx, client, host, apiToken, vdom, name, certPEM, keyPEM)
}

// deleteCertificate deletes a certificate from FortiGate
func (p *Provider) deleteCertificate(ctx context.Context, client *http.Client, host, apiToken, vdom, name string) error {
	url := fmt.Sprintf("https://%s/api/v2/cmdb/certificate/local/%s?vdom=%s", host, name, vdom)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		// Already deleted
		return nil
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// sanitizeName converts a common name to a valid FortiGate resource name
func sanitizeName(name string) string {
	// Replace invalid characters with underscores
	result := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' || r == '.' {
			return r
		}
		return '_'
	}, name)

	// Remove leading/trailing underscores
	result = strings.Trim(result, "_")

	// FortiGate has a 35 character limit for certificate names
	if len(result) > 35 {
		result = result[:35]
	}

	// Ensure it doesn't start with a number
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "cert_" + result
		if len(result) > 35 {
			result = result[:35]
		}
	}

	return result
}
