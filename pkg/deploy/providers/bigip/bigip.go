package bigip

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
	ProviderType = "bigip"
)

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "F5 BIG-IP",
		Description: "Deploy SSL/TLS certificates to F5 BIG-IP load balancers via iControl REST API",
		Caps: &registry.ProviderCapabilities{
			SupportsVerification: true,
			SupportsRollback:     true,
			RequiredConfigFields: []string{"partition"},
			RequiredCredFields:   []string{"host", "username", "password"},
		},
	})
}

// Provider implements the F5 BIG-IP deployment provider
type Provider struct{}

// GetCapabilities returns the provider's capabilities
func (p *Provider) GetCapabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     true,
		RequiredConfigFields: []string{"partition"},
		RequiredCredFields:   []string{"host", "username", "password"},
	}
}

// ValidateCredentials validates BIG-IP credentials by checking connectivity
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	host, ok := credentials["host"].(string)
	if !ok || host == "" {
		return fmt.Errorf("host is required")
	}

	username, ok := credentials["username"].(string)
	if !ok || username == "" {
		return fmt.Errorf("username is required")
	}

	password, ok := credentials["password"].(string)
	if !ok || password == "" {
		return fmt.Errorf("password is required")
	}

	// Create HTTP client with TLS skip verify (BIG-IP often uses self-signed certs)
	client := p.createHTTPClient()

	// Test connectivity by getting system info
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/version", host)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to connect to BIG-IP: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 401 {
		return fmt.Errorf("authentication failed: invalid username or password")
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("BIG-IP API error (HTTP %d): %s", resp.StatusCode, string(body))
	}

	return nil
}

// Deploy deploys a certificate to BIG-IP
func (p *Provider) Deploy(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	startTime := time.Now()

	// Validate credentials
	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}

	host := credentials["host"].(string)
	username := credentials["username"].(string)
	password := credentials["password"].(string)

	partition := "Common"
	if p, ok := config["partition"].(string); ok && p != "" {
		partition = p
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

	// Step 1: Upload the certificate
	progressCb(20, "Uploading certificate to BIG-IP")
	certFullName := fmt.Sprintf("/%s/%s.crt", partition, certName)
	if err := p.uploadCertificate(ctx, client, host, username, password, certFullName, cert.CertificatePEM); err != nil {
		return nil, fmt.Errorf("failed to upload certificate: %w", err)
	}

	// Step 2: Upload the private key
	progressCb(40, "Uploading private key to BIG-IP")
	keyFullName := fmt.Sprintf("/%s/%s.key", partition, certName)
	if err := p.uploadKey(ctx, client, host, username, password, keyFullName, cert.PrivateKeyPEM); err != nil {
		return nil, fmt.Errorf("failed to upload private key: %w", err)
	}

	// Step 3: Upload CA chain if provided
	if cert.CertificateChain != "" {
		progressCb(55, "Uploading certificate chain to BIG-IP")
		chainFullName := fmt.Sprintf("/%s/%s-chain.crt", partition, certName)
		if err := p.uploadCertificate(ctx, client, host, username, password, chainFullName, cert.CertificateChain); err != nil {
			// Non-fatal: log but continue
			progressCb(60, "Warning: failed to upload certificate chain")
		}
	}

	// Step 4: Create or update SSL profile if specified
	sslProfileName := ""
	if name, ok := config["ssl_profile"].(string); ok && name != "" {
		progressCb(70, "Creating/updating SSL profile")
		sslProfileName = name
		profileFullName := fmt.Sprintf("/%s/%s", partition, sslProfileName)
		if err := p.createOrUpdateSSLProfile(ctx, client, host, username, password, profileFullName, certFullName, keyFullName); err != nil {
			return nil, fmt.Errorf("failed to create/update SSL profile: %w", err)
		}
	}

	progressCb(90, "Verifying deployment")

	// Verify the certificate was uploaded
	if err := p.verifyCertExists(ctx, client, host, username, password, certFullName); err != nil {
		return nil, fmt.Errorf("verification failed: %w", err)
	}

	progressCb(100, "Deployment complete")

	resourceID := certFullName
	details := map[string]any{
		"host":             host,
		"partition":        partition,
		"certificate_name": certFullName,
		"key_name":         keyFullName,
	}
	if sslProfileName != "" {
		details["ssl_profile"] = sslProfileName
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate deployed successfully to F5 BIG-IP",
		ResourceID: resourceID,
		Details:    details,
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
	username := credentials["username"].(string)
	password := credentials["password"].(string)

	partition := "Common"
	if p, ok := config["partition"].(string); ok && p != "" {
		partition = p
	}

	certName := sanitizeName(cert.CommonName)
	if certName == "" {
		certName = fmt.Sprintf("cert-%s", cert.ID[:8])
	}

	client := p.createHTTPClient()
	certFullName := fmt.Sprintf("/%s/%s.crt", partition, certName)

	if err := p.verifyCertExists(ctx, client, host, username, password, certFullName); err != nil {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Certificate not found: %v", err),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate verified on BIG-IP",
		ResourceID: certFullName,
		Details: map[string]any{
			"host":             host,
			"partition":        partition,
			"certificate_name": certFullName,
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
	username := credentials["username"].(string)
	password := credentials["password"].(string)

	partition := "Common"
	if p, ok := config["partition"].(string); ok && p != "" {
		partition = p
	}

	certName := sanitizeName(cert.CommonName)
	if certName == "" {
		certName = fmt.Sprintf("cert-%s", cert.ID[:8])
	}

	client := p.createHTTPClient()

	// Delete SSL profile first if it exists
	if sslProfileName, ok := config["ssl_profile"].(string); ok && sslProfileName != "" {
		profileFullName := fmt.Sprintf("/%s/%s", partition, sslProfileName)
		if err := p.deleteResource(ctx, client, host, username, password, "ltm/profile/client-ssl", profileFullName); err != nil {
			// SSL profile deletion is best-effort; continue with cert cleanup
			_ = err
		}
	}

	// Delete the certificate and key
	certFullName := fmt.Sprintf("/%s/%s.crt", partition, certName)
	keyFullName := fmt.Sprintf("/%s/%s.key", partition, certName)
	chainFullName := fmt.Sprintf("/%s/%s-chain.crt", partition, certName)

	var errors []string

	if err := p.deleteResource(ctx, client, host, username, password, "sys/crypto/cert", certFullName); err != nil {
		errors = append(errors, fmt.Sprintf("certificate: %v", err))
	}
	if err := p.deleteResource(ctx, client, host, username, password, "sys/crypto/key", keyFullName); err != nil {
		errors = append(errors, fmt.Sprintf("key: %v", err))
	}
	// Chain cert may not exist if the original deployment didn't include one
	if err := p.deleteResource(ctx, client, host, username, password, "sys/crypto/cert", chainFullName); err != nil {
		_ = err // intentionally ignored: chain cert is optional
	}

	if len(errors) > 0 {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Rollback partially failed: %s", strings.Join(errors, "; ")),
			DurationMs: time.Since(startTime).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate and key removed from BIG-IP",
		DurationMs: time.Since(startTime).Milliseconds(),
		Details: map[string]any{
			"deleted_cert": certFullName,
			"deleted_key":  keyFullName,
		},
	}, nil
}

// createHTTPClient creates an HTTP client for BIG-IP API calls
func (p *Provider) createHTTPClient() *http.Client {
	return &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true, // BIG-IP often uses self-signed certs
			},
		},
	}
}

// uploadFile uploads a file to BIG-IP's /var/tmp directory
func (p *Provider) uploadFile(ctx context.Context, client *http.Client, host, username, password, filename, content string) error {
	url := fmt.Sprintf("https://%s/mgmt/shared/file-transfer/uploads/%s", host, filename)

	contentBytes := []byte(content)
	contentLen := len(contentBytes)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(contentBytes))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Range", fmt.Sprintf("0-%d/%d", contentLen-1, contentLen))

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("file upload error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// uploadCertificate uploads a certificate to BIG-IP
func (p *Provider) uploadCertificate(ctx context.Context, client *http.Client, host, username, password, name, certPEM string) error {
	// Extract just the name without partition for temp filename
	parts := strings.Split(name, "/")
	tempFilename := parts[len(parts)-1]

	// Step 1: Upload the certificate file (uploads to /var/config/rest/downloads/)
	if err := p.uploadFile(ctx, client, host, username, password, tempFilename, certPEM); err != nil {
		return fmt.Errorf("failed to upload certificate file: %w", err)
	}

	// Step 2: Install the certificate from the uploaded file
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/cert", host)

	payload := map[string]any{
		"command":         "install",
		"name":            name,
		"from-local-file": fmt.Sprintf("/var/config/rest/downloads/%s", tempFilename),
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		respBody, _ := io.ReadAll(resp.Body)
		// Check if it's a "already exists" error - if so, try to update
		if resp.StatusCode == 409 || strings.Contains(string(respBody), "already exists") {
			return p.updateCertificate(ctx, client, host, username, password, name, certPEM)
		}
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// updateCertificate updates an existing certificate on BIG-IP
func (p *Provider) updateCertificate(ctx context.Context, client *http.Client, host, username, password, name, certPEM string) error {
	// Extract just the name without partition for temp filename
	parts := strings.Split(name, "/")
	tempFilename := parts[len(parts)-1]

	// Step 1: Upload the certificate file (uploads to /var/config/rest/downloads/)
	if err := p.uploadFile(ctx, client, host, username, password, tempFilename, certPEM); err != nil {
		return fmt.Errorf("failed to upload certificate file: %w", err)
	}

	// Step 2: Install with overwrite flag
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/cert", host)

	payload := map[string]any{
		"command":         "install",
		"name":            name,
		"from-local-file": fmt.Sprintf("/var/config/rest/downloads/%s", tempFilename),
		"overwrite":       true,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
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

// uploadKey uploads a private key to BIG-IP
func (p *Provider) uploadKey(ctx context.Context, client *http.Client, host, username, password, name, keyPEM string) error {
	// Extract just the name without partition for temp filename
	parts := strings.Split(name, "/")
	tempFilename := parts[len(parts)-1]

	// Step 1: Upload the key file (uploads to /var/config/rest/downloads/)
	if err := p.uploadFile(ctx, client, host, username, password, tempFilename, keyPEM); err != nil {
		return fmt.Errorf("failed to upload key file: %w", err)
	}

	// Step 2: Install the key from the uploaded file
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/key", host)

	payload := map[string]any{
		"command":         "install",
		"name":            name,
		"from-local-file": fmt.Sprintf("/var/config/rest/downloads/%s", tempFilename),
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		respBody, _ := io.ReadAll(resp.Body)
		// Check if it's an "already exists" error - if so, try to update
		if resp.StatusCode == 409 || strings.Contains(string(respBody), "already exists") {
			return p.updateKey(ctx, client, host, username, password, name, keyPEM)
		}
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// updateKey updates an existing private key on BIG-IP
func (p *Provider) updateKey(ctx context.Context, client *http.Client, host, username, password, name, keyPEM string) error {
	// Extract just the name without partition for temp filename
	parts := strings.Split(name, "/")
	tempFilename := parts[len(parts)-1]

	// Step 1: Upload the key file (uploads to /var/config/rest/downloads/)
	if err := p.uploadFile(ctx, client, host, username, password, tempFilename, keyPEM); err != nil {
		return fmt.Errorf("failed to upload key file: %w", err)
	}

	// Step 2: Install with overwrite flag
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/key", host)

	payload := map[string]any{
		"command":         "install",
		"name":            name,
		"from-local-file": fmt.Sprintf("/var/config/rest/downloads/%s", tempFilename),
		"overwrite":       true,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
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

// createOrUpdateSSLProfile creates or updates a client-ssl profile
func (p *Provider) createOrUpdateSSLProfile(ctx context.Context, client *http.Client, host, username, password, profileName, certName, keyName string) error {
	// First try to create the profile
	url := fmt.Sprintf("https://%s/mgmt/tm/ltm/profile/client-ssl", host)

	payload := map[string]any{
		"name":    profileName,
		"cert":    certName,
		"key":     keyName,
		"chain":   "none",
		"ciphers": "DEFAULT",
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		return nil
	}

	// If profile exists, try to update it
	if resp.StatusCode == 409 {
		return p.updateSSLProfile(ctx, client, host, username, password, profileName, certName, keyName)
	}

	respBody, _ := io.ReadAll(resp.Body)
	return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
}

// updateSSLProfile updates an existing client-ssl profile
func (p *Provider) updateSSLProfile(ctx context.Context, client *http.Client, host, username, password, profileName, certName, keyName string) error {
	// URL encode the profile name for the URL path
	encodedName := strings.ReplaceAll(profileName, "/", "~")
	url := fmt.Sprintf("https://%s/mgmt/tm/ltm/profile/client-ssl/%s", host, encodedName)

	payload := map[string]any{
		"cert": certName,
		"key":  keyName,
	}
	body, _ := json.Marshal(payload)

	req, err := http.NewRequestWithContext(ctx, "PATCH", url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// verifyCertExists checks if a certificate exists on BIG-IP
func (p *Provider) verifyCertExists(ctx context.Context, client *http.Client, host, username, password, certName string) error {
	encodedName := strings.ReplaceAll(certName, "/", "~")
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/cert/%s", host, encodedName)

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return fmt.Errorf("certificate not found")
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// deleteResource deletes a resource from BIG-IP
func (p *Provider) deleteResource(ctx context.Context, client *http.Client, host, username, password, resourceType, name string) error {
	encodedName := strings.ReplaceAll(name, "/", "~")
	url := fmt.Sprintf("https://%s/mgmt/tm/%s/%s", host, resourceType, encodedName)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		// Resource doesn't exist, consider it a success
		return nil
	}
	if resp.StatusCode != 200 {
		respBody, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("API error (HTTP %d): %s", resp.StatusCode, string(respBody))
	}

	return nil
}

// sanitizeName converts a common name to a valid BIG-IP resource name
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

	// Ensure it doesn't start with a number
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "cert_" + result
	}

	return result
}
