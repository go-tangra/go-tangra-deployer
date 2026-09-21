// Package cloudflare deploys custom SSL/TLS certificates to a Cloudflare zone
// using the Cloudflare API v4 Custom SSL feature. On Deploy it uploads the
// certificate bundle (leaf + chain) and private key to the zone's
// custom_certificates collection, updating an existing certificate for the same
// hostname when one is found. Verify confirms the certificate is present in the
// zone. Rollback is not supported by Cloudflare and reports a clean failure.
//
// Authentication uses a Cloudflare API token sent as `Authorization: Bearer
// <api_token>`. The token and private key are never logged or returned in any
// Result or error message.
package cloudflare

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

func init() { provider.Register(Provider{}) }

const defaultAPIBase = "https://api.cloudflare.com/client/v4"

// Provider uploads custom SSL certificates to a Cloudflare zone.
type Provider struct{}

// Capabilities describes the Cloudflare provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "cloudflare",
		DisplayName:      "Cloudflare",
		SupportsVerify:   true,
		SupportsRollback: false,
		ConfigFields: []provider.Field{
			{Key: "zone_id", Label: "Zone ID", Required: true},
		},
		CredentialFields: []provider.Field{
			{Key: "api_token", Label: "API Token", Secret: true, Required: true},
		},
	}
}

// note guards the optional progress callback.
func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}

func httpClient() *http.Client { return &http.Client{Timeout: 60 * time.Second} }

// apiBase returns the Cloudflare API base URL, overridable via config["api_base"]
// so tests can target an httptest.Server. Falls back to the real API.
func apiBase(config map[string]any) string {
	if s, ok := config["api_base"].(string); ok && s != "" {
		return s
	}
	return defaultAPIBase
}

func strFrom(m map[string]any, key string) string {
	if s, ok := m[key].(string); ok {
		return s
	}
	return ""
}

// cfEnvelope is the standard Cloudflare API v4 response wrapper.
type cfEnvelope struct {
	Success bool `json:"success"`
	Errors  []struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
	Result json.RawMessage `json:"result"`
}

// clientError returns a client-safe error string from a Cloudflare envelope.
// It only surfaces Cloudflare's own error messages, never the token or key.
func (e cfEnvelope) clientError(status int) string {
	if len(e.Errors) > 0 {
		return fmt.Sprintf("cloudflare API error: %s (code %d)", e.Errors[0].Message, e.Errors[0].Code)
	}
	return fmt.Sprintf("cloudflare API returned an unsuccessful response (HTTP %d)", status)
}

// call performs a Cloudflare API request with bearer auth and decodes the
// standard envelope. It returns the envelope and a client-safe error. The
// api_token is used only for the Authorization header and never appears in
// returned errors.
func call(ctx context.Context, method, url, apiToken string, body []byte) (cfEnvelope, error) {
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return cfEnvelope{}, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+apiToken)
	req.Header.Set("Content-Type", "application/json")

	resp, err := httpClient().Do(req)
	if err != nil {
		return cfEnvelope{}, fmt.Errorf("cloudflare request failed: %w", err)
	}
	defer resp.Body.Close()

	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var env cfEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return cfEnvelope{}, fmt.Errorf("cloudflare returned an unparseable response (HTTP %d)", resp.StatusCode)
	}
	if !env.Success {
		return env, fmt.Errorf("%s", env.clientError(resp.StatusCode))
	}
	return env, nil
}

// findExistingCert returns the ID of a custom certificate whose hosts include
// hostname, or "" when none exists.
func findExistingCert(ctx context.Context, base, apiToken, zoneID, hostname string) (string, error) {
	url := fmt.Sprintf("%s/zones/%s/custom_certificates", base, zoneID)
	env, err := call(ctx, http.MethodGet, url, apiToken, nil)
	if err != nil {
		return "", err
	}
	var list []struct {
		ID    string   `json:"id"`
		Hosts []string `json:"hosts"`
	}
	if len(env.Result) > 0 {
		_ = json.Unmarshal(env.Result, &list)
	}
	for _, c := range list {
		for _, h := range c.Hosts {
			if h == hostname {
				return c.ID, nil
			}
		}
	}
	return "", nil
}

// putCert uploads (POST) a new certificate or updates (PATCH) an existing one,
// returning the resulting certificate ID.
func putCert(ctx context.Context, base, apiToken, zoneID, certID, certBundle, privateKey string) (string, error) {
	payload, err := json.Marshal(map[string]any{
		"certificate":   certBundle,
		"private_key":   privateKey,
		"bundle_method": "ubiquitous",
	})
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	method := http.MethodPost
	url := fmt.Sprintf("%s/zones/%s/custom_certificates", base, zoneID)
	if certID != "" {
		method = http.MethodPatch
		url = fmt.Sprintf("%s/zones/%s/custom_certificates/%s", base, zoneID, certID)
	}
	env, err := call(ctx, method, url, apiToken, payload)
	if err != nil {
		return "", err
	}
	var res struct {
		ID string `json:"id"`
	}
	if len(env.Result) > 0 {
		_ = json.Unmarshal(env.Result, &res)
	}
	return res.ID, nil
}

// Deploy uploads the certificate bundle and private key to the Cloudflare zone.
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	apiToken := strFrom(creds, "api_token")
	if apiToken == "" {
		return nil, fmt.Errorf("api_token is required")
	}
	zoneID := strFrom(config, "zone_id")
	if zoneID == "" {
		return nil, fmt.Errorf("zone_id is required")
	}
	if cert == nil || cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}
	base := apiBase(config)

	note(progress, 10, "preparing certificate bundle")
	certBundle := cert.CertificatePEM
	if cert.CertificateChain != "" {
		certBundle = certBundle + "\n" + cert.CertificateChain
	}

	note(progress, 30, "checking for existing certificate")
	existingID, err := findExistingCert(ctx, base, apiToken, zoneID, cert.CommonName)
	if err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}

	if existingID != "" {
		note(progress, 50, "updating existing certificate")
	} else {
		note(progress, 50, "uploading new certificate")
	}
	resourceID, err := putCert(ctx, base, apiToken, zoneID, existingID, certBundle, cert.PrivateKeyPEM)
	if err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}

	note(progress, 100, "deployment complete")
	return &provider.Result{
		Success: true,
		Message: "certificate deployed to Cloudflare zone",
		Details: map[string]any{
			"zone_id":        zoneID,
			"certificate_id": resourceID,
			"was_update":     existingID != "",
		},
	}, nil
}

// Verify confirms the certificate is present in the Cloudflare zone.
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	apiToken := strFrom(creds, "api_token")
	if apiToken == "" {
		return nil, fmt.Errorf("api_token is required")
	}
	zoneID := strFrom(config, "zone_id")
	if zoneID == "" {
		return nil, fmt.Errorf("zone_id is required")
	}
	if cert == nil {
		return nil, fmt.Errorf("certificate data is required")
	}
	base := apiBase(config)

	certID, err := findExistingCert(ctx, base, apiToken, zoneID, cert.CommonName)
	if err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}
	if certID == "" {
		return &provider.Result{Success: false, Message: "certificate not found in Cloudflare zone"}, nil
	}
	return &provider.Result{
		Success: true,
		Message: "certificate verified in Cloudflare zone",
		Details: map[string]any{"zone_id": zoneID, "certificate_id": certID},
	}, nil
}

// Rollback is not supported by Cloudflare; it reports a clean failure so the job
// records an explicit unsupported outcome.
func (p Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	return &provider.Result{
		Success: false,
		Message: "rollback is not supported by the Cloudflare provider",
	}, nil
}

// ValidateCredentials checks that the required token and zone are present.
func (p Provider) ValidateCredentials(ctx context.Context, creds, config map[string]any) error {
	if strFrom(creds, "api_token") == "" {
		return fmt.Errorf("api_token is required")
	}
	if strFrom(config, "zone_id") == "" {
		return fmt.Errorf("zone_id is required")
	}
	return nil
}
