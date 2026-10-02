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
	"regexp"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func init() { provider.Register(Provider{}) }

const defaultAPIBase = "https://api.cloudflare.com/client/v4"

// Provider uploads custom SSL certificates to a Cloudflare zone.
//
// apiBase is a test-only override of the API base URL. It is deliberately
// unexported and never read from a configuration: a stored configuration that
// could set it would send someone else's sealed API token to an arbitrary
// host. The registered provider always talks to the real Cloudflare API.
// transport is likewise a test-only hook (nil = the default transport).
type Provider struct {
	apiBase   string
	transport http.RoundTripper
}

// Capabilities describes the Cloudflare provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "cloudflare",
		DisplayName:      "Cloudflare",
		Description:      "Uploads the certificate as a Cloudflare custom certificate for one zone.",
		SupportsVerify:   true,
		SupportsRollback: false,
		SchemaVersion:    1,
		ConfigFields: []provider.Field{
			{Key: "zone_id", Label: "Zone ID", Type: provider.TypeString, Required: true, Overridable: true, Group: provider.GroupConnection,
				Pattern: `^[a-fA-F0-9]{32}$`, MaxLength: 32, Placeholder: "023e105f4ecef8ad9ca31a8372d0c353",
				Help: "Cloudflare dashboard → the zone → Overview → API → Zone ID."},
		},
		CredentialFields: []provider.Field{
			{Key: "api_token", Label: "API token", Type: provider.TypeString, Secret: true, Required: true, Group: provider.GroupCredentials,
				MaxLength: 256, Help: "API token with the permission Zone → SSL and Certificates → Edit for this zone."},
		},
	}
}

// note guards the optional progress callback.
func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}

func (p Provider) httpClient() *http.Client {
	return &http.Client{Timeout: 60 * time.Second, Transport: p.transport, CheckRedirect: provider.NoRedirect}
}

// base returns the Cloudflare API base URL. Only the test-only
// Provider.apiBase can change it; config is never consulted (a stored
// "api_base" key is ignored, see provider.RedirectKeys).
func (p Provider) base() string {
	if p.apiBase != "" {
		return p.apiBase
	}
	return defaultAPIBase
}

// zonePattern is the Cloudflare zone id shape (32 hex characters). The zone id
// is interpolated into the request path, so anything else is refused before a
// request is built.
var zonePattern = regexp.MustCompile(`^[a-fA-F0-9]{32}$`)

// zoneFrom returns the configured zone id, refusing a missing or malformed one.
func zoneFrom(config map[string]any) (string, error) {
	z := strFrom(config, "zone_id")
	if z == "" {
		return "", fmt.Errorf("zone_id is required")
	}
	if !zonePattern.MatchString(z) {
		return "", fmt.Errorf("zone_id must be 32 hexadecimal characters")
	}
	return z, nil
}

// ValidateConfig is the save-time check (provider.ConfigValidator): a zone id,
// when given, must be 32 hexadecimal characters.
func (Provider) ValidateConfig(config map[string]any) error {
	if _, present := config["zone_id"]; !present {
		return nil
	}
	if !zonePattern.MatchString(strFrom(config, "zone_id")) {
		return &provider.FieldError{Field: "config.zone_id", Msg: "zone_id must be 32 hexadecimal characters"}
	}
	return nil
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
func (p Provider) call(ctx context.Context, method, url, apiToken string, body []byte) (cfEnvelope, error) {
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

	resp, err := p.httpClient().Do(req)
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
func (p Provider) findExistingCert(ctx context.Context, base, apiToken, zoneID, hostname string) (string, error) {
	url := fmt.Sprintf("%s/zones/%s/custom_certificates", base, zoneID)
	env, err := p.call(ctx, http.MethodGet, url, apiToken, nil)
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
func (p Provider) putCert(ctx context.Context, base, apiToken, zoneID, certID, certBundle, privateKey string) (string, error) {
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
	env, err := p.call(ctx, method, url, apiToken, payload)
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
	zoneID, err := zoneFrom(config)
	if err != nil {
		return nil, err
	}
	if cert == nil || cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}
	base := p.base()

	note(progress, 10, "preparing certificate bundle")
	certBundle := cert.CertificatePEM
	if cert.CertificateChain != "" {
		certBundle = certBundle + "\n" + cert.CertificateChain
	}

	note(progress, 30, "checking for existing certificate")
	existingID, err := p.findExistingCert(ctx, base, apiToken, zoneID, cert.CommonName)
	if err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}

	if existingID != "" {
		note(progress, 50, "updating existing certificate")
	} else {
		note(progress, 50, "uploading new certificate")
	}
	resourceID, err := p.putCert(ctx, base, apiToken, zoneID, existingID, certBundle, cert.PrivateKeyPEM)
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
	zoneID, err := zoneFrom(config)
	if err != nil {
		return nil, err
	}
	if cert == nil {
		return nil, fmt.Errorf("certificate data is required")
	}
	base := p.base()

	certID, err := p.findExistingCert(ctx, base, apiToken, zoneID, cert.CommonName)
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
	_, err := zoneFrom(config)
	return err
}
