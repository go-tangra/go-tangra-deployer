// Package webhook is a generic deployment provider that POSTs the certificate
// bundle to a configurable HTTP endpoint. It fits any system that can receive a
// JSON webhook (a config-management hook, a custom installer, a CI trigger). The
// endpoint decides how to install the material; the provider reports the
// endpoint's success/failure. Deploy/verify/rollback are distinguished by the
// payload's action field, and optional verify_url/rollback_url route them
// separately.
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

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func init() { provider.Register(Provider{}) }

// Provider posts certificate bundles to an HTTP endpoint.
type Provider struct{}

// Capabilities describes the webhook provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "webhook",
		DisplayName:      "Webhook (generic HTTP)",
		SupportsVerify:   true,
		SupportsRollback: true,
		ConfigFields: []provider.Field{
			{Key: "url", Label: "Webhook URL", Required: true},
			{Key: "verify_url", Label: "Verify URL (optional)"},
			{Key: "rollback_url", Label: "Rollback URL (optional)"},
		},
		CredentialFields: []provider.Field{
			{Key: "token", Label: "Bearer token", Secret: true},
			{Key: "secret", Label: "Webhook signing secret", Secret: true},
		},
	}
}

// payload is the JSON body sent to the endpoint. Key material is included only
// on deploy (verify/rollback identify the cert by id/serial).
type payload struct {
	Action           string            `json:"action"`
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

// endpointResponse is the optional structured reply from the endpoint.
type endpointResponse struct {
	Success    bool           `json:"success"`
	Message    string         `json:"message,omitempty"`
	ResourceID string         `json:"resource_id,omitempty"`
	Details    map[string]any `json:"details,omitempty"`
}

func urlFrom(config map[string]any, key string) string {
	if s, ok := config[key].(string); ok {
		return s
	}
	return ""
}

// ValidateCredentials checks the URL shape and best-effort probes the endpoint.
func (p Provider) ValidateCredentials(ctx context.Context, creds, config map[string]any) error {
	url := urlFrom(config, "url")
	if url == "" {
		return fmt.Errorf("url is required")
	}
	if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
		return fmt.Errorf("url must start with http:// or https://")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return fmt.Errorf("invalid url: %w", err)
	}
	addHeaders(req, creds, config)
	resp, err := client(config).Do(req)
	if err != nil {
		return nil // unreachable at validation time is not fatal
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("endpoint rejected the credentials (HTTP %d)", resp.StatusCode)
	}
	return nil
}

// Deploy sends the full bundle to the endpoint.
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	url := urlFrom(config, "url")
	if url == "" {
		return nil, fmt.Errorf("url is required")
	}
	note(progress, 10, "preparing payload")
	pl := payload{
		Action: "deploy", CertificateID: cert.ID, SerialNumber: cert.SerialNumber,
		CommonName: cert.CommonName, SANs: cert.SANs, CertificatePEM: cert.CertificatePEM,
		CertificateChain: cert.CertificateChain, PrivateKeyPEM: cert.PrivateKeyPEM,
	}
	if !cert.ExpiresAt.IsZero() {
		pl.ExpiresAt = cert.ExpiresAt.Unix()
	}
	if md, ok := config["metadata"].(map[string]any); ok {
		pl.Metadata = map[string]string{}
		for k, v := range md {
			if s, ok := v.(string); ok {
				pl.Metadata[k] = s
			}
		}
	}
	note(progress, 40, "sending webhook")
	res, err := send(ctx, url, pl, config, creds)
	if err != nil {
		return nil, err
	}
	note(progress, 100, "done")
	return res, nil
}

// Verify asks the endpoint to confirm the certificate is installed.
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	return p.probe(ctx, "verify", "verify_url", cert, config, creds)
}

// Rollback asks the endpoint to restore the previous certificate.
func (p Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	return p.probe(ctx, "rollback", "rollback_url", cert, config, creds)
}

func (p Provider) probe(ctx context.Context, action, urlKey string, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	url := urlFrom(config, urlKey)
	if url == "" {
		url = urlFrom(config, "url")
	}
	if url == "" {
		return nil, fmt.Errorf("url is required")
	}
	pl := payload{Action: action, CertificateID: cert.ID, SerialNumber: cert.SerialNumber, CommonName: cert.CommonName, SANs: cert.SANs}
	res, err := send(ctx, url, pl, config, creds)
	if err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}
	return res, nil
}

// send POSTs the payload and maps the endpoint's reply to a Result. A 2xx with
// a non-JSON body counts as success; a 4xx/5xx counts as failure.
func send(ctx context.Context, url string, pl payload, config, creds map[string]any) (*provider.Result, error) {
	body, err := json.Marshal(pl)
	if err != nil {
		return nil, fmt.Errorf("marshal payload: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "freya-deployer-webhook/1.0")
	addHeaders(req, creds, config)
	resp, err := client(config).Do(req)
	if err != nil {
		return nil, fmt.Errorf("webhook request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	ok2xx := resp.StatusCode >= 200 && resp.StatusCode < 300

	var er endpointResponse
	if json.Unmarshal(raw, &er) != nil {
		return &provider.Result{Success: ok2xx, Message: fmt.Sprintf("endpoint returned HTTP %d", resp.StatusCode)}, nil
	}
	success := er.Success || (ok2xx && er.Message == "")
	if !ok2xx {
		success = false
	}
	msg := er.Message
	if msg == "" {
		msg = fmt.Sprintf("endpoint returned HTTP %d", resp.StatusCode)
	}
	details := er.Details
	if er.ResourceID != "" {
		if details == nil {
			details = map[string]any{}
		}
		details["resource_id"] = er.ResourceID
	}
	return &provider.Result{Success: success, Message: msg, Details: details}, nil
}

func client(config map[string]any) *http.Client {
	timeout := 60 * time.Second
	if t, ok := config["timeout_seconds"].(float64); ok && t > 0 {
		timeout = time.Duration(t) * time.Second
	}
	skip := false
	if s, ok := config["skip_tls_verify"].(bool); ok {
		skip = s
	}
	return &http.Client{Timeout: timeout, Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: skip}}} //nolint:gosec // opt-in for private endpoints with self-signed certs
}

// addHeaders applies auth (bearer token / api key / raw authorization) and any
// configured custom headers or signing secret.
func addHeaders(req *http.Request, creds, config map[string]any) {
	if v, ok := creds["authorization"].(string); ok && v != "" {
		req.Header.Set("Authorization", v)
	} else if v, ok := creds["token"].(string); ok && v != "" {
		req.Header.Set("Authorization", "Bearer "+v)
	} else if v, ok := creds["api_key"].(string); ok && v != "" {
		req.Header.Set("X-API-Key", v)
	}
	if v, ok := creds["secret"].(string); ok && v != "" {
		req.Header.Set("X-Webhook-Secret", v)
	}
	if h, ok := config["headers"].(map[string]any); ok {
		for k, v := range h {
			if s, ok := v.(string); ok {
				req.Header.Set(k, s)
			}
		}
	}
}

func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}
