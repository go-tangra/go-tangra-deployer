// Package fortigate is a deployment provider that installs an issued
// certificate + private key onto a FortiGate firewall via the FortiOS REST API.
//
// The import uses the monitor endpoint POST /api/v2/monitor/vpn-certificate/local/import
// (type=regular), which loads a leaf certificate paired with its key as a local
// (server) certificate. Verify reads the object back through the cmdb endpoint
// GET /api/v2/cmdb/certificate/local/<name>; Rollback removes it via
// DELETE on the same cmdb path. Every call is scoped to a VDOM via the mandatory
// ?vdom= query parameter and authenticated with a bearer API token.
//
// This is a focused single-file port of go-tangra's FortiGate provider: it keeps
// the real endpoints, payloads and auth but drops tangra's naming/versioning,
// reference-rebinding and ssl-profile machinery. The certificate object name is
// derived from the certificate subject's common name.
//
// The api_token and private key are never logged or returned in any Result or
// error message.
package fortigate

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

func init() { provider.Register(Provider{}) }

// maxBodyBytes caps how much of a FortiOS response we read into memory.
const maxBodyBytes = 4 << 20

// fortiNameMaxLen is FortiGate's certificate-name length limit.
const fortiNameMaxLen = 35

// Provider installs certificates onto a FortiGate firewall via the FortiOS REST API.
type Provider struct{}

// Capabilities describes the FortiGate provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "fortigate",
		DisplayName:      "FortiGate",
		SupportsVerify:   true,
		SupportsRollback: true,
		ConfigFields: []provider.Field{
			{Key: "vdom", Label: "VDOM", Required: true},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "FortiGate host", Required: true},
			{Key: "api_token", Label: "API token", Secret: true, Required: true},
		},
	}
}

// ValidateCredentials checks the required credentials are present and probes the
// device for connectivity/authorization. An unreachable device is not fatal;
// an explicit 401/403 is.
func (p Provider) ValidateCredentials(ctx context.Context, creds, config map[string]any) error {
	host := credString(creds, "host")
	if host == "" {
		return fmt.Errorf("host is required")
	}
	if credString(creds, "api_token") == "" {
		return fmt.Errorf("api_token is required")
	}
	c := newClient(creds, config)
	r, err := c.do(ctx, http.MethodGet, "cmdb/system/global", nil)
	if err != nil {
		return nil // unreachable at validation time is not fatal
	}
	if r.StatusCode == http.StatusUnauthorized || r.StatusCode == http.StatusForbidden {
		return fmt.Errorf("authentication failed: invalid or unauthorized API token (HTTP %d)", r.StatusCode)
	}
	return nil
}

// Deploy imports the certificate + key onto the FortiGate as a local certificate
// and verifies it is present afterwards. If an object with the same name already
// exists it is deleted first (FortiOS rejects re-importing over an existing name).
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	if err := p.ValidateCredentials(ctx, creds, config); err != nil {
		return nil, err
	}
	if cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}
	c := newClient(creds, config)

	note(progress, 10, "preparing certificate")
	// FortiOS local (server) import expects a single leaf certificate; passing
	// the full chain is rejected with error -145.
	leaf := leafCertPEM(cert.CertificatePEM)
	name := certName(leaf, cert)

	note(progress, 30, "checking for existing certificate")
	exists, err := c.certExists(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("check existing certificate: %w", err)
	}
	if exists {
		note(progress, 45, "removing superseded certificate")
		if err := c.deleteCert(ctx, name); err != nil {
			return nil, fmt.Errorf("replace certificate %s: %w", name, err)
		}
	}

	note(progress, 60, "importing certificate "+name)
	if err := c.importCert(ctx, name, leaf, cert.PrivateKeyPEM, cfgString(config, "import_scope", "global")); err != nil {
		return nil, fmt.Errorf("import certificate: %w", err)
	}

	note(progress, 85, "verifying import")
	ok, err := c.certExists(ctx, name)
	if err != nil || !ok {
		return nil, fmt.Errorf("verification failed: certificate %s not found after import", name)
	}

	note(progress, 100, "deployment complete")
	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("Certificate deployed to FortiGate as %s", name),
		Details: map[string]any{
			"host":             c.host,
			"vdom":             c.vdom,
			"certificate_name": name,
			"was_update":       exists,
		},
	}, nil
}

// Verify reads the certificate object back from the device.
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	if err := p.ValidateCredentials(ctx, creds, config); err != nil {
		return nil, err
	}
	c := newClient(creds, config)
	name := certName(leafCertPEM(cert.CertificatePEM), cert)

	ok, err := c.certExists(ctx, name)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("verify failed: %v", err)}, nil
	}
	if !ok {
		return &provider.Result{Success: false, Message: "certificate not found on FortiGate"}, nil
	}
	return &provider.Result{
		Success: true,
		Message: "Certificate verified on FortiGate",
		Details: map[string]any{"host": c.host, "vdom": c.vdom, "certificate_name": name},
	}, nil
}

// Rollback removes the deployed certificate object. FortiOS refuses to delete a
// still-referenced certificate, so an in-use certificate is left in place and
// reported rather than breaking a live configuration.
func (p Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	if err := p.ValidateCredentials(ctx, creds, config); err != nil {
		return nil, err
	}
	c := newClient(creds, config)
	name := certName(leafCertPEM(cert.CertificatePEM), cert)

	if err := c.deleteCert(ctx, name); err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("rollback failed: %v", err)}, nil
	}
	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("Rollback removed certificate %s from FortiGate", name),
		Details: map[string]any{"host": c.host, "vdom": c.vdom, "certificate_name": name},
	}, nil
}

// ---- FortiOS REST client ----------------------------------------------------

// fgClient is a thin FortiOS REST client scoped to a single host + vdom.
type fgClient struct {
	http  *http.Client
	host  string
	token string
	vdom  string
}

func newClient(creds, config map[string]any) *fgClient {
	host := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(credString(creds, "host"), "https://"), "http://"), "/")
	vdom := cfgString(config, "vdom", "root")
	return &fgClient{
		http: &http.Client{
			Timeout: 60 * time.Second,
			// FortiGate management interfaces typically use self-signed certs.
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // FortiGate mgmt uses self-signed certs
		},
		host:  host,
		token: credString(creds, "api_token"),
		vdom:  vdom,
	}
}

// fortiEnvelope is the standard FortiOS REST response wrapper.
type fortiEnvelope struct {
	Status     string          `json:"status"`
	HTTPStatus int             `json:"http_status"`
	Error      int             `json:"error"`
	CLIError   string          `json:"cli_error"`
	Message    string          `json:"message"`
	Results    json.RawMessage `json:"results"`
}

// apiResponse holds the decoded result of a single API call.
type apiResponse struct {
	StatusCode int
	Body       []byte
	Env        fortiEnvelope
}

func (r *apiResponse) ok() bool {
	return r.StatusCode == http.StatusOK || r.StatusCode == http.StatusCreated
}

// do performs a request against /api/v2/<apiPath>, always pinning the vdom and
// presenting the bearer API token.
func (c *fgClient) do(ctx context.Context, method, apiPath string, body any) (*apiResponse, error) {
	q := url.Values{"vdom": {c.vdom}}
	u := fmt.Sprintf("https://%s/api/v2/%s?%s", c.host, apiPath, q.Encode())

	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fortigate %s %s: %w", method, apiPath, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("read response (HTTP %d): %w", resp.StatusCode, err)
	}
	out := &apiResponse{StatusCode: resp.StatusCode, Body: raw}
	_ = json.Unmarshal(raw, &out.Env) // best-effort; not every body is the envelope
	return out, nil
}

// certExists reports whether a local certificate with the given name exists.
func (c *fgClient) certExists(ctx context.Context, name string) (bool, error) {
	r, err := c.do(ctx, http.MethodGet, "cmdb/certificate/local/"+url.PathEscape(name), nil)
	if err != nil {
		return false, err
	}
	if r.StatusCode == http.StatusNotFound {
		return false, nil
	}
	if !r.ok() {
		return false, apiError("check certificate", r)
	}
	return true, nil
}

// importCert imports a new local certificate via the monitor API. scope is
// "global" or "vdom". The certificate and key are base64-encoded per FortiOS.
func (c *fgClient) importCert(ctx context.Context, name, certPEM, keyPEM, scope string) error {
	payload := map[string]any{
		"type":             "regular",
		"certname":         name,
		"file_content":     base64.StdEncoding.EncodeToString([]byte(certPEM)),
		"key_file_content": base64.StdEncoding.EncodeToString([]byte(keyPEM)),
		"scope":            scope,
	}
	r, err := c.do(ctx, http.MethodPost, "monitor/vpn-certificate/local/import", payload)
	if err != nil {
		return err
	}
	if !r.ok() {
		return apiError("import certificate "+name, r)
	}
	return nil
}

// deleteCert deletes a local certificate. A 404 is treated as success.
func (c *fgClient) deleteCert(ctx context.Context, name string) error {
	r, err := c.do(ctx, http.MethodDelete, "cmdb/certificate/local/"+url.PathEscape(name), nil)
	if err != nil {
		return err
	}
	if r.StatusCode == http.StatusNotFound {
		return nil
	}
	if !r.ok() {
		return apiError("delete certificate "+name, r)
	}
	return nil
}

// apiError renders a FortiOS error response into a client-safe Go error. It
// includes envelope diagnostics and a truncated raw body but never the token.
func apiError(op string, r *apiResponse) error {
	body := strings.TrimSpace(string(r.Body))
	if len(body) > 512 {
		body = body[:512] + "…"
	}
	msg := fmt.Sprintf("fortigate: %s failed: HTTP %d", op, r.StatusCode)
	if r.Env.Status != "" {
		msg += fmt.Sprintf(", status=%s", r.Env.Status)
	}
	if r.Env.Error != 0 {
		msg += fmt.Sprintf(", error=%d", r.Env.Error)
	}
	if r.Env.CLIError != "" {
		msg += fmt.Sprintf(", cli_error=%q", r.Env.CLIError)
	}
	if r.StatusCode == http.StatusFailedDependency || r.Env.Error == -23 {
		msg += " [object is referenced/in-use]"
	}
	if body != "" {
		msg += ": " + body
	}
	return fmt.Errorf("%s", msg)
}

// ---- helpers ----------------------------------------------------------------

func credString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func cfgString(m map[string]any, key, def string) string {
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}

// certName derives the FortiGate object name from the certificate subject's
// common name (falling back to the first DNS SAN, then the supplied metadata,
// then a stable id-based name).
func certName(leafPEM string, cert *provider.CertificateData) string {
	base := cert.CommonName
	if parsed, err := parseLeaf(leafPEM); err == nil && parsed != nil {
		if cn := strings.TrimSpace(parsed.Subject.CommonName); cn != "" {
			base = cn
		} else if len(parsed.DNSNames) > 0 {
			base = parsed.DNSNames[0]
		}
	}
	name := sanitizeName(base)
	if name == "" {
		id := cert.ID
		if len(id) > 8 {
			id = id[:8]
		}
		name = sanitizeName("cert_" + id)
	}
	return name
}

// parseLeaf parses the first CERTIFICATE block of a PEM bundle.
func parseLeaf(pemData string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(leafCertPEM(pemData)))
	if block == nil {
		return nil, fmt.Errorf("no certificate PEM block found")
	}
	return x509.ParseCertificate(block.Bytes)
}

// leafCertPEM returns only the first CERTIFICATE block from a PEM bundle, so the
// leaf is imported without intermediates (which FortiOS rejects with error -145).
func leafCertPEM(pemData string) string {
	rest := []byte(pemData)
	for {
		block, remainder := pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			return string(pem.EncodeToMemory(block))
		}
		rest = remainder
	}
	return pemData
}

// sanitizeName converts a common name into a valid FortiGate resource name:
// wildcard "*" → "star", dots and other invalid characters → "_", collapsed and
// trimmed, leading digit prefixed with "cert_", truncated to 35 characters.
func sanitizeName(name string) string {
	result := strings.Replace(name, "*", "star", 1)
	result = strings.ReplaceAll(result, ".", "_")
	result = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, result)
	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}
	result = strings.Trim(result, "_")
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "cert_" + result
	}
	if len(result) > fortiNameMaxLen {
		result = strings.TrimRight(result[:fortiNameMaxLen], "_")
	}
	return result
}
