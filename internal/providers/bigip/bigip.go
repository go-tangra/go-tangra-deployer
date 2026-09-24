// Package bigip is a deployment provider for F5 BIG-IP load balancers. It
// installs an issued certificate and its private key onto an appliance through
// the iControl REST API and binds them into a client-SSL profile inside a
// partition. Material is uploaded to the appliance's file-transfer endpoint,
// installed as sys/crypto cert+key objects, then referenced from an
// ltm/profile/client-ssl profile. The appliance authenticates the caller with
// HTTP Basic auth (username/password) and normally presents a self-signed
// management certificate, so the client is configured to skip TLS verification.
//
// Security: the password and private-key PEM are never logged and never placed
// into a Result.Message or an error returned to the caller.
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

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func init() { provider.Register(Provider{}) }

const (
	// httpTimeout bounds every call to the appliance.
	httpTimeout = 60 * time.Second
	// maxErrBody caps how much of a response body is read into an error.
	maxErrBody = 2 << 10
	// downloadsDir is where the file-transfer endpoint lands uploaded files.
	downloadsDir = "/var/config/rest/downloads"
)

// Provider installs certificates onto an F5 BIG-IP via iControl REST.
type Provider struct{}

// Capabilities describes the BIG-IP provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "bigip",
		DisplayName:      "F5 BIG-IP",
		SupportsVerify:   true,
		SupportsRollback: true,
		ConfigFields: []provider.Field{
			{Key: "partition", Label: "Partition", Required: true},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "Host", Required: true},
			{Key: "username", Label: "Username", Required: true},
			{Key: "password", Label: "Password", Secret: true, Required: true},
		},
	}
}

// ValidateCredentials checks that host, username and password are present and,
// best-effort, probes the appliance. An unreachable appliance is not fatal at
// validation time; rejected credentials are.
func (p Provider) ValidateCredentials(ctx context.Context, creds, config map[string]any) error {
	host, username, password, err := requireCreds(creds)
	if err != nil {
		return err
	}
	req, err := newJSONRequest(ctx, http.MethodGet, fmt.Sprintf("https://%s/mgmt/tm/sys/version", host), username, password, nil)
	if err != nil {
		return fmt.Errorf("invalid host: %w", err)
	}
	resp, err := httpClient().Do(req)
	if err != nil {
		return nil // unreachable at validation time is not fatal
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("%w: BIG-IP rejected the username/password (HTTP %d)", provider.ErrCredentials, resp.StatusCode)
	}
	return nil
}

// Deploy uploads the certificate and key, installs them as sys/crypto objects,
// and binds them into a client-SSL profile in the configured partition.
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	host, username, password, err := requireCreds(creds)
	if err != nil {
		return nil, err
	}
	partition := partitionFrom(config)
	if partition == "" {
		return nil, fmt.Errorf("partition is required")
	}
	if cert == nil || cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}

	note(progress, 5, "preparing certificate objects")
	base := certBaseName(cert)
	certFull := fmt.Sprintf("/%s/%s.crt", partition, base)
	keyFull := fmt.Sprintf("/%s/%s.key", partition, base)
	profileFull := fmt.Sprintf("/%s/%s_clientssl", partition, base)

	client := httpClient()

	note(progress, 20, "uploading certificate")
	if err := installCrypto(ctx, client, host, username, password, "cert", certFull, cert.CertificatePEM); err != nil {
		return &provider.Result{Success: false, Message: "certificate upload failed: " + err.Error()}, nil
	}

	note(progress, 45, "uploading private key")
	if err := installCrypto(ctx, client, host, username, password, "key", keyFull, cert.PrivateKeyPEM); err != nil {
		return &provider.Result{Success: false, Message: "private key upload failed: " + err.Error()}, nil
	}

	chainFull := ""
	if cert.CertificateChain != "" {
		note(progress, 60, "uploading certificate chain")
		chainFull = fmt.Sprintf("/%s/%s_chain.crt", partition, base)
		if err := installCrypto(ctx, client, host, username, password, "cert", chainFull, cert.CertificateChain); err != nil {
			// Non-fatal: the profile can still bind cert+key without the chain.
			note(progress, 62, "warning: certificate chain upload failed")
			chainFull = ""
		}
	}

	note(progress, 75, "binding client-SSL profile")
	if err := bindClientSSLProfile(ctx, client, host, username, password, profileFull, certFull, keyFull, chainFull); err != nil {
		return &provider.Result{Success: false, Message: "client-SSL profile binding failed: " + err.Error()}, nil
	}

	note(progress, 90, "verifying installation")
	if err := certExists(ctx, client, host, username, password, certFull); err != nil {
		return &provider.Result{Success: false, Message: "post-deploy verification failed: " + err.Error()}, nil
	}

	note(progress, 100, "deployment complete")
	details := map[string]any{
		"host":             host,
		"partition":        partition,
		"certificate_name": certFull,
		"key_name":         keyFull,
		"ssl_profile":      profileFull,
	}
	if chainFull != "" {
		details["chain_name"] = chainFull
	}
	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("certificate installed and bound to client-SSL profile %s", profileFull),
		Details: details,
	}, nil
}

// Verify confirms the certificate object exists in the partition.
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	host, username, password, err := requireCreds(creds)
	if err != nil {
		return nil, err
	}
	partition := partitionFrom(config)
	if partition == "" {
		return nil, fmt.Errorf("partition is required")
	}
	certFull := fmt.Sprintf("/%s/%s.crt", partition, certBaseName(cert))
	if err := certExists(ctx, httpClient(), host, username, password, certFull); err != nil {
		return &provider.Result{Success: false, Message: "certificate not found: " + err.Error()}, nil
	}
	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("certificate %s present on BIG-IP", certFull),
		Details: map[string]any{"host": host, "partition": partition, "certificate_name": certFull},
	}, nil
}

// Rollback removes the client-SSL profile, certificate, key and chain objects.
func (p Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	host, username, password, err := requireCreds(creds)
	if err != nil {
		return nil, err
	}
	partition := partitionFrom(config)
	if partition == "" {
		return nil, fmt.Errorf("partition is required")
	}
	base := certBaseName(cert)
	client := httpClient()

	// Profile references the cert+key, so it must go first.
	_ = deleteObject(ctx, client, host, username, password, "ltm/profile/client-ssl", fmt.Sprintf("/%s/%s_clientssl", partition, base))

	var failures []string
	if err := deleteObject(ctx, client, host, username, password, "sys/crypto/cert", fmt.Sprintf("/%s/%s.crt", partition, base)); err != nil {
		failures = append(failures, "certificate: "+err.Error())
	}
	if err := deleteObject(ctx, client, host, username, password, "sys/crypto/key", fmt.Sprintf("/%s/%s.key", partition, base)); err != nil {
		failures = append(failures, "key: "+err.Error())
	}
	// Chain is optional; a 404 is treated as success by deleteObject.
	_ = deleteObject(ctx, client, host, username, password, "sys/crypto/cert", fmt.Sprintf("/%s/%s_chain.crt", partition, base))

	if len(failures) > 0 {
		return &provider.Result{Success: false, Message: "rollback partially failed: " + strings.Join(failures, "; ")}, nil
	}
	return &provider.Result{
		Success: true,
		Message: "certificate, key and client-SSL profile removed from BIG-IP",
		Details: map[string]any{"host": host, "partition": partition},
	}, nil
}

// installCrypto uploads a PEM to the appliance's file-transfer endpoint and then
// installs it as a sys/crypto object of the given kind ("cert" or "key"). If the
// object already exists it is reinstalled with overwrite set.
func installCrypto(ctx context.Context, client *http.Client, host, username, password, kind, fullName, pem string) error {
	tempFile := lastSegment(fullName)
	if err := uploadFile(ctx, client, host, username, password, tempFile, pem); err != nil {
		return err
	}
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/%s", host, kind)
	payload := map[string]any{
		"command":         "install",
		"name":            fullName,
		"from-local-file": downloadsDir + "/" + tempFile,
	}
	status, body, err := doJSON(ctx, client, http.MethodPost, url, username, password, payload)
	if err != nil {
		return err
	}
	if status == http.StatusOK || status == http.StatusCreated {
		return nil
	}
	if status == http.StatusConflict || strings.Contains(strings.ToLower(body), "already exists") {
		payload["overwrite"] = true
		status, body, err = doJSON(ctx, client, http.MethodPost, url, username, password, payload)
		if err != nil {
			return err
		}
		if status == http.StatusOK || status == http.StatusCreated {
			return nil
		}
	}
	return apiError(status, body)
}

// uploadFile POSTs a file to /mgmt/shared/file-transfer/uploads using the
// Content-Range header the appliance expects for a single-chunk upload.
func uploadFile(ctx context.Context, client *http.Client, host, username, password, filename, content string) error {
	url := fmt.Sprintf("https://%s/mgmt/shared/file-transfer/uploads/%s", host, filename)
	data := []byte(content)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("Content-Range", fmt.Sprintf("0-%d/%d", len(data)-1, len(data)))
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("upload request failed: %w", transportError(err))
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		return apiError(resp.StatusCode, string(body))
	}
	return nil
}

// bindClientSSLProfile creates the client-SSL profile referencing the cert+key,
// or PATCHes an existing profile to point at them.
func bindClientSSLProfile(ctx context.Context, client *http.Client, host, username, password, profileFull, certFull, keyFull, chainFull string) error {
	chain := "none"
	if chainFull != "" {
		chain = chainFull
	}
	create := map[string]any{
		"name":    profileFull,
		"cert":    certFull,
		"key":     keyFull,
		"chain":   chain,
		"ciphers": "DEFAULT",
	}
	url := fmt.Sprintf("https://%s/mgmt/tm/ltm/profile/client-ssl", host)
	status, body, err := doJSON(ctx, client, http.MethodPost, url, username, password, create)
	if err != nil {
		return err
	}
	if status == http.StatusOK || status == http.StatusCreated {
		return nil
	}
	if status == http.StatusConflict || strings.Contains(strings.ToLower(body), "already exists") {
		patchURL := fmt.Sprintf("https://%s/mgmt/tm/ltm/profile/client-ssl/%s", host, encodeName(profileFull))
		patch := map[string]any{"cert": certFull, "key": keyFull, "chain": chain}
		status, body, err = doJSON(ctx, client, http.MethodPatch, patchURL, username, password, patch)
		if err != nil {
			return err
		}
		if status == http.StatusOK {
			return nil
		}
	}
	return apiError(status, body)
}

// certExists issues a GET for the crypto/cert object; 404 means absent.
func certExists(ctx context.Context, client *http.Client, host, username, password, certFull string) error {
	url := fmt.Sprintf("https://%s/mgmt/tm/sys/crypto/cert/%s", host, encodeName(certFull))
	status, body, err := doJSON(ctx, client, http.MethodGet, url, username, password, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK {
		return nil
	}
	if status == http.StatusNotFound {
		return fmt.Errorf("certificate object not present")
	}
	return apiError(status, body)
}

// deleteObject issues a DELETE for a tm object; a 404 counts as already-removed.
func deleteObject(ctx context.Context, client *http.Client, host, username, password, objType, fullName string) error {
	url := fmt.Sprintf("https://%s/mgmt/tm/%s/%s", host, objType, encodeName(fullName))
	status, body, err := doJSON(ctx, client, http.MethodDelete, url, username, password, nil)
	if err != nil {
		return err
	}
	if status == http.StatusOK || status == http.StatusNotFound {
		return nil
	}
	return apiError(status, body)
}

// doJSON performs a JSON iControl request and returns status, body and any
// transport error. A nil payload sends no body.
func doJSON(ctx context.Context, client *http.Client, method, url, username, password string, payload any) (int, string, error) {
	var bodyReader io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return 0, "", fmt.Errorf("marshal request: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}
	req, err := newJSONRequest(ctx, method, url, username, password, bodyReader)
	if err != nil {
		return 0, "", fmt.Errorf("build request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("request failed: %w", transportError(err))
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrBody))
	return resp.StatusCode, string(raw), nil
}

func newJSONRequest(ctx context.Context, method, url, username, password string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(username, password)
	req.Header.Set("Content-Type", "application/json")
	return req, nil
}

// httpClient returns a client that skips TLS verification: BIG-IP management
// interfaces present a self-signed certificate.
func httpClient() *http.Client {
	return &http.Client{
		Timeout:   httpTimeout,
		Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, //nolint:gosec // BIG-IP management uses a self-signed cert
	}
}

// requireCreds extracts and validates the three required credentials. The
// returned host has any scheme/trailing slash trimmed.
func requireCreds(creds map[string]any) (host, username, password string, err error) {
	host = hostString(creds)
	if host == "" {
		return "", "", "", fmt.Errorf("host is required")
	}
	username, _ = creds["username"].(string)
	if username == "" {
		return "", "", "", fmt.Errorf("username is required")
	}
	password, _ = creds["password"].(string)
	if password == "" {
		return "", "", "", fmt.Errorf("password is required")
	}
	return host, username, password, nil
}

func hostString(creds map[string]any) string {
	h, _ := creds["host"].(string)
	h = strings.TrimSpace(h)
	h = strings.TrimPrefix(h, "https://")
	h = strings.TrimPrefix(h, "http://")
	return strings.TrimRight(h, "/")
}

func partitionFrom(config map[string]any) string {
	if p, ok := config["partition"].(string); ok {
		return strings.Trim(strings.TrimSpace(p), "/")
	}
	return ""
}

// certBaseName derives a BIG-IP-safe object base name from the certificate.
func certBaseName(cert *provider.CertificateData) string {
	if cert == nil {
		return "freya_cert"
	}
	name := sanitizeName(cert.CommonName)
	if name == "" {
		id := cert.ID
		if len(id) > 8 {
			id = id[:8]
		}
		if id == "" {
			id = sanitizeName(cert.SerialNumber)
		}
		if id == "" {
			return "freya_cert"
		}
		name = "cert_" + id
	}
	return name
}

// encodeName encodes a full object path (/partition/name) for use in an
// iControl URL path segment: slashes become tildes.
func encodeName(fullName string) string {
	return strings.ReplaceAll(fullName, "/", "~")
}

func lastSegment(fullName string) string {
	parts := strings.Split(fullName, "/")
	return parts[len(parts)-1]
}

// apiError builds a client-safe error from an appliance response. The status
// code and a bounded snippet of the body are included; credentials are never
// part of a response body, so nothing secret is echoed.
func apiError(status int, body string) error {
	snippet := strings.TrimSpace(body)
	if len(snippet) > 300 {
		snippet = snippet[:300]
	}
	if snippet == "" {
		return fmt.Errorf("BIG-IP API error (HTTP %d)", status)
	}
	return fmt.Errorf("BIG-IP API error (HTTP %d): %s", status, snippet)
}

// transportError returns a client-safe transport error. The URL (which never
// contains credentials, since Basic auth is set via a header) is preserved by
// the stdlib error; this keeps the wording stable and secret-free.
func transportError(err error) error {
	return err
}

// sanitizeName converts a common name to a valid BIG-IP object name.
func sanitizeName(name string) string {
	result := strings.Replace(name, "*", "star", 1)
	result = strings.ReplaceAll(result, ".", "_")
	result = strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_', r == '-':
			return r
		default:
			return '_'
		}
	}, result)
	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}
	result = strings.Trim(result, "_")
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "cert_" + result
	}
	return result
}

// note guards an optional progress callback.
func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}
