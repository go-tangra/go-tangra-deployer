package bigip

// iControl REST client: Basic auth on every request, JSON bodies, the
// single-chunk file-transfer upload, sys/crypto install with v3's
// "already exists → re-upload and install with overwrite" retry, object GET
// and DELETE. Appliance responses are scrubbed of the password and the
// private key before they can reach an error.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

const (
	// httpTimeout bounds every call to the appliance (v3: 60 s).
	httpTimeout = 60 * time.Second
	// maxBody caps how much of a response body is read.
	maxBody = 64 << 10
	// maxSnippet caps how much of a response body an error carries.
	maxSnippet = 300
	// downloadsDir is where the file-transfer endpoint lands uploaded files.
	downloadsDir = "/var/config/rest/downloads"
	// redacted replaces secret material echoed by the appliance.
	redacted = "[REDACTED]"
)

// client talks to one appliance.
type client struct {
	host     string
	username string
	password string
	http     *http.Client
	secrets  []string // scrubbed from every response body
}

// response is an appliance answer with its (bounded, scrubbed) body.
type response struct {
	status int
	body   string
}

// snippet is the bounded body an error carries.
func (r *response) snippet() string {
	s := strings.TrimSpace(r.body)
	if len(s) > maxSnippet {
		s = s[:maxSnippet]
	}
	return s
}

func (r *response) ok() bool { return r.status == http.StatusOK || r.status == http.StatusCreated }

// buildError marks a request that could not be built (invalid host).
type buildError struct{ err error }

func (e *buildError) Error() string { return "failed to create request: " + e.err.Error() }
func (e *buildError) Unwrap() error { return e.err }

func isBuildError(err error) bool {
	var be *buildError
	return errors.As(err, &be)
}

func newClient(host, username, password string) *client {
	return &client{host: host, username: username, password: password, http: httpClient(), secrets: []string{password}}
}

// httpClient returns a client that skips TLS verification: BIG-IP management
// interfaces present a self-signed certificate (v3). Redirects are never
// followed (v4).
func httpClient() *http.Client {
	return &http.Client{
		Timeout:       httpTimeout,
		CheckRedirect: provider.NoRedirect,
		Transport:     &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, // #nosec G402 -- v3 behaviour: BIG-IP management uses a self-signed certificate (follow-up: CA pin option)
	}
}

// addSecret registers secret material (the private key) to scrub, in the
// forms an appliance could echo it: as sent and JSON-escaped.
func (c *client) addSecret(pem string) {
	key := strings.TrimSpace(pem)
	if key == "" {
		return
	}
	escaped, _ := json.Marshal(key)
	c.secrets = append(c.secrets, pem, key, strings.Trim(string(escaped), `"`))
}

// scrub replaces every registered secret in a response body; a body still
// mentioning a private key in some other encoding is dropped.
func (c *client) scrub(raw []byte) string {
	for _, s := range c.secrets {
		if s != "" && bytes.Contains(raw, []byte(s)) {
			raw = bytes.ReplaceAll(raw, []byte(s), []byte(redacted))
		}
	}
	if bytes.Contains(raw, []byte("PRIVATE KEY")) {
		return redacted
	}
	return string(raw)
}

// send performs one request with Basic auth and reads the scrubbed answer.
func (c *client) send(ctx context.Context, method, path, contentType string, body io.Reader, header map[string]string) (*response, error) {
	req, err := http.NewRequestWithContext(ctx, method, "https://"+c.host+path, body)
	if err != nil {
		return nil, &buildError{err}
	}
	req.SetBasicAuth(c.username, c.password)
	req.Header.Set("Content-Type", contentType)
	for k, v := range header {
		req.Header.Set(k, v)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, maxBody))
	return &response{status: resp.StatusCode, body: c.scrub(raw)}, nil
}

// do performs a JSON iControl request; a nil payload sends no body.
func (c *client) do(ctx context.Context, method, path string, payload any) (*response, error) {
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, fmt.Errorf("marshal request: %w", err)
		}
		body = bytes.NewReader(raw)
	}
	return c.send(ctx, method, path, "application/json", body, nil)
}

// apiError is v3's "API error (HTTP n): body" with a bounded, scrubbed body.
func apiError(r *response) error {
	return fmt.Errorf("API error (HTTP %d): %s", r.status, r.snippet())
}

// uploadFile uploads a file to /var/config/rest/downloads/<filename> in a
// single chunk (Content-Range 0-<n-1>/<n>).
func (c *client) uploadFile(ctx context.Context, filename, content string) error {
	data := []byte(content)
	r, err := c.send(ctx, http.MethodPost, "/mgmt/shared/file-transfer/uploads/"+filename, "application/octet-stream",
		bytes.NewReader(data), map[string]string{"Content-Range": fmt.Sprintf("0-%d/%d", len(data)-1, len(data))})
	if err != nil {
		return err
	}
	if !r.ok() {
		return fmt.Errorf("file upload error (HTTP %d): %s", r.status, r.snippet())
	}
	return nil
}

// installCrypto uploads a PEM and installs it as sys/crypto/<kind> object
// fullName (v3 uploadCertificate/uploadKey). The upload is named after the
// object (its last path segment). When the object already exists (HTTP 409 or
// "already exists" in the answer) the file is uploaded again and installed
// with overwrite (v3 updateCertificate/updateKey). label names the material
// in an upload error ("certificate" or "key").
func (c *client) installCrypto(ctx context.Context, kind, label, fullName, pem string) error {
	tempFile := lastSegment(fullName)
	if err := c.uploadFile(ctx, tempFile, pem); err != nil {
		return fmt.Errorf("failed to upload %s file: %w", label, err)
	}
	payload := map[string]any{
		"command":         "install",
		"name":            fullName,
		"from-local-file": downloadsDir + "/" + tempFile,
	}
	r, err := c.do(ctx, http.MethodPost, "/mgmt/tm/sys/crypto/"+kind, payload)
	if err != nil {
		return err
	}
	if r.ok() {
		return nil
	}
	if r.status != http.StatusConflict && !strings.Contains(r.body, "already exists") {
		return apiError(r)
	}
	// Already exists: upload again and install with overwrite.
	if err := c.uploadFile(ctx, tempFile, pem); err != nil {
		return fmt.Errorf("failed to upload %s file: %w", label, err)
	}
	payload["overwrite"] = true
	if r, err = c.do(ctx, http.MethodPost, "/mgmt/tm/sys/crypto/"+kind, payload); err != nil {
		return err
	}
	if !r.ok() {
		return apiError(r)
	}
	return nil
}

// verifyCertExists GETs the sys/crypto/cert object.
func (c *client) verifyCertExists(ctx context.Context, certFull string) error {
	r, err := c.do(ctx, http.MethodGet, "/mgmt/tm/sys/crypto/cert/"+encodeName(certFull), nil)
	if err != nil {
		return err
	}
	if r.status == http.StatusNotFound {
		return fmt.Errorf("certificate not found")
	}
	if r.status != http.StatusOK {
		return apiError(r)
	}
	return nil
}

// deleteResource DELETEs /mgmt/tm/<resourceType>/<name>; a 404 counts as
// already removed.
func (c *client) deleteResource(ctx context.Context, resourceType, fullName string) error {
	r, err := c.do(ctx, http.MethodDelete, "/mgmt/tm/"+resourceType+"/"+encodeName(fullName), nil)
	if err != nil {
		return err
	}
	if r.status == http.StatusNotFound || r.status == http.StatusOK {
		return nil
	}
	return apiError(r)
}

// encodeName encodes a full object path (/partition/name) for an iControl URL
// path segment: slashes become tildes.
func encodeName(fullName string) string {
	return strings.ReplaceAll(fullName, "/", "~")
}

func lastSegment(fullName string) string {
	parts := strings.Split(fullName, "/")
	return parts[len(parts)-1]
}
