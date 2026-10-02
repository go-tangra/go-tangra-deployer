package fortigate

// FortiOS REST client, ported from the v3 provider's client.go. v4 additions:
// credential-bearing requests never follow redirects (provider.NoRedirect),
// the host accepts an optional https:// prefix and trailing slash, and the API
// token is scrubbed from any response body before it can reach an error.

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// maxBodyBytes caps how much of a FortiOS response we read into memory.
const maxBodyBytes = 4 << 20

// redacted replaces secret material echoed by a device.
const redacted = "[REDACTED]"

// fgClient is a thin FortiOS REST API client scoped to a single host + vdom.
type fgClient struct {
	http  *http.Client
	host  string
	token string
	vdom  string
}

func newClient(host, token, vdom string) *fgClient {
	if vdom == "" {
		vdom = "root"
	}
	host = strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(host, "https://"), "http://"), "/")
	return &fgClient{
		http: &http.Client{
			Timeout:       60 * time.Second,
			CheckRedirect: provider.NoRedirect,
			// FortiGate management interfaces typically use self-signed certs.
			Transport: &http.Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}, // #nosec G402 -- pre-033 behaviour: FortiGate management interfaces use self-signed certificates (follow-up: CA pin option)
		},
		host:  host,
		token: token,
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
			return nil, fmt.Errorf("fortigate: marshal request body: %w", err)
		}
		rdr = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, u, rdr)
	if err != nil {
		return nil, fmt.Errorf("fortigate: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fortigate: %s %s: %w", method, apiPath, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxBodyBytes))
	if err != nil {
		return nil, fmt.Errorf("fortigate: read response (HTTP %d): %w", resp.StatusCode, err)
	}
	raw = scrub(raw, c.token)

	out := &apiResponse{StatusCode: resp.StatusCode, Body: raw}
	_ = json.Unmarshal(raw, &out.Env) // best-effort; not all bodies are the envelope
	return out, nil
}

// scrub replaces every occurrence of a secret in a device response.
func scrub(raw []byte, secrets ...string) []byte {
	for _, s := range secrets {
		if s != "" && bytes.Contains(raw, []byte(s)) {
			raw = bytes.ReplaceAll(raw, []byte(s), []byte(redacted))
		}
	}
	return raw
}

// cmdb / monitor convenience wrappers. mkey segments must already be escaped by
// the caller via escapeMkey.
func (c *fgClient) cmdbGet(ctx context.Context, path string) (*apiResponse, error) {
	return c.do(ctx, http.MethodGet, "cmdb/"+path, nil)
}

func (c *fgClient) cmdbPut(ctx context.Context, path string, body any) (*apiResponse, error) {
	return c.do(ctx, http.MethodPut, "cmdb/"+path, body)
}

func (c *fgClient) cmdbPost(ctx context.Context, path string, body any) (*apiResponse, error) {
	return c.do(ctx, http.MethodPost, "cmdb/"+path, body)
}

func (c *fgClient) cmdbDelete(ctx context.Context, path string) (*apiResponse, error) {
	return c.do(ctx, http.MethodDelete, "cmdb/"+path, nil)
}

func (c *fgClient) monitorPost(ctx context.Context, path string, body any) (*apiResponse, error) {
	return c.do(ctx, http.MethodPost, "monitor/"+path, body)
}

// escapeMkey escapes an object key for use as a path segment (FortiGate object
// names may contain spaces, e.g. "Jobs-Tech SSL Inspection").
func escapeMkey(mkey string) string { return url.PathEscape(mkey) }

// apiError renders a FortiOS error response into an actionable Go error,
// including the envelope fields and a truncated raw body for diagnosis. The
// body was scrubbed of the API token in do.
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
	if r.Env.Message != "" {
		msg += fmt.Sprintf(", message=%q", r.Env.Message)
	}
	if isReferenced(r) {
		msg += " [object is referenced/in-use]"
	} else if r.StatusCode == http.StatusForbidden {
		msg += " [forbidden — verify the API token has write permission and source-IP trust]"
	}
	if body != "" {
		msg += ": " + body
	}
	return fmt.Errorf("%s", msg)
}

// isReferenced reports whether a failed response indicates the object could not
// be modified/deleted because another object references it ("in use").
//
// FortiOS signals this inconsistently across versions: HTTP 424 (Failed
// Dependency), the legacy CLI error code -23 (datasource conflict), or a
// message mentioning the object is "used". A bare 403 is intentionally NOT
// treated as in-use because it is also returned for read-only tokens.
func isReferenced(r *apiResponse) bool {
	if r.StatusCode == http.StatusFailedDependency || r.Env.Error == -23 {
		return true
	}
	hay := strings.ToLower(string(r.Body) + " " + r.Env.CLIError + " " + r.Env.Message)
	for _, s := range []string{"in use", "being used", "is used", "used by", "currently being used", "datasource conflict"} {
		if strings.Contains(hay, s) {
			return true
		}
	}
	return false
}
