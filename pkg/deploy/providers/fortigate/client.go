package fortigate

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
)

// maxBodyBytes caps how much of a FortiOS response we read into memory.
const maxBodyBytes = 4 << 20

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
	return &fgClient{
		http: &http.Client{
			Timeout: 60 * time.Second,
			Transport: &http.Transport{
				// FortiGate management interfaces typically use self-signed certs.
				TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
			},
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

func (r *apiResponse) ok() bool { return r.StatusCode == 200 || r.StatusCode == 201 }

// do performs a request against /api/v2/<apiPath>, always pinning the vdom.
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

	out := &apiResponse{StatusCode: resp.StatusCode, Body: raw}
	_ = json.Unmarshal(raw, &out.Env) // best-effort; not all bodies are the envelope
	return out, nil
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
// including the envelope fields and a truncated raw body for diagnosis.
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
	} else if r.StatusCode == 403 {
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
	if r.StatusCode == 424 || r.Env.Error == -23 {
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
