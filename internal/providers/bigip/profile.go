package bigip

// Binding into an operator-managed client-SSL profile (feature 033 US8,
// research D26). With ssl_profile set the provider never creates, deletes or
// recreates a profile: it checks that the named profile exists before any
// upload, PATCHes only its cert/key(/chain) after the upload, and refuses a
// rollback while the profile still references the deployed objects.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// sslProfilePattern is the descriptor pattern of ssl_profile: a bare name in
// the configured partition or a full path /Partition/name.
var sslProfilePattern = regexp.MustCompile(`^(/[A-Za-z0-9_.-]{1,64}/)?[A-Za-z0-9_][A-Za-z0-9_.-]{0,254}$`)

// errProfileNotFound marks a client-SSL profile the appliance does not have.
var errProfileNotFound = fmt.Errorf("client-SSL profile not found")

// clientSSLProfile is the subset of an ltm/profile/client-ssl object read here.
type clientSSLProfile struct {
	FullPath string `json:"fullPath"`
	Cert     string `json:"cert"`
	Key      string `json:"key"`
	Chain    string `json:"chain"`
}

// resolveProfilePath turns an ssl_profile value into a full /Partition/name
// path: a bare name lives in the configured partition, a full path is used as
// is. ok is false for a value outside the descriptor pattern (never sent to
// the appliance).
func resolveProfilePath(value, partition string) (string, bool) {
	value = strings.TrimSpace(value)
	if value == "" || !sslProfilePattern.MatchString(value) {
		return "", false
	}
	if strings.HasPrefix(value, "/") {
		return value, true
	}
	return "/" + partition + "/" + value, true
}

// getClientSSLProfile reads a client-SSL profile; errProfileNotFound on 404.
func getClientSSLProfile(ctx context.Context, client *http.Client, host, username, password, profileFull string) (*clientSSLProfile, error) {
	url := fmt.Sprintf("https://%s/mgmt/tm/ltm/profile/client-ssl/%s", host, encodeName(profileFull))
	status, body, err := doJSON(ctx, client, http.MethodGet, url, username, password, nil)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNotFound {
		return nil, errProfileNotFound
	}
	if status != http.StatusOK {
		return nil, apiError(status, body)
	}
	var prof clientSSLProfile
	if err := json.Unmarshal([]byte(body), &prof); err != nil {
		return nil, fmt.Errorf("decode client-SSL profile: %w", err)
	}
	return &prof, nil
}

// patchExistingProfile points an existing profile at the deployed objects.
// Only cert, key and — when a chain object was installed — chain are sent;
// every other profile setting is left untouched. No POST is ever issued.
func patchExistingProfile(ctx context.Context, client *http.Client, host, username, password, profileFull, certFull, keyFull, chainFull string) error {
	patch := map[string]any{"cert": certFull, "key": keyFull}
	if chainFull != "" {
		patch["chain"] = chainFull
	}
	url := fmt.Sprintf("https://%s/mgmt/tm/ltm/profile/client-ssl/%s", host, encodeName(profileFull))
	status, body, err := doJSON(ctx, client, http.MethodPatch, url, username, password, patch)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return apiError(status, body)
	}
	return nil
}

// boundTo reports whether the profile references the deployed cert and key.
func (p *clientSSLProfile) boundTo(certFull, keyFull string) bool {
	return p != nil && p.Cert == certFull && p.Key == keyFull
}

// references reports whether the profile references any of the objects.
func (p *clientSSLProfile) references(objects ...string) bool {
	if p == nil {
		return false
	}
	for _, o := range objects {
		if o != "" && (p.Cert == o || p.Key == o || p.Chain == o) {
			return true
		}
	}
	return false
}

// profileNotFoundResult is the permanent failure for a missing profile: a
// retry cannot create it and the provider never creates one (Q11).
func profileNotFoundResult(host, partition, profileFull string) *provider.Result {
	return &provider.Result{
		Success:   false,
		Permanent: true,
		Message:   fmt.Sprintf("client-SSL profile %s not found", profileFull),
		Details: map[string]any{
			"host":             host,
			"partition":        partition,
			"ssl_profile":      profileFull,
			"ssl_profile_mode": "existing",
		},
	}
}

// invalidProfileResult refuses an ssl_profile outside the descriptor pattern
// (legacy row or bad override); the value is not echoed.
func invalidProfileResult() *provider.Result {
	return &provider.Result{Success: false, Permanent: true, Message: "ssl_profile is not a valid client-SSL profile name or /Partition/name path"}
}
