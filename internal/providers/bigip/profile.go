package bigip

// Client-SSL profile (v3 createOrUpdateSSLProfile / updateSSLProfile). With
// ssl_profile set, Deploy POSTs a new profile referencing the deployed
// certificate and key; when the profile already exists (HTTP 409) only its
// cert and key are PATCHed, every other setting is left as it is. Unlike v3,
// the uploaded chain is bound too (v4, user decision 2026-10-03): v3 left
// chain "none", so clients that do not fetch intermediates got no chain.

import (
	"context"
	"net/http"
	"regexp"
	"strings"
)

// sslProfilePattern is the descriptor and runtime pattern of ssl_profile: a
// bare name in the configured partition or a full path /Partition/name. No
// segment may contain a slash or a tilde or start with a dot.
var sslProfilePattern = regexp.MustCompile(`^(/` + namePattern + `/)?[A-Za-z0-9_][A-Za-z0-9_.-]{0,254}$`)

// resolveProfilePath turns an ssl_profile value into a full /Partition/name
// path: a bare name lives in the configured partition (v3), a full path is
// used as is. ok is false for a value outside the pattern (never sent).
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

// createOrUpdateSSLProfile creates the client-SSL profile (v3 body: name,
// cert, key, chain, ciphers "DEFAULT"; BIG-IP fills the rest from its
// clientssl parent) or, on HTTP 409, updates the existing one. chainFull is
// the installed chain object, or "" when there is none: then a new profile
// gets chain "none" (v3) and an update leaves the profile's chain alone.
func (c *client) createOrUpdateSSLProfile(ctx context.Context, profileFull, certFull, keyFull, chainFull string) error {
	chain := chainFull
	if chain == "" {
		chain = "none"
	}
	payload := map[string]any{
		"name":    profileFull,
		"cert":    certFull,
		"key":     keyFull,
		"chain":   chain,
		"ciphers": "DEFAULT",
	}
	r, err := c.do(ctx, http.MethodPost, "/mgmt/tm/ltm/profile/client-ssl", payload)
	if err != nil {
		return err
	}
	if r.ok() {
		return nil
	}
	if r.status == http.StatusConflict {
		return c.updateSSLProfile(ctx, profileFull, certFull, keyFull, chainFull)
	}
	return apiError(r)
}

// updateSSLProfile PATCHes cert and key of an existing profile, plus its
// chain when one was installed; ciphers, parent and every other setting are
// kept.
func (c *client) updateSSLProfile(ctx context.Context, profileFull, certFull, keyFull, chainFull string) error {
	payload := map[string]any{
		"cert": certFull,
		"key":  keyFull,
	}
	if chainFull != "" {
		payload["chain"] = chainFull
	}
	r, err := c.do(ctx, http.MethodPatch, "/mgmt/tm/ltm/profile/client-ssl/"+encodeName(profileFull), payload)
	if err != nil {
		return err
	}
	if r.status != http.StatusOK {
		return apiError(r)
	}
	return nil
}
