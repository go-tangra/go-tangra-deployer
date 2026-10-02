package provider

import (
	"net/url"
	"regexp"
	"sort"
	"strings"
)

// RedirectKeys are configuration keys that once changed where a provider sent
// its sealed credentials (aws_acm "endpoint", cloudflare "api_base"). They were
// test hooks read from the stored configuration, so anyone able to edit a
// configuration or a target override could point someone else's sealed
// credentials at an arbitrary host. Providers no longer read them (tests use
// unexported options); they are refused on save and ignored at deploy time.
var RedirectKeys = []string{"endpoint", "api_base"}

// FieldError is a save-time refusal naming the offending field
// ("config.<key>" or "config.headers.<name>"). Msg is a fixed, client-safe
// text; it never contains the submitted value.
type FieldError struct{ Field, Msg string }

func (e *FieldError) Error() string { return e.Field + ": " + e.Msg }

// ConfigValidator is implemented by providers that check their configuration
// at save time (create/update, target override, validate).
type ConfigValidator interface {
	ValidateConfig(config map[string]any) error
}

// authHeaderNames are refused in webhook custom headers: they carry
// credentials, and config is stored unsealed and readable by every
// configuration reader. The sealed webhook credentials (token, authorization,
// api_key, secret) are the place for them.
var authHeaderNames = map[string]struct{}{
	"authorization":       {},
	"proxy-authorization": {},
	"cookie":              {},
	"x-api-key":           {},
}

var authHeaderPattern = regexp.MustCompile(`(?i)token|secret|key|auth|cookie|password|credential|jwt|session|bearer|signature`)

// IsAuthHeader reports whether a custom header name looks like it carries a
// credential.
func IsAuthHeader(name string) bool {
	n := strings.ToLower(strings.TrimSpace(name))
	if _, ok := authHeaderNames[n]; ok {
		return true
	}
	return authHeaderPattern.MatchString(n)
}

// CheckConfig is the provider-independent save-time policy for a provider
// configuration or a target override: no redirect keys and no
// credential-carrying custom headers. The first violation (in a stable order)
// is returned.
func CheckConfig(config map[string]any) *FieldError {
	for _, k := range RedirectKeys {
		if _, ok := config[k]; ok {
			return &FieldError{Field: "config." + k, Msg: "not allowed: this setting cannot be stored in a configuration"}
		}
	}
	if names := AuthHeaders(config); len(names) > 0 {
		return &FieldError{Field: "config.headers." + clip(names[0]), Msg: "credential headers are not allowed in headers; use the sealed webhook credentials (token, authorization, api_key, secret)"}
	}
	return nil
}

// ValidateConfig runs CheckConfig and, when the provider implements
// ConfigValidator, its own checks.
func ValidateConfig(providerType string, config map[string]any) error {
	if fe := CheckConfig(config); fe != nil {
		return fe
	}
	p, err := Get(providerType)
	if err != nil {
		return nil // unknown types are refused by the caller
	}
	if v, ok := p.(ConfigValidator); ok {
		return v.ValidateConfig(config)
	}
	return nil
}

// IgnoredKeys returns the redirect keys present in config (sorted). They are
// ignored at deploy time; callers log/report the names, never the values.
func IgnoredKeys(config map[string]any) []string {
	var out []string
	for _, k := range RedirectKeys {
		if _, ok := config[k]; ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// AuthHeaders returns the credential-like custom header names in
// config["headers"] (sorted). Names only, never values.
func AuthHeaders(config map[string]any) []string {
	h, ok := config["headers"].(map[string]any)
	if !ok {
		return nil
	}
	var out []string
	for name := range h {
		if IsAuthHeader(name) {
			out = append(out, name)
		}
	}
	sort.Strings(out)
	return out
}

// StripRedirectKeys returns a copy of config without the redirect keys.
// Defense in depth for the deploy path: no provider reads them any more.
func StripRedirectKeys(config map[string]any) map[string]any {
	out := make(map[string]any, len(config))
	for k, v := range config {
		out[k] = v
	}
	for _, k := range RedirectKeys {
		delete(out, k)
	}
	return out
}

func clip(s string) string {
	if len(s) > 64 {
		return s[:64]
	}
	return s
}

// DestinationKeys are configuration keys that name where a provider sends its
// requests — and with them the sealed credentials and the private key (webhook
// url / verify_url / rollback_url). Changing them on a configuration that holds
// sealed credentials requires re-entering the credentials; a target override
// may not set them for such a configuration.
var DestinationKeys = []string{"url", "verify_url", "rollback_url"}

// DestinationCredentialKeys are non-secret credential fields that name the
// endpoint the secret credentials are sent to (BIG-IP and FortiGate host).
var DestinationCredentialKeys = []string{"host"}

// CredentialDestinationChange reports the first destination credential key
// whose value (trimmed, case-insensitive) differs between the stored and the
// next credentials.
func CredentialDestinationChange(stored, next map[string]any) (string, bool) {
	for _, k := range DestinationCredentialKeys {
		a, _ := stored[k].(string)
		b, _ := next[k].(string)
		if !strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b)) {
			return k, true
		}
	}
	return "", false
}

// OverrideDestinationKeys returns the destination keys present in an override
// (sorted). Names only.
func OverrideDestinationKeys(ov map[string]any) []string {
	var out []string
	for _, k := range DestinationKeys {
		if _, ok := ov[k]; ok {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// CheckDestinationURL validates one destination URL: absolute http(s) with a
// host and no embedded user info. http stays allowed (existing private
// endpoints use it). The value is never echoed.
func CheckDestinationURL(key string, v any) *FieldError {
	s, ok := v.(string)
	if !ok {
		return &FieldError{Field: "config." + key, Msg: "must be a URL string"}
	}
	if s == "" && key != "url" {
		return nil // optional; falls back to url
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Hostname() == "" {
		return &FieldError{Field: "config." + key, Msg: "must be an absolute http:// or https:// URL"}
	}
	if u.User != nil {
		return &FieldError{Field: "config." + key, Msg: "must not contain user info; use the sealed credentials"}
	}
	return nil
}

// DestinationChange reports the first destination key whose effective origin
// (scheme, host, port) differs between the stored and the new configuration.
// verify_url and rollback_url fall back to url, as the webhook provider does.
func DestinationChange(stored, next map[string]any) (string, bool) {
	for _, k := range DestinationKeys {
		if origin(effectiveDest(stored, k)) != origin(effectiveDest(next, k)) {
			return k, true
		}
	}
	return "", false
}

func effectiveDest(m map[string]any, k string) string {
	if s, _ := m[k].(string); s != "" {
		return s
	}
	if k != "url" {
		s, _ := m["url"].(string)
		return s
	}
	return ""
}

// origin normalises a URL to scheme://host:port; unparsable values compare by
// their raw text so any change still counts as a change.
func origin(raw string) string {
	if raw == "" {
		return ""
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "raw:" + raw
	}
	scheme := strings.ToLower(u.Scheme)
	port := u.Port()
	if port == "" {
		switch scheme {
		case "http":
			port = "80"
		case "https":
			port = "443"
		}
	}
	return scheme + "://" + strings.ToLower(u.Hostname()) + ":" + port + "|" + u.User.String()
}
