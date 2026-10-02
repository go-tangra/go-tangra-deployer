package inventoryagent

import (
	"math"
	"regexp"
	"strconv"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// Configuration keys (contracts/deployer-provider.md §2).
const (
	keyHostIDs    = "host_ids"
	keyHostTags   = "host_tags"
	keyCertName   = "cert_name"
	keyKeyPolicy  = "key_policy"
	keyRequireAll = "require_all_success"
	keyWait       = "wait_seconds"
)

// Key policies.
const (
	KeyPolicyRequire         = "require"
	KeyPolicyCertificateOnly = "certificate_only"
)

// Bounds.
const (
	MaxHostIDs         = 1000
	MaxHostTags        = 16
	MaxWaitSeconds     = 240
	DefaultWaitSeconds = 60
)

// tagPattern is the descriptor pattern of a host tag selector (RE2; the
// contract's \u0000-\u001f written as \x00-\x1f so JavaScript and Go agree).
const tagPattern = `^[A-Za-z0-9_.:/-]{1,63}(=[^\x00-\x1f]{0,255})?$`

// namePattern is the descriptor pattern of cert_name ("no .." is checked by
// ValidName).
const namePattern = `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Config is a parsed inventory-agent configuration.
type Config struct {
	HostIDs     []string
	HostTags    []string
	CertName    string // "" = derived from the certificate's common name
	KeyPolicy   string
	RequireAll  bool
	WaitSeconds int
}

// HasSelector reports whether the configuration selects any host.
func (c Config) HasSelector() bool { return len(c.HostIDs) > 0 || len(c.HostTags) > 0 }

// ParseConfig parses and validates a configuration (or a configuration merged
// with a target override). Missing values take their defaults; an empty host
// selection is allowed here (it may be supplied by targets) — callers that
// deploy check HasSelector. Every error is a provider.FieldErrors with
// "config.<key>" paths and codes; values are never echoed.
func ParseConfig(m map[string]any) (Config, error) {
	c := Config{KeyPolicy: KeyPolicyRequire, WaitSeconds: DefaultWaitSeconds}
	errs := provider.FieldErrors{}
	for k, v := range m {
		switch k {
		case keyHostIDs:
			ids, code := stringList(v, MaxHostIDs, func(s string) bool { return uuidPattern.MatchString(s) }, "invalid_uuid")
			if code != "" {
				errs[provider.PathConfig+k] = code
			}
			c.HostIDs = ids
		case keyHostTags:
			tags, code := stringList(v, MaxHostTags, ValidTag, provider.CodePattern)
			if code != "" {
				errs[provider.PathConfig+k] = code
			}
			c.HostTags = tags
		case keyCertName:
			s, ok := v.(string)
			switch {
			case v == nil:
			case !ok:
				errs[provider.PathConfig+k] = provider.CodeWrongType
			case s != "" && !ValidName(s):
				errs[provider.PathConfig+k] = provider.CodePattern
			default:
				c.CertName = s
			}
		case keyKeyPolicy:
			s, ok := v.(string)
			switch {
			case v == nil || s == "" && ok:
			case !ok:
				errs[provider.PathConfig+k] = provider.CodeWrongType
			case s != KeyPolicyRequire && s != KeyPolicyCertificateOnly:
				errs[provider.PathConfig+k] = provider.CodeNotInOptions
			default:
				c.KeyPolicy = s
			}
		case keyRequireAll:
			b, ok := v.(bool)
			if !ok && v != nil {
				errs[provider.PathConfig+k] = provider.CodeWrongType
			}
			c.RequireAll = b
		case keyWait:
			if v == nil || v == "" {
				continue
			}
			n, ok := toInt(v)
			switch {
			case !ok:
				errs[provider.PathConfig+k] = provider.CodeWrongType
			case n < 0 || n > MaxWaitSeconds:
				errs[provider.PathConfig+k] = provider.CodeOutOfRange + ":0.." + strconv.Itoa(MaxWaitSeconds)
			default:
				c.WaitSeconds = int(n)
			}
		default:
			errs[provider.PathConfig+safeKey(k)] = provider.CodeUnknownField
		}
	}
	if len(errs) > 0 {
		return Config{}, errs
	}
	return c, nil
}

// stringList parses a list of unique strings each accepted by valid.
func stringList(v any, max int, valid func(string) bool, badCode string) ([]string, string) {
	var raw []any
	switch x := v.(type) {
	case nil:
		return nil, ""
	case []string:
		for _, s := range x {
			raw = append(raw, s)
		}
	case []any:
		raw = x
	default:
		return nil, provider.CodeWrongType
	}
	if len(raw) > max {
		return nil, provider.CodeTooManyItems + ":" + strconv.Itoa(max)
	}
	seen := make(map[string]bool, len(raw))
	out := make([]string, 0, len(raw))
	for _, it := range raw {
		s, ok := it.(string)
		if !ok {
			return nil, provider.CodeWrongType
		}
		if !valid(s) {
			return nil, badCode
		}
		if seen[s] {
			return nil, "duplicate_item"
		}
		seen[s] = true
		out = append(out, s)
	}
	return out, ""
}

func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		if x != math.Trunc(x) || math.Abs(x) > 1<<31 {
			return 0, false
		}
		return int64(x), true
	}
	return 0, false
}

func safeKey(k string) string {
	if len(k) > 64 {
		k = k[:64]
	}
	b := []byte(k)
	for i, c := range b {
		if !isAlnum(c) && c != '_' && c != '-' {
			b[i] = '_'
		}
	}
	return string(b)
}
