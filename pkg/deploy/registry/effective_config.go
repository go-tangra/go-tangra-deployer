package registry

import (
	"fmt"
	"sort"
)

// MergeConfig returns the provider config to actually deploy with: the
// configuration's own config, with the target's per-attachment overrides
// applied on top.
//
// This is what lets one shared configuration — one Cloudflare API token, say —
// serve several deployment targets that each point at a different zone_id,
// instead of duplicating the configuration and its credentials per zone.
//
// The merge is shallow and per key: an override replaces a base key outright
// rather than being merged into it. Provider config values are scalars
// (zone_id, region, a port) and a deep merge would make it impossible to
// *replace* a nested object, which is the more useful operation.
//
// Neither input is mutated — the base map belongs to a cached entity and the
// override map to the target row, so writing through either would corrupt
// state shared with other jobs.
//
// Credentials are deliberately not a parameter. They are never overridable:
// sharing them is the point of this mechanism.
func MergeConfig(base, override map[string]any) map[string]any {
	out := make(map[string]any, len(base)+len(override))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
}

// ValidateRequiredConfig checks that every field the provider declares
// required is present and non-empty in cfg.
//
// It is meant to run on the MERGED config: an override can legitimately supply
// a required field the base configuration leaves blank (a shared credential-only
// configuration with the zone_id filled in per target), and validating the base
// alone would reject that valid setup.
func ValidateRequiredConfig(caps *ProviderCapabilities, cfg map[string]any) error {
	if caps == nil {
		return nil
	}
	var missing []string
	for _, field := range caps.RequiredConfigFields {
		v, ok := cfg[field]
		if !ok || isEmptyValue(v) {
			missing = append(missing, field)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	sort.Strings(missing)
	return fmt.Errorf("missing required config field(s): %v", missing)
}

// isEmptyValue reports whether v carries no usable value. A present-but-empty
// string is treated as missing: a config key left blank in the UI arrives as ""
// and is no more usable than an absent one.
func isEmptyValue(v any) bool {
	if v == nil {
		return true
	}
	s, ok := v.(string)
	return ok && s == ""
}
