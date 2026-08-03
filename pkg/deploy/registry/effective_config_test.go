package registry

import (
	"strings"
	"testing"
)

// The use case this exists for: one shared Cloudflare configuration (the API
// token) reused by targets that each deploy to a different zone.
func TestMergeConfigOverridesZoneID(t *testing.T) {
	base := map[string]any{"zone_id": "shared-zone", "ttl": 3600}
	got := MergeConfig(base, map[string]any{"zone_id": "target-zone"})

	if got["zone_id"] != "target-zone" {
		t.Errorf("zone_id = %v, want \"target-zone\"", got["zone_id"])
	}
	if got["ttl"] != 3600 {
		t.Errorf("ttl = %v, want the base value 3600 to survive", got["ttl"])
	}
}

// A configuration may hold only credentials, with every target supplying its
// own zone. The override has to be able to introduce a key the base lacks.
func TestMergeConfigAddsNewKeys(t *testing.T) {
	got := MergeConfig(map[string]any{}, map[string]any{"zone_id": "z1"})
	if got["zone_id"] != "z1" {
		t.Errorf("zone_id = %v, want \"z1\"", got["zone_id"])
	}
}

func TestMergeConfigNilInputs(t *testing.T) {
	tests := []struct {
		name     string
		base     map[string]any
		override map[string]any
		wantLen  int
	}{
		{"both nil", nil, nil, 0},
		{"nil override keeps base", map[string]any{"a": 1}, nil, 1},
		{"nil base takes override", nil, map[string]any{"a": 1}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := MergeConfig(tt.base, tt.override)
			if got == nil {
				t.Fatal("MergeConfig returned nil; callers pass the result straight to a provider")
			}
			if len(got) != tt.wantLen {
				t.Errorf("len = %d, want %d (%v)", len(got), tt.wantLen, got)
			}
		})
	}
}

// The base map belongs to a cached configuration entity shared across jobs, and
// the override map to the target row. Mutating either would leak one target's
// zone into another's deployment.
func TestMergeConfigDoesNotMutateInputs(t *testing.T) {
	base := map[string]any{"zone_id": "base-zone"}
	override := map[string]any{"zone_id": "override-zone"}

	got := MergeConfig(base, override)
	got["zone_id"] = "mutated"
	got["extra"] = "added"

	if base["zone_id"] != "base-zone" {
		t.Errorf("base was mutated: zone_id = %v", base["zone_id"])
	}
	if override["zone_id"] != "override-zone" {
		t.Errorf("override was mutated: zone_id = %v", override["zone_id"])
	}
	if _, leaked := base["extra"]; leaked {
		t.Error("a key added to the result leaked into base")
	}
}

// An override replaces a key outright rather than merging into it, so a nested
// object can actually be replaced.
func TestMergeConfigReplacesNestedValues(t *testing.T) {
	base := map[string]any{"opts": map[string]any{"a": 1, "b": 2}}
	got := MergeConfig(base, map[string]any{"opts": map[string]any{"a": 9}})

	opts, ok := got["opts"].(map[string]any)
	if !ok {
		t.Fatalf("opts = %T, want map", got["opts"])
	}
	if opts["a"] != 9 {
		t.Errorf("opts[a] = %v, want 9", opts["a"])
	}
	if _, survived := opts["b"]; survived {
		t.Error("opts[b] survived; the override should replace the value, not deep-merge it")
	}
}

func TestValidateRequiredConfig(t *testing.T) {
	caps := &ProviderCapabilities{RequiredConfigFields: []string{"zone_id"}}

	tests := []struct {
		name    string
		cfg     map[string]any
		wantErr bool
	}{
		{"present", map[string]any{"zone_id": "z1"}, false},
		{"absent", map[string]any{}, true},
		{"present but empty", map[string]any{"zone_id": ""}, true},
		{"present but nil", map[string]any{"zone_id": nil}, true},
		{"non-string value counts as present", map[string]any{"zone_id": 42}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateRequiredConfig(caps, tt.cfg)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRequiredConfig() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// The point of validating the merged config: a credential-only configuration
// with the zone supplied per target is a valid setup and must not be rejected.
func TestValidateRequiredConfigAcceptsOverrideSuppliedField(t *testing.T) {
	caps := &ProviderCapabilities{RequiredConfigFields: []string{"zone_id"}}
	base := map[string]any{}

	if err := ValidateRequiredConfig(caps, base); err == nil {
		t.Fatal("expected the base-only config to be rejected, got nil")
	}
	merged := MergeConfig(base, map[string]any{"zone_id": "z1"})
	if err := ValidateRequiredConfig(caps, merged); err != nil {
		t.Errorf("merged config rejected: %v", err)
	}
}

func TestValidateRequiredConfigReportsEveryMissingField(t *testing.T) {
	caps := &ProviderCapabilities{RequiredConfigFields: []string{"zone_id", "region"}}
	err := ValidateRequiredConfig(caps, map[string]any{})
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
	for _, want := range []string{"zone_id", "region"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention missing field %q", err, want)
		}
	}
}

func TestValidateRequiredConfigNilCaps(t *testing.T) {
	if err := ValidateRequiredConfig(nil, nil); err != nil {
		t.Errorf("nil capabilities should validate trivially, got %v", err)
	}
}
