package deployermanifest_test

import (
	"slices"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
)

// TestRoles checks the module role set (feature 019, research D9): slugs,
// display names and permissions, all of them the module's own.
func TestRoles(t *testing.T) {
	want := map[string]struct {
		name  string
		perms []string
	}{
		"administrator": {"Deployer administrator", deployermanifest.PermissionRefs()},
		"operator":      {"Deployer operator", []string{"configurations:read", "targets:read", "jobs:read", "jobs:manage", "deploy:execute"}},
		"viewer":        {"Deployer viewer", []string{"configurations:read", "targets:read", "jobs:read", "stats:read"}},
	}
	own := map[string]bool{}
	for _, p := range deployermanifest.PermissionRefs() {
		own[p] = true
	}
	if len(deployermanifest.Roles) != len(want) {
		t.Fatalf("%d roles, want %d", len(deployermanifest.Roles), len(want))
	}
	for _, r := range deployermanifest.Roles {
		w, ok := want[r.Slug]
		if !ok {
			t.Fatalf("unexpected role %q", r.Slug)
		}
		if r.DisplayName != w.name || r.Description == "" {
			t.Errorf("%s: name %q, description %q", r.Slug, r.DisplayName, r.Description)
		}
		if !slices.Equal(r.Permissions, w.perms) {
			t.Errorf("%s: %v, want %v", r.Slug, r.Permissions, w.perms)
		}
		for _, p := range r.Permissions {
			if !own[p] {
				t.Errorf("%s names %q, not a deployer permission", r.Slug, p)
			}
		}
	}
}

// TestRegistration checks the registration sent to auth: module identity,
// every permission, the role set and the built-in grants; auth's rules hold.
func TestRegistration(t *testing.T) {
	reg := deployermanifest.Registration()
	if err := reg.Validate(); err != nil {
		t.Fatal(err)
	}
	if reg.Module != "deployer" || reg.DisplayName != "Deployer" || len(reg.Permissions) != len(deployermanifest.Permissions) || len(reg.Roles) != 3 {
		t.Fatalf("%+v", reg)
	}
	for i, p := range deployermanifest.Permissions {
		if got := reg.Permissions[i]; got.Resource != p.Resource || got.Action != p.Action || got.Description != p.Description {
			t.Errorf("permission %d: %+v", i, got)
		}
	}
	for _, slug := range []string{"owner", "admin", "member", "auditor", "operator"} {
		if !slices.Equal(reg.BuiltinGrants[slug], deployermanifest.Grants[slug]) {
			t.Errorf("grant %s: %v", slug, reg.BuiltinGrants[slug])
		}
	}
}
