package contract

import (
	"testing"

	"github.com/go-freya/freya/services/deployer/pkg/deployermanifest"
)

func TestManifestDerivesRoutesAndPermissions(t *testing.T) {
	m, err := deployermanifest.Manifest()
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	if m.Module != "deployer" || len(m.Prefixes) == 0 || m.Prefixes[0] != "/api/deployer" {
		t.Fatalf("module/prefix: %+v", m.Prefixes)
	}
	if len(m.Routes) < 25 {
		t.Fatalf("routes = %d, want >= 25", len(m.Routes))
	}
	// Every non-public route must carry a declared permission.
	known := map[string]bool{}
	for _, p := range deployermanifest.PermissionRefs() {
		known[p] = true
	}
	for _, r := range m.Routes {
		if r.Public {
			continue
		}
		if r.Permission == "" || !known[r.Permission] {
			t.Fatalf("route %s %s has bad permission %q", r.Method, r.Path, r.Permission)
		}
	}
}
