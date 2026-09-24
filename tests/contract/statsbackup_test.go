package contract

import (
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
)

// TestStatsBackupRoutesDerived checks the US5 statistics and backup endpoints
// derive from the OpenAPI with the permissions the gateway enforces.
func TestStatsBackupRoutesDerived(t *testing.T) {
	m, err := deployermanifest.Manifest()
	if err != nil {
		t.Fatalf("Manifest: %v", err)
	}
	want := map[string]string{
		"GET /api/deployer/v1/statistics":        "stats:read",
		"GET /api/deployer/v1/statistics/tenant": "stats:read",
		"POST /api/deployer/v1/backup/export":    "backup:manage",
		"POST /api/deployer/v1/backup/import":    "backup:manage",
	}
	got := map[string]string{}
	for _, r := range m.Routes {
		got[r.Method+" "+r.Path] = r.Permission
	}
	for key, perm := range want {
		if got[key] != perm {
			t.Errorf("route %q permission = %q, want %q", key, got[key], perm)
		}
	}
}
