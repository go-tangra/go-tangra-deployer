package all_test

import (
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"

	_ "github.com/go-freya/freya/services/deployer/internal/providers/all"
)

// TestCatalogueRegistered asserts every shipped provider self-registers with the
// capability matrix the spec (and the UI) depends on.
func TestCatalogueRegistered(t *testing.T) {
	want := map[string]struct {
		display  string
		verify   bool
		rollback bool
	}{
		"dummy":      {"Dummy (testing)", true, true},
		"webhook":    {"Webhook (generic HTTP)", true, true},
		"aws_acm":    {"AWS Certificate Manager", true, false},
		"cloudflare": {"Cloudflare", true, false},
		"bigip":      {"F5 BIG-IP", true, true},
		"fortigate":  {"FortiGate", true, true},
	}
	list := provider.List()
	if len(list) != len(want) {
		t.Fatalf("registered %d providers, want %d: %+v", len(list), len(want), providerTypes(list))
	}
	for typ, w := range want {
		p, err := provider.Get(typ)
		if err != nil {
			t.Errorf("%s not registered: %v", typ, err)
			continue
		}
		c := p.Capabilities()
		if c.DisplayName != w.display || c.SupportsVerify != w.verify || c.SupportsRollback != w.rollback {
			t.Errorf("%s capabilities = %+v, want display=%q verify=%v rollback=%v", typ, c, w.display, w.verify, w.rollback)
		}
	}
}

func providerTypes(caps []provider.Capabilities) []string {
	out := make([]string, len(caps))
	for i, c := range caps {
		out[i] = c.Type
	}
	return out
}
