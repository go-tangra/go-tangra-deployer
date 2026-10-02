package all_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/providers/inventoryagent"
)

// TestMain (T052): without an inventory peer the catalogue holds the six
// self-registering providers only; the inventory-agent provider is registered
// at app wiring when inventory.service is configured — the tests of this
// package then run against the full catalogue.
func TestMain(m *testing.M) {
	for _, c := range provider.List() {
		if c.Type == inventoryagent.Type {
			fmt.Fprintln(os.Stderr, "inventory-agent registered without an inventory peer")
			os.Exit(1)
		}
	}
	if n := len(provider.List()); n != 6 {
		fmt.Fprintf(os.Stderr, "catalogue without inventory has %d providers, want 6\n", n)
		os.Exit(1)
	}
	provider.Register(inventoryagent.New(nil))
	os.Exit(m.Run())
}
