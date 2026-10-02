package webhook

import (
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider/providertest"
)

// TestDeclaredKeysCoverReads (T080): every key this provider reads is declared
// in its field descriptors.
func TestDeclaredKeysCoverReads(t *testing.T) {
	providertest.AssertDeclaredKeysCoverReads(t, Provider{}.Capabilities(), "webhook.go")
}
