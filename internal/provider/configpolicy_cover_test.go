package provider

import "testing"

func TestValidateConfigWithoutProviderValidator(t *testing.T) {
	reset()
	t.Cleanup(reset)
	Register(fake{caps: Capabilities{Type: "plain"}})
	if err := ValidateConfig("plain", map[string]any{"a": 1}); err != nil {
		t.Fatalf("provider without ConfigValidator: %v", err)
	}
}

func TestOriginEdgeCases(t *testing.T) {
	if got := origin("ftp://Host.Example/x"); got != "ftp://host.example:|" {
		t.Fatalf("origin = %q", got)
	}
	if got := origin("not a url"); got != "raw:not a url" {
		t.Fatalf("origin = %q", got)
	}
}
