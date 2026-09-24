package sealed

import "testing"

// TestADConfigBindsConfigID confirms the config additional-data helper binds a
// sealed credential to its configuration id (distinct per id, stable per id),
// which is what lets a backup's sealed blob unseal only under the same config id.
func TestADConfigBindsConfigID(t *testing.T) {
	if got := string(ADConfig("abc")); got != "config:abc" {
		t.Fatalf("ADConfig = %q, want %q", got, "config:abc")
	}
	if string(ADConfig("a")) == string(ADConfig("b")) {
		t.Fatal("ADConfig must differ per configuration id")
	}
}
