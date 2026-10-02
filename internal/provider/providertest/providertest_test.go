package providertest

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func TestKeysReadIdioms(t *testing.T) {
	src := `package x
func f(config, creds map[string]any) {
	_ = config["alpha"]
	_, _ = creds["beta"].(string)
	_ = str(m, "gamma")
	_ = cfgString(config, "delta", "d")
	_, _ = p.probe(ctx, "verify", "verify_url", cert, config, creds)
}`
	file := filepath.Join(t.TempDir(), "x.go")
	if err := os.WriteFile(file, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	got := KeysRead(t, file)
	if !reflect.DeepEqual(got, []string{"alpha", "beta", "delta", "gamma", "verify_url"}) {
		t.Fatalf("KeysRead = %v", got)
	}
	ft := &testing.T{}
	caps := provider.Capabilities{Type: "x", ConfigFields: []provider.Field{{Key: "alpha", Label: "A"}}}
	done := make(chan struct{})
	go func() { defer close(done); AssertDeclaredKeysCoverReads(ft, caps, file) }()
	<-done
	if !ft.Failed() {
		t.Fatal("undeclared reads not reported")
	}
}
