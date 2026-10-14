package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/config"
	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
	"github.com/go-tangra/go-tangra/v4/authz"
	"github.com/go-tangra/go-tangra/v4/catalogue"
)

var placeholderRE = regexp.MustCompile(`\$\{([A-Z][A-Z0-9_]*)\}`)

// joinValues are values of the shape the gateway fills in: core values,
// generated secrets and the host inputs.
var joinValues = map[string]string{
	"TRUST_DOMAIN": "infra.example.org", "GATEWAY_ISSUER": "https://portal.example.org:8443",
	"LCM_ENROLL_URL": "https://portal.example.org:8443/api/lcm/v1/enroll", "LCM_GRPC": "portal.example.org:9945",
	"GATEWAY_GRPC": "portal.example.org:9643", "AUTH_GRPC": "portal.example.org:9543", "MESH_TENANT_ID": "00000000-0000-0000-0000-000000000001",
	"MODULE_IMAGE": "ghcr.io/go-tangra/go-tangra-deployer", "MODULE_VERSION": "4.4.0",
	"GEN_PASSWORD_1": strings.Repeat("a", 48), "GEN_PASSWORD_2": strings.Repeat("b", 48),
	"GEN_KEY_1":             "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
	"MODULE_ADVERTISE_HOST": "deployer.example.org", "MODULE_BIND_IP": "10.0.0.5",
	"VALKEY_ADDRESS": "valkey.example.org:6380", "VALKEY_PASSWORD": strings.Repeat("c", 32), "VALKEY_CA_FILE": "/etc/tangra/valkey-ca.pem",
}

func render(t *testing.T, name string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "deploy", "bundle", name))
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	rendered := placeholderRE.ReplaceAllStringFunc(string(raw), func(m string) string {
		k := placeholderRE.FindStringSubmatch(m)[1]
		v, ok := joinValues[k]
		if !ok {
			missing = append(missing, k)
		}
		return v
	})
	if len(missing) > 0 {
		t.Fatalf("%s uses placeholders the gateway does not fill: %v", name, missing)
	}
	return rendered
}

// tangra-module.yaml is a valid descriptor whose host inputs accept values
// of the shape operators give, and builds a valid catalogue entry with the
// packed bundle and the manifest's permissions.
func TestCatalogueDescriptorBuildsEntry(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "tangra-module.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := catalogue.ParseDescriptor(raw)
	if err != nil {
		t.Fatal(err)
	}
	if d.Module != deployermanifest.Module || !strings.HasPrefix(d.Routes.Prefixes[0], "/api/"+d.Module) {
		t.Fatalf("%+v", d)
	}
	for _, in := range d.HostInputs {
		if !regexp.MustCompile(in.Pattern).MatchString(joinValues[in.Key]) {
			t.Fatalf("host input %s refuses %q", in.Key, joinValues[in.Key])
		}
	}
	bundle, err := catalogue.PackBundle(os.DirFS(filepath.Join("..", "..", d.Bundle.Dir)))
	if err != nil {
		t.Fatal(err)
	}
	e, err := catalogue.BuildEntry(d, "4.4.0", "go-tangra/go-tangra-deployer", deployermanifest.PermissionRefs(), bundle)
	if err != nil {
		t.Fatal(err)
	}
	if err := e.CheckBundle(bundle); err != nil {
		t.Fatal(err)
	}
	files, err := catalogue.ReadBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range d.Bundle.Templates {
		if _, ok := files[name]; !ok {
			t.Fatalf("template %s missing from the bundle", name)
		}
	}
}

// The join bundle's templates, rendered with values of the shape the gateway
// fills in, are a valid production configuration and policy, and the compose
// file uses only values the gateway writes to .env.
func TestJoinBundleRendersToValidProductionConfig(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"config.yaml", "policy.yaml"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(render(t, name)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	render(t, "compose.yaml")
	cfg, err := config.Load(filepath.Join(dir, "config.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("rendered config invalid: %v", err)
	}
	if !cfg.IsProduction() || cfg.Gateway.Issuer != "https://portal.example.org:8443" || !cfg.Events.Enabled {
		t.Fatalf("%+v %+v", cfg.Gateway, cfg.Events)
	}
	if cfg.Valkey.AllowPlaintext || cfg.Valkey.Addresses[0] != "valkey.example.org:6380" || cfg.Valkey.CAFile == "" {
		t.Fatalf("%+v", cfg.Valkey.Addresses)
	}
	pol, _ := os.ReadFile(filepath.Join(dir, "policy.yaml"))
	if _, err := authz.Load(strings.NewReader(string(pol))); err != nil || !strings.Contains(string(pol), "spiffe://infra.example.org/svc/gateway") {
		t.Fatal(err, string(pol))
	}
}
