package fortigate

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// fakeForti is a minimal stateful FortiOS REST simulator for deploy tests.
type fakeForti struct {
	mu            sync.Mutex
	certs         map[string]bool
	profiles      map[string][]string // profile name -> server-cert names
	vpnServercert string
	adminCert     string
}

func newFakeForti() *fakeForti {
	return &fakeForti{
		certs:         map[string]bool{"star_factory_bg": true, "star_jobs_bg_2025": true},
		profiles:      map[string][]string{"Jobs-Tech SSL Inspection": {"star_jobs_bg_2025", "star_factory_bg"}},
		vpnServercert: "jobs-bg-vpn",
		adminCert:     "self-sign",
	}
}

// referenced reports whether a cert is bound anywhere (blocks delete).
func (f *fakeForti) referenced(name string) bool {
	for _, certs := range f.profiles {
		for _, c := range certs {
			if c == name {
				return true
			}
		}
	}
	return name == f.vpnServercert || name == f.adminCert
}

func writeEnv(w http.ResponseWriter, code int, results any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	status := "success"
	if code >= 400 {
		status = "error"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": status, "http_status": code, "results": results,
	})
}

func (f *fakeForti) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		p := strings.TrimPrefix(r.URL.Path, "/api/v2/")

		switch {
		case r.Method == http.MethodGet && p == "cmdb/system/global":
			writeEnv(w, 200, map[string]any{"admin-server-cert": f.adminCert})
		case r.Method == http.MethodGet && p == "cmdb/vpn.ssl/settings":
			writeEnv(w, 200, map[string]any{"servercert": f.vpnServercert})
		case r.Method == http.MethodPut && p == "cmdb/vpn.ssl/settings":
			f.vpnServercert = putField(r, "servercert", f.vpnServercert)
			writeEnv(w, 200, nil)
		case r.Method == http.MethodPut && p == "cmdb/system/global":
			f.adminCert = putField(r, "admin-server-cert", f.adminCert)
			writeEnv(w, 200, nil)

		case r.Method == http.MethodGet && p == "cmdb/certificate/local":
			var list []map[string]any
			for n := range f.certs {
				list = append(list, map[string]any{"name": n})
			}
			writeEnv(w, 200, list)
		case r.Method == http.MethodGet && strings.HasPrefix(p, "cmdb/certificate/local/"):
			name := mustUnescape(strings.TrimPrefix(p, "cmdb/certificate/local/"))
			if f.certs[name] {
				writeEnv(w, 200, []map[string]any{{"name": name}})
			} else {
				writeEnv(w, 404, nil)
			}
		case r.Method == http.MethodDelete && strings.HasPrefix(p, "cmdb/certificate/local/"):
			name := mustUnescape(strings.TrimPrefix(p, "cmdb/certificate/local/"))
			if !f.certs[name] {
				writeEnv(w, 404, nil)
				return
			}
			if f.referenced(name) {
				writeEnv(w, 424, nil) // in use
				return
			}
			delete(f.certs, name)
			writeEnv(w, 200, nil)
		case r.Method == http.MethodPost && p == "monitor/vpn-certificate/local/import":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			name, _ := body["certname"].(string)
			f.certs[name] = true
			writeEnv(w, 200, nil)

		case r.Method == http.MethodGet && p == "cmdb/firewall/ssl-ssh-profile":
			var list []map[string]any
			for name, certs := range f.profiles {
				var sc []map[string]string
				for _, c := range certs {
					sc = append(sc, map[string]string{"name": c, "q_origin_key": c})
				}
				list = append(list, map[string]any{"name": name, "server-cert": sc})
			}
			writeEnv(w, 200, list)
		case r.Method == http.MethodGet && strings.HasPrefix(p, "cmdb/firewall/ssl-ssh-profile/"):
			name := mustUnescape(strings.TrimPrefix(p, "cmdb/firewall/ssl-ssh-profile/"))
			certs, ok := f.profiles[name]
			if !ok {
				writeEnv(w, 404, nil)
				return
			}
			var sc []map[string]string
			for _, c := range certs {
				sc = append(sc, map[string]string{"name": c})
			}
			writeEnv(w, 200, []map[string]any{{"name": name, "server-cert": sc}})
		case r.Method == http.MethodPost && p == "cmdb/firewall/ssl-ssh-profile":
			var body struct {
				Name       string     `json:"name"`
				ServerCert []namedRef `json:"server-cert"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			names := make([]string, 0, len(body.ServerCert))
			for _, e := range body.ServerCert {
				names = append(names, e.Name)
			}
			f.profiles[body.Name] = names
			writeEnv(w, 200, nil)
		case r.Method == http.MethodPut && strings.HasPrefix(p, "cmdb/firewall/ssl-ssh-profile/"):
			name := mustUnescape(strings.TrimPrefix(p, "cmdb/firewall/ssl-ssh-profile/"))
			var body struct {
				ServerCert []namedRef `json:"server-cert"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			names := make([]string, 0, len(body.ServerCert))
			for _, e := range body.ServerCert {
				names = append(names, e.Name)
			}
			f.profiles[name] = names
			writeEnv(w, 200, nil)

		case r.Method == http.MethodGet && p == "cmdb/firewall/vip":
			writeEnv(w, 200, []map[string]any{}) // no VIPs

		default:
			writeEnv(w, 404, nil)
		}
	})
}

func putField(r *http.Request, field, cur string) string {
	b, _ := io.ReadAll(r.Body)
	var m map[string]any
	if json.Unmarshal(b, &m) == nil {
		if v, ok := m[field].(string); ok {
			return v
		}
	}
	return cur
}

func mustUnescape(s string) string {
	if u, err := url.PathUnescape(s); err == nil {
		return u
	}
	return s
}

func TestDeployRebind_EndToEnd(t *testing.T) {
	fake := newFakeForti()
	ts := httptest.NewTLSServer(fake.handler())
	defer ts.Close()
	host := strings.TrimPrefix(ts.URL, "https://")

	p := &Provider{}
	cert := &registry.CertificateData{
		CommonName:     "*.factory.bg",
		CertificatePEM: "-----CERT-----",
		PrivateKeyPEM:  "-----KEY-----",
	}
	creds := map[string]any{"host": host, "api_token": "tok"}
	cfg := map[string]any{"vdom": "root", "replace_strategy": "rebind"}

	res, err := p.Deploy(context.Background(), cert, cfg, creds, func(int32, string) {})
	if err != nil {
		t.Fatalf("Deploy error: %v", err)
	}
	if !res.Success {
		t.Fatalf("Deploy not successful: %s", res.Message)
	}

	newName := versionedName("star_factory_bg", time.Now().UTC().Format("20060102"))

	fake.mu.Lock()
	defer fake.mu.Unlock()

	// New cert imported.
	if !fake.certs[newName] {
		t.Errorf("expected new cert %q to exist; certs=%v", newName, fake.certs)
	}
	// Old family member pruned (it was rebound away, so no longer referenced).
	if fake.certs["star_factory_bg"] {
		t.Errorf("expected old cert star_factory_bg to be pruned; certs=%v", fake.certs)
	}
	// Unrelated manual cert untouched.
	if !fake.certs["star_jobs_bg_2025"] {
		t.Error("unrelated cert star_jobs_bg_2025 must not be pruned")
	}
	// Profile rebound: sibling preserved, family member repointed.
	got := fake.profiles["Jobs-Tech SSL Inspection"]
	want := []string{"star_jobs_bg_2025", newName}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("profile server-cert = %v, want %v", got, want)
	}
	if res.ResourceID != newName {
		t.Errorf("ResourceID = %q, want %q", res.ResourceID, newName)
	}
}

func deployForTest(t *testing.T, fake *fakeForti, cfg map[string]any) *registry.DeploymentResult {
	t.Helper()
	ts := httptest.NewTLSServer(fake.handler())
	t.Cleanup(ts.Close)
	host := strings.TrimPrefix(ts.URL, "https://")
	res, err := (&Provider{}).Deploy(context.Background(),
		&registry.CertificateData{CommonName: "*.factory.bg", CertificatePEM: "C", PrivateKeyPEM: "K"},
		cfg, map[string]any{"host": host, "api_token": "tok"}, func(int32, string) {})
	if err != nil {
		t.Fatalf("Deploy error: %v", err)
	}
	return res
}

// ssl_profile (default) strategy: certificate referenced ONLY by the
// convention-named profile → import new dated cert + update that profile, never
// deleting the old cert.
func TestDeploySSLProfile_UpdatesConventionalProfile(t *testing.T) {
	fake := &fakeForti{
		certs:         map[string]bool{"star_factory_bg": true},
		profiles:      map[string][]string{"star_factory_bg_ssl_profile": {"star_factory_bg"}},
		vpnServercert: "jobs-bg-vpn",
		adminCert:     "self-sign",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"})
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	newCert := versionedName("star_factory_bg", time.Now().UTC().Format("20060102"))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.certs[newCert] {
		t.Errorf("new cert %q not imported", newCert)
	}
	if !fake.certs["star_factory_bg"] {
		t.Error("old cert must NOT be deleted in ssl_profile mode")
	}
	if got := fake.profiles["star_factory_bg_ssl_profile"]; strings.Join(got, ",") != newCert {
		t.Errorf("profile server-cert = %v, want [%s]", got, newCert)
	}
}

// ssl_profile: convention profile does not exist and no other reference → create it.
func TestDeploySSLProfile_CreatesProfile(t *testing.T) {
	fake := &fakeForti{
		certs:         map[string]bool{},
		profiles:      map[string][]string{}, // no profiles reference the family
		vpnServercert: "jobs-bg-vpn",
		adminCert:     "self-sign",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"})
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	newCert := versionedName("star_factory_bg", time.Now().UTC().Format("20060102"))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	got, ok := fake.profiles["star_factory_bg_ssl_profile"]
	if !ok {
		t.Fatalf("expected profile star_factory_bg_ssl_profile to be created; profiles=%v", fake.profiles)
	}
	if strings.Join(got, ",") != newCert {
		t.Errorf("created profile server-cert = %v, want [%s]", got, newCert)
	}
}

// ssl_profile: cert referenced by a DIFFERENTLY-named profile → bail to manual
// review, upload the cert but touch nothing else.
func TestDeploySSLProfile_BailsOnForeignReference(t *testing.T) {
	fake := newFakeForti() // Jobs-Tech SSL Inspection references star_factory_bg
	before := append([]string(nil), fake.profiles["Jobs-Tech SSL Inspection"]...)

	res := deployForTest(t, fake, map[string]any{"vdom": "root"})
	if res.Success {
		t.Fatal("expected manual-review (non-success) on foreign reference")
	}
	if !strings.Contains(res.Message, "MANUAL REVIEW") {
		t.Errorf("expected manual-review message, got: %s", res.Message)
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected Details.manual_review_required=true")
	}
	newCert := versionedName("star_factory_bg", time.Now().UTC().Format("20060102"))

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if !fake.certs[newCert] {
		t.Errorf("cert %q should still be uploaded on bail", newCert)
	}
	// Foreign profile and any conventional profile must be untouched.
	if strings.Join(fake.profiles["Jobs-Tech SSL Inspection"], ",") != strings.Join(before, ",") {
		t.Errorf("foreign profile must not be modified: before=%v after=%v", before, fake.profiles["Jobs-Tech SSL Inspection"])
	}
	if _, exists := fake.profiles["star_factory_bg_ssl_profile"]; exists {
		t.Error("must not create the conventional profile when bailing")
	}
}

func TestDeployDelete_ReferencedReturnsHelpfulError(t *testing.T) {
	fake := newFakeForti()
	ts := httptest.NewTLSServer(fake.handler())
	defer ts.Close()
	host := strings.TrimPrefix(ts.URL, "https://")

	p := &Provider{}
	cert := &registry.CertificateData{
		CommonName:     "*.factory.bg",
		CertificatePEM: "-----CERT-----",
		PrivateKeyPEM:  "-----KEY-----",
	}
	creds := map[string]any{"host": host, "api_token": "tok"}
	cfg := map[string]any{"vdom": "root", "replace_strategy": "delete"}

	_, err := p.Deploy(context.Background(), cert, cfg, creds, func(int32, string) {})
	if err == nil {
		t.Fatal("expected delete strategy to fail on referenced cert")
	}
	if !strings.Contains(err.Error(), "replace_strategy=rebind") {
		t.Errorf("expected actionable hint, got: %v", err)
	}
}
