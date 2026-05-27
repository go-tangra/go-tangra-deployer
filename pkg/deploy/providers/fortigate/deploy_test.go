package fortigate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// genTestCert returns a self-signed cert PEM and key PEM for the given CN and
// serial. Used so the provider's real cert parsing (subject + serial) works.
func genTestCert(t *testing.T, cn string, serial int64) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(serial),
		Subject:      pkix.Name{CommonName: cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	kder, _ := x509.MarshalECPrivateKey(key)
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: kder}))
	return certPEM, keyPEM
}

// fakeForti is a minimal stateful FortiOS REST simulator for deploy tests.
// certs maps a local-cert name to its PEM so serial lookups work.
type fakeForti struct {
	mu            sync.Mutex
	certs         map[string]string
	profiles      map[string][]string // profile name -> server-cert names
	vpnServercert string
	adminCert     string
}

// newFakeForti seeds the rebind/delete scenarios. The pre-existing certs are
// never parsed by those strategies, so placeholder PEMs are fine.
func newFakeForti() *fakeForti {
	return &fakeForti{
		certs:         map[string]string{"star_factory_bg": "placeholder", "star_jobs_bg_2025": "placeholder"},
		profiles:      map[string][]string{"Jobs-Tech SSL Inspection": {"star_jobs_bg_2025", "star_factory_bg"}},
		vpnServercert: "jobs-bg-vpn",
		adminCert:     "self-sign",
	}
}

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
			if pemStr, ok := f.certs[name]; ok {
				writeEnv(w, 200, []map[string]any{{"name": name, "certificate": pemStr}})
			} else {
				writeEnv(w, 404, nil)
			}
		case r.Method == http.MethodDelete && strings.HasPrefix(p, "cmdb/certificate/local/"):
			name := mustUnescape(strings.TrimPrefix(p, "cmdb/certificate/local/"))
			if _, ok := f.certs[name]; !ok {
				writeEnv(w, 404, nil)
				return
			}
			if f.referenced(name) {
				writeEnv(w, 424, nil)
				return
			}
			delete(f.certs, name)
			writeEnv(w, 200, nil)
		case r.Method == http.MethodPost && p == "monitor/vpn-certificate/local/import":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			name, _ := body["certname"].(string)
			fc, _ := body["file_content"].(string)
			dec, _ := base64.StdEncoding.DecodeString(fc)
			f.certs[name] = string(dec)
			writeEnv(w, 200, map[string]any{})

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
			f.profiles[body.Name] = certNames(body.ServerCert)
			writeEnv(w, 200, nil)
		case r.Method == http.MethodPut && strings.HasPrefix(p, "cmdb/firewall/ssl-ssh-profile/"):
			name := mustUnescape(strings.TrimPrefix(p, "cmdb/firewall/ssl-ssh-profile/"))
			var body struct {
				ServerCert []namedRef `json:"server-cert"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			f.profiles[name] = certNames(body.ServerCert)
			writeEnv(w, 200, nil)

		case r.Method == http.MethodGet && p == "cmdb/firewall/vip":
			writeEnv(w, 200, []map[string]any{})

		default:
			writeEnv(w, 404, nil)
		}
	})
}

func certNames(refs []namedRef) []string {
	out := make([]string, 0, len(refs))
	for _, e := range refs {
		out = append(out, e.Name)
	}
	return out
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

// deployForTest runs Deploy against a fresh test server wrapping fake.
func deployForTest(t *testing.T, fake *fakeForti, cfg map[string]any, commonName, certPEM, keyPEM string) *registry.DeploymentResult {
	t.Helper()
	ts := httptest.NewTLSServer(fake.handler())
	t.Cleanup(ts.Close)
	host := strings.TrimPrefix(ts.URL, "https://")
	res, err := (&Provider{}).Deploy(context.Background(),
		&registry.CertificateData{CommonName: commonName, CertificatePEM: certPEM, PrivateKeyPEM: keyPEM},
		cfg, map[string]any{"host": host, "api_token": "tok"}, func(int32, string) {})
	if err != nil {
		t.Fatalf("Deploy error: %v", err)
	}
	return res
}

// ssl_profile (default): fresh cert (not on device) → import + create profile.
func TestDeploySSLProfile_CreatesProfile(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 1001)
	fake := &fakeForti{certs: map[string]string{}, profiles: map[string][]string{}, vpnServercert: "x", adminCert: "y"}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	base := sanitizeName("test.example.com")
	newCert := versionedName(base, time.Now().UTC().Format("20060102"))
	prof := profileNameFor(base, "")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.certs[newCert]; !ok {
		t.Errorf("expected import of %q; certs=%v", newCert, mapKeys(fake.certs))
	}
	if got := fake.profiles[prof]; strings.Join(got, ",") != newCert {
		t.Errorf("profile %q = %v, want [%s]", prof, got, newCert)
	}
}

// ssl_profile idempotency: cert already on device (by serial) → reuse, no import.
func TestDeploySSLProfile_ReusesExistingBySerial(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 2002)
	fake := &fakeForti{
		certs:         map[string]string{"preexisting_cert": certPEM},
		profiles:      map[string][]string{},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	prof := profileNameFor(sanitizeName("test.example.com"), "")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.certs) != 1 {
		t.Errorf("expected NO new import (idempotent), certs=%v", mapKeys(fake.certs))
	}
	if got := fake.profiles[prof]; strings.Join(got, ",") != "preexisting_cert" {
		t.Errorf("profile %q = %v, want [preexisting_cert]", prof, got)
	}
	if res.ResourceID != "preexisting_cert" {
		t.Errorf("ResourceID = %q, want preexisting_cert", res.ResourceID)
	}
}

// #2: base name derived from the cert SUBJECT, not the metadata common_name.
func TestDeploySSLProfile_UsesCertSubjectNotMetadata(t *testing.T) {
	// Subject is apex.example.com; metadata lies and says *.example.com.
	certPEM, keyPEM := genTestCert(t, "apex.example.com", 4004)
	fake := &fakeForti{certs: map[string]string{}, profiles: map[string][]string{}, vpnServercert: "x", adminCert: "y"}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "*.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	wantProf := "apex_example_com_ssl_profile" // from subject, NOT star_example_com
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.profiles[wantProf]; !ok {
		t.Errorf("expected profile from cert subject %q; profiles=%v", wantProf, mapKeysS(fake.profiles))
	}
}

// ssl_profile: cert referenced by a differently-named profile → bail.
func TestDeploySSLProfile_BailsOnForeignReference(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 3003)
	fake := &fakeForti{
		certs:         map[string]string{"preexisting_cert": certPEM},
		profiles:      map[string][]string{"Other Inspection": {"preexisting_cert"}},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected manual-review (non-success) on foreign reference")
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected Details.manual_review_required=true")
	}
	prof := profileNameFor(sanitizeName("test.example.com"), "")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, exists := fake.profiles[prof]; exists {
		t.Error("must not create the conventional profile when bailing")
	}
	if strings.Join(fake.profiles["Other Inspection"], ",") != "preexisting_cert" {
		t.Error("foreign profile must not be modified")
	}
}

// rebind strategy (opt-in): repoints references and prunes old family members.
func TestDeployRebind_EndToEnd(t *testing.T) {
	fake := newFakeForti()
	certPEM, keyPEM := genTestCert(t, "*.factory.bg", 5005)
	res := deployForTest(t, fake, map[string]any{"vdom": "root", "replace_strategy": "rebind"}, "*.factory.bg", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("Deploy not successful: %s", res.Message)
	}
	newName := versionedName("star_factory_bg", time.Now().UTC().Format("20060102"))
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.certs[newName]; !ok {
		t.Errorf("expected new cert %q; certs=%v", newName, mapKeys(fake.certs))
	}
	if _, ok := fake.certs["star_factory_bg"]; ok {
		t.Errorf("expected old cert star_factory_bg pruned; certs=%v", mapKeys(fake.certs))
	}
	if _, ok := fake.certs["star_jobs_bg_2025"]; !ok {
		t.Error("unrelated cert star_jobs_bg_2025 must not be pruned")
	}
	want := []string{"star_jobs_bg_2025", newName}
	if got := fake.profiles["Jobs-Tech SSL Inspection"]; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("profile server-cert = %v, want %v", got, want)
	}
}

// delete strategy (legacy): referenced cert returns an actionable error.
func TestDeployDelete_ReferencedReturnsHelpfulError(t *testing.T) {
	fake := newFakeForti()
	certPEM, keyPEM := genTestCert(t, "*.factory.bg", 6006)
	ts := httptest.NewTLSServer(fake.handler())
	defer ts.Close()
	host := strings.TrimPrefix(ts.URL, "https://")
	_, err := (&Provider{}).Deploy(context.Background(),
		&registry.CertificateData{CommonName: "*.factory.bg", CertificatePEM: certPEM, PrivateKeyPEM: keyPEM},
		map[string]any{"vdom": "root", "replace_strategy": "delete"},
		map[string]any{"host": host, "api_token": "tok"}, func(int32, string) {})
	if err == nil {
		t.Fatal("expected delete strategy to fail on referenced cert")
	}
	if !strings.Contains(err.Error(), "replace_strategy=rebind") {
		t.Errorf("expected actionable hint, got: %v", err)
	}
}

func mapKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
func mapKeysS(m map[string][]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
