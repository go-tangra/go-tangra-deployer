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
	"slices"
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
	profileModes  map[string]string   // profile name -> server-cert-mode (default "replace")
	policies      map[string]string   // firewall policy name -> bound ssl-ssh-profile
	vpnServercert string
	adminCert     string
	putCount      map[string]int // path key -> count, for asserting dedup
}

// modeFor returns the configured mode for a profile, defaulting to "replace"
// so existing tests need not set profileModes explicitly.
func (f *fakeForti) modeFor(name string) string {
	if m := f.profileModes[name]; m != "" {
		return m
	}
	return "replace"
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
			if _, exists := f.certs[name]; exists {
				// FortiOS rejects importing a local cert under a name that is
				// already taken (it does not overwrite). Mirror that here so
				// same-day name-collision regressions surface in tests.
				writeEnv(w, 500, map[string]any{"error": "duplicate certificate name"})
				return
			}
			fc, _ := body["file_content"].(string)
			dec, _ := base64.StdEncoding.DecodeString(fc)
			f.certs[name] = string(dec)
			writeEnv(w, 200, map[string]any{})

		case r.Method == http.MethodGet && p == "cmdb/firewall/ssl-ssh-profile":
			var list []map[string]any
			for name, certs := range f.profiles {
				var sc []map[string]any
				for _, c := range certs {
					sc = append(sc, map[string]any{"name": c, "q_origin_key": c})
				}
				list = append(list, map[string]any{"name": name, "server-cert": sc, "server-cert-mode": f.modeFor(name), "ssl-exempt": []any{}})
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
			writeEnv(w, 200, []map[string]any{{"name": name, "server-cert": sc, "server-cert-mode": f.modeFor(name)}})
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
			if f.putCount == nil {
				f.putCount = map[string]int{}
			}
			f.putCount["ssl-ssh-profile:"+name]++
			writeEnv(w, 200, nil)

		case r.Method == http.MethodGet && p == "cmdb/firewall/policy":
			list := make([]map[string]any, 0, len(f.policies))
			for name, prof := range f.policies {
				list = append(list, map[string]any{"name": name, "ssl-ssh-profile": prof})
			}
			writeEnv(w, 200, list)

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
	fake := &fakeForti{certs: map[string]string{}, profiles: map[string][]string{"deep-inspection": {"tmpl_cert"}}, vpnServercert: "x", adminCert: "y"}
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
		profiles:      map[string][]string{"deep-inspection": {"tmpl_cert"}},
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
	fake := &fakeForti{certs: map[string]string{}, profiles: map[string][]string{"deep-inspection": {"tmpl_cert"}}, vpnServercert: "x", adminCert: "y"}
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

// ssl_profile: profile missing AND no template to clone → bail (but cert still imported).
func TestDeploySSLProfile_BailsWhenNoTemplate(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 7007)
	fake := &fakeForti{certs: map[string]string{}, profiles: map[string][]string{}, vpnServercert: "x", adminCert: "y"}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected bail when no template profile exists to clone")
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected manual_review_required")
	}
	base := sanitizeName("test.example.com")
	newCert := versionedName(base, time.Now().UTC().Format("20060102"))
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.certs[newCert]; !ok {
		t.Error("cert should still be imported even when bailing on missing template")
	}
}

// ssl_profile + default_ssl_profile: production-bound profile contains an
// old family member and a sibling cert; deploy swaps the family member in
// place and preserves the sibling so multi-domain inspection keeps working.
func TestDeploySSLProfile_DefaultProfile_SwapsExistingFamily(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app1.example.com", 8001)
	base := sanitizeName("app1.example.com")
	auditProf := profileNameFor(base, "")
	defaultProf := "multi_domain_ssl_profile"
	oldFamily := "app1_example_com_20260101"
	sibling := "app2_example_com_20260101"
	fake := &fakeForti{
		certs:         map[string]string{oldFamily: "placeholder"},
		profiles:      map[string][]string{defaultProf: {oldFamily, sibling}},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, fake, cfg, "app1.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s — details=%v", res.Message, res.Details)
	}
	newCert := versionedName(base, time.Now().UTC().Format("20060102"))
	fake.mu.Lock()
	defer fake.mu.Unlock()
	got := fake.profiles[defaultProf]
	if len(got) != 2 || !slices.Contains(got, newCert) || !slices.Contains(got, sibling) {
		t.Errorf("default profile = %v, want [%s, %s] (any order)", got, newCert, sibling)
	}
	if slices.Contains(got, oldFamily) {
		t.Errorf("default profile still contains old family member %q: %v", oldFamily, got)
	}
	if _, ok := fake.profiles[auditProf]; !ok {
		t.Errorf("audit profile %q must also be created/updated", auditProf)
	}
	if act, _ := res.Details["default_profile_action"].(string); !strings.Contains(act, "updated") {
		t.Errorf("Details.default_profile_action = %q, want \"updated\"", act)
	}
}

// ssl_profile + default_ssl_profile: family is absent from the production
// profile (e.g. operator manually removed it). Deploy must APPEND so future
// renewals re-converge instead of silently no-op'ing.
func TestDeploySSLProfile_DefaultProfile_AppendsWhenAbsent(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app3.example.com", 8002)
	defaultProf := "multi_domain_ssl_profile"
	sibling := "app2_example_com_20260101"
	fake := &fakeForti{
		certs:         map[string]string{},
		profiles:      map[string][]string{defaultProf: {sibling}},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, fake, cfg, "app3.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	newCert := versionedName(sanitizeName("app3.example.com"), time.Now().UTC().Format("20060102"))
	fake.mu.Lock()
	defer fake.mu.Unlock()
	got := fake.profiles[defaultProf]
	if len(got) != 2 || !slices.Contains(got, sibling) || !slices.Contains(got, newCert) {
		t.Errorf("default profile = %v, want sibling+new (any order)", got)
	}
	if act, _ := res.Details["default_profile_action"].(string); !strings.Contains(act, "appended") {
		t.Errorf("Details.default_profile_action = %q, want \"appended\"", act)
	}
}

// ssl_profile + default_ssl_profile: profile is missing → manual review, no
// writes to either profile. The cert is still imported (idempotent).
func TestDeploySSLProfile_DefaultProfileMissing_ManualReview(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 8003)
	// A replace-mode template must exist or we'd bail for a different reason.
	fake := &fakeForti{
		certs:         map[string]string{},
		profiles:      map[string][]string{"deep-inspection": {"tmpl_cert"}},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": "nonexistent_profile"}
	res := deployForTest(t, fake, cfg, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected manual review when default_ssl_profile is missing")
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected Details.manual_review_required=true")
	}
	if !strings.Contains(res.Message, "does not exist") {
		t.Errorf("expected message to explain the missing profile, got: %s", res.Message)
	}
	auditProf := profileNameFor(sanitizeName("test.example.com"), "")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.profiles[auditProf]; ok {
		t.Error("audit profile must NOT be created when bailing on missing default profile")
	}
	newCert := versionedName(sanitizeName("test.example.com"), time.Now().UTC().Format("20060102"))
	if _, ok := fake.certs[newCert]; !ok {
		t.Error("cert should still be imported (idempotent) even when bailing")
	}
}

// ssl_profile + default_ssl_profile: profile exists but mode is not "replace"
// → manual review, because PUTting server-cert would be a silent no-op.
func TestDeploySSLProfile_DefaultProfileWrongMode_ManualReview(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 8004)
	defaultProf := "deep-inspection-resign"
	fake := &fakeForti{
		certs:         map[string]string{},
		profiles:      map[string][]string{defaultProf: {"some_ca"}, "deep-inspection": {"tmpl_cert"}},
		profileModes:  map[string]string{defaultProf: "re-sign"},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, fake, cfg, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected manual review when default profile is not in replace mode")
	}
	if !strings.Contains(res.Message, "server-cert-mode") {
		t.Errorf("expected message to call out server-cert-mode, got: %s", res.Message)
	}
	auditProf := profileNameFor(sanitizeName("test.example.com"), "")
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.profiles[auditProf]; ok {
		t.Error("audit profile must NOT be created when bailing on wrong-mode default profile")
	}
	if got := strings.Join(fake.profiles[defaultProf], ","); got != "some_ca" {
		t.Errorf("default profile must be untouched, got %q", got)
	}
}

// ssl_profile + default_ssl_profile: when default_ssl_profile equals the
// audit profile name, the deploy must only PUT once (not twice).
func TestDeploySSLProfile_DefaultEqualsAudit_SinglePUT(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 8005)
	base := sanitizeName("test.example.com")
	auditProf := profileNameFor(base, "")
	old := "test_example_com_20260101"
	fake := &fakeForti{
		certs:         map[string]string{old: "placeholder"},
		profiles:      map[string][]string{auditProf: {old}},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": auditProf}
	res := deployForTest(t, fake, cfg, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if got := fake.putCount["ssl-ssh-profile:"+auditProf]; got != 1 {
		t.Errorf("expected exactly 1 PUT to %q, got %d", auditProf, got)
	}
	if _, ok := res.Details["default_profile"]; ok {
		t.Error("Details.default_profile must be omitted when default collapses to audit")
	}
}

// ssl_profile + default_ssl_profile: the production profile is one of two
// references to the cert; this must NOT trigger the foreign-reference bail.
func TestDeploySSLProfile_DefaultProfileReference_NotFlaggedAsForeign(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app1.example.com", 8006)
	base := sanitizeName("app1.example.com")
	auditProf := profileNameFor(base, "")
	defaultProf := "multi_domain_ssl_profile"
	pre := "preexisting_app1_cert"
	fake := &fakeForti{
		certs:         map[string]string{pre: certPEM},
		profiles:      map[string][]string{defaultProf: {pre, "app2_cert"}, auditProf: {pre}},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, fake, cfg, "app1.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success (not foreign), got: %s — details=%v", res.Message, res.Details)
	}
	if res.ResourceID != pre {
		t.Errorf("ResourceID = %q, want preexisting %q (idempotent)", res.ResourceID, pre)
	}
}

// ssl_profile + default_ssl_profile: bound_policies enumeration surfaces the
// firewall policies that will start serving the new cert.
func TestDeploySSLProfile_DefaultProfile_ReportsBoundPolicies(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app1.example.com", 8007)
	defaultProf := "multi_domain_ssl_profile"
	fake := &fakeForti{
		certs:         map[string]string{},
		profiles:      map[string][]string{defaultProf: {"sibling_cert"}},
		policies:      map[string]string{"WAN-to-DMZ": defaultProf, "Internal-to-DMZ": defaultProf, "Other-Policy": "different_profile"},
		vpnServercert: "x", adminCert: "y",
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, fake, cfg, "app1.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	bp, _ := res.Details["bound_policies"].([]string)
	if len(bp) != 2 || !slices.Contains(bp, "WAN-to-DMZ") || !slices.Contains(bp, "Internal-to-DMZ") {
		t.Errorf("Details.bound_policies = %v, want both WAN-to-DMZ and Internal-to-DMZ", bp)
	}
	if slices.Contains(bp, "Other-Policy") {
		t.Error("Details.bound_policies must not include policies bound to a different profile")
	}
}

// ssl_profile + same-day reissue: morning cert occupies <base>_YYYYMMDD;
// afternoon deploys a different serial under the same base on the same day.
// The deploy must NOT collide on the dated name — instead it suffix-probes
// to <base>_YYYYMMDD_01.
func TestDeploySSLProfile_SameDayCollision_UsesSequenceSuffix(t *testing.T) {
	base := sanitizeName("test.example.com")
	date := time.Now().UTC().Format("20060102")
	morningName := versionedName(base, date)
	morningPEM, _ := genTestCert(t, "test.example.com", 9001)
	afternoonPEM, afternoonKey := genTestCert(t, "test.example.com", 9002)
	fake := &fakeForti{
		certs:         map[string]string{morningName: morningPEM},
		profiles:      map[string][]string{"deep-inspection": {"tmpl_cert"}},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", afternoonPEM, afternoonKey)
	if !res.Success {
		t.Fatalf("expected success, got: %s — details=%v", res.Message, res.Details)
	}
	wantNew := suffixedVersionedName(base, date, 1)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.certs[wantNew]; !ok {
		t.Errorf("expected afternoon import as %q, certs=%v", wantNew, mapKeys(fake.certs))
	}
	if _, ok := fake.certs[morningName]; !ok {
		t.Errorf("morning cert %q must NOT be removed or overwritten", morningName)
	}
	if collided, _ := res.Details["same_day_collision"].(bool); !collided {
		t.Error("Details.same_day_collision must be true on suffix-probed import")
	}
}

// ssl_profile + same-day reissue: morning AND first afternoon names both
// taken; the deploy must probe past _01 to _02.
func TestDeploySSLProfile_SameDayCollision_ProbesUntilFree(t *testing.T) {
	base := sanitizeName("test.example.com")
	date := time.Now().UTC().Format("20060102")
	morning := versionedName(base, date)
	midday := suffixedVersionedName(base, date, 1)
	morningPEM, _ := genTestCert(t, "test.example.com", 9101)
	middayPEM, _ := genTestCert(t, "test.example.com", 9102)
	afternoonPEM, afternoonKey := genTestCert(t, "test.example.com", 9103)
	fake := &fakeForti{
		certs:         map[string]string{morning: morningPEM, midday: middayPEM},
		profiles:      map[string][]string{"deep-inspection": {"tmpl_cert"}},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", afternoonPEM, afternoonKey)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	want := suffixedVersionedName(base, date, 2)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.certs[want]; !ok {
		t.Errorf("expected import as %q, certs=%v", want, mapKeys(fake.certs))
	}
}

// ssl_profile + same-day re-trigger (same serial): findLocalCertBySerial hits
// the morning cert and we reuse it — no suffix probe, no new import.
func TestDeploySSLProfile_SameDaySameSerial_StaysIdempotent(t *testing.T) {
	base := sanitizeName("test.example.com")
	date := time.Now().UTC().Format("20060102")
	morningName := versionedName(base, date)
	certPEM, keyPEM := genTestCert(t, "test.example.com", 9200)
	fake := &fakeForti{
		certs:         map[string]string{morningName: certPEM},
		profiles:      map[string][]string{"deep-inspection": {"tmpl_cert"}},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.certs) != 1 {
		t.Errorf("expected no new import (idempotent), certs=%v", mapKeys(fake.certs))
	}
	if collided, _ := res.Details["same_day_collision"].(bool); collided {
		t.Error("Details.same_day_collision must be false on idempotent reuse")
	}
	if res.ResourceID != morningName {
		t.Errorf("ResourceID = %q, want %q", res.ResourceID, morningName)
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

// rebind same-day reissue: morning cert occupies <base>_YYYYMMDD; afternoon
// deploys a different serial under the same base on the same day. The fix
// imports as <base>_YYYYMMDD_01, rebinds refs there, and prunes the morning
// cert (it's in family and no longer referenced after the rebind).
func TestDeployRebind_SameDayCollision_UsesSequenceSuffixAndPrunesMorning(t *testing.T) {
	base := sanitizeName("*.factory.bg")
	date := time.Now().UTC().Format("20060102")
	morningName := versionedName(base, date)
	morningPEM, _ := genTestCert(t, "*.factory.bg", 7501)
	afternoonPEM, afternoonKey := genTestCert(t, "*.factory.bg", 7502)
	// Morning state: dated cert on device, referenced by a profile.
	fake := &fakeForti{
		certs:         map[string]string{morningName: morningPEM},
		profiles:      map[string][]string{"Jobs-Tech SSL Inspection": {morningName}},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake,
		map[string]any{"vdom": "root", "replace_strategy": "rebind"},
		"*.factory.bg", afternoonPEM, afternoonKey)
	if !res.Success {
		t.Fatalf("expected success, got: %s — details=%v", res.Message, res.Details)
	}
	wantNew := suffixedVersionedName(base, date, 1)
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if _, ok := fake.certs[wantNew]; !ok {
		t.Errorf("expected afternoon import as %q, certs=%v", wantNew, mapKeys(fake.certs))
	}
	// Morning cert should be pruned (in family, no longer referenced).
	if _, ok := fake.certs[morningName]; ok {
		t.Errorf("expected morning cert %q to be pruned, certs=%v", morningName, mapKeys(fake.certs))
	}
	// Profile must be rebound to the suffixed name.
	if got := strings.Join(fake.profiles["Jobs-Tech SSL Inspection"], ","); got != wantNew {
		t.Errorf("profile server-cert = %q, want %q", got, wantNew)
	}
	if collided, _ := res.Details["same_day_collision"].(bool); !collided {
		t.Error("Details.same_day_collision must be true on suffix-probed rebind")
	}
}

// rebind same-day re-trigger (same serial): findLocalCertBySerial reuses the
// existing name; no new import, no suffix probe. Was the only path that
// "accidentally worked" under the old code; must keep working under the new.
func TestDeployRebind_SameDaySameSerial_StaysIdempotent(t *testing.T) {
	base := sanitizeName("*.factory.bg")
	date := time.Now().UTC().Format("20060102")
	morningName := versionedName(base, date)
	certPEM, keyPEM := genTestCert(t, "*.factory.bg", 7600)
	fake := &fakeForti{
		certs:         map[string]string{morningName: certPEM},
		profiles:      map[string][]string{"Jobs-Tech SSL Inspection": {morningName}},
		vpnServercert: "x", adminCert: "y",
	}
	res := deployForTest(t, fake,
		map[string]any{"vdom": "root", "replace_strategy": "rebind"},
		"*.factory.bg", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.certs) != 1 {
		t.Errorf("expected no new import on same-serial retry, certs=%v", mapKeys(fake.certs))
	}
	if res.ResourceID != morningName {
		t.Errorf("ResourceID = %q, want %q", res.ResourceID, morningName)
	}
	if collided, _ := res.Details["same_day_collision"].(bool); collided {
		t.Error("Details.same_day_collision must be false on idempotent reuse")
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
