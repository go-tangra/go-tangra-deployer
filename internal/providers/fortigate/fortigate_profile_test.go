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
	"errors"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// T107 (033 US8, research D27): default_ssl_profile mode against a stateful
// fake FortiOS that records every call; the clock is injected.

func certWithSerial(t *testing.T, cn string, serial int64) (certPEM, keyPEM string) {
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
	keyDER, _ := x509.MarshalPKCS8PrivateKey(key)
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})),
		string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
}

type fgCall struct{ method, path, body string }

type fakeFortiOS struct {
	mu          sync.Mutex
	calls       []fgCall
	certs       map[string]string // name → PEM
	order       []string          // listing order
	profiles    map[string]*sslSSHProfile
	vpnCert     string
	adminCert   string
	vips        []map[string]any
	policies    []map[string]any
	listPEM     bool // include the PEM in the certificate listing
	failGet     map[string]int
	failPut     int
	failDelete  int
	failImport  int
	tokenHeader string
}

func newFakeFortiOS() *fakeFortiOS {
	return &fakeFortiOS{certs: map[string]string{}, profiles: map[string]*sslSSHProfile{}, failGet: map[string]int{}}
}

func (f *fakeFortiOS) addCert(name, pemStr string) {
	f.certs[name] = pemStr
	f.order = append(f.order, name)
}

func (f *fakeFortiOS) referenced(name string) bool {
	for _, p := range f.profiles {
		if containsName(p.ServerCert, name) {
			return true
		}
	}
	return f.vpnCert == name || f.adminCert == name
}

func reply(w http.ResponseWriter, status int, results any) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"http_status": status, "status": "success", "results": results})
}

func (f *fakeFortiOS) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		raw, _ := io.ReadAll(r.Body)
		f.tokenHeader = r.Header.Get("Authorization")
		p := strings.TrimPrefix(r.URL.Path, "/api/v2/")
		f.calls = append(f.calls, fgCall{r.Method, p, string(raw)})
		if code := f.failGet[p]; code != 0 && r.Method == http.MethodGet {
			w.WriteHeader(code)
			_, _ = io.WriteString(w, `{"status":"error","error":-1}`)
			return
		}
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		const local, prof = "cmdb/certificate/local", "cmdb/firewall/ssl-ssh-profile"
		switch {
		case p == "cmdb/system/global":
			reply(w, 200, map[string]any{"admin-server-cert": f.adminCert})
		case p == "cmdb/vpn.ssl/settings":
			reply(w, 200, map[string]any{"servercert": f.vpnCert})
		case p == "cmdb/firewall/vip":
			reply(w, 200, f.vips)
		case p == "cmdb/firewall/policy":
			reply(w, 200, f.policies)
		case p == local:
			list := []map[string]any{}
			for _, n := range f.order {
				if pemStr, ok := f.certs[n]; ok {
					e := map[string]any{"name": n}
					if f.listPEM {
						e["certificate"] = pemStr
					}
					list = append(list, e)
				}
			}
			reply(w, 200, list)
		case strings.HasPrefix(p, local+"/"):
			n := strings.TrimPrefix(p, local+"/")
			pemStr, ok := f.certs[n]
			switch {
			case !ok:
				reply(w, 404, nil)
			case r.Method == http.MethodDelete:
				if f.failDelete != 0 || f.referenced(n) {
					w.WriteHeader(500)
					_, _ = io.WriteString(w, `{"status":"error","error":-23}`)
					return
				}
				delete(f.certs, n)
				reply(w, 200, nil)
			default:
				reply(w, 200, []map[string]any{{"name": n, "certificate": pemStr}})
			}
		case p == "monitor/vpn-certificate/local/import":
			if f.failImport != 0 {
				w.WriteHeader(f.failImport)
				return
			}
			n, _ := body["certname"].(string)
			if _, dup := f.certs[n]; dup {
				w.WriteHeader(500)
				return
			}
			dec, _ := base64.StdEncoding.DecodeString(body["file_content"].(string))
			f.addCert(n, string(dec))
			reply(w, 200, nil)
		case p == prof:
			list := []*sslSSHProfile{}
			for _, x := range f.profiles {
				list = append(list, x)
			}
			reply(w, 200, list)
		case strings.HasPrefix(p, prof+"/"):
			n := strings.TrimPrefix(p, prof+"/")
			x, ok := f.profiles[n]
			if !ok {
				reply(w, 404, nil)
				return
			}
			if r.Method == http.MethodPut {
				if f.failPut != 0 {
					w.WriteHeader(f.failPut)
					return
				}
				var upd sslSSHProfile
				_ = json.Unmarshal(raw, &upd)
				x.ServerCert = upd.ServerCert
			}
			reply(w, 200, []*sslSSHProfile{x})
		default:
			reply(w, 404, nil)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeFortiOS) count(method, prefix string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.method == method && strings.HasPrefix(c.path, prefix) {
			n++
		}
	}
	return n
}

func (f *fakeFortiOS) writes() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if c.method != http.MethodGet {
			n++
		}
	}
	return n
}

func fixedClock(t *testing.T, day string) {
	t.Helper()
	ts, err := time.Parse("20060102", day)
	if err != nil {
		t.Fatal(err)
	}
	old := now
	now = func() time.Time { return ts.Add(13 * time.Hour) }
	t.Cleanup(func() { now = old })
}

func profileCfg() map[string]any {
	return map[string]any{"vdom": "root", "default_ssl_profile": "inbound-www"}
}

func fgCreds(srv *httptest.Server) map[string]any {
	return map[string]any{"host": strings.TrimPrefix(srv.URL, "https://"), "api_token": testToken}
}

func newCertData(t *testing.T, serial int64) *provider.CertificateData {
	c, k := certWithSerial(t, "www.example.com", serial)
	return &provider.CertificateData{ID: "abcdef1234567890", CommonName: "www.example.com", CertificatePEM: c, PrivateKeyPEM: k}
}

func noSecrets(t *testing.T, cd *provider.CertificateData, res *provider.Result, err error) {
	t.Helper()
	var texts []string
	if res != nil {
		b, _ := json.Marshal(res.Details)
		texts = append(texts, res.Message, string(b))
	}
	if err != nil {
		texts = append(texts, err.Error())
	}
	for _, s := range texts {
		if strings.Contains(s, testToken) || (cd != nil && strings.Contains(s, strings.TrimSpace(cd.PrivateKeyPEM))) || strings.Contains(s, "PRIVATE KEY") {
			t.Fatalf("secret material in %q", s)
		}
	}
}

func TestProfileDeployImportsDatedNameAndReplacesFamilyEntry(t *testing.T) {
	fixedClock(t, "20261002")
	f := newFakeFortiOS()
	oldPEM, _ := certWithSerial(t, "www.example.com", 1)
	f.addCert("www_example_com", oldPEM)
	f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCertMode: "replace",
		ServerCert: []namedRef{{"shop_example_com"}, {"www_example_com"}, {"api_example_com_20260101"}}}
	f.policies = []map[string]any{{"name": "in-www", "ssl-ssh-profile": "inbound-www"}, {"policyid": float64(7), "ssl-ssh-profile": "inbound-www"}, {"name": "x", "ssl-ssh-profile": "other"}}
	srv := f.serve(t)
	cd := newCertData(t, 2)

	var pct int
	res, err := (Provider{}).Deploy(context.Background(), cd, profileCfg(), fgCreds(srv), func(p int, _ string) { pct = p })
	noSecrets(t, cd, res, err)
	if err != nil || !res.Success || pct != 100 {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	assertNames(t, f.profiles["inbound-www"].ServerCert, "shop_example_com", "www_example_com_20261002", "api_example_com_20260101")
	if _, ok := f.certs["www_example_com"]; !ok {
		t.Fatal("old certificate deleted")
	}
	if f.count(http.MethodDelete, "") != 0 || f.count(http.MethodPost, "cmdb/") != 0 {
		t.Fatal("delete or create during deploy")
	}
	if f.count(http.MethodPut, "") != 1 {
		t.Fatalf("puts = %d", f.count(http.MethodPut, ""))
	}
	var put map[string]any
	for _, c := range f.calls {
		if c.method == http.MethodPut {
			_ = json.Unmarshal([]byte(c.body), &put)
		}
	}
	if len(put) != 1 || put["server-cert"] == nil {
		t.Fatalf("PUT body = %v", put)
	}
	d := res.Details
	if d["certificate_name"] != "www_example_com_20261002" || d["imported"] != true || d["profile_action"] != actionUpdated || d["ssl_profile"] != "inbound-www" {
		t.Fatalf("details = %v", d)
	}
	if b := d["bound_policies"].([]string); len(b) != 2 || b[0] != "in-www" || b[1] != "#7" {
		t.Fatalf("bound_policies = %v", b)
	}
	if strings.Contains(f.tokenHeader, testToken) == false {
		t.Fatal("token not sent")
	}

	// Idempotent: the same certificate again is reused, nothing written.
	before := f.writes()
	res, err = (Provider{}).Deploy(context.Background(), cd, profileCfg(), fgCreds(srv), nil)
	if err != nil || !res.Success || res.Details["imported"] != false || res.Details["profile_action"] != actionUnchanged {
		t.Fatalf("redeploy = %+v, %v", res, err)
	}
	if f.writes() != before {
		t.Fatal("redeploy wrote to the device")
	}

	// Verify: serial present and listed.
	vr, err := (Provider{}).Verify(context.Background(), cd, profileCfg(), fgCreds(srv))
	if err != nil || !vr.Success {
		t.Fatalf("verify = %+v, %v", vr, err)
	}
}

func TestProfileDeploySameDayCollisionAndAppend(t *testing.T) {
	fixedClock(t, "20261002")
	f := newFakeFortiOS()
	f.listPEM = true
	morning, _ := certWithSerial(t, "www.example.com", 10)
	f.addCert("www_example_com_20261002", morning)
	otherPEM, _ := certWithSerial(t, "shop.example.com", 11)
	f.addCert("www_example_com_20261002_01", otherPEM)
	f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCert: []namedRef{{"shop_example_com"}}}
	f.failGet["cmdb/firewall/policy"] = 500
	srv := f.serve(t)
	cd := newCertData(t, 12)

	res, err := (Provider{}).Deploy(context.Background(), cd, profileCfg(), fgCreds(srv), nil)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	if res.Details["certificate_name"] != "www_example_com_20261002_02" || res.Details["same_day_collision"] != true || res.Details["profile_action"] != actionAppended {
		t.Fatalf("details = %v", res.Details)
	}
	assertNames(t, f.profiles["inbound-www"].ServerCert, "shop_example_com", "www_example_com_20261002_02")
	if res.Details["bound_policies"] != nil && len(res.Details["bound_policies"].([]string)) != 0 {
		t.Fatalf("bound_policies on failure = %v", res.Details["bound_policies"])
	}
}

func TestProfileDeployManualReview(t *testing.T) {
	fixedClock(t, "20261002")
	cases := []struct {
		name   string
		setup  func(f *fakeFortiOS)
		reason string
		refs   []string
	}{
		{"profile missing", func(f *fakeFortiOS) { delete(f.profiles, "inbound-www") }, "does not exist", nil},
		{"wrong mode", func(f *fakeFortiOS) { f.profiles["inbound-www"].ServerCertMode = "re-sign" }, "server-cert-mode", nil},
		{"vip", func(f *fakeFortiOS) {
			f.vips = []map[string]any{{"name": "vip-www", "ssl-certificate": []any{map[string]any{"name": "www_example_com"}}}}
		}, "outside the profile", []string{holderVIP + ":vip-www"}},
		{"vip scalar", func(f *fakeFortiOS) {
			f.vips = []map[string]any{{"name": "vip-old", "ssl-certificate": "www_example_com_20250101"}}
		}, "outside the profile", []string{holderVIP + ":vip-old"}},
		{"ssl-vpn", func(f *fakeFortiOS) { f.vpnCert = "www_example_com" }, "outside the profile", []string{holderSSLVPN + ":vpn.ssl/settings"}},
		{"admin gui", func(f *fakeFortiOS) { f.adminCert = "www_example_com" }, "outside the profile", []string{holderAdminGUI + ":system/global"}},
		{"other profile", func(f *fakeFortiOS) {
			f.profiles["other"] = &sslSSHProfile{Name: "other", ServerCert: []namedRef{{"www_example_com"}}}
		}, "outside the profile", []string{holderSSLSSHProfile + ":other"}},
		{"scan error", func(f *fakeFortiOS) { f.failGet["cmdb/firewall/vip"] = 500 }, "could not scan", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeFortiOS()
			oldPEM, _ := certWithSerial(t, "www.example.com", 1)
			f.addCert("www_example_com", oldPEM)
			f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCertMode: "replace", ServerCert: []namedRef{{"www_example_com"}}}
			tc.setup(f)
			srv := f.serve(t)
			cd := newCertData(t, 2)
			res, err := (Provider{}).Deploy(context.Background(), cd, profileCfg(), fgCreds(srv), nil)
			noSecrets(t, cd, res, err)
			if err != nil || res.Success || !res.Permanent || !strings.HasPrefix(res.Message, "MANUAL REVIEW REQUIRED") {
				t.Fatalf("result = %+v, %v", res, err)
			}
			d := res.Details
			if d["manual_review_required"] != true || !strings.Contains(d["reason"].(string), tc.reason) || d["imported"] != false {
				t.Fatalf("details = %v", d)
			}
			refs := d["foreign_references"].([]string)
			if len(refs) != len(tc.refs) || (len(refs) > 0 && refs[0] != tc.refs[0]) {
				t.Fatalf("refs = %v, want %v", refs, tc.refs)
			}
			if f.writes() != 0 {
				t.Fatalf("writes on manual review: %+v", f.calls)
			}
		})
	}
}

func TestProfileDeployErrors(t *testing.T) {
	fixedClock(t, "20261002")
	setup := func() *fakeFortiOS {
		f := newFakeFortiOS()
		f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCertMode: "replace"}
		return f
	}
	for _, tc := range []struct {
		name string
		mod  func(f *fakeFortiOS)
	}{
		{"list error", func(f *fakeFortiOS) { f.failGet["cmdb/certificate/local"] = 500 }},
		{"profile read error", func(f *fakeFortiOS) { f.failGet["cmdb/firewall/ssl-ssh-profile/inbound-www"] = 500 }},
		{"name probe error", func(f *fakeFortiOS) { f.failGet["cmdb/certificate/local/www_example_com_20261002"] = 500 }},
		{"import error", func(f *fakeFortiOS) { f.failImport = 500 }},
		{"put error", func(f *fakeFortiOS) { f.failPut = 500 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup()
			tc.mod(f)
			srv := f.serve(t)
			cd := newCertData(t, 3)
			res, err := (Provider{}).Deploy(context.Background(), cd, profileCfg(), fgCreds(srv), nil)
			noSecrets(t, cd, res, err)
			if err == nil {
				t.Fatalf("expected a retryable error, got %+v", res)
			}
			if f.count(http.MethodDelete, "") != 0 {
				t.Fatal("delete on error")
			}
		})
	}

	// No free same-day name.
	f := setup()
	pemStr, _ := certWithSerial(t, "x", 50)
	f.addCert("www_example_com_20261002", pemStr)
	for i := 1; i <= maxSameDaySequence; i++ {
		f.addCert(suffixedVersionedName("www_example_com", "20261002", i), pemStr)
	}
	srv := f.serve(t)
	if _, err := (Provider{}).Deploy(context.Background(), newCertData(t, 4), profileCfg(), fgCreds(srv), nil); err == nil {
		t.Fatal("expected exhaustion error")
	}

	// Unparseable certificate and invalid profile name.
	f = setup()
	srv = f.serve(t)
	bad := &provider.CertificateData{CommonName: "www.example.com", CertificatePEM: "junk", PrivateKeyPEM: "k"}
	if _, err := (Provider{}).Deploy(context.Background(), bad, profileCfg(), fgCreds(srv), nil); err == nil {
		t.Fatal("expected parse error")
	}
	res, err := (Provider{}).Deploy(context.Background(), newCertData(t, 5), map[string]any{"default_ssl_profile": `a"b`}, fgCreds(srv), nil)
	if err != nil || res.Success || !res.Permanent || strings.Contains(res.Message, `a"b`) {
		t.Fatalf("invalid name = %+v, %v", res, err)
	}
}

func TestProfileVerify(t *testing.T) {
	f := newFakeFortiOS()
	cd := newCertData(t, 20)
	f.addCert("www_example_com_20261002", cd.CertificatePEM)
	f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCert: []namedRef{{"other"}}}
	srv := f.serve(t)
	ctx := context.Background()

	res, _ := (Provider{}).Verify(ctx, cd, profileCfg(), fgCreds(srv))
	if res.Success || !strings.Contains(res.Message, "not listed") {
		t.Fatalf("unlisted verify = %+v", res)
	}
	res, _ = (Provider{}).Verify(ctx, newCertData(t, 21), profileCfg(), fgCreds(srv))
	if res.Success || !strings.Contains(res.Message, "not found") {
		t.Fatalf("absent verify = %+v", res)
	}
	delete(f.profiles, "inbound-www")
	res, _ = (Provider{}).Verify(ctx, cd, profileCfg(), fgCreds(srv))
	if res.Success || !strings.Contains(res.Message, "profile") {
		t.Fatalf("missing profile verify = %+v", res)
	}
	f.failGet["cmdb/firewall/ssl-ssh-profile/inbound-www"] = 500
	if res, _ = (Provider{}).Verify(ctx, cd, profileCfg(), fgCreds(srv)); res.Success {
		t.Fatal("profile error verify succeeded")
	}
	f.failGet["cmdb/certificate/local"] = 500
	if res, _ = (Provider{}).Verify(ctx, cd, profileCfg(), fgCreds(srv)); res.Success {
		t.Fatal("list error verify succeeded")
	}
	if res, err := (Provider{}).Verify(ctx, cd, map[string]any{"default_ssl_profile": "a/b"}, fgCreds(srv)); err != nil || res.Success || !res.Permanent {
		t.Fatalf("invalid name verify = %+v, %v", res, err)
	}
	if f.writes() != 0 {
		t.Fatal("verify wrote to the device")
	}
}

func TestProfileRollback(t *testing.T) {
	ctx := context.Background()
	build := func(t *testing.T, cd *provider.CertificateData) *fakeFortiOS {
		f := newFakeFortiOS()
		bare, _ := certWithSerial(t, "www.example.com", 1)
		older, _ := certWithSerial(t, "www.example.com", 2)
		f.addCert("www_example_com", bare)
		f.addCert("www_example_com_20260101", older)
		f.addCert("www_example_com_20261002", cd.CertificatePEM)
		f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCert: []namedRef{{"shop"}, {"www_example_com_20261002"}}}
		return f
	}

	t.Run("re-points and deletes", func(t *testing.T) {
		cd := newCertData(t, 30)
		f := build(t, cd)
		srv := f.serve(t)
		res, err := (Provider{}).Rollback(ctx, cd, profileCfg(), fgCreds(srv))
		noSecrets(t, cd, res, err)
		if err != nil || !res.Success || res.Details["restored_certificate"] != "www_example_com_20260101" || res.Details["certificate_deleted"] != true {
			t.Fatalf("rollback = %+v, %v", res, err)
		}
		assertNames(t, f.profiles["inbound-www"].ServerCert, "shop", "www_example_com_20260101")
		if _, ok := f.certs["www_example_com_20261002"]; ok {
			t.Fatal("deployed certificate not deleted")
		}
	})

	t.Run("still referenced elsewhere", func(t *testing.T) {
		cd := newCertData(t, 31)
		f := build(t, cd)
		f.vpnCert = "www_example_com_20261002"
		srv := f.serve(t)
		res, err := (Provider{}).Rollback(ctx, cd, profileCfg(), fgCreds(srv))
		if err != nil || !res.Success || res.Details["certificate_deleted"] != false || !strings.Contains(res.Message, "still referenced") {
			t.Fatalf("rollback = %+v, %v", res, err)
		}
		if _, ok := f.certs["www_example_com_20261002"]; !ok {
			t.Fatal("referenced certificate deleted")
		}
	})

	t.Run("no previous certificate", func(t *testing.T) {
		cd := newCertData(t, 32)
		f := newFakeFortiOS()
		f.addCert("www_example_com_20261002", cd.CertificatePEM)
		f.addCert("www_example_com_LE", cd.CertificatePEM+"x")
		f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www", ServerCert: []namedRef{{"www_example_com_20261002"}}}
		srv := f.serve(t)
		res, err := (Provider{}).Rollback(ctx, cd, profileCfg(), fgCreds(srv))
		if err != nil || res.Success || res.Message != "no previous certificate to restore; profile unchanged" {
			t.Fatalf("rollback = %+v, %v", res, err)
		}
		if f.writes() != 0 {
			t.Fatal("rollback wrote without a previous certificate")
		}
	})

	t.Run("failures", func(t *testing.T) {
		for _, tc := range []struct {
			name string
			mod  func(f *fakeFortiOS)
			cd   func(cd *provider.CertificateData) *provider.CertificateData
		}{
			{"list error", func(f *fakeFortiOS) { f.failGet["cmdb/certificate/local"] = 500 }, nil},
			{"deployed absent", func(f *fakeFortiOS) {}, func(*provider.CertificateData) *provider.CertificateData { return newCertData(t, 99) }},
			{"profile missing", func(f *fakeFortiOS) { delete(f.profiles, "inbound-www") }, nil},
			{"profile error", func(f *fakeFortiOS) { f.failGet["cmdb/firewall/ssl-ssh-profile/inbound-www"] = 500 }, nil},
			{"put error", func(f *fakeFortiOS) { f.failPut = 500 }, nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				cd := newCertData(t, 40)
				f := build(t, cd)
				tc.mod(f)
				if tc.cd != nil {
					cd = tc.cd(cd)
				}
				srv := f.serve(t)
				res, err := (Provider{}).Rollback(ctx, cd, profileCfg(), fgCreds(srv))
				if err != nil || res.Success {
					t.Fatalf("rollback = %+v, %v", res, err)
				}
				if f.count(http.MethodDelete, "") != 0 {
					t.Fatal("deleted on failure")
				}
			})
		}
	})

	t.Run("invalid name", func(t *testing.T) {
		cd := newCertData(t, 41)
		f := build(t, cd)
		srv := f.serve(t)
		if res, err := (Provider{}).Rollback(ctx, cd, map[string]any{"default_ssl_profile": "a\\b"}, fgCreds(srv)); err != nil || res.Success {
			t.Fatalf("rollback = %+v, %v", res, err)
		}
	})
}

func TestValidateCredentialsDefaultProfile(t *testing.T) {
	f := newFakeFortiOS()
	f.profiles["inbound-www"] = &sslSSHProfile{Name: "inbound-www"}
	srv := f.serve(t)
	ctx := context.Background()
	if err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), profileCfg()); err != nil {
		t.Fatalf("present: %v", err)
	}
	var fe *provider.FieldError
	err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), map[string]any{"default_ssl_profile": "missing"})
	if !errors.As(err, &fe) || fe.Field != "config.default_ssl_profile" || fe.Msg != provider.CodeNotFoundOnEndpoint {
		t.Fatalf("missing: %v", err)
	}
	err = (Provider{}).ValidateCredentials(ctx, fgCreds(srv), map[string]any{"default_ssl_profile": "a/b"})
	if !errors.As(err, &fe) || fe.Msg != provider.CodePattern {
		t.Fatalf("pattern: %v", err)
	}
	f.failGet["cmdb/firewall/ssl-ssh-profile/x"] = 500
	if err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), map[string]any{"default_ssl_profile": "x"}); err != nil {
		t.Fatalf("profile error is not not-found: %v", err)
	}
	if f.writes() != 0 {
		t.Fatal("validation wrote")
	}
}

func TestDefaultProfileDescriptor(t *testing.T) {
	caps := Provider{}.Capabilities()
	for _, bad := range []string{"a/b", `a"b`, "a\\b", "x\ny", strings.Repeat("a", 36)} {
		if errs, _ := provider.ValidateInput(caps, map[string]any{"vdom": "root", "default_ssl_profile": bad}, nil, provider.ModeConfiguration); errs["config.default_ssl_profile"] == "" {
			t.Errorf("%q accepted", bad)
		}
	}
	if errs, _ := provider.ValidateInput(caps, map[string]any{"vdom": "root", "default_ssl_profile": "inbound-www 2"}, nil, provider.ModeConfiguration); errs["config.default_ssl_profile"] != "" {
		t.Errorf("valid name refused: %v", errs)
	}
}

func TestProfileClientEdgeCases(t *testing.T) {
	ctx := context.Background()
	body := map[string]string{
		"/api/v2/cmdb/certificate/local":              `{"results":{"not":"a list"}}`,
		"/api/v2/cmdb/certificate/local/empty":        `{"results":[]}`,
		"/api/v2/cmdb/certificate/local/bad":          `{"results":"x"}`,
		"/api/v2/cmdb/firewall/ssl-ssh-profile/empty": `{"results":[]}`,
		"/api/v2/cmdb/firewall/ssl-ssh-profile/bad":   `{"results":"x"}`,
		"/api/v2/cmdb/certificate/local/garbage":      `{"results":[{"name":"garbage","certificate":"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"}]}`,
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b, ok := body[r.URL.Path]; ok {
			_, _ = io.WriteString(w, b)
			return
		}
		w.WriteHeader(500)
	}))
	c := newClient(fgCreds(srv), map[string]any{})
	if _, err := c.listLocalCerts(ctx); err == nil {
		t.Error("bad listing decoded")
	}
	if pemStr, err := c.getLocalCertPEM(ctx, "empty"); err != nil || pemStr != "" {
		t.Errorf("empty = %q, %v", pemStr, err)
	}
	if _, err := c.getLocalCertPEM(ctx, "bad"); err == nil {
		t.Error("bad cert decoded")
	}
	if _, err := c.getLocalCertPEM(ctx, "boom"); err == nil {
		t.Error("500 accepted")
	}
	if n := c.findLocalCertBySerial(ctx, []localCert{{Name: "boom"}, {Name: "empty"}, {Name: "garbage"}}, "1"); n != "" {
		t.Errorf("found %q", n)
	}
	if _, found, err := c.getSSLSSHProfile(ctx, "empty"); err != nil || found {
		t.Errorf("empty profile = %v, %v", found, err)
	}
	if _, _, err := c.getSSLSSHProfile(ctx, "bad"); err == nil {
		t.Error("bad profile decoded")
	}
	if err := c.setSSLSSHProfileServerCert(ctx, "x", nil); err == nil {
		t.Error("500 PUT accepted")
	}
	srv.Close()
	if _, err := c.listLocalCerts(ctx); err == nil {
		t.Error("closed server listed")
	}
	if _, err := c.getLocalCertPEM(ctx, "x"); err == nil {
		t.Error("closed server read")
	}
	if _, _, err := c.getSSLSSHProfile(ctx, "x"); err == nil {
		t.Error("closed server profile")
	}
	if err := c.setSSLSSHProfileServerCert(ctx, "x", nil); err == nil {
		t.Error("closed server PUT")
	}
	if err := c.cmdbGet(ctx, "x", nil); err == nil {
		t.Error("closed server cmdb")
	}
}
