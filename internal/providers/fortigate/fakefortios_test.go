package fortigate

import (
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
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// A stateful fake FortiOS REST API shared by the provider tests. It records
// every call (method, path, body) so tests can pin request bodies field by
// field; the clock (now) and the delete-settle pause are injected.

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
	profExtra   map[string]map[string]any // extra (template) fields per profile
	created     map[string]map[string]any // POSTed profile bodies
	vpnCert     string
	adminCert   string
	vips        []map[string]any
	policies    []map[string]any
	listPEM     bool           // include the PEM in the certificate listing
	failGet     map[string]int // path → status for GETs
	getsOK      map[string]int // path → GETs served before failGet applies
	gets        map[string]int
	failPut     int
	failPutPath map[string]int
	failPost    int
	failDelete  int
	failImport  int
	importBody  string // body of a failing import response
	tokenHeader string
}

func newFakeFortiOS() *fakeFortiOS {
	return &fakeFortiOS{
		certs: map[string]string{}, profiles: map[string]*sslSSHProfile{}, profExtra: map[string]map[string]any{},
		created: map[string]map[string]any{}, failGet: map[string]int{}, getsOK: map[string]int{}, gets: map[string]int{},
		failPutPath: map[string]int{},
	}
}

func (f *fakeFortiOS) addCert(name, pemStr string) {
	if _, ok := f.certs[name]; !ok {
		f.order = append(f.order, name)
	}
	f.certs[name] = pemStr
}

func (f *fakeFortiOS) addProfile(name, mode string, certs ...string) *sslSSHProfile {
	p := &sslSSHProfile{Name: name, ServerCertMode: mode}
	for _, c := range certs {
		p.ServerCert = append(p.ServerCert, namedRef{c})
	}
	f.profiles[name] = p
	return p
}

func (f *fakeFortiOS) referenced(name string) bool {
	for _, p := range f.profiles {
		if containsName(p.ServerCert, name) {
			return true
		}
	}
	for _, v := range f.vips {
		switch sc := v["ssl-certificate"].(type) {
		case string:
			if sc == name {
				return true
			}
		case []any:
			for _, e := range sc {
				if m, _ := e.(map[string]any); m["name"] == name {
					return true
				}
			}
		}
	}
	return f.vpnCert == name || f.adminCert == name
}

func reply(w http.ResponseWriter, status int, results any) {
	w.WriteHeader(status)
	st := "success"
	if status >= 400 {
		st = "error"
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"http_status": status, "status": st, "results": results})
}

func (f *fakeFortiOS) profileJSON(p *sslSSHProfile) map[string]any {
	out := map[string]any{}
	for k, v := range f.profExtra[p.Name] {
		out[k] = v
	}
	sc := []any{}
	for _, e := range p.ServerCert {
		sc = append(sc, map[string]any{"name": e.Name, "q_origin_key": e.Name})
	}
	out["name"] = p.Name
	out["q_origin_key"] = p.Name
	out["server-cert"] = sc
	out["server-cert-mode"] = p.ServerCertMode
	return out
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
		if r.Method == http.MethodGet {
			f.gets[p]++
			if code := f.failGet[p]; code != 0 && f.gets[p] > f.getsOK[p] {
				w.WriteHeader(code)
				_, _ = io.WriteString(w, `{"status":"error","error":-1}`)
				return
			}
		}
		if code := f.failPutPath[p]; code != 0 && r.Method == http.MethodPut {
			w.WriteHeader(code)
			_, _ = io.WriteString(w, `{"status":"error","error":-1}`)
			return
		}
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		const local, prof = "cmdb/certificate/local", "cmdb/firewall/ssl-ssh-profile"
		switch {
		case p == "cmdb/system/global" && r.Method == http.MethodPut:
			if v, ok := body["admin-server-cert"].(string); ok {
				f.adminCert = v
			}
			reply(w, 200, nil)
		case p == "cmdb/system/global":
			reply(w, 200, map[string]any{"admin-server-cert": f.adminCert})
		case p == "cmdb/vpn.ssl/settings" && r.Method == http.MethodPut:
			if v, ok := body["servercert"].(string); ok {
				f.vpnCert = v
			}
			reply(w, 200, nil)
		case p == "cmdb/vpn.ssl/settings":
			reply(w, 200, map[string]any{"servercert": f.vpnCert})
		case p == "cmdb/firewall/vip":
			reply(w, 200, f.vips)
		case strings.HasPrefix(p, "cmdb/firewall/vip/") && r.Method == http.MethodPut:
			n := strings.TrimPrefix(p, "cmdb/firewall/vip/")
			for _, v := range f.vips {
				if v["name"] == n {
					v["ssl-certificate"] = body["ssl-certificate"]
				}
			}
			reply(w, 200, nil)
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
				if f.failDelete != 0 {
					w.WriteHeader(f.failDelete)
					_, _ = io.WriteString(w, `{"status":"error","error":-1}`)
					return
				}
				if f.referenced(n) {
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
				_, _ = io.WriteString(w, f.importBody)
				return
			}
			n, _ := body["certname"].(string)
			if _, dup := f.certs[n]; dup {
				w.WriteHeader(500)
				_, _ = io.WriteString(w, `{"status":"error","error":-145}`)
				return
			}
			dec, _ := base64.StdEncoding.DecodeString(body["file_content"].(string))
			f.addCert(n, string(dec))
			reply(w, 200, nil)
		case p == prof && r.Method == http.MethodPost:
			if f.failPost != 0 {
				w.WriteHeader(f.failPost)
				_, _ = io.WriteString(w, `{"status":"error","error":-651}`)
				return
			}
			n, _ := body["name"].(string)
			f.created[n] = body
			var upd sslSSHProfile
			_ = json.Unmarshal(raw, &upd)
			f.profiles[n] = &upd
			reply(w, 200, nil)
		case p == prof:
			names := make([]string, 0, len(f.profiles))
			for n := range f.profiles {
				names = append(names, n)
			}
			sort.Strings(names)
			list := []map[string]any{}
			for _, n := range names {
				list = append(list, f.profileJSON(f.profiles[n]))
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
			reply(w, 200, []map[string]any{f.profileJSON(x)})
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

// bodies returns the decoded request bodies of method+path, in order.
func (f *fakeFortiOS) bodies(method, path string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []map[string]any
	for _, c := range f.calls {
		if c.method == method && c.path == path {
			var m map[string]any
			_ = json.Unmarshal([]byte(c.body), &m)
			out = append(out, m)
		}
	}
	return out
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

func (f *fakeFortiOS) serverCerts(profile string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, ok := f.profiles[profile]
	if !ok {
		return nil
	}
	out := []string{}
	for _, e := range p.ServerCert {
		out = append(out, e.Name)
	}
	return out
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

func noSettle(t *testing.T) *int {
	t.Helper()
	n := 0
	old := settle
	settle = func() { n++ }
	t.Cleanup(func() { settle = old })
	return &n
}

func fgCreds(srv *httptest.Server) map[string]any {
	return map[string]any{"host": strings.TrimPrefix(srv.URL, "https://"), "api_token": testToken}
}

func newCertData(t *testing.T, serial int64) *provider.CertificateData {
	c, k := certWithSerial(t, "www.example.com", serial)
	return &provider.CertificateData{ID: "abcdef1234567890", CommonName: "www.example.com", CertificatePEM: c, PrivateKeyPEM: k}
}

// noSecrets fails when the API token or private key material appears in a
// result message, its details or an error.
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
		if strings.Contains(s, testToken) || strings.Contains(s, "PRIVATE KEY") {
			t.Fatalf("secret material in %q", s)
		}
		if cd != nil && cd.PrivateKeyPEM != "" {
			key := strings.TrimSpace(cd.PrivateKeyPEM)
			if strings.Contains(s, key) || strings.Contains(s, base64.StdEncoding.EncodeToString([]byte(cd.PrivateKeyPEM))) {
				t.Fatalf("private key in %q", s)
			}
		}
	}
}

func assertList(t *testing.T, got []string, want ...string) {
	t.Helper()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("list = %v, want %v", got, want)
	}
}
