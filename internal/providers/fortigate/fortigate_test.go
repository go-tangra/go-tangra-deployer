package fortigate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

const testToken = "SUPERSECRETTOKEN-do-not-leak-123"

// makeCert returns a self-signed leaf cert + key PEM with the given common name.
func makeCert(t *testing.T, cn string) (certPEM, keyPEM string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("genkey: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(4242),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     []string{cn},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("createcert: %v", err)
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatalf("marshalkey: %v", err)
	}
	certPEM = string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	keyPEM = string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}))
	return certPEM, keyPEM
}

func testCert(t *testing.T) *provider.CertificateData {
	certPEM, keyPEM := makeCert(t, "www.example.com")
	return &provider.CertificateData{
		ID:             "abcdef1234567890",
		SerialNumber:   "4242",
		CommonName:     "www.example.com",
		CertificatePEM: certPEM,
		PrivateKeyPEM:  keyPEM,
	}
}

func hostOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	return strings.TrimPrefix(srv.URL, "https://")
}

// captured records what the mock FortiGate observed.
type captured struct {
	authHeader string
	vdom       string
	importSeen bool
}

func TestRegistrationAndCapabilities(t *testing.T) {
	if !provider.Exists("fortigate") {
		t.Fatal("fortigate provider not registered in init()")
	}
	got := Provider{}.Capabilities()
	want := provider.Capabilities{
		Type:             "fortigate",
		DisplayName:      "FortiGate",
		SupportsVerify:   true,
		SupportsRollback: true,
		ConfigFields: []provider.Field{
			{Key: "vdom", Label: "VDOM", Required: true},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "FortiGate host", Required: true},
			{Key: "api_token", Label: "API token", Secret: true, Required: true},
		},
	}
	if got.Type != want.Type || got.DisplayName != want.DisplayName ||
		got.SupportsVerify != want.SupportsVerify || got.SupportsRollback != want.SupportsRollback {
		t.Fatalf("capabilities scalar mismatch:\n got=%+v\nwant=%+v", got, want)
	}
	// Keys, required and secret flags; the full descriptors are pinned by the
	// golden catalogue in internal/providers/all.
	same := func(a, b provider.Field) bool {
		return a.Key == b.Key && a.Required == b.Required && a.Secret == b.Secret
	}
	if len(got.ConfigFields) < 1 || !same(got.ConfigFields[0], want.ConfigFields[0]) {
		t.Fatalf("config fields mismatch: %+v", got.ConfigFields)
	}
	if len(got.CredentialFields) != 2 ||
		!same(got.CredentialFields[0], want.CredentialFields[0]) ||
		!same(got.CredentialFields[1], want.CredentialFields[1]) {
		t.Fatalf("credential fields mismatch: %+v", got.CredentialFields)
	}
}

func TestDeployHappyPath(t *testing.T) {
	cap := &captured{}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cap.authHeader = r.Header.Get("Authorization")
		cap.vdom = r.URL.Query().Get("vdom")
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/monitor/vpn-certificate/local/import"):
			cap.importSeen = true
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			if cap.importSeen {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"status":"success","results":[{"name":"www_example_com"}]}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	creds := map[string]any{"host": hostOf(t, srv), "api_token": testToken}
	config := map[string]any{"vdom": "root", "replace_strategy": "delete"}

	var lastPct int
	res, err := Provider{}.Deploy(context.Background(), testCert(t), config, creds, func(pct int, _ string) { lastPct = pct })
	if err != nil {
		t.Fatalf("Deploy error: %v", err)
	}
	if !res.Success {
		t.Fatalf("Deploy not successful: %+v", res)
	}
	if !cap.importSeen {
		t.Fatal("import endpoint was never hit")
	}
	if cap.authHeader != "Bearer "+testToken {
		t.Fatalf("token not presented; Authorization=%q", cap.authHeader)
	}
	if cap.vdom != "root" {
		t.Fatalf("vdom query param not set; got %q", cap.vdom)
	}
	if lastPct != 100 {
		t.Fatalf("progress did not reach 100: %d", lastPct)
	}
	if name, _ := res.Details["certificate_name"].(string); name != "www_example_com" {
		t.Fatalf("unexpected certificate_name: %v", res.Details["certificate_name"])
	}
}

func TestDeployFailureDoesNotLeakToken(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodGet && strings.Contains(r.URL.Path, "/cmdb/certificate/local/"):
			w.WriteHeader(http.StatusNotFound)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/import"):
			// FortiOS rejects the import: 424 Failed Dependency.
			w.WriteHeader(http.StatusFailedDependency)
			_, _ = w.Write([]byte(`{"status":"error","error":-23,"cli_error":"object in use"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	creds := map[string]any{"host": hostOf(t, srv), "api_token": testToken}
	config := map[string]any{"vdom": "root", "replace_strategy": "delete"}

	res, err := Provider{}.Deploy(context.Background(), testCert(t), config, creds, nil)

	// A device-side rejection surfaces as a non-nil error and/or a Result with
	// Success=false; either way the api_token must never appear in the message.
	if err == nil && (res == nil || res.Success) {
		t.Fatalf("expected failure, got res=%+v err=%v", res, err)
	}
	msg := ""
	if err != nil {
		msg += err.Error()
	}
	if res != nil {
		if res.Success {
			t.Fatalf("expected Success=false, got %+v", res)
		}
		msg += " " + res.Message
	}
	if strings.Contains(msg, testToken) {
		t.Fatalf("api_token leaked into failure message: %q", msg)
	}
}

func TestVerifyHappyPath(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/system/global"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success"}`))
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/cmdb/certificate/local"):
			if r.URL.Query().Get("vdom") == "" {
				t.Errorf("vdom query param missing on verify GET")
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"success","results":[{"name":"www_example_com_20261002"},{"name":"other"},{"name":"www_example_com"},{"name":"www_example_com_20250101_01"}]}`))
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	creds := map[string]any{"host": hostOf(t, srv), "api_token": testToken}
	config := map[string]any{"vdom": "root"}

	res, err := Provider{}.Verify(context.Background(), testCert(t), config, creds)
	if err != nil {
		t.Fatalf("Verify error: %v", err)
	}
	if !res.Success || res.Message != "Certificate verified on FortiGate" {
		t.Fatalf("Verify not successful: %+v", res)
	}
	// The newest dated family member sorts last.
	if name, _ := res.Details["certificate_name"].(string); name != "www_example_com_20261002" {
		t.Fatalf("unexpected certificate_name: %v", res.Details["certificate_name"])
	}
	assertList(t, res.Details["family_members"].([]string), "www_example_com", "www_example_com_20250101_01", "www_example_com_20261002")
}

func TestValidateCredentialsMissingFields(t *testing.T) {
	ctx := context.Background()
	config := map[string]any{"vdom": "root"}

	if err := (Provider{}).ValidateCredentials(ctx, map[string]any{"api_token": testToken}, config); err == nil {
		t.Fatal("expected error when host is missing")
	}
	if err := (Provider{}).ValidateCredentials(ctx, map[string]any{"host": "fw.example.com"}, config); err == nil {
		t.Fatal("expected error when api_token is missing")
	}
}

// TestSanitizeName is the v3 provider's fortigate_test.go.
func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain domain", "www.example.com", "www_example_com"},
		{"wildcard", "*.example.com", "star_example_com"},
		{"multiple dots", "a.b.c.example.com", "a_b_c_example_com"},
		{"collapse underscores", "weird__name..com", "weird_name_com"},
		{"leading digit", "1.example.com", "cert_1_example_com"},
		{"trailing junk", "example.com--", "example_com--"},
		{
			name: "over 35 chars truncates without trailing underscore",
			in:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.example.com",
			want: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{"empty", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeName(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeName(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if len(got) > 35 {
				t.Fatalf("result %q exceeds FortiGate's 35-char limit", got)
			}
			if strings.HasSuffix(got, "_") {
				t.Fatalf("result %q has a trailing underscore", got)
			}
		})
	}
}

func TestBaseNameFallbacks(t *testing.T) {
	sanOnly := func() string {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), DNSNames: []string{"san.example.com"},
			NotBefore: time.Now(), NotAfter: time.Now().Add(time.Hour)}
		der, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
		return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}()
	cases := []struct {
		pem, cn, id, want string
	}{
		{sanOnly, "meta.example.com", "", "san_example_com"},             // first DNS SAN when the subject has no CN
		{"not a pem", "*.meta.example.com", "", "star_meta_example_com"}, // metadata fallback
		{"", "", "0123456789abcdef", "cert_01234567"},                    // id prefix
		{"", "", "abc", "cert_abc"},                                      // short id
	}
	for _, tc := range cases {
		if got := baseName(tc.pem, &provider.CertificateData{CommonName: tc.cn, ID: tc.id}); got != tc.want {
			t.Errorf("baseName(%q) = %q, want %q", tc.cn, got, tc.want)
		}
	}
	if got := safeIDPrefix("abc"); got != "abc" {
		t.Fatal(got)
	}
}

func TestRebindAllHolders(t *testing.T) {
	f := newFakeFortiOS()
	for _, n := range []string{"star_factory_bg", "star_factory_bg_20250101", "star_factory_bg_20250101_01", "keep_me"} {
		f.addCert(n, "placeholder")
	}
	f.addProfile("p1", "replace", "keep_me", "star_factory_bg", "star_factory_bg_20250101")
	f.addProfile("p2", "replace", "keep_me")
	f.vpnCert = "star_factory_bg_20250101"
	f.adminCert = "star_factory_bg"
	f.vips = []map[string]any{
		{"name": "vip-list", "ssl-certificate": []any{map[string]any{"name": "star_factory_bg_20250101_01"}, map[string]any{"name": "keep_me"}}},
		{"name": "vip-scalar", "ssl-certificate": "star_factory_bg"},
		{"name": "vip-other", "ssl-certificate": "keep_me"},
		{"name": "vip-none"},
	}
	cd := &provider.CertificateData{CommonName: "*.factory.bg"}
	cd.CertificatePEM, cd.PrivateKeyPEM = certWithSerial(t, "*.factory.bg", 77)
	res, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind", "rebind_references": "yes", "prune_old": "on"}, cd)
	if err != nil || !res.Success {
		t.Fatalf("rebind = %+v, %v", res, err)
	}
	nn := "star_factory_bg_" + testDate
	assertList(t, f.serverCerts("p1"), "keep_me", nn)
	assertList(t, f.serverCerts("p2"), "keep_me")
	if f.vpnCert != nn || f.adminCert != nn {
		t.Fatalf("scalars = %q %q", f.vpnCert, f.adminCert)
	}
	if f.vips[1]["ssl-certificate"] != nn || f.vips[2]["ssl-certificate"] != "keep_me" {
		t.Fatalf("vips = %v", f.vips)
	}
	if got := f.bodies(http.MethodPut, "cmdb/firewall/vip/vip-list"); len(got) != 1 {
		t.Fatalf("vip list PUT = %v", got)
	}
	if f.count(http.MethodPut, "cmdb/firewall/vip/vip-other") != 0 || f.count(http.MethodPut, "cmdb/firewall/ssl-ssh-profile/p2") != 0 {
		t.Fatal("unrelated holder written")
	}
	rebound := res.Details["rebound"].([]string)
	if len(rebound) != 5 || res.Message != "Certificate "+nn+" deployed (5 reference(s) rebound, 3 superseded cert(s) pruned)" {
		t.Fatalf("result = %+v", res)
	}
	assertList(t, res.Details["pruned"].([]string), "star_factory_bg", "star_factory_bg_20250101", "star_factory_bg_20250101_01")
	if _, ok := f.certs["keep_me"]; !ok {
		t.Fatal("unrelated certificate pruned")
	}
	if res.Details["strategy"] != "rebind" || res.Details["imported"] != true {
		t.Fatalf("details = %v", res.Details)
	}
}

func TestRebindFailuresAndSwitches(t *testing.T) {
	setup := func() *fakeFortiOS {
		f := newFakeFortiOS()
		f.addCert("www_example_com", "placeholder")
		f.addProfile("p1", "replace", "www_example_com")
		f.vpnCert = "www_example_com"
		f.vips = []map[string]any{{"name": "v", "ssl-certificate": "www_example_com"}}
		return f
	}
	t.Run("rebind errors fail the job, prune keeps referenced", func(t *testing.T) {
		f := setup()
		f.failPutPath["cmdb/firewall/ssl-ssh-profile/p1"] = 500
		f.failPutPath["cmdb/vpn.ssl/settings"] = 403
		f.failPutPath["cmdb/firewall/vip/v"] = 500
		cd := newCertData(t, 5)
		res, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind"}, cd)
		if err != nil || res.Success || !strings.HasPrefix(res.Message, "Certificate "+newName+" imported but 3 reference rebind(s) failed: ") {
			t.Fatalf("result = %+v, %v", res, err)
		}
		if !strings.Contains(res.Message, "write permission") {
			t.Fatalf("403 hint missing: %q", res.Message)
		}
		if errs := res.Details["prune_errors"].([]string); len(errs) != 1 || !strings.Contains(errs[0], "referenced/in-use") {
			t.Fatalf("prune_errors = %v", errs)
		}
		if _, ok := f.certs["www_example_com"]; !ok {
			t.Fatal("referenced certificate pruned")
		}
	})
	t.Run("switches off", func(t *testing.T) {
		f := setup()
		res, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind", "rebind_references": false, "prune_old": "0"}, newCertData(t, 6))
		if err != nil || !res.Success || len(res.Details["rebound"].([]string)) != 0 || len(res.Details["pruned"].([]string)) != 0 {
			t.Fatalf("result = %+v, %v", res, err)
		}
		if f.count(http.MethodPut, "") != 0 || f.count(http.MethodDelete, "") != 0 {
			t.Fatal("rebind or prune with switches off")
		}
	})
	t.Run("rebind read errors", func(t *testing.T) {
		f := setup()
		for _, p := range []string{"cmdb/firewall/ssl-ssh-profile", "cmdb/vpn.ssl/settings", "cmdb/firewall/vip"} {
			f.failGet[p] = 500
		}
		f.failGet["cmdb/system/global"] = 500
		f.getsOK["cmdb/system/global"] = 1 // the connectivity check passes
		res, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind", "prune_old": false}, newCertData(t, 7))
		if err != nil || res.Success || len(res.Details["rebind_errors"].([]string)) != 4 {
			t.Fatalf("result = %+v, %v", res, err)
		}
	})
	for _, tc := range []struct{ name, path, want string }{
		{"list", "cmdb/certificate/local", "failed to look up existing certificate"},
		{"probe", "cmdb/certificate/local/" + newName, "failed to resolve import name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup()
			f.failGet[tc.path] = 500
			if _, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind"}, newCertData(t, 8)); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v", err)
			}
		})
	}
	t.Run("import and verify", func(t *testing.T) {
		f := setup()
		f.failImport = 500
		if _, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind"}, newCertData(t, 9)); err == nil || !strings.Contains(err.Error(), "failed to import certificate") {
			t.Fatalf("err = %v", err)
		}
		f = setup()
		f.failGet["cmdb/certificate/local/"+newName] = 404
		if _, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind"}, newCertData(t, 10)); err == nil || !strings.Contains(err.Error(), "not found after import") {
			t.Fatalf("err = %v", err)
		}
		f = setup()
		if _, err := runDeploy(t, f, map[string]any{"replace_strategy": "rebind"}, &provider.CertificateData{CommonName: "x", CertificatePEM: "junk", PrivateKeyPEM: "KEYDATA-not-pem"}); err == nil || !strings.Contains(err.Error(), "failed to parse certificate") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestDeleteStrategy(t *testing.T) {
	f := newFakeFortiOS()
	f.addCert("www_example_com", "old")
	cd := newCertData(t, 11)
	settled := noSettle(t)
	srv := f.serve(t)
	var notes []string
	res, err := (Provider{}).Deploy(context.Background(), cd, map[string]any{"replace_strategy": "delete", "import_scope": "vdom"}, fgCreds(srv), func(_ int, n string) { notes = append(notes, n) })
	noSecrets(t, cd, res, err)
	if err != nil || !res.Success || res.Message != "Certificate deployed successfully to FortiGate" || res.Details["was_update"] != true || res.Details["strategy"] != "delete" {
		t.Fatalf("delete = %+v, %v", res, err)
	}
	if *settled != 1 || f.count(http.MethodDelete, "cmdb/certificate/local/www_example_com") != 1 {
		t.Fatalf("settled=%d calls=%+v", *settled, f.calls)
	}
	assertList(t, notes, "Validating certificate data", "Checking for existing certificate", "Deleting existing certificate", "Importing certificate", "Verifying deployment", "Deployment complete")
	if imp := f.bodies(http.MethodPost, "monitor/vpn-certificate/local/import"); imp[0]["certname"] != "www_example_com" || imp[0]["scope"] != "vdom" {
		t.Fatalf("import = %v", imp)
	}
	f.failImport = 500
	if _, err := (Provider{}).Deploy(context.Background(), cd, map[string]any{"replace_strategy": "delete"}, fgCreds(srv), nil); err == nil || !strings.Contains(err.Error(), "failed to import certificate") {
		t.Fatalf("import error = %v", err)
	}
}

func TestVerifyAndRollbackFamily(t *testing.T) {
	ctx := context.Background()
	build := func() *fakeFortiOS {
		f := newFakeFortiOS()
		for _, n := range []string{"www_example_com", "www_example_com_20260101", "www_example_com_20261002", "www_example_com_LE", "shop_example_com"} {
			f.addCert(n, "placeholder")
		}
		f.addProfile("inbound", "replace", "www_example_com_20261002")
		return f
	}
	f := build()
	srv := f.serve(t)
	// The family is derived from the certificate subject, not the metadata.
	cd := newCertData(t, 12)
	cd.CommonName = "*.example.com"
	res, err := (Provider{}).Verify(ctx, cd, map[string]any{}, fgCreds(srv))
	if err != nil || !res.Success || res.Details["certificate_name"] != "www_example_com_20261002" || res.Details["vdom"] != "root" {
		t.Fatalf("verify = %+v, %v", res, err)
	}
	if f.writes() != 0 {
		t.Fatal("verify wrote")
	}
	res, err = (Provider{}).Rollback(ctx, cd, map[string]any{}, fgCreds(srv))
	noSecrets(t, cd, res, err)
	if err != nil || !res.Success || res.Message != "Rollback removed 2 certificate(s); 1 still referenced" {
		t.Fatalf("rollback = %+v, %v", res, err)
	}
	assertList(t, res.Details["deleted"].([]string), "www_example_com", "www_example_com_20260101")
	if sk := res.Details["skipped"].([]string); len(sk) != 1 || !strings.HasPrefix(sk[0], "www_example_com_20261002 (") || !strings.Contains(sk[0], "referenced/in-use") {
		t.Fatalf("skipped = %v", sk)
	}
	for _, n := range []string{"www_example_com_LE", "shop_example_com", "www_example_com_20261002"} {
		if _, ok := f.certs[n]; !ok {
			t.Fatalf("%s deleted", n)
		}
	}
	// Nothing of the family left but the referenced one; an empty family.
	delete(f.certs, "www_example_com_20261002")
	res, _ = (Provider{}).Verify(ctx, cd, map[string]any{}, fgCreds(srv))
	if res.Success || res.Message != "Certificate not found on FortiGate" {
		t.Fatalf("verify empty = %+v", res)
	}
	f.failGet["cmdb/certificate/local"] = 500
	if res, _ = (Provider{}).Verify(ctx, cd, map[string]any{}, fgCreds(srv)); res.Success || !strings.HasPrefix(res.Message, "Failed to verify: ") {
		t.Fatalf("verify error = %+v", res)
	}
	if res, _ = (Provider{}).Rollback(ctx, cd, map[string]any{}, fgCreds(srv)); res.Success || !strings.HasPrefix(res.Message, "Rollback failed: ") {
		t.Fatalf("rollback error = %+v", res)
	}
	// Connectivity failures are errors.
	f.failGet["cmdb/system/global"] = 401
	if _, err := (Provider{}).Verify(ctx, cd, map[string]any{}, fgCreds(srv)); err == nil || !strings.Contains(err.Error(), "authentication failed") {
		t.Fatalf("verify auth = %v", err)
	}
	if _, err := (Provider{}).Rollback(ctx, cd, map[string]any{}, fgCreds(srv)); err == nil {
		t.Fatal("rollback without connectivity succeeded")
	}
	srv.Close()
	if _, err := (Provider{}).Deploy(ctx, cd, map[string]any{}, fgCreds(srv), nil); err == nil || !strings.Contains(err.Error(), "failed to connect to FortiGate") {
		t.Fatalf("deploy unreachable = %v", err)
	}
}

func TestCapabilitiesDeclareV3Options(t *testing.T) {
	caps := Provider{}.Capabilities()
	want := map[string]struct {
		typ         string
		def         any
		overridable bool
	}{
		"vdom":                {provider.TypeString, "root", true},
		"import_scope":        {provider.TypeEnum, "global", true},
		"replace_strategy":    {provider.TypeEnum, "ssl_profile", false},
		"profile_suffix":      {provider.TypeString, "_ssl_profile", true},
		"default_ssl_profile": {provider.TypeString, nil, true},
		"rebind_references":   {provider.TypeBool, true, false},
		"prune_old":           {provider.TypeBool, true, false},
	}
	if len(caps.ConfigFields) != len(want) {
		t.Fatalf("config fields = %d", len(caps.ConfigFields))
	}
	for _, f := range caps.ConfigFields {
		w, ok := want[f.Key]
		if !ok || f.Type != w.typ || f.Default != w.def || f.Overridable != w.overridable {
			t.Errorf("%s = %+v", f.Key, f)
		}
	}
	// The descriptor defaults are v3's parseConfig defaults.
	cfg := parseConfig(map[string]any{})
	if cfg.vdom != "root" || cfg.importScope != "global" || cfg.strategy != "ssl_profile" || cfg.profileSuffix != "_ssl_profile" ||
		cfg.defaultProfile != "" || !cfg.rebindRefs || !cfg.pruneOld {
		t.Fatalf("defaults = %+v", cfg)
	}
}
