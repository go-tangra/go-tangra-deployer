package fortigate

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// The ssl_profile strategy's profile creation and update paths (v3 parity),
// pinned request by request against the fake FortiOS.

const (
	ownedProf = "www_example_com_ssl_profile"
	newName   = "www_example_com_" + testDate
)

func profileCfg(extra ...string) map[string]any {
	cfg := map[string]any{"vdom": "root"}
	for i := 0; i+1 < len(extra); i += 2 {
		cfg[extra[i]] = extra[i+1]
	}
	return cfg
}

func runDeploy(t *testing.T, f *fakeFortiOS, cfg map[string]any, cd *provider.CertificateData) (*provider.Result, error) {
	t.Helper()
	fixedClock(t, testDate)
	noSettle(t)
	srv := f.serve(t)
	var pct int
	res, err := (Provider{}).Deploy(context.Background(), cd, cfg, fgCreds(srv), func(p int, _ string) { pct = p })
	noSecrets(t, cd, res, err)
	if err == nil && res.Success && pct != 100 {
		t.Fatalf("progress = %d", pct)
	}
	return res, err
}

// templateFake has two profiles that must NOT be cloned (wrong mode, empty
// server-cert list) listed before the valid replace-mode template.
func templateFake() *fakeFortiOS {
	f := newFakeFortiOS()
	f.addProfile("a-resign", "re-sign", "ca_cert")
	f.addProfile("b-empty", "replace")
	f.addProfile("c-template", "replace", "tmpl_cert")
	f.addProfile("d-template", "replace", "tmpl2_cert")
	f.profExtra["c-template"] = map[string]any{
		"comment":    "inbound",
		"https":      map[string]any{"ports": []any{float64(443)}, "status": "deep-inspection", "q_origin_key": "x"},
		"ssl-exempt": []any{map[string]any{"id": float64(1), "q_origin_key": float64(1), "fortiguard-category": float64(31)}},
	}
	return f
}

func TestProfileCreateClonesTemplate(t *testing.T) {
	f := templateFake()
	cd := newCertData(t, 2)
	res, err := runDeploy(t, f, profileCfg(), cd)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	posts := f.bodies(http.MethodPost, "cmdb/firewall/ssl-ssh-profile")
	if len(posts) != 1 {
		t.Fatalf("profile creates = %d", len(posts))
	}
	// The first replace-mode profile with a server certificate is cloned
	// (q_origin_key stripped at every depth); name, mode and list are set.
	want := map[string]any{
		"name":             ownedProf,
		"comment":          "inbound",
		"https":            map[string]any{"ports": []any{float64(443)}, "status": "deep-inspection"},
		"ssl-exempt":       []any{map[string]any{"id": float64(1), "fortiguard-category": float64(31)}},
		"server-cert-mode": "replace",
		"server-cert":      []any{map[string]any{"name": newName}},
	}
	if !reflect.DeepEqual(posts[0], want) {
		b, _ := json.Marshal(posts[0])
		t.Fatalf("create body = %s", b)
	}
	// The template and the other profiles are read, never written.
	if f.count(http.MethodPut, "") != 0 || f.count(http.MethodDelete, "") != 0 {
		t.Fatalf("unexpected writes: %+v", f.calls)
	}
	assertList(t, f.serverCerts("c-template"), "tmpl_cert")
	d := res.Details
	if d["profile"] != ownedProf || d["profile_action"] != "created (cloned from template)" || d["strategy"] != "ssl_profile" ||
		d["imported"] != true || d["certificate_name"] != newName || d["vdom"] != "root" {
		t.Fatalf("details = %v", d)
	}
	for _, k := range []string{"default_profile", "default_profile_action", "bound_policies", "same_day_collision"} {
		if _, ok := d[k]; ok {
			t.Fatalf("details has %s: %v", k, d)
		}
	}
	if res.Message != `Certificate `+newName+` imported; audit profile "`+ownedProf+`" created (cloned from template) (no certificates or profiles deleted)` {
		t.Fatalf("message = %q", res.Message)
	}
	// Import: one leaf, the key, the configured scope.
	imp := f.bodies(http.MethodPost, "monitor/vpn-certificate/local/import")
	if len(imp) != 1 || imp[0]["type"] != "regular" || imp[0]["certname"] != newName || imp[0]["scope"] != "global" || imp[0]["key_file_content"] == "" {
		t.Fatalf("import = %v", imp)
	}
	if f.tokenHeader != "Bearer "+testToken {
		t.Fatalf("Authorization = %q", f.tokenHeader)
	}
}

func TestProfileCreateWithSuffixAndChain(t *testing.T) {
	f := templateFake()
	cd := newCertData(t, 3)
	inter, _ := certWithSerial(t, "Intermediate CA", 99)
	cd.CertificatePEM += inter // a chain: only the leaf is imported (-145)
	res, err := runDeploy(t, f, profileCfg("profile_suffix", "-inspect", "import_scope", "vdom"), cd)
	if err != nil || !res.Success || res.Details["profile"] != "www_example_com-inspect" {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	if strings.Count(f.certs[newName], "BEGIN CERTIFICATE") != 1 {
		t.Fatalf("imported %q", f.certs[newName])
	}
	if imp := f.bodies(http.MethodPost, "monitor/vpn-certificate/local/import"); imp[0]["scope"] != "vdom" {
		t.Fatalf("scope = %v", imp[0]["scope"])
	}
}

// No usable template (a re-sign profile, a replace profile without
// certificates): the owned profile is created from FortiOS defaults with only
// name, comment, mode and the certificate; nothing else is written.
func TestProfileCreateWithoutTemplate(t *testing.T) {
	f := newFakeFortiOS()
	f.addProfile("a-resign", "re-sign", "ca_cert")
	f.addProfile("b-empty", "replace")
	cd := newCertData(t, 2)
	res, err := runDeploy(t, f, profileCfg(), cd)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	posts := f.bodies(http.MethodPost, "cmdb/firewall/ssl-ssh-profile")
	want := map[string]any{
		"name":             ownedProf,
		"comment":          createdByComment,
		"server-cert-mode": "replace",
		"server-cert":      []any{map[string]any{"name": newName}},
	}
	if len(posts) != 1 || !reflect.DeepEqual(posts[0], want) {
		b, _ := json.Marshal(posts)
		t.Fatalf("create bodies = %s", b)
	}
	if f.count(http.MethodPut, "") != 0 || f.count(http.MethodDelete, "") != 0 {
		t.Fatalf("unexpected writes: %+v", f.calls)
	}
	assertList(t, f.serverCerts("a-resign"), "ca_cert")
	if d := res.Details; d["profile"] != ownedProf || d["profile_action"] != "created (FortiOS defaults, no template)" || d["imported"] != true {
		t.Fatalf("details = %v", d)
	}
}

func TestProfileOwnedUpdatePreservesOthers(t *testing.T) {
	f := newFakeFortiOS()
	oldPEM, _ := certWithSerial(t, "www.example.com", 1)
	f.addCert("www_example_com", oldPEM)
	f.addCert("www_example_com_20260101", oldPEM)
	// Owned profile: other domains kept in order, both family members collapse
	// into one entry at the first family position. Its mode is not checked.
	f.addProfile(ownedProf, "re-sign", "shop_example_com", "www_example_com", "api_example_com", "www_example_com_20260101")
	cd := newCertData(t, 2)
	res, err := runDeploy(t, f, profileCfg(), cd)
	if err != nil || !res.Success || res.Details["profile_action"] != "updated" {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	puts := f.bodies(http.MethodPut, "cmdb/firewall/ssl-ssh-profile/"+ownedProf)
	want := map[string]any{"server-cert": []any{
		map[string]any{"name": "shop_example_com"}, map[string]any{"name": newName}, map[string]any{"name": "api_example_com"}}}
	if len(puts) != 1 || !reflect.DeepEqual(puts[0], want) {
		t.Fatalf("PUT bodies = %v", puts)
	}
	if f.count(http.MethodPost, "cmdb/") != 0 || f.count(http.MethodDelete, "") != 0 {
		t.Fatal("create or delete on update")
	}
	if _, ok := f.certs["www_example_com"]; !ok {
		t.Fatal("old certificate deleted")
	}
}

func TestProfileOwnedUpdateAppendsAndAlwaysPuts(t *testing.T) {
	f := newFakeFortiOS()
	f.addProfile(ownedProf, "replace", "shop_example_com")
	cd := newCertData(t, 2)
	res, err := runDeploy(t, f, profileCfg(), cd)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	assertList(t, f.serverCerts(ownedProf), "shop_example_com", newName)

	// Redeploy of the same certificate: reused, and v3 still PUTs the owned
	// profile (unchanged list) — only the default profile skips a no-op PUT.
	res, err = (Provider{}).Deploy(context.Background(), cd, profileCfg(), fgCreds(f.serve(t)), nil)
	if err != nil || !res.Success || res.Details["imported"] != false || res.Details["profile_action"] != "updated" {
		t.Fatalf("redeploy = %+v, %v", res, err)
	}
	if !strings.Contains(res.Message, "reused (already present)") {
		t.Fatalf("message = %q", res.Message)
	}
	puts := f.bodies(http.MethodPut, "cmdb/firewall/ssl-ssh-profile/"+ownedProf)
	if len(puts) != 2 || !reflect.DeepEqual(puts[0], puts[1]) {
		t.Fatalf("PUTs = %v", puts)
	}
	if f.count(http.MethodPost, "monitor/") != 1 {
		t.Fatal("re-imported an identical certificate")
	}
}

func TestProfileDefaultUpdateInPlace(t *testing.T) {
	const def = "Jobs-Tech SSL Inspection"
	f := newFakeFortiOS()
	f.addProfile(ownedProf, "replace", "www_example_com")
	f.addProfile(def, "", "shop_example_com", "www_example_com", "api_example_com_20260101")
	f.policies = []map[string]any{{"name": "in-www", "ssl-ssh-profile": def}, {"policyid": float64(7), "ssl-ssh-profile": def}, {"name": "x", "ssl-ssh-profile": "other"}}
	cd := newCertData(t, 2)
	res, err := runDeploy(t, f, profileCfg("default_ssl_profile", def), cd)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	puts := f.bodies(http.MethodPut, "cmdb/firewall/ssl-ssh-profile/"+def)
	want := map[string]any{"server-cert": []any{
		map[string]any{"name": "shop_example_com"}, map[string]any{"name": newName}, map[string]any{"name": "api_example_com_20260101"}}}
	if len(puts) != 1 || !reflect.DeepEqual(puts[0], want) {
		t.Fatalf("default PUTs = %v", puts)
	}
	// The owned profile is written before the default profile.
	var order []string
	for _, c := range f.calls {
		if c.method == http.MethodPut {
			order = append(order, c.path)
		}
	}
	assertList(t, order, "cmdb/firewall/ssl-ssh-profile/"+ownedProf, "cmdb/firewall/ssl-ssh-profile/"+def)
	d := res.Details
	if d["default_profile"] != def || d["default_profile_action"] != "updated" {
		t.Fatalf("details = %v", d)
	}
	assertList(t, d["bound_policies"].([]string), "in-www", "#7")
	if !strings.HasSuffix(res.Message, `; default profile "`+def+`" updated (no certificates or profiles deleted)`) {
		t.Fatalf("message = %q", res.Message)
	}

	// Redeploy: the default profile is already current → no PUT to it.
	res, err = (Provider{}).Deploy(context.Background(), cd, profileCfg("default_ssl_profile", def), fgCreds(f.serve(t)), nil)
	if err != nil || !res.Success || res.Details["default_profile_action"] != "no-change (already current)" {
		t.Fatalf("redeploy = %+v, %v", res, err)
	}
	if n := len(f.bodies(http.MethodPut, "cmdb/firewall/ssl-ssh-profile/"+def)); n != 1 {
		t.Fatalf("no-op default PUT sent (%d)", n)
	}
}

func TestProfileDefaultAppendAndPolicyListFailure(t *testing.T) {
	f := newFakeFortiOS()
	f.addProfile(ownedProf, "replace", "www_example_com")
	f.addProfile("inbound", "replace", "shop_example_com")
	f.failGet["cmdb/firewall/policy"] = 500
	cd := newCertData(t, 2)
	res, err := runDeploy(t, f, profileCfg("default_ssl_profile", "inbound"), cd)
	if err != nil || !res.Success || res.Details["default_profile_action"] != "appended (no family member was present)" {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	want := map[string]any{"server-cert": []any{map[string]any{"name": "shop_example_com"}, map[string]any{"name": newName}}}
	if puts := f.bodies(http.MethodPut, "cmdb/firewall/ssl-ssh-profile/inbound"); len(puts) != 1 || !reflect.DeepEqual(puts[0], want) {
		t.Fatalf("PUTs = %v", puts)
	}
	if bp := res.Details["bound_policies"].([]string); len(bp) != 0 {
		t.Fatalf("bound_policies = %v", bp)
	}
}

// A failure after the owned profile was written is returned as a retryable
// error; v3 does not undo the owned-profile update (the retry re-converges).
func TestProfileFailuresMidway(t *testing.T) {
	setup := func() *fakeFortiOS {
		f := newFakeFortiOS()
		f.addProfile(ownedProf, "replace", "www_example_com")
		f.addProfile("inbound", "replace", "shop_example_com", "www_example_com")
		return f
	}
	t.Run("default PUT fails", func(t *testing.T) {
		f := setup()
		f.failPutPath["cmdb/firewall/ssl-ssh-profile/inbound"] = 500
		_, err := runDeploy(t, f, profileCfg("default_ssl_profile", "inbound"), newCertData(t, 2))
		if err == nil || !strings.Contains(err.Error(), "failed to update default profile inbound") {
			t.Fatalf("err = %v", err)
		}
		assertList(t, f.serverCerts(ownedProf), newName)
		assertList(t, f.serverCerts("inbound"), "shop_example_com", "www_example_com")
	})
	t.Run("owned PUT fails", func(t *testing.T) {
		f := setup()
		f.failPut = 500
		_, err := runDeploy(t, f, profileCfg("default_ssl_profile", "inbound"), newCertData(t, 2))
		if err == nil || !strings.Contains(err.Error(), "failed to update profile "+ownedProf) {
			t.Fatalf("err = %v", err)
		}
		assertList(t, f.serverCerts("inbound"), "shop_example_com", "www_example_com")
	})
	t.Run("create fails", func(t *testing.T) {
		f := templateFake()
		f.failPost = 500
		_, err := runDeploy(t, f, profileCfg(), newCertData(t, 2))
		if err == nil || !strings.Contains(err.Error(), "failed to create profile "+ownedProf) || !strings.Contains(err.Error(), "error=-651") {
			t.Fatalf("err = %v", err)
		}
	})
	for _, tc := range []struct{ name, path, want string }{
		{"owned read", "cmdb/firewall/ssl-ssh-profile/" + ownedProf, "failed to read profile"},
		{"default read", "cmdb/firewall/ssl-ssh-profile/inbound", "failed to read default profile inbound"},
		{"list certificates", "cmdb/certificate/local", "failed to look up existing certificate"},
		{"name probe", "cmdb/certificate/local/" + newName, "failed to resolve import name"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := setup()
			f.failGet[tc.path] = 500
			_, err := runDeploy(t, f, profileCfg("default_ssl_profile", "inbound"), newCertData(t, 2))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v", err)
			}
			if f.count(http.MethodPut, "") != 0 || f.count(http.MethodPost, "cmdb/") != 0 {
				t.Fatal("profile written before the failure")
			}
		})
	}
	t.Run("template lookup", func(t *testing.T) {
		f := newFakeFortiOS()
		f.failGet["cmdb/firewall/ssl-ssh-profile"] = 500
		f.getsOK["cmdb/firewall/ssl-ssh-profile"] = 1 // the reference scan succeeds
		_, err := runDeploy(t, f, profileCfg(), newCertData(t, 2))
		if err == nil || !strings.Contains(err.Error(), "failed to find a profile template") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("import fails, echo scrubbed", func(t *testing.T) {
		f := setup()
		f.failImport = 500
		cd := newCertData(t, 2)
		echo, _ := json.Marshal(map[string]any{"status": "error", "error": -145, "cli_error": "bad key " + cd.PrivateKeyPEM, "message": testToken})
		f.importBody = string(echo)
		_, err := runDeploy(t, f, profileCfg(), cd) // runDeploy checks token and key absence
		if err == nil || !strings.Contains(err.Error(), "failed to import certificate") || !strings.Contains(err.Error(), "error=-145") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("import fails, other encoding dropped", func(t *testing.T) {
		f := setup()
		f.failImport = 500
		cd := newCertData(t, 2)
		f.importBody = "-----BEGIN PRIVATE KEY-----\r\nxx"
		_, err := runDeploy(t, f, profileCfg(), cd)
		if err == nil || !strings.Contains(err.Error(), redacted) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("not found after import", func(t *testing.T) {
		f := setup()
		f.failGet["cmdb/certificate/local/"+newName] = 404 // probe: free; after import: absent
		_, err := runDeploy(t, f, profileCfg(), newCertData(t, 2))
		if err == nil || !strings.Contains(err.Error(), "verification failed: certificate "+newName+" not found after import") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("unparseable certificate", func(t *testing.T) {
		f := setup()
		_, err := runDeploy(t, f, profileCfg(), &provider.CertificateData{CommonName: "www.example.com", CertificatePEM: "junk", PrivateKeyPEM: "KEYDATA-not-pem"})
		if err == nil || !strings.Contains(err.Error(), "failed to parse certificate") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no free same-day name", func(t *testing.T) {
		f := setup()
		pemStr, _ := certWithSerial(t, "x", 50)
		f.addCert(newName, pemStr)
		for i := 1; i <= maxSameDaySequence; i++ {
			f.addCert(suffixedVersionedName("www_example_com", testDate, i), pemStr)
		}
		_, err := runDeploy(t, f, profileCfg(), newCertData(t, 4))
		if err == nil || !strings.Contains(err.Error(), "after probing 99 sequence suffixes") {
			t.Fatalf("err = %v", err)
		}
	})
}

func TestProfileManualReviewForeignReferences(t *testing.T) {
	cases := []struct {
		name  string
		setup func(f *fakeFortiOS)
		refs  []string
	}{
		{"vip list", func(f *fakeFortiOS) {
			f.vips = []map[string]any{{"name": "vip-www", "ssl-certificate": []any{map[string]any{"name": "www_example_com"}}}}
		}, []string{holderVIP + ":vip-www"}},
		{"vip scalar", func(f *fakeFortiOS) {
			f.vips = []map[string]any{{"name": "vip-old", "ssl-certificate": "www_example_com_20250101"}}
		}, []string{holderVIP + ":vip-old"}},
		{"ssl-vpn", func(f *fakeFortiOS) { f.vpnCert = "www_example_com" }, []string{holderSSLVPN + ":vpn.ssl/settings"}},
		{"admin gui", func(f *fakeFortiOS) { f.adminCert = "www_example_com" }, []string{holderAdminGUI + ":system/global"}},
		{"other profile", func(f *fakeFortiOS) { f.addProfile("other", "replace", "www_example_com") }, []string{holderSSLSSHProfile + ":other"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeFortiOS()
			oldPEM, _ := certWithSerial(t, "www.example.com", 1)
			f.addCert("www_example_com", oldPEM)
			f.addProfile(ownedProf, "replace", "www_example_com")
			tc.setup(f)
			cd := newCertData(t, 2)
			// default_ssl_profile equal to the owned profile collapses to it.
			res, err := runDeploy(t, f, profileCfg("default_ssl_profile", ownedProf), cd)
			if err != nil || res.Success || !res.Permanent || !strings.HasPrefix(res.Message, "MANUAL REVIEW REQUIRED: certificate "+newName+" was uploaded but NOT auto-bound — certificate is referenced by 1 object(s) outside the expected profile(s).") {
				t.Fatalf("result = %+v, %v", res, err)
			}
			d := res.Details
			assertList(t, d["foreign_references"].([]string), tc.refs...)
			if d["imported"] != true || d["default_profile"] != ownedProf || d["expected_profile"] != ownedProf || d["reason"] == "" {
				t.Fatalf("details = %v", d)
			}
			if f.count(http.MethodPut, "") != 0 || f.count(http.MethodPost, "cmdb/") != 0 || f.count(http.MethodDelete, "") != 0 {
				t.Fatalf("writes on manual review: %+v", f.calls)
			}
		})
	}
}

func TestProfileScanErrorIsRetryable(t *testing.T) {
	f := newFakeFortiOS()
	f.failGet["cmdb/firewall/vip"] = 500
	res, err := runDeploy(t, f, profileCfg(), newCertData(t, 2))
	if err != nil || res.Success || res.Permanent || res.Details["manual_review_required"] != true ||
		!strings.Contains(res.Details["reason"].(string), "could not scan references: fortigate: list vips failed: HTTP 500") {
		t.Fatalf("result = %+v, %v", res, err)
	}
}

func TestProfileInvalidNamesRefusedBeforeAnyWrite(t *testing.T) {
	for _, cfg := range []map[string]any{
		profileCfg("default_ssl_profile", `a"b`), profileCfg("default_ssl_profile", ".."), profileCfg("default_ssl_profile", "a/b"),
		profileCfg("profile_suffix", "/../x"), profileCfg("profile_suffix", "a\\b"),
	} {
		f := newFakeFortiOS()
		res, err := runDeploy(t, f, cfg, newCertData(t, 2))
		if err != nil || res.Success || !res.Permanent || strings.Contains(res.Message, `a"b`) {
			t.Fatalf("%v: %+v, %v", cfg, res, err)
		}
		if len(f.calls) != 1 { // only the connectivity check
			t.Fatalf("%v: calls = %+v", cfg, f.calls)
		}
	}
	// The other strategies ignore the profile options.
	f := newFakeFortiOS()
	res, err := runDeploy(t, f, profileCfg("default_ssl_profile", "a/b", "replace_strategy", "rebind"), newCertData(t, 2))
	if err != nil || !res.Success {
		t.Fatalf("rebind = %+v, %v", res, err)
	}
}

func TestValidateCredentialsDefaultProfile(t *testing.T) {
	f := newFakeFortiOS()
	f.addProfile("inbound-www", "replace")
	srv := f.serve(t)
	ctx := context.Background()
	if err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), profileCfg("default_ssl_profile", "inbound-www")); err != nil {
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
	// The rebind and delete strategies do not use the profile.
	if err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), map[string]any{"default_ssl_profile": "missing", "replace_strategy": "delete"}); err != nil {
		t.Fatalf("delete strategy: %v", err)
	}
	f.failGet["cmdb/firewall/ssl-ssh-profile/x"] = 500
	if err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), map[string]any{"default_ssl_profile": "x"}); err != nil {
		t.Fatalf("profile error is not not-found: %v", err)
	}
	f.failGet["cmdb/system/global"] = 500
	if err := (Provider{}).ValidateCredentials(ctx, fgCreds(srv), map[string]any{}); err == nil || !strings.Contains(err.Error(), "connectivity check") {
		t.Fatalf("500 probe: %v", err)
	}
	if f.writes() != 0 {
		t.Fatal("validation wrote")
	}
}

func TestProfileDescriptors(t *testing.T) {
	caps := Provider{}.Capabilities()
	check := func(key string, v any) string {
		errs, _ := provider.ValidateInput(caps, map[string]any{"vdom": "root", key: v}, nil, provider.ModeConfiguration)
		return errs["config."+key]
	}
	for _, bad := range []string{"a/b", `a"b`, "a\\b", "x\ny", strings.Repeat("a", 36), ".", "..", ".hidden"} {
		if check("default_ssl_profile", bad) == "" {
			t.Errorf("default_ssl_profile %q accepted", bad)
		}
	}
	if code := check("default_ssl_profile", "inbound-www 2"); code != "" {
		t.Errorf("valid name refused: %s", code)
	}
	for _, bad := range []string{"/x", `"`, "a\\b", "x\ny", strings.Repeat("a", 36)} {
		if check("profile_suffix", bad) == "" {
			t.Errorf("profile_suffix %q accepted", bad)
		}
	}
	for _, ok := range []string{"_ssl_profile", "-inspect", ".prod"} {
		if code := check("profile_suffix", ok); code != "" {
			t.Errorf("profile_suffix %q refused: %s", ok, code)
		}
	}
	if check("replace_strategy", "x") != provider.CodeNotInOptions {
		t.Error("unknown strategy accepted")
	}
	for _, s := range []string{"ssl_profile", "rebind", "delete"} {
		if code := check("replace_strategy", s); code != "" {
			t.Errorf("strategy %s refused: %s", s, code)
		}
	}
	if check("prune_old", "yes") == "" {
		t.Error("string bool accepted at save time")
	}
	// Destructive switches and credentials are never target-overridable.
	ov := provider.ValidateOverride(caps, map[string]any{"vdom": "root"}, map[string]any{"replace_strategy": "delete", "prune_old": false, "rebind_references": false, "host": "evil"})
	for _, k := range []string{"replace_strategy", "prune_old", "rebind_references", "host"} {
		if ov["config_overrides."+k] != provider.CodeNotOverridable {
			t.Errorf("%s overridable: %v", k, ov)
		}
	}
	if ov := provider.ValidateOverride(caps, map[string]any{"vdom": "root"}, map[string]any{"profile_suffix": "-x", "default_ssl_profile": "p", "import_scope": "vdom", "vdom": "dmz"}); len(ov) != 0 {
		t.Errorf("overridable options refused: %v", ov)
	}
}

func TestClientEdgeCases(t *testing.T) {
	ctx := context.Background()
	body := map[string]string{
		"/api/v2/cmdb/certificate/local":              `{"results":{"not":"a list"}}`,
		"/api/v2/cmdb/certificate/local/empty":        `{"results":[]}`,
		"/api/v2/cmdb/certificate/local/bad":          `{"results":"x"}`,
		"/api/v2/cmdb/firewall/ssl-ssh-profile/empty": `{"results":[]}`,
		"/api/v2/cmdb/firewall/ssl-ssh-profile/bad":   `{"results":"x"}`,
		"/api/v2/cmdb/certificate/local/garbage":      `{"results":[{"name":"garbage","certificate":"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"}]}`,
		"/api/v2/cmdb/firewall/ssl-ssh-profile":       `{"results":"x"}`,
		"/api/v2/cmdb/firewall/policy":                `{"results":"x"}`,
		"/api/v2/cmdb/firewall/vip":                   `{"results":"x"}`,
		"/api/v2/cmdb/vpn.ssl/settings":               `{"results":"x"}`,
	}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if b, ok := body[r.URL.Path]; ok && r.Method == http.MethodGet {
			_, _ = io.WriteString(w, b)
			return
		}
		w.WriteHeader(500)
	}))
	c := newClient("https://"+strings.TrimPrefix(srv.URL, "https://")+"/", testToken, "")
	if c.vdom != "root" || strings.Contains(c.host, "/") {
		t.Fatalf("client = %q %q", c.host, c.vdom)
	}
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
	leaf, _ := parseLeaf(newCertData(t, 1).CertificatePEM)
	if n := c.matchLocalCert(ctx, []localCert{{Name: "boom"}, {Name: "empty"}, {Name: "garbage"}}, leaf); n != "" {
		t.Errorf("found %q", n)
	}
	if _, found, err := c.getSSLSSHProfile(ctx, "empty"); err != nil || found {
		t.Errorf("empty profile = %v, %v", found, err)
	}
	if _, _, err := c.getSSLSSHProfile(ctx, "bad"); err == nil {
		t.Error("bad profile decoded")
	}
	if _, _, err := c.getSSLSSHProfile(ctx, "boom"); err == nil {
		t.Error("500 profile accepted")
	}
	if err := c.setSSLSSHProfileServerCert(ctx, "x", nil); err == nil {
		t.Error("500 PUT accepted")
	}
	if err := c.createSSLSSHProfileFromTemplate(ctx, "x", "y", nil); err == nil {
		t.Error("500 POST accepted")
	}
	if _, err := c.findReplaceModeTemplate(ctx, ""); err == nil {
		t.Error("bad profile listing decoded")
	}
	if _, err := c.findPoliciesBoundToProfile(ctx, "x"); err == nil {
		t.Error("bad policy listing decoded")
	}
	if _, err := scanReferences(ctx, c, func(string) bool { return true }); err == nil {
		t.Error("bad scan decoded")
	}
	if _, err := scanVIPs(ctx, c, func(string) bool { return true }); err == nil {
		t.Error("bad vips decoded")
	}
	if _, err := scanScalar(ctx, c, "vpn.ssl/settings", "servercert", holderSSLVPN, func(string) bool { return true }); err == nil {
		t.Error("bad scalar decoded")
	}
	if _, err := scanScalar(ctx, c, "system/global", "admin-server-cert", holderAdminGUI, func(string) bool { return true }); err == nil {
		t.Error("500 scalar accepted")
	}
	if err := c.deleteCert(ctx, "x"); err == nil {
		t.Error("500 delete accepted")
	}
	if _, err := c.certExists(ctx, "x"); err == nil {
		t.Error("500 exists accepted")
	}
	if names, err := c.listLocalCertNames(ctx); err == nil || names != nil {
		t.Error("bad names decoded")
	}
	if pruned, errs := pruneFamily(ctx, c, func(string) bool { return true }, "k"); pruned != nil || len(errs) != 1 {
		t.Errorf("prune on bad listing = %v %v", pruned, errs)
	}
	if res := rebindReferences(ctx, c, func(string) bool { return true }, "n"); len(res.Errors) != 4 || len(res.Rebound) != 0 {
		t.Errorf("rebind on bad device = %+v", res)
	}
	if _, err := c.do(ctx, http.MethodPost, "x", map[string]any{"c": make(chan int)}); err == nil {
		t.Error("unmarshalable body accepted")
	}
	if _, err := c.do(ctx, "BAD METHOD", "x", nil); err == nil {
		t.Error("invalid method accepted")
	}
	srv.Close()
	for name, fn := range map[string]func() error{
		"list":     func() error { _, err := c.listLocalCerts(ctx); return err },
		"get":      func() error { _, err := c.getLocalCertPEM(ctx, "x"); return err },
		"profile":  func() error { _, _, err := c.getSSLSSHProfile(ctx, "x"); return err },
		"put":      func() error { return c.setSSLSSHProfileServerCert(ctx, "x", nil) },
		"post":     func() error { return c.createSSLSSHProfileFromTemplate(ctx, "x", "y", nil) },
		"template": func() error { _, err := c.findReplaceModeTemplate(ctx, ""); return err },
		"policies": func() error { _, err := c.findPoliciesBoundToProfile(ctx, "x"); return err },
		"scan":     func() error { _, err := scanReferences(ctx, c, func(string) bool { return true }); return err },
		"vips":     func() error { _, err := scanVIPs(ctx, c, func(string) bool { return true }); return err },
		"scalar": func() error {
			_, err := scanScalar(ctx, c, "x", "y", "z", func(string) bool { return true })
			return err
		},
		"import": func() error { return c.importCert(ctx, "x", "c", "k", "global") },
		"delete": func() error { return c.deleteCert(ctx, "x") },
		"exists": func() error { _, err := c.certExists(ctx, "x"); return err },
	} {
		if err := fn(); err == nil {
			t.Errorf("closed server: %s succeeded", name)
		}
	}
	if res := rebindReferences(ctx, c, func(string) bool { return true }, "n"); len(res.Errors) != 4 {
		t.Errorf("rebind on closed device = %+v", res)
	}
}

func TestStripQOriginKeyAndTemplate(t *testing.T) {
	in := map[string]any{"q_origin_key": "a", "x": []any{map[string]any{"q_origin_key": 1, "id": 1}}, "s": "v"}
	want := map[string]any{"x": []any{map[string]any{"id": 1}}, "s": "v"}
	if got := stripQOriginKey(in); !reflect.DeepEqual(got, want) {
		t.Fatalf("strip = %v", got)
	}
	if in["q_origin_key"] != "a" {
		t.Fatal("strip mutated its input")
	}
	// A nil template still yields a valid create body.
	f := newFakeFortiOS()
	srv := f.serve(t)
	c := newClient(strings.TrimPrefix(srv.URL, "https://"), testToken, "root")
	if err := c.createSSLSSHProfileFromTemplate(context.Background(), "p", "cert", nil); err != nil {
		t.Fatal(err)
	}
	want = map[string]any{"name": "p", "server-cert-mode": "replace", "server-cert": []any{map[string]any{"name": "cert"}}}
	if got := f.bodies(http.MethodPost, "cmdb/firewall/ssl-ssh-profile"); !reflect.DeepEqual(got[0], want) {
		t.Fatalf("create = %v", got)
	}
	// findReplaceModeTemplate skips the excluded name.
	delete(f.profiles, "p")
	f.addProfile("only", "replace", "c1")
	if tmpl, err := c.findReplaceModeTemplate(context.Background(), "only"); err != nil || tmpl != nil {
		t.Fatalf("excluded template = %v, %v", tmpl, err)
	}
	if tmpl, err := c.findReplaceModeTemplate(context.Background(), "other"); err != nil || tmpl["name"] != "only" {
		t.Fatalf("template = %v, %v", tmpl, err)
	}
}
