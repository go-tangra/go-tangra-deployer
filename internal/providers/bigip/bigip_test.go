package bigip

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// testCert is a small, self-consistent set of certificate material. The PEM
// bodies are placeholders; the provider never parses them, it only ships them.
var testCert = &provider.CertificateData{
	ID:               "abcdef0123456789",
	SerialNumber:     "01",
	CommonName:       "www.example.com",
	SANs:             []string{"www.example.com"},
	CertificatePEM:   "-----BEGIN CERTIFICATE-----\nMIIC\n-----END CERTIFICATE-----\n",
	PrivateKeyPEM:    "-----BEGIN PRIVATE KEY-----\nMIIE\n-----END PRIVATE KEY-----\n",
	CertificateChain: "-----BEGIN CERTIFICATE-----\nMIID\n-----END CERTIFICATE-----\n",
}

const (
	testUser = "admin"
	testPass = "s3cr3t-P@ss"
)

type step struct {
	pct int
	msg string
}

func deployWith(t *testing.T, f *fakeIControl, cfg map[string]any, cert *provider.CertificateData) (*provider.Result, []step) {
	t.Helper()
	var steps []step
	res, err := Provider{}.Deploy(context.Background(), cert, cfg, f.creds(), func(pct int, msg string) {
		steps = append(steps, step{pct, msg})
	})
	if err != nil {
		t.Fatalf("deploy error: %v", err)
	}
	if res == nil {
		t.Fatal("deploy returned no result")
	}
	return res, steps
}

func jsonBody(t *testing.T, c call) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(c.body), &m); err != nil {
		t.Fatalf("%s %s body %q: %v", c.method, c.path, c.body, err)
	}
	return m
}

func assertBody(t *testing.T, c call, want map[string]any) {
	t.Helper()
	if got := jsonBody(t, c); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s %s body = %v, want %v", c.method, c.path, got, want)
	}
}

func assertSeq(t *testing.T, got []call, want ...string) {
	t.Helper()
	if s := sig(got); !reflect.DeepEqual(s, want) {
		t.Fatalf("calls =\n  %s\nwant\n  %s", strings.Join(s, "\n  "), strings.Join(want, "\n  "))
	}
}

func TestRegistrationAndCapabilities(t *testing.T) {
	p, err := provider.Get("bigip")
	if err != nil {
		t.Fatalf("bigip not registered: %v", err)
	}
	c := p.Capabilities()
	if c.Type != "bigip" || c.DisplayName != "F5 BIG-IP" || !c.SupportsVerify || !c.SupportsRollback || !c.TestConnection {
		t.Fatalf("capabilities: %+v", c)
	}
	if len(c.ConfigFields) != 2 {
		t.Fatalf("config fields: %+v", c.ConfigFields)
	}
	part, prof := c.ConfigFields[0], c.ConfigFields[1]
	if part.Key != "partition" || !part.Required || !part.Overridable || part.Default != "Common" {
		t.Fatalf("partition field: %+v", part)
	}
	if prof.Key != "ssl_profile" || prof.Required || !prof.Overridable || prof.Default != nil {
		t.Fatalf("ssl_profile field: %+v", prof)
	}
	want := map[string]struct{ secret, required bool }{
		"host": {false, true}, "username": {false, true}, "password": {true, true},
	}
	if len(c.CredentialFields) != len(want) {
		t.Fatalf("credential fields: %+v", c.CredentialFields)
	}
	for _, f := range c.CredentialFields {
		w, ok := want[f.Key]
		if !ok || f.Secret != w.secret || f.Required != w.required || f.Overridable {
			t.Fatalf("credential field %+v", f)
		}
	}
}

// TestDescriptorRefusesTraversal: partition and ssl_profile values that could
// form a path segment never pass save-time validation.
func TestDescriptorRefusesTraversal(t *testing.T) {
	caps := Provider{}.Capabilities()
	for _, bad := range []string{".", "..", ".hidden", "a/b", "/Common", "~Common", "a b", "Com~mon", strings.Repeat("a", 65)} {
		errs, _ := provider.ValidateInput(caps, map[string]any{"partition": bad}, nil, provider.ModeConfiguration)
		if errs["config.partition"] == "" {
			t.Errorf("partition %q accepted", bad)
		}
	}
	for _, bad := range []string{"a/b", "bad name", "/Common/x/y", "~x", "../evil", "/../p", "/./p", "/Common/..", "/Common/.p", ".p", "-lead", strings.Repeat("a", 400)} {
		errs, _ := provider.ValidateInput(caps, map[string]any{"partition": "Common", "ssl_profile": bad}, nil, provider.ModeConfiguration)
		if errs["config.ssl_profile"] == "" {
			t.Errorf("ssl_profile %q accepted", bad)
		}
	}
	for _, good := range []map[string]any{
		{"partition": "Common"},
		{"partition": "Prod_1.a-b"},
		{"partition": "Common", "ssl_profile": "www_clientssl"},
		{"partition": "Common", "ssl_profile": "/Shared/www.clientssl-1"},
	} {
		if errs, _ := provider.ValidateInput(caps, good, nil, provider.ModeConfiguration); len(errs) != 0 {
			t.Errorf("%v refused: %v", good, errs)
		}
	}
}

func TestSanitizeName(t *testing.T) {
	for in, want := range map[string]string{
		"www.example.com":       "www_example_com",
		"*.example.com":         "star_example_com",
		"*.*.example.com":       "star_example_com", // only the first "*" becomes "star"
		"api-1.example.com":     "api-1_example_com",
		"1.example.com":         "cert_1_example_com",
		"__a..b__":              "a_b",
		"ex ample/with:chars.o": "ex_ample_with_chars_o",
		"...":                   "",
		"":                      "",
		"Ünïcode.example":       "n_code_example",
	} {
		if got := sanitizeName(in); got != want {
			t.Errorf("sanitizeName(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestCertBaseName(t *testing.T) {
	cases := []struct {
		name string
		cert *provider.CertificateData
		want string
	}{
		{"common name", &provider.CertificateData{ID: "abcdef0123", CommonName: "www.example.com"}, "www_example_com"},
		{"v3 ID fallback", &provider.CertificateData{ID: "abcdef0123456789"}, "cert-abcdef01"},
		{"CN that sanitises to nothing", &provider.CertificateData{ID: "abcdef0123456789", CommonName: "..."}, "cert-abcdef01"},
		{"short ID", &provider.CertificateData{ID: "abc"}, "cert-abc"},
		{"ID with odd characters", &provider.CertificateData{ID: "a/b~c.d/e-f_g123"}, "cert-abcde-f_"},
		{"serial fallback", &provider.CertificateData{SerialNumber: "0A:1B"}, "cert-cert_0A_1B"},
		{"nothing", &provider.CertificateData{}, "freya_cert"},
		{"nil", nil, "freya_cert"},
	}
	for _, c := range cases {
		if got := certBaseName(c.cert); got != c.want {
			t.Errorf("%s: certBaseName = %q, want %q", c.name, got, c.want)
		}
	}
}

// TestDeployFlow pins the v3 request sequence, every request body, the upload
// headers, Basic auth, progress steps and the result of a fresh deployment
// without ssl_profile: no profile is created or touched.
func TestDeployFlow(t *testing.T) {
	f := newFake(t)
	res, steps := deployWith(t, f, map[string]any{"partition": "Prod"}, testCert)
	if !res.Success || res.Permanent || res.Message != "Certificate deployed successfully to F5 BIG-IP" {
		t.Fatalf("result = %+v", res)
	}
	wantDetails := map[string]any{
		"host":             strings.TrimPrefix(f.srv.URL, "https://"),
		"partition":        "Prod",
		"certificate_name": "/Prod/www_example_com.crt",
		"key_name":         "/Prod/www_example_com.key",
	}
	if !reflect.DeepEqual(res.Details, wantDetails) {
		t.Fatalf("details = %v", res.Details)
	}
	calls := f.recorded()
	assertSeq(t, calls,
		"GET /mgmt/tm/sys/version",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.key",
		"POST /mgmt/tm/sys/crypto/key",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com_chain.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"GET /mgmt/tm/sys/crypto/cert/~Prod~www_example_com.crt",
	)
	for _, c := range calls {
		if !c.authOK || c.user != testUser || c.pass != testPass {
			t.Fatalf("%s %s without Basic auth", c.method, c.path)
		}
	}
	for i, want := range []struct{ pem, file string }{
		{testCert.CertificatePEM, "www_example_com.crt"},
		{testCert.PrivateKeyPEM, "www_example_com.key"},
		{testCert.CertificateChain, "www_example_com_chain.crt"},
	} {
		up := calls[1+2*i]
		if up.body != want.pem || up.contentType != "application/octet-stream" {
			t.Fatalf("upload %s: body %q type %q", want.file, up.body, up.contentType)
		}
		if wantRange := "0-" + strconv.Itoa(len(want.pem)-1) + "/" + strconv.Itoa(len(want.pem)); up.contentRange != wantRange {
			t.Fatalf("upload %s Content-Range = %q, want %q", want.file, up.contentRange, wantRange)
		}
		install := calls[2+2*i]
		if install.contentType != "application/json" {
			t.Fatalf("install content type %q", install.contentType)
		}
	}
	assertBody(t, calls[2], map[string]any{"command": "install", "name": "/Prod/www_example_com.crt", "from-local-file": "/var/config/rest/downloads/www_example_com.crt"})
	assertBody(t, calls[4], map[string]any{"command": "install", "name": "/Prod/www_example_com.key", "from-local-file": "/var/config/rest/downloads/www_example_com.key"})
	assertBody(t, calls[6], map[string]any{"command": "install", "name": "/Prod/www_example_com_chain.crt", "from-local-file": "/var/config/rest/downloads/www_example_com_chain.crt"})

	wantSteps := []step{
		{10, "Validating certificate data"},
		{20, "Uploading certificate to BIG-IP"},
		{40, "Uploading private key to BIG-IP"},
		{55, "Uploading certificate chain to BIG-IP"},
		{90, "Verifying deployment"},
		{100, "Deployment complete"},
	}
	if !reflect.DeepEqual(steps, wantSteps) {
		t.Fatalf("progress = %v", steps)
	}
	if len(f.profiles) != 0 {
		t.Fatalf("a profile was created without ssl_profile: %v", f.profiles)
	}
	if f.certs["/Prod/www_example_com.crt"] != testCert.CertificatePEM || f.keys["/Prod/www_example_com.key"] != testCert.PrivateKeyPEM ||
		f.certs["/Prod/www_example_com_chain.crt"] != testCert.CertificateChain {
		t.Fatalf("objects = %v / %v", f.certs, f.keys)
	}
}

// TestDeployDefaultPartition: a missing, empty or blank partition is v3's
// Common; surrounding slashes and spaces are tolerated.
func TestDeployDefaultPartition(t *testing.T) {
	for _, cfg := range []map[string]any{nil, {}, {"partition": ""}, {"partition": "  "}, {"partition": " /Common/ "}, {"partition": 7}} {
		f := newFake(t)
		res, _ := deployWith(t, f, cfg, testCert)
		if !res.Success || res.Details["partition"] != "Common" || res.Details["certificate_name"] != "/Common/www_example_com.crt" {
			t.Fatalf("cfg %v: %+v", cfg, res)
		}
	}
}

// TestDeployWithoutChain: no chain → no chain upload, no step 55.
func TestDeployWithoutChain(t *testing.T) {
	f := newFake(t)
	cert := *testCert
	cert.CertificateChain = ""
	res, steps := deployWith(t, f, nil, &cert)
	if !res.Success {
		t.Fatalf("result = %+v", res)
	}
	for _, c := range f.recorded() {
		if strings.Contains(c.path, "chain") || strings.Contains(c.body, "chain") {
			t.Fatalf("chain call %+v", c)
		}
	}
	for _, s := range steps {
		if s.pct == 55 || s.pct == 60 {
			t.Fatalf("chain step %v", s)
		}
	}
}

// TestDeployIDFallbackName: a certificate without a usable common name is
// named cert-<first 8 ID characters> (v3).
func TestDeployIDFallbackName(t *testing.T) {
	f := newFake(t)
	cert := *testCert
	cert.CommonName = ""
	res, _ := deployWith(t, f, nil, &cert)
	if !res.Success || res.Details["certificate_name"] != "/Common/cert-abcdef01.crt" || res.Details["key_name"] != "/Common/cert-abcdef01.key" {
		t.Fatalf("result = %+v", res)
	}
	if _, ok := f.uploads["cert-abcdef01_chain.crt"]; !ok {
		t.Fatalf("uploads = %v", f.uploads)
	}
}

// TestDeployOverwritesExistingObjects: an existing cert/key/chain object
// answers 409; v3 uploads the file again and installs with overwrite.
func TestDeployOverwritesExistingObjects(t *testing.T) {
	f := newFake(t)
	res, _ := deployWith(t, f, nil, testCert)
	if !res.Success {
		t.Fatalf("first deploy = %+v", res)
	}
	renewed := *testCert
	renewed.CertificatePEM = "-----BEGIN CERTIFICATE-----\nNEW\n-----END CERTIFICATE-----\n"
	renewed.PrivateKeyPEM = "-----BEGIN PRIVATE KEY-----\nNEWKEY\n-----END PRIVATE KEY-----\n"
	f.reset()
	res, _ = deployWith(t, f, nil, &renewed)
	if !res.Success {
		t.Fatalf("redeploy = %+v", res)
	}
	calls := f.recorded()
	assertSeq(t, calls,
		"GET /mgmt/tm/sys/version",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.key",
		"POST /mgmt/tm/sys/crypto/key",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.key",
		"POST /mgmt/tm/sys/crypto/key",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com_chain.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com_chain.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"GET /mgmt/tm/sys/crypto/cert/~Common~www_example_com.crt",
	)
	assertBody(t, calls[2], map[string]any{"command": "install", "name": "/Common/www_example_com.crt", "from-local-file": "/var/config/rest/downloads/www_example_com.crt"})
	assertBody(t, calls[4], map[string]any{"command": "install", "name": "/Common/www_example_com.crt", "from-local-file": "/var/config/rest/downloads/www_example_com.crt", "overwrite": true})
	assertBody(t, calls[8], map[string]any{"command": "install", "name": "/Common/www_example_com.key", "from-local-file": "/var/config/rest/downloads/www_example_com.key", "overwrite": true})
	if calls[3].body != renewed.CertificatePEM || calls[7].body != renewed.PrivateKeyPEM {
		t.Fatal("re-upload did not carry the new material")
	}
	if f.certs["/Common/www_example_com.crt"] != renewed.CertificatePEM || f.keys["/Common/www_example_com.key"] != renewed.PrivateKeyPEM {
		t.Fatal("objects not overwritten")
	}
}

// TestDeployAlreadyExistsBody: a non-409 answer whose body says "already
// exists" also takes the overwrite path (v3).
func TestDeployAlreadyExistsBody(t *testing.T) {
	f := newFake(t)
	f.failOnce(http.MethodPost, "/mgmt/tm/sys/crypto/key", http.StatusBadRequest, `{"code":400,"message":"key already exists"}`)
	res, _ := deployWith(t, f, nil, testCert)
	if !res.Success {
		t.Fatalf("result = %+v", res)
	}
	var keyInstalls []call
	for _, c := range f.recorded() {
		if c.path == "/mgmt/tm/sys/crypto/key" {
			keyInstalls = append(keyInstalls, c)
		}
	}
	if len(keyInstalls) != 2 || jsonBody(t, keyInstalls[1])["overwrite"] != true {
		t.Fatalf("key installs = %+v", keyInstalls)
	}
}

func TestDeployFailures(t *testing.T) {
	const (
		upCrt   = "/mgmt/shared/file-transfer/uploads/www_example_com.crt"
		upKey   = "/mgmt/shared/file-transfer/uploads/www_example_com.key"
		insCert = "/mgmt/tm/sys/crypto/cert"
		insKey  = "/mgmt/tm/sys/crypto/key"
		getCert = "/mgmt/tm/sys/crypto/cert/~Common~www_example_com.crt"
	)
	type fail struct {
		method, path string
		status       int
		body         string
		once         bool
	}
	cases := []struct {
		name  string
		setup []fail
		want  string
	}{
		{"certificate upload", []fail{{http.MethodPost, upCrt, 500, `{"message":"disk full"}`, false}},
			`failed to upload certificate: failed to upload certificate file: file upload error (HTTP 500): {"message":"disk full"}`},
		{"certificate install", []fail{{http.MethodPost, insCert, 400, `{"message":"bad cert"}`, true}},
			`failed to upload certificate: API error (HTTP 400): {"message":"bad cert"}`},
		{"certificate overwrite install", []fail{
			{http.MethodPost, insCert, 409, `{"message":"exists"}`, true},
			{http.MethodPost, insCert, 500, `{"message":"in use"}`, true}},
			`failed to upload certificate: API error (HTTP 500): {"message":"in use"}`},
		{"certificate re-upload", []fail{
			{http.MethodPost, insCert, 409, `{}`, true},
			{http.MethodPost, upCrt, 200, `{}`, true},
			{http.MethodPost, upCrt, 503, `busy`, true}},
			`failed to upload certificate: failed to upload certificate file: file upload error (HTTP 503): busy`},
		{"key upload", []fail{{http.MethodPost, upKey, 500, `nope`, false}},
			`failed to upload private key: failed to upload key file: file upload error (HTTP 500): nope`},
		{"key install", []fail{{http.MethodPost, insKey, 400, `{"message":"key mismatch"}`, true}},
			`failed to upload private key: API error (HTTP 400): {"message":"key mismatch"}`},
		{"key re-upload", []fail{
			{http.MethodPost, insKey, 409, `{}`, true},
			{http.MethodPost, upKey, 200, `{}`, true},
			{http.MethodPost, upKey, 500, `x`, true}},
			`failed to upload private key: failed to upload key file: file upload error (HTTP 500): x`},
		{"verification 404", []fail{{http.MethodGet, getCert, 404, `{}`, false}},
			`verification failed: certificate not found`},
		{"verification error", []fail{{http.MethodGet, getCert, 500, `{"message":"mcpd down"}`, false}},
			`verification failed: API error (HTTP 500): {"message":"mcpd down"}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			for _, s := range tc.setup {
				if s.once {
					f.failOnce(s.method, s.path, s.status, s.body)
				} else {
					f.failAlways(s.method, s.path, s.status, s.body)
				}
			}
			res, _ := deployWith(t, f, nil, testCert)
			if res.Success || res.Permanent || res.Message != tc.want {
				t.Fatalf("result = %+v\nwant message %q", res, tc.want)
			}
			assertNoSecret(t, res)
		})
	}

	// The failed upload of the first certificate stops before the key.
	f := newFake(t)
	f.failAlways(http.MethodPost, upCrt, 500, `x`)
	_, _ = deployWith(t, f, nil, testCert)
	for _, c := range f.recorded() {
		if c.path == upKey {
			t.Fatal("key uploaded after the certificate failed")
		}
	}
}

// TestDeployChainFailureIsNonFatal: a failed chain upload or install only
// emits the v3 warning step; the deployment still succeeds.
func TestDeployChainFailureIsNonFatal(t *testing.T) {
	for _, name := range []string{"upload", "install"} {
		f := newFake(t)
		if name == "upload" {
			f.failAlways(http.MethodPost, "/mgmt/shared/file-transfer/uploads/www_example_com_chain.crt", 500, `x`)
		} else {
			f.certs["/Common/www_example_com.crt"] = "pre"                     // answers the verify GET
			f.failOnce(http.MethodPost, "/mgmt/tm/sys/crypto/cert", 200, `{}`) // certificate install
			f.failOnce(http.MethodPost, "/mgmt/tm/sys/crypto/cert", 500, `{}`) // chain install
		}
		res, steps := deployWith(t, f, nil, testCert)
		if !res.Success {
			t.Fatalf("%s: result = %+v", name, res)
		}
		found := false
		for _, s := range steps {
			if s == (step{60, "Warning: failed to upload certificate chain"}) {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: no warning step: %v", name, steps)
		}
	}
}

func TestDeployGuards(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	for _, tc := range []struct {
		creds map[string]any
		want  string
	}{
		{map[string]any{"username": testUser, "password": testPass}, "host is required"},
		{map[string]any{"host": " https:// "}, "host is required"},
		{map[string]any{"host": "h"}, "username is required"},
		{map[string]any{"host": "h", "username": testUser}, "password is required"},
	} {
		res, err := Provider{}.Deploy(ctx, testCert, nil, tc.creds, nil)
		if res != nil || err == nil || err.Error() != tc.want {
			t.Fatalf("creds %v: %v, %v", tc.creds, res, err)
		}
	}
	for _, cert := range []*provider.CertificateData{nil, {}, {CertificatePEM: "x"}, {PrivateKeyPEM: "y"}} {
		res, err := Provider{}.Deploy(ctx, cert, nil, f.creds(), nil)
		if res != nil || err == nil || err.Error() != "certificate and private key are required" {
			t.Fatalf("cert %+v: %v, %v", cert, res, err)
		}
	}
	if len(f.writes()) != 0 {
		t.Fatalf("writes without material: %v", sig(f.writes()))
	}
}

// TestInvalidSettingsRefusedBeforeAnyRequest (v4): a stored partition or
// ssl_profile outside the descriptor pattern is a permanent failure before
// the appliance is contacted; the value is not echoed.
func TestInvalidSettingsRefusedBeforeAnyRequest(t *testing.T) {
	ctx := context.Background()
	for _, cfg := range []map[string]any{
		{"partition": ".."},
		{"partition": "a/b"},
		{"partition": "evil~x"},
		{"ssl_profile": "../evil"},
		{"ssl_profile": "/../evil"},
		{"partition": "Common", "ssl_profile": "evil name"},
	} {
		f := newFake(t)
		res, err := Provider{}.Deploy(ctx, testCert, cfg, f.creds(), nil)
		if err != nil || res == nil || res.Success || !res.Permanent || strings.Contains(res.Message, "evil") || strings.Contains(res.Message, "..") {
			t.Fatalf("deploy %v: %+v, %v", cfg, res, err)
		}
		if res2, _ := (Provider{}).Verify(ctx, testCert, cfg, f.creds()); res2.Success || !res2.Permanent {
			t.Fatalf("verify %v: %+v", cfg, res2)
		}
		if res3, _ := (Provider{}).Rollback(ctx, testCert, cfg, f.creds()); res3.Success || !res3.Permanent {
			t.Fatalf("rollback %v: %+v", cfg, res3)
		}
		if n := len(f.recorded()); n != 0 {
			t.Fatalf("%v: appliance contacted %d times", cfg, n)
		}
		err = Provider{}.ValidateCredentials(ctx, f.creds(), cfg)
		var fe *provider.FieldError
		if !errors.As(err, &fe) || fe.Msg != provider.CodePattern {
			t.Fatalf("validate %v: %v", cfg, err)
		}
	}
	if r := invalidSettingResult(&provider.FieldError{Field: "config.partition"}); !strings.Contains(r.Message, "partition") {
		t.Fatal(r.Message)
	}
}

// TestConnectFailures: v3 runs the version probe before every Deploy,
// Verify and Rollback; a failed probe stops before any write.
func TestConnectFailures(t *testing.T) {
	ctx := context.Background()
	type op func(map[string]any) (*provider.Result, error)
	ops := map[string]op{
		"deploy":   func(c map[string]any) (*provider.Result, error) { return Provider{}.Deploy(ctx, testCert, nil, c, nil) },
		"verify":   func(c map[string]any) (*provider.Result, error) { return Provider{}.Verify(ctx, testCert, nil, c) },
		"rollback": func(c map[string]any) (*provider.Result, error) { return Provider{}.Rollback(ctx, testCert, nil, c) },
	}
	for name, run := range ops {
		t.Run(name, func(t *testing.T) {
			f := newFake(t)
			bad := f.creds()
			bad["password"] = "wrong"
			res, err := run(bad)
			if err != nil || res.Success || res.Message != "authentication failed: invalid username or password" {
				t.Fatalf("401: %+v, %v", res, err)
			}
			f.version = http.StatusInternalServerError
			res, _ = run(f.creds())
			if res.Success || res.Message != "BIG-IP API error (HTTP 500): " {
				t.Fatalf("500: %+v", res)
			}
			f.version = http.StatusForbidden
			res, _ = run(f.creds())
			if res.Success || !strings.HasPrefix(res.Message, "BIG-IP API error (HTTP 403)") {
				t.Fatalf("403: %+v", res)
			}
			if len(f.writes()) != 0 {
				t.Fatalf("writes after a failed probe: %v", sig(f.writes()))
			}
			res, _ = run(map[string]any{"host": "127.0.0.1:1", "username": "u", "password": testPass})
			if res.Success || !strings.HasPrefix(res.Message, "failed to connect to BIG-IP: ") {
				t.Fatalf("unreachable: %+v", res)
			}
			assertNoSecret(t, res)
			res, err = run(map[string]any{"host": "bad host\x7f", "username": "u", "password": "p"})
			if err != nil || res.Success || !strings.HasPrefix(res.Message, "failed to create request: ") {
				t.Fatalf("bad host: %+v, %v", res, err)
			}
		})
	}
}

// TestRedirectNotFollowed (v4): a redirecting appliance is reported, never
// followed with the credentials.
func TestRedirectNotFollowed(t *testing.T) {
	hits := 0
	elsewhere := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hits++ }))
	defer elsewhere.Close()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.URL+r.URL.Path, http.StatusFound)
	}))
	defer srv.Close()
	creds := map[string]any{"host": strings.TrimPrefix(srv.URL, "https://"), "username": testUser, "password": testPass}
	res, _ := Provider{}.Deploy(context.Background(), testCert, nil, creds, nil)
	if res.Success || !strings.HasPrefix(res.Message, "BIG-IP API error (HTTP 302)") || hits != 0 {
		t.Fatalf("redirect: %+v hits=%d", res, hits)
	}
	if err := (Provider{}).ValidateCredentials(context.Background(), creds, nil); err == nil || hits != 0 {
		t.Fatalf("validate redirect: %v hits=%d", err, hits)
	}
}

func TestVerify(t *testing.T) {
	f := newFake(t)
	ctx := context.Background()
	cfg := map[string]any{"partition": "Prod", "ssl_profile": "www_clientssl"}
	res, err := Provider{}.Verify(ctx, testCert, cfg, f.creds())
	if err != nil || res.Success || res.Message != "Certificate not found: certificate not found" {
		t.Fatalf("absent: %+v, %v", res, err)
	}
	f.certs["/Prod/www_example_com.crt"] = "x"
	f.reset()
	res, err = Provider{}.Verify(ctx, testCert, cfg, f.creds())
	if err != nil || !res.Success || res.Message != "Certificate verified on BIG-IP" {
		t.Fatalf("present: %+v, %v", res, err)
	}
	want := map[string]any{"host": strings.TrimPrefix(f.srv.URL, "https://"), "partition": "Prod", "certificate_name": "/Prod/www_example_com.crt"}
	if !reflect.DeepEqual(res.Details, want) {
		t.Fatalf("details = %v", res.Details)
	}
	// v3: Verify checks the certificate object only, never the profile.
	assertSeq(t, f.recorded(), "GET /mgmt/tm/sys/version", "GET /mgmt/tm/sys/crypto/cert/~Prod~www_example_com.crt")

	f.failAlways(http.MethodGet, "/mgmt/tm/sys/crypto/cert/~Prod~www_example_com.crt", 500, `{"message":"boom"}`)
	res, _ = Provider{}.Verify(ctx, testCert, cfg, f.creds())
	if res.Success || res.Message != `Certificate not found: API error (HTTP 500): {"message":"boom"}` {
		t.Fatalf("error: %+v", res)
	}
	if _, err := (Provider{}).Verify(ctx, testCert, nil, map[string]any{}); err == nil || err.Error() != "host is required" {
		t.Fatalf("guard: %v", err)
	}
}

func TestRollback(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	res, _ := deployWith(t, f, map[string]any{"partition": "Prod"}, testCert)
	if !res.Success {
		t.Fatal(res)
	}
	f.reset()
	res, err := Provider{}.Rollback(ctx, testCert, map[string]any{"partition": "Prod"}, f.creds())
	if err != nil || !res.Success || res.Message != "Certificate and key removed from BIG-IP" {
		t.Fatalf("rollback = %+v, %v", res, err)
	}
	if !reflect.DeepEqual(res.Details, map[string]any{"deleted_cert": "/Prod/www_example_com.crt", "deleted_key": "/Prod/www_example_com.key"}) {
		t.Fatalf("details = %v", res.Details)
	}
	// No ssl_profile: no profile is touched.
	assertSeq(t, f.recorded(),
		"GET /mgmt/tm/sys/version",
		"DELETE /mgmt/tm/sys/crypto/cert/~Prod~www_example_com.crt",
		"DELETE /mgmt/tm/sys/crypto/key/~Prod~www_example_com.key",
		"DELETE /mgmt/tm/sys/crypto/cert/~Prod~www_example_com_chain.crt",
	)
	if len(f.certs)+len(f.keys) != 0 {
		t.Fatalf("left behind: %v %v", f.certs, f.keys)
	}
	// Everything already gone: 404s count as removed.
	if res, _ = (Provider{}).Rollback(ctx, testCert, map[string]any{"partition": "Prod"}, f.creds()); !res.Success {
		t.Fatalf("second rollback = %+v", res)
	}
}

func TestRollbackPartialFailure(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	f.failAlways(http.MethodDelete, "/mgmt/tm/sys/crypto/cert/~Common~www_example_com.crt", 400, `{"message":"in use by profile"}`)
	f.failAlways(http.MethodDelete, "/mgmt/tm/sys/crypto/key/~Common~www_example_com.key", 500, `boom`)
	f.failAlways(http.MethodDelete, "/mgmt/tm/sys/crypto/cert/~Common~www_example_com_chain.crt", 500, `ignored`)
	res, err := Provider{}.Rollback(ctx, testCert, nil, f.creds())
	want := `Rollback partially failed: certificate: API error (HTTP 400): {"message":"in use by profile"}; key: API error (HTTP 500): boom`
	if err != nil || res.Success || res.Permanent || res.Message != want || res.Details != nil {
		t.Fatalf("rollback = %+v, %v", res, err)
	}
	// The chain failure alone is ignored.
	g := newFake(t)
	g.failAlways(http.MethodDelete, "/mgmt/tm/sys/crypto/cert/~Common~www_example_com_chain.crt", 500, `ignored`)
	if res, _ = (Provider{}).Rollback(ctx, testCert, nil, g.creds()); !res.Success {
		t.Fatalf("chain-only failure = %+v", res)
	}
	if _, err := (Provider{}).Rollback(ctx, testCert, nil, map[string]any{"host": "h", "username": "u"}); err == nil || err.Error() != "password is required" {
		t.Fatalf("guard: %v", err)
	}
}

func TestValidateCredentials(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	p := Provider{}
	if err := p.ValidateCredentials(ctx, f.creds(), map[string]any{"partition": "Common"}); err != nil {
		t.Fatalf("valid: %v", err)
	}
	withScheme := f.creds()
	withScheme["host"] = " " + f.srv.URL + "/ "
	if err := p.ValidateCredentials(ctx, withScheme, nil); err != nil {
		t.Fatalf("scheme host: %v", err)
	}
	// A profile that does not exist yet is fine: Deploy creates it.
	if err := p.ValidateCredentials(ctx, f.creds(), map[string]any{"ssl_profile": "not_there_yet"}); err != nil {
		t.Fatalf("missing profile: %v", err)
	}
	for _, c := range f.recorded() {
		if c.path != "/mgmt/tm/sys/version" || c.method != http.MethodGet {
			t.Fatalf("validation called %s %s", c.method, c.path)
		}
	}
	bad := f.creds()
	bad["password"] = "wrong"
	if err := p.ValidateCredentials(ctx, bad, nil); err == nil || err.Error() != "authentication failed: invalid username or password" {
		t.Fatalf("401: %v", err)
	}
	f.version = http.StatusServiceUnavailable
	if err := p.ValidateCredentials(ctx, f.creds(), nil); err == nil || err.Error() != "BIG-IP API error (HTTP 503): " {
		t.Fatalf("503: %v", err)
	}
	if err := p.ValidateCredentials(ctx, map[string]any{"host": "127.0.0.1:1", "username": "u", "password": "p"}, nil); err != nil {
		t.Fatalf("unreachable should be non-fatal: %v", err)
	}
	if err := p.ValidateCredentials(ctx, map[string]any{"host": "bad host\x7f", "username": "u", "password": "p"}, nil); err == nil || !strings.HasPrefix(err.Error(), "failed to create request: ") {
		t.Fatalf("bad host: %v", err)
	}
	for _, tc := range []struct {
		creds map[string]any
		want  string
	}{
		{map[string]any{"username": "u", "password": "p"}, "host is required"},
		{map[string]any{"host": "h", "password": "p"}, "username is required"},
		{map[string]any{"host": "h", "username": "u"}, "password is required"},
	} {
		if err := p.ValidateCredentials(ctx, tc.creds, nil); err == nil || err.Error() != tc.want {
			t.Fatalf("%v: %v", tc.creds, err)
		}
	}
}

func TestClientEdgeCases(t *testing.T) {
	c := newClient("h", "u", "p")
	if _, err := c.do(context.Background(), http.MethodPost, "/x", func() {}); err == nil || !strings.HasPrefix(err.Error(), "marshal request") {
		t.Fatalf("marshal: %v", err)
	}
	c.addSecret("   ")
	if len(c.secrets) != 1 {
		t.Fatalf("blank secret registered: %v", c.secrets)
	}
	long := &response{status: 500, body: strings.Repeat("x", 1000)}
	if got := long.snippet(); len(got) != maxSnippet {
		t.Fatalf("snippet length %d", len(got))
	}
	// Transport errors on every call site.
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	srv.Close()
	dead := newClient(strings.TrimPrefix(srv.URL, "https://"), "u", "p")
	ctx := context.Background()
	if err := dead.uploadFile(ctx, "f", "x"); err == nil {
		t.Error("upload")
	}
	if err := dead.verifyCertExists(ctx, "/Common/x.crt"); err == nil {
		t.Error("verify")
	}
	if err := dead.deleteResource(ctx, "sys/crypto/cert", "/Common/x.crt"); err == nil {
		t.Error("delete")
	}
	if err := dead.createOrUpdateSSLProfile(ctx, "/Common/p", "c", "k"); err == nil {
		t.Error("profile create")
	}
	if err := dead.updateSSLProfile(ctx, "/Common/p", "c", "k"); err == nil {
		t.Error("profile update")
	}
}
