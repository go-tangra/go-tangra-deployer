package bigip

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// T106 (033 US8, research D26): binding into an operator-managed client-SSL
// profile against a stateful fake iControl REST that records every call.

type call struct{ method, path, body string }

type fakeIControl struct {
	mu        sync.Mutex
	calls     []call
	profiles  map[string]*clientSSLProfile // full path → profile
	certs     map[string]bool              // installed crypto object names
	profileGE int                          // status forced on profile GET (0 = real)
	patchFail int                          // status forced on profile PATCH
	deleteErr int                          // status forced on DELETE
}

func newFakeIControl(profiles ...*clientSSLProfile) *fakeIControl {
	f := &fakeIControl{profiles: map[string]*clientSSLProfile{}, certs: map[string]bool{}}
	for _, p := range profiles {
		f.profiles[p.FullPath] = p
	}
	return f
}

func (f *fakeIControl) serve(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		defer f.mu.Unlock()
		raw, _ := io.ReadAll(r.Body)
		f.calls = append(f.calls, call{r.Method, r.URL.Path, string(raw)})
		var body map[string]any
		_ = json.Unmarshal(raw, &body)
		const profPrefix = "/mgmt/tm/ltm/profile/client-ssl/"
		switch {
		case r.URL.Path == "/mgmt/tm/sys/version":
			_, _ = io.WriteString(w, `{}`)
		case strings.HasPrefix(r.URL.Path, "/mgmt/shared/file-transfer/uploads/"):
			_, _ = io.WriteString(w, `{}`)
		case r.Method == http.MethodPost && (r.URL.Path == "/mgmt/tm/sys/crypto/cert" || r.URL.Path == "/mgmt/tm/sys/crypto/key"):
			n, _ := body["name"].(string)
			f.certs[n] = true
			_, _ = io.WriteString(w, `{}`)
		case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/mgmt/tm/sys/crypto/cert/"):
			if !f.certs[strings.ReplaceAll(strings.TrimPrefix(r.URL.Path, "/mgmt/tm/sys/crypto/cert/"), "~", "/")] {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, `{}`)
		case r.Method == http.MethodDelete:
			if f.deleteErr != 0 {
				w.WriteHeader(f.deleteErr)
				_, _ = io.WriteString(w, `{"message":"in use"}`)
				return
			}
			_, _ = io.WriteString(w, `{}`)
		case strings.HasPrefix(r.URL.Path, profPrefix):
			full := strings.ReplaceAll(strings.TrimPrefix(r.URL.Path, profPrefix), "~", "/")
			p, ok := f.profiles[full]
			if r.Method == http.MethodGet && f.profileGE != 0 {
				w.WriteHeader(f.profileGE)
				_, _ = io.WriteString(w, `{"message":"boom"}`)
				return
			}
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				_, _ = io.WriteString(w, `{"code":404}`)
				return
			}
			if r.Method == http.MethodPatch {
				if f.patchFail != 0 {
					w.WriteHeader(f.patchFail)
					_, _ = io.WriteString(w, `{"message":"profile gone"}`)
					return
				}
				p.Cert, _ = body["cert"].(string)
				p.Key, _ = body["key"].(string)
				if c, ok := body["chain"].(string); ok {
					p.Chain = c
				}
			}
			_ = json.NewEncoder(w).Encode(p)
		case r.URL.Path == "/mgmt/tm/ltm/profile/client-ssl":
			_, _ = io.WriteString(w, `{}`)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func (f *fakeIControl) count(pred func(call) bool) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, c := range f.calls {
		if pred(c) {
			n++
		}
	}
	return n
}

func isWrite(c call) bool  { return c.method != http.MethodGet }
func isDelete(c call) bool { return c.method == http.MethodDelete }
func isPatch(c call) bool  { return c.method == http.MethodPatch }
func isProfilePost(c call) bool {
	return c.method == http.MethodPost && c.path == "/mgmt/tm/ltm/profile/client-ssl"
}

func (f *fakeIControl) assertNoSecret(t *testing.T, texts ...string) {
	t.Helper()
	for _, s := range texts {
		if strings.Contains(s, testPass) || strings.Contains(s, "MIIE") {
			t.Fatalf("secret material in %q", s)
		}
	}
}

func resultTexts(r *provider.Result) []string {
	if r == nil {
		return nil
	}
	b, _ := json.Marshal(r.Details)
	return []string{r.Message, string(b)}
}

func TestResolveProfilePath(t *testing.T) {
	cases := []struct {
		in, part, want string
		ok             bool
	}{
		{"www_clientssl_prod", "Common", "/Common/www_clientssl_prod", true},
		{"www_clientssl_prod", "Prod", "/Prod/www_clientssl_prod", true},
		{"/Shared/www.ssl", "Prod", "/Shared/www.ssl", true},
		{"", "Common", "", false},
		{"/Common/", "Common", "", false},
		{"a/b", "Common", "", false},
		{"bad name", "Common", "", false},
		{"/Common/x/y", "Common", "", false},
		{"~Common~x", "Common", "", false},
		{"-lead", "Common", "", false},
	}
	for _, c := range cases {
		got, ok := resolveProfilePath(c.in, c.part)
		if got != c.want || ok != c.ok {
			t.Errorf("resolveProfilePath(%q,%q) = %q,%v want %q,%v", c.in, c.part, got, ok, c.want, c.ok)
		}
	}
}

func TestSSLProfileDescriptorRefusesBadPatterns(t *testing.T) {
	caps := Provider{}.Capabilities()
	for _, bad := range []string{"a/b", "bad name", "/Common/x/y", "~x", strings.Repeat("a", 400)} {
		errs, _ := provider.ValidateInput(caps, map[string]any{"partition": "Common", "ssl_profile": bad}, nil, provider.ModeConfiguration)
		if errs["config.ssl_profile"] == "" {
			t.Errorf("ssl_profile %q accepted by the descriptor", bad)
		}
	}
	if errs, _ := provider.ValidateInput(caps, map[string]any{"partition": "Common", "ssl_profile": "/Common/www_clientssl_prod"}, nil, provider.ModeConfiguration); errs["config.ssl_profile"] != "" {
		t.Errorf("valid full path refused: %v", errs)
	}
}

func TestDeployExistingProfileMissing(t *testing.T) {
	f := newFakeIControl()
	srv := f.serve(t)
	res, err := Provider{}.Deploy(context.Background(), testCert,
		map[string]any{"partition": "Common", "ssl_profile": "www_clientssl_prod"}, credsFor(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Success || !res.Permanent || res.Message != "client-SSL profile /Common/www_clientssl_prod not found" {
		t.Fatalf("result = %+v", res)
	}
	if n := f.count(isWrite); n != 0 {
		t.Fatalf("%d writes before the pre-check failed: %+v", n, f.calls)
	}
	f.assertNoSecret(t, resultTexts(res)...)
}

func TestDeployExistingProfileCheckError(t *testing.T) {
	f := newFakeIControl()
	f.profileGE = http.StatusInternalServerError
	srv := f.serve(t)
	res, err := Provider{}.Deploy(context.Background(), testCert,
		map[string]any{"partition": "Common", "ssl_profile": "x"}, credsFor(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Success || res.Permanent || !strings.Contains(res.Message, "profile check failed") {
		t.Fatalf("result = %+v", res)
	}
	if f.count(isWrite) != 0 {
		t.Fatal("writes after a failed pre-check")
	}
}

func TestDeployExistingProfileInvalidValue(t *testing.T) {
	f := newFakeIControl()
	srv := f.serve(t)
	res, err := Provider{}.Deploy(context.Background(), testCert,
		map[string]any{"partition": "Common", "ssl_profile": "../evil"}, credsFor(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Success || !res.Permanent || strings.Contains(res.Message, "evil") {
		t.Fatalf("result = %+v", res)
	}
	if len(f.calls) != 0 {
		t.Fatalf("appliance contacted: %+v", f.calls)
	}
}

func TestDeployExistingProfileBinds(t *testing.T) {
	for _, tc := range []struct {
		name, value, path string
		chain             bool
	}{
		{"bare name with chain", "www_clientssl_prod", "/Prod/www_clientssl_prod", true},
		{"full path without chain", "/Common/www_clientssl_prod", "/Common/www_clientssl_prod", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prof := &clientSSLProfile{FullPath: tc.path, Cert: "/Common/old.crt", Key: "/Common/old.key", Chain: "/Common/old_chain.crt"}
			f := newFakeIControl(prof)
			srv := f.serve(t)
			cert := *testCert
			if !tc.chain {
				cert.CertificateChain = ""
			}
			res, err := Provider{}.Deploy(context.Background(), &cert,
				map[string]any{"partition": "Prod", "ssl_profile": tc.value}, credsFor(srv), nil)
			if err != nil || !res.Success {
				t.Fatalf("deploy = %+v, %v", res, err)
			}
			if f.count(isProfilePost) != 0 {
				t.Fatal("provider created its own client-SSL profile")
			}
			if f.count(isDelete) != 0 {
				t.Fatal("deploy deleted something")
			}
			var patches []call
			for _, c := range f.calls {
				if isPatch(c) {
					patches = append(patches, c)
				}
			}
			if len(patches) != 1 || patches[0].path != "/mgmt/tm/ltm/profile/client-ssl/"+encodeName(tc.path) {
				t.Fatalf("patches = %+v", patches)
			}
			var body map[string]any
			_ = json.Unmarshal([]byte(patches[0].body), &body)
			wantKeys := 2
			if tc.chain {
				wantKeys = 3
				if body["chain"] != "/Prod/www_example_com_chain.crt" {
					t.Fatalf("chain = %v", body["chain"])
				}
			}
			if len(body) != wantKeys || body["cert"] != "/Prod/www_example_com.crt" || body["key"] != "/Prod/www_example_com.key" {
				t.Fatalf("patch body = %v", body)
			}
			// The pre-check precedes the first upload.
			if f.calls[0].method != http.MethodGet || !strings.HasPrefix(f.calls[0].path, "/mgmt/tm/ltm/profile/client-ssl/") {
				t.Fatalf("first call = %+v", f.calls[0])
			}
			if res.Details["ssl_profile"] != tc.path || res.Details["ssl_profile_mode"] != "existing" {
				t.Fatalf("details = %v", res.Details)
			}
			if prof.Cert != "/Prod/www_example_com.crt" {
				t.Fatalf("profile not pointed at the new certificate: %+v", prof)
			}
			f.assertNoSecret(t, resultTexts(res)...)

			// Idempotent: a second deployment re-binds the same objects.
			res, err = Provider{}.Deploy(context.Background(), &cert,
				map[string]any{"partition": "Prod", "ssl_profile": tc.value}, credsFor(srv), nil)
			if err != nil || !res.Success || f.count(isProfilePost) != 0 || f.count(isDelete) != 0 {
				t.Fatalf("redeploy = %+v, %v", res, err)
			}
		})
	}
}

func TestDeployExistingProfilePatchFails(t *testing.T) {
	f := newFakeIControl(&clientSSLProfile{FullPath: "/Common/p"})
	f.patchFail = http.StatusNotFound
	srv := f.serve(t)
	res, err := Provider{}.Deploy(context.Background(), testCert,
		map[string]any{"partition": "Common", "ssl_profile": "p"}, credsFor(srv), nil)
	if err != nil {
		t.Fatal(err)
	}
	if res.Success || res.Permanent || !strings.Contains(res.Message, "binding failed") {
		t.Fatalf("result = %+v", res)
	}
	f.assertNoSecret(t, resultTexts(res)...)
}

func TestVerifyExistingProfile(t *testing.T) {
	cfg := map[string]any{"partition": "Common", "ssl_profile": "p"}
	bound := &clientSSLProfile{FullPath: "/Common/p", Cert: "/Common/www_example_com.crt", Key: "/Common/www_example_com.key"}
	f := newFakeIControl(bound)
	f.certs["/Common/www_example_com.crt"] = true
	srv := f.serve(t)
	res, err := Provider{}.Verify(context.Background(), testCert, cfg, credsFor(srv))
	if err != nil || !res.Success || res.Details["ssl_profile"] != "/Common/p" {
		t.Fatalf("bound verify = %+v, %v", res, err)
	}

	bound.Cert, bound.Key = "/Common/other.crt", "/Common/other.key"
	res, _ = Provider{}.Verify(context.Background(), testCert, cfg, credsFor(srv))
	if res.Success || res.Message != "profile /Common/p is bound to a different certificate" {
		t.Fatalf("elsewhere verify = %+v", res)
	}

	delete(f.profiles, "/Common/p")
	res, _ = Provider{}.Verify(context.Background(), testCert, cfg, credsFor(srv))
	if res.Success || !strings.Contains(res.Message, "not found") {
		t.Fatalf("missing profile verify = %+v", res)
	}

	f.profileGE = http.StatusInternalServerError
	res, _ = Provider{}.Verify(context.Background(), testCert, cfg, credsFor(srv))
	if res.Success || !strings.Contains(res.Message, "profile check failed") {
		t.Fatalf("profile error verify = %+v", res)
	}

	res, _ = Provider{}.Verify(context.Background(), testCert, map[string]any{"partition": "Common", "ssl_profile": "a b"}, credsFor(srv))
	if res.Success || !res.Permanent {
		t.Fatalf("invalid verify = %+v", res)
	}
	if f.count(isWrite) != 0 {
		t.Fatal("verify wrote to the appliance")
	}
}

func TestRollbackExistingProfile(t *testing.T) {
	cfg := map[string]any{"partition": "Common", "ssl_profile": "/Shared/p"}
	prof := &clientSSLProfile{FullPath: "/Shared/p", Cert: "/Common/www_example_com.crt", Key: "/Common/www_example_com.key"}
	f := newFakeIControl(prof)
	srv := f.serve(t)

	res, err := Provider{}.Rollback(context.Background(), testCert, cfg, credsFor(srv))
	if err != nil {
		t.Fatal(err)
	}
	if res.Success || res.Message != "certificate is bound to client-SSL profile /Shared/p; bind another certificate before rolling back" {
		t.Fatalf("bound rollback = %+v", res)
	}
	if f.count(isWrite) != 0 {
		t.Fatalf("rollback wrote while bound: %+v", f.calls)
	}

	// Only the chain still referenced also blocks.
	prof.Cert, prof.Key, prof.Chain = "/Common/new.crt", "/Common/new.key", "/Common/www_example_com_chain.crt"
	if res, _ = (Provider{}).Rollback(context.Background(), testCert, cfg, credsFor(srv)); res.Success {
		t.Fatalf("chain-bound rollback = %+v", res)
	}

	// Rebound elsewhere: objects removed, profile untouched.
	prof.Chain = "/Common/new_chain.crt"
	f.calls = nil
	res, err = Provider{}.Rollback(context.Background(), testCert, cfg, credsFor(srv))
	if err != nil || !res.Success {
		t.Fatalf("unbound rollback = %+v, %v", res, err)
	}
	for _, c := range f.calls {
		if strings.HasPrefix(c.path, "/mgmt/tm/ltm/profile/") && c.method != http.MethodGet {
			t.Fatalf("profile touched: %+v", c)
		}
	}
	if f.count(isDelete) != 3 {
		t.Fatalf("deletes = %d (%+v)", f.count(isDelete), f.calls)
	}

	// Profile deleted by the operator meanwhile: objects removed too.
	delete(f.profiles, "/Shared/p")
	if res, _ = (Provider{}).Rollback(context.Background(), testCert, cfg, credsFor(srv)); !res.Success {
		t.Fatalf("profile-gone rollback = %+v", res)
	}

	f.profileGE = http.StatusInternalServerError
	f.calls = nil
	if res, _ = (Provider{}).Rollback(context.Background(), testCert, cfg, credsFor(srv)); res.Success || f.count(isWrite) != 0 {
		t.Fatalf("profile-error rollback = %+v", res)
	}
	if res, _ = (Provider{}).Rollback(context.Background(), testCert, map[string]any{"partition": "Common", "ssl_profile": "a b"}, credsFor(srv)); res.Success {
		t.Fatalf("invalid rollback = %+v", res)
	}
}

func TestValidateCredentialsExistingProfile(t *testing.T) {
	f := newFakeIControl(&clientSSLProfile{FullPath: "/Common/present"})
	srv := f.serve(t)
	ctx := context.Background()
	p := Provider{}

	if err := p.ValidateCredentials(ctx, credsFor(srv), map[string]any{"partition": "Common", "ssl_profile": "present"}); err != nil {
		t.Fatalf("present profile: %v", err)
	}
	// Target-supplied partition: the descriptor default is used.
	if err := p.ValidateCredentials(ctx, credsFor(srv), map[string]any{"ssl_profile": "present"}); err != nil {
		t.Fatalf("default partition: %v", err)
	}
	err := p.ValidateCredentials(ctx, credsFor(srv), map[string]any{"partition": "Common", "ssl_profile": "missing"})
	var fe *provider.FieldError
	if !errors.As(err, &fe) || fe.Field != "config.ssl_profile" || fe.Msg != provider.CodeNotFoundOnEndpoint {
		t.Fatalf("missing profile: %v", err)
	}
	err = p.ValidateCredentials(ctx, credsFor(srv), map[string]any{"partition": "Common", "ssl_profile": "a b"})
	if !errors.As(err, &fe) || fe.Msg != provider.CodePattern {
		t.Fatalf("bad pattern: %v", err)
	}
	if strings.Contains(fmt.Sprint(err), testPass) {
		t.Fatal("password in error")
	}
	// Other profile errors are not reported as not found.
	f.profileGE = http.StatusInternalServerError
	if err := p.ValidateCredentials(ctx, credsFor(srv), map[string]any{"partition": "Common", "ssl_profile": "x"}); err != nil {
		t.Fatalf("profile error: %v", err)
	}
	if f.count(isWrite) != 0 {
		t.Fatal("validation wrote to the appliance")
	}
}

func TestNoProfileOptionKeepsOwnProfile(t *testing.T) {
	f := newFakeIControl()
	srv := f.serve(t)
	res, err := Provider{}.Deploy(context.Background(), testCert, map[string]any{"partition": "Common", "ssl_profile": "  "}, credsFor(srv), nil)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	if f.count(isProfilePost) != 1 || res.Details["ssl_profile"] != "/Common/www_example_com_clientssl" || res.Details["ssl_profile_mode"] != nil {
		t.Fatalf("own profile not used: %+v %v", f.calls, res.Details)
	}
}

func TestGetClientSSLProfileEdgeCases(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `not json`)
	}))
	ctx := context.Background()
	if _, err := getClientSSLProfile(ctx, httpClient(), hostOf(srv), testUser, testPass, "/Common/p"); err == nil {
		t.Error("bad JSON decoded")
	}
	srv.Close()
	if _, err := getClientSSLProfile(ctx, httpClient(), hostOf(srv), testUser, testPass, "/Common/p"); err == nil {
		t.Error("closed server read")
	}
	if err := patchExistingProfile(ctx, httpClient(), hostOf(srv), testUser, testPass, "/Common/p", "c", "k", ""); err == nil {
		t.Error("closed server patch")
	}
}
