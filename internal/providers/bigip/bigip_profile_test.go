package bigip

import (
	"context"
	"net/http"
	"reflect"
	"strings"
	"testing"
)

// Client-SSL profile creation and update (v3 createOrUpdateSSLProfile /
// updateSSLProfile) against the stateful fake iControl REST.

func TestResolveProfilePath(t *testing.T) {
	cases := []struct {
		in, part, want string
		ok             bool
	}{
		{"www_clientssl_prod", "Common", "/Common/www_clientssl_prod", true},
		{"www_clientssl_prod", "Prod", "/Prod/www_clientssl_prod", true},
		{" www_clientssl ", "Prod", "/Prod/www_clientssl", true},
		{"/Shared/www.ssl", "Prod", "/Shared/www.ssl", true},
		{"", "Common", "", false},
		{"/Common/", "Common", "", false},
		{"a/b", "Common", "", false},
		{"bad name", "Common", "", false},
		{"/Common/x/y", "Common", "", false},
		{"~Common~x", "Common", "", false},
		{"-lead", "Common", "", false},
		{".hidden", "Common", "", false},
		{"/../x", "Common", "", false},
		{"/./x", "Common", "", false},
		{"/Common/..", "Common", "", false},
	}
	for _, c := range cases {
		got, ok := resolveProfilePath(c.in, c.part)
		if got != c.want || ok != c.ok {
			t.Errorf("resolveProfilePath(%q,%q) = %q,%v want %q,%v", c.in, c.part, got, ok, c.want, c.ok)
		}
	}
}

// profileCalls returns the client-SSL profile requests.
func profileCalls(f *fakeIControl) []call {
	var out []call
	for _, c := range f.recorded() {
		if strings.HasPrefix(c.path, "/mgmt/tm/ltm/profile/client-ssl") {
			out = append(out, c)
		}
	}
	return out
}

// TestProfileCreatedWhenMissing: a profile that does not exist is created
// with v3's body — name, cert, key, chain "none", ciphers "DEFAULT" — after
// the certificate, key and chain are installed and before the verification.
func TestProfileCreatedWhenMissing(t *testing.T) {
	f := newFake(t)
	res, steps := deployWith(t, f, map[string]any{"partition": "Prod", "ssl_profile": "www_clientssl"}, testCert)
	if !res.Success || res.Message != "Certificate deployed successfully to F5 BIG-IP" {
		t.Fatalf("result = %+v", res)
	}
	if res.Details["ssl_profile"] != "www_clientssl" || len(res.Details) != 5 {
		t.Fatalf("details = %v", res.Details)
	}
	assertSeq(t, f.recorded(),
		"GET /mgmt/tm/sys/version",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com.key",
		"POST /mgmt/tm/sys/crypto/key",
		"POST /mgmt/shared/file-transfer/uploads/www_example_com_chain.crt",
		"POST /mgmt/tm/sys/crypto/cert",
		"POST /mgmt/tm/ltm/profile/client-ssl",
		"GET /mgmt/tm/sys/crypto/cert/~Prod~www_example_com.crt",
	)
	pc := profileCalls(f)
	if pc[0].contentType != "application/json" || !pc[0].authOK {
		t.Fatalf("profile create headers: %+v", pc[0])
	}
	assertBody(t, pc[0], map[string]any{
		"name":    "/Prod/www_clientssl",
		"cert":    "/Prod/www_example_com.crt",
		"key":     "/Prod/www_example_com.key",
		"chain":   "none",
		"ciphers": "DEFAULT",
	})
	if p := f.profile("/Prod/www_clientssl"); p == nil || p["cert"] != "/Prod/www_example_com.crt" {
		t.Fatalf("profile = %v", p)
	}
	if !containsStep(steps, step{70, "Creating/updating SSL profile"}) {
		t.Fatalf("progress = %v", steps)
	}
}

// TestProfileUpdatedWhenExisting: an existing profile answers 409 on create;
// only cert and key are PATCHed, every other setting (chain, ciphers,
// parent, SNI) is left as the operator configured it.
func TestProfileUpdatedWhenExisting(t *testing.T) {
	f := newFake(t)
	f.addProfile("/Prod/www_clientssl", map[string]any{
		"cert": "/Prod/old.crt", "key": "/Prod/old.key", "chain": "/Common/intermediate.crt",
		"ciphers": "ECDHE+AES-GCM", "defaultsFrom": "/Common/clientssl-secure", "serverName": "www.example.com", "sniDefault": "true",
	})
	res, _ := deployWith(t, f, map[string]any{"partition": "Prod", "ssl_profile": "www_clientssl"}, testCert)
	if !res.Success || res.Details["ssl_profile"] != "www_clientssl" {
		t.Fatalf("result = %+v", res)
	}
	pc := profileCalls(f)
	assertSeq(t, pc,
		"POST /mgmt/tm/ltm/profile/client-ssl",
		"PATCH /mgmt/tm/ltm/profile/client-ssl/~Prod~www_clientssl",
	)
	assertBody(t, pc[0], map[string]any{
		"name": "/Prod/www_clientssl", "cert": "/Prod/www_example_com.crt", "key": "/Prod/www_example_com.key",
		"chain": "none", "ciphers": "DEFAULT",
	})
	assertBody(t, pc[1], map[string]any{"cert": "/Prod/www_example_com.crt", "key": "/Prod/www_example_com.key"})
	if pc[1].contentType != "application/json" || !pc[1].authOK {
		t.Fatalf("patch headers: %+v", pc[1])
	}
	want := map[string]any{
		"fullPath": "/Prod/www_clientssl",
		"cert":     "/Prod/www_example_com.crt", "key": "/Prod/www_example_com.key", "chain": "/Common/intermediate.crt",
		"ciphers": "ECDHE+AES-GCM", "defaultsFrom": "/Common/clientssl-secure", "serverName": "www.example.com", "sniDefault": "true",
	}
	if got := f.profile("/Prod/www_clientssl"); !reflect.DeepEqual(got, want) {
		t.Fatalf("profile after update = %v", got)
	}
}

// TestProfileFullPath: /Partition/name places the profile in that partition
// while the certificate objects stay in the configured one.
func TestProfileFullPath(t *testing.T) {
	f := newFake(t)
	res, _ := deployWith(t, f, map[string]any{"partition": "Prod", "ssl_profile": "/Shared/www_clientssl"}, testCert)
	if !res.Success || res.Details["ssl_profile"] != "/Shared/www_clientssl" {
		t.Fatalf("result = %+v", res)
	}
	pc := profileCalls(f)
	if len(pc) != 1 || jsonBody(t, pc[0])["name"] != "/Shared/www_clientssl" || jsonBody(t, pc[0])["cert"] != "/Prod/www_example_com.crt" {
		t.Fatalf("profile calls = %+v", pc)
	}
	// Second deployment: the profile exists → PATCH under the full path.
	f.reset()
	res, _ = deployWith(t, f, map[string]any{"partition": "Prod", "ssl_profile": "/Shared/www_clientssl"}, testCert)
	if !res.Success {
		t.Fatal(res)
	}
	assertSeq(t, profileCalls(f), "POST /mgmt/tm/ltm/profile/client-ssl", "PATCH /mgmt/tm/ltm/profile/client-ssl/~Shared~www_clientssl")
}

// TestProfileDefaultPartition: a bare profile name without partition lives
// in Common (v3).
func TestProfileDefaultPartition(t *testing.T) {
	f := newFake(t)
	res, _ := deployWith(t, f, map[string]any{"ssl_profile": "p"}, testCert)
	if !res.Success || f.profile("/Common/p") == nil {
		t.Fatalf("result = %+v profiles = %v", res, f.profiles)
	}
}

// TestProfileIdempotentRedeploy: deploying the same certificate again
// overwrites the objects and re-points the profile; nothing is deleted.
func TestProfileIdempotentRedeploy(t *testing.T) {
	f := newFake(t)
	cfg := map[string]any{"partition": "Common", "ssl_profile": "www_clientssl"}
	for i := 0; i < 3; i++ {
		res, _ := deployWith(t, f, cfg, testCert)
		if !res.Success {
			t.Fatalf("deploy %d = %+v", i, res)
		}
	}
	for _, c := range f.recorded() {
		if c.method == http.MethodDelete {
			t.Fatalf("deploy deleted %s", c.path)
		}
	}
	if n := len(profileCalls(f)); n != 5 { // 1 create + 2×(POST 409 + PATCH)
		t.Fatalf("profile calls = %d", n)
	}
}

// TestProfileFailures: a create answer other than 2xx/409 fails without a
// PATCH (v3 checks the status only, not an "already exists" body); a failed
// PATCH fails; neither runs the verification.
func TestProfileFailures(t *testing.T) {
	cfg := map[string]any{"partition": "Common", "ssl_profile": "p"}
	cases := []struct {
		name  string
		setup func(*fakeIControl)
		want  string
		calls []string
	}{
		{"create rejected", func(f *fakeIControl) {
			f.failAlways(http.MethodPost, "/mgmt/tm/ltm/profile/client-ssl", 400, `{"message":"profile already exists elsewhere"}`)
		}, `failed to create/update SSL profile: API error (HTTP 400): {"message":"profile already exists elsewhere"}`,
			[]string{"POST /mgmt/tm/ltm/profile/client-ssl"}},
		{"patch rejected", func(f *fakeIControl) {
			f.addProfile("/Common/p", nil)
			f.failAlways(http.MethodPatch, "/mgmt/tm/ltm/profile/client-ssl/~Common~p", 400, `{"message":"key and certificate do not match"}`)
		}, `failed to create/update SSL profile: API error (HTTP 400): {"message":"key and certificate do not match"}`,
			[]string{"POST /mgmt/tm/ltm/profile/client-ssl", "PATCH /mgmt/tm/ltm/profile/client-ssl/~Common~p"}},
		{"patch not 200", func(f *fakeIControl) {
			f.failOnce(http.MethodPost, "/mgmt/tm/ltm/profile/client-ssl", 409, `{}`)
			f.failOnce(http.MethodPatch, "/mgmt/tm/ltm/profile/client-ssl/~Common~p", 201, `{}`)
		}, `failed to create/update SSL profile: API error (HTTP 201): {}`,
			[]string{"POST /mgmt/tm/ltm/profile/client-ssl", "PATCH /mgmt/tm/ltm/profile/client-ssl/~Common~p"}},
		{"profile deleted between POST and PATCH", func(f *fakeIControl) {
			f.failOnce(http.MethodPost, "/mgmt/tm/ltm/profile/client-ssl", 409, `{}`)
		}, `failed to create/update SSL profile: API error (HTTP 404): {"code":404,"message":"01020036:3: The requested profile was not found."}`,
			[]string{"POST /mgmt/tm/ltm/profile/client-ssl", "PATCH /mgmt/tm/ltm/profile/client-ssl/~Common~p"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFake(t)
			tc.setup(f)
			res, _ := deployWith(t, f, cfg, testCert)
			if res.Success || res.Permanent || res.Message != tc.want {
				t.Fatalf("result = %+v\nwant %q", res, tc.want)
			}
			assertSeq(t, profileCalls(f), tc.calls...)
			for _, c := range f.recorded() {
				if strings.HasPrefix(c.path, "/mgmt/tm/sys/crypto/cert/") {
					t.Fatal("verification ran after a profile failure")
				}
			}
			assertNoSecret(t, res)
		})
	}
}

// TestRollbackDeletesProfileFirst: with ssl_profile the profile is deleted
// first (best effort), then the certificate, key and chain.
func TestRollbackDeletesProfileFirst(t *testing.T) {
	ctx := context.Background()
	f := newFake(t)
	cfg := map[string]any{"partition": "Prod", "ssl_profile": "www_clientssl"}
	if res, _ := deployWith(t, f, cfg, testCert); !res.Success {
		t.Fatal(res)
	}
	f.reset()
	res, err := Provider{}.Rollback(ctx, testCert, cfg, f.creds())
	if err != nil || !res.Success || res.Message != "Certificate and key removed from BIG-IP" {
		t.Fatalf("rollback = %+v, %v", res, err)
	}
	assertSeq(t, f.recorded(),
		"GET /mgmt/tm/sys/version",
		"DELETE /mgmt/tm/ltm/profile/client-ssl/~Prod~www_clientssl",
		"DELETE /mgmt/tm/sys/crypto/cert/~Prod~www_example_com.crt",
		"DELETE /mgmt/tm/sys/crypto/key/~Prod~www_example_com.key",
		"DELETE /mgmt/tm/sys/crypto/cert/~Prod~www_example_com_chain.crt",
	)
	if len(f.profiles) != 0 {
		t.Fatalf("profile left: %v", f.profiles)
	}

	// Full path; profile already gone (404) is fine.
	f.reset()
	res, _ = Provider{}.Rollback(ctx, testCert, map[string]any{"partition": "Prod", "ssl_profile": "/Shared/x"}, f.creds())
	if !res.Success || f.recorded()[1].path != "/mgmt/tm/ltm/profile/client-ssl/~Shared~x" {
		t.Fatalf("full path rollback = %+v %v", res, sig(f.recorded()))
	}

	// A profile still attached to a virtual server cannot be deleted: the
	// failure is ignored and the certificate deletes report their own errors.
	g := newFake(t)
	g.failAlways(http.MethodDelete, "/mgmt/tm/ltm/profile/client-ssl/~Common~p", 400, `{"message":"profile in use by virtual server"}`)
	g.failAlways(http.MethodDelete, "/mgmt/tm/sys/crypto/cert/~Common~www_example_com.crt", 400, `{"message":"in use"}`)
	res, _ = Provider{}.Rollback(ctx, testCert, map[string]any{"ssl_profile": "p"}, g.creds())
	if res.Success || res.Message != `Rollback partially failed: certificate: API error (HTTP 400): {"message":"in use"}` {
		t.Fatalf("in-use rollback = %+v", res)
	}
}

func containsStep(steps []step, s step) bool {
	for _, x := range steps {
		if x == s {
			return true
		}
	}
	return false
}
