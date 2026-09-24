package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-auth/sdk/v4/pkg/authclient"
	"github.com/go-tangra/go-tangra/v4/freyatest/testrt"
	"github.com/go-tangra/go-tangra/v4/freyatest/testutil"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/backup"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/deploy"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/stats"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/dummy"
)

const (
	apiTenant = "11111111-1111-7111-8111-111111111111"
	apiAdmin  = "22222222-2222-7222-8222-222222222222"
)

// fakeVerifier maps a bearer token to a fixed identity.
type fakeVerifier struct {
	ids map[string]authclient.Identity
}

func (f fakeVerifier) Verify(_ context.Context, token string) (authclient.Identity, error) {
	if id, ok := f.ids[token]; ok {
		return id, nil
	}
	return authclient.Identity{}, ErrUnauthenticated
}

// fakeCerts is a no-op certificate fetcher (the worker is not run in these tests).
type fakeCerts struct{}

func (fakeCerts) FetchCertificate(_ context.Context, _, id string, _ bool) (provider.CertificateData, error) {
	return provider.CertificateData{ID: id, SerialNumber: "01", CommonName: "api.example.com"}, nil
}

type apiFixture struct {
	s   *Server
	mem *memstore.Mem
}

func newAPI(t *testing.T) *apiFixture {
	t.Helper()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	mem := memstore.New()
	rt := testrt.New(t, testutil.MustCA("example.org"), "deployer")
	az := authz.New(mem)
	cs := configs.New(mem, env, az)
	ds := deploy.New(mem, az)
	tgs := targets.New(mem, az)
	js := jobs.New(mem, az, fakeCerts{}, cs, nil, jobs.Config{Workers: 1})

	v := fakeVerifier{ids: map[string]authclient.Identity{
		"admin": {UserID: apiAdmin, TenantID: apiTenant, Roles: []string{"admin"}},
	}}
	s, err := NewHandler(rt, WithVerifier(v))
	if err != nil {
		t.Fatal(err)
	}
	s.Register(Deps{Configs: cs, Targets: tgs, Deploy: ds, Jobs: js, Stats: stats.New(mem), Backup: backup.New(mem)})
	return &apiFixture{s: s, mem: mem}
}

// req drives one request as the admin (empty tok => no Authorization). Mutating
// methods carry the required CSRF header.
func (f *apiFixture) req(t *testing.T, method, path, tok, body string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, "https://localhost"+path, strings.NewReader(body))
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if tok != "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	if method != "GET" {
		r.Header.Set("X-CSRF-Token", "t")
	}
	w := httptest.NewRecorder()
	f.s.Handler().ServeHTTP(w, r)
	return w
}

func decode(t *testing.T, w *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &m); err != nil {
		t.Fatalf("decode %q: %v", w.Body.String(), err)
	}
	return m
}

const p = "/api/deployer/v1"

func TestAuthRequired(t *testing.T) {
	f := newAPI(t)
	if w := f.req(t, "GET", p+"/providers", "", ""); w.Code != 401 {
		t.Fatalf("no token: %d", w.Code)
	}
	if w := f.req(t, "GET", p+"/providers", "bad", ""); w.Code != 401 {
		t.Fatalf("bad token: %d", w.Code)
	}
	if w := f.req(t, "GET", p+"/providers", "admin", ""); w.Code != 200 {
		t.Fatalf("admin: %d %s", w.Code, w.Body.String())
	}
}

func TestProvidersCatalogue(t *testing.T) {
	f := newAPI(t)
	w := f.req(t, "GET", p+"/providers", "admin", "")
	if w.Code != 200 {
		t.Fatalf("providers: %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "dummy") {
		t.Fatalf("catalogue missing dummy: %s", w.Body.String())
	}
}

func TestConfigurationsCRUDAndRedaction(t *testing.T) {
	f := newAPI(t)
	// Create with credentials.
	w := f.req(t, "POST", p+"/configurations", "admin",
		`{"name":"dummy-ep","provider_type":"dummy","config":{"region":"eu"},"credentials":{"token":"secret-xyz"}}`)
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	created := decode(t, w)
	id, _ := created["id"].(string)
	if id == "" || created["has_credentials"] != true {
		t.Fatalf("created: %+v", created)
	}
	// The secret must never appear in any response body.
	if strings.Contains(w.Body.String(), "secret-xyz") {
		t.Fatal("credential leaked in create response")
	}

	// List + Get.
	if w := f.req(t, "GET", p+"/configurations", "admin", ""); w.Code != 200 || !strings.Contains(w.Body.String(), "dummy-ep") {
		t.Fatalf("list: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "GET", p+"/configurations/"+id, "admin", ""); w.Code != 200 || strings.Contains(w.Body.String(), "secret-xyz") {
		t.Fatalf("get leaked or failed: %d %s", w.Code, w.Body.String())
	}
	// Validate.
	if w := f.req(t, "POST", p+"/configurations/validate", "admin", `{"provider_type":"dummy","credentials":{}}`); w.Code != 200 {
		t.Fatalf("validate: %d %s", w.Code, w.Body.String())
	}
	// Update.
	if w := f.req(t, "PUT", p+"/configurations/"+id, "admin", `{"name":"dummy-ep","description":"changed"}`); w.Code != 200 {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
	// Missing config → 404.
	if w := f.req(t, "GET", p+"/configurations/44444444-4444-4444-4444-444444444444", "admin", ""); w.Code != 404 {
		t.Fatalf("missing: %d", w.Code)
	}
	// Delete.
	if w := f.req(t, "POST", p+"/configurations/"+id+"/remove", "admin", `{}`); w.Code/100 != 2 {
		t.Fatalf("remove: %d %s", w.Code, w.Body.String())
	}
}

func TestTargetsAndDeploy(t *testing.T) {
	f := newAPI(t)
	// A configuration to deploy to.
	cw := f.req(t, "POST", p+"/configurations", "admin", `{"name":"ep","provider_type":"dummy"}`)
	cfgID := decode(t, cw)["id"].(string)

	// Target CRUD + attach.
	tw := f.req(t, "POST", p+"/targets", "admin", `{"name":"prod","auto_deploy":true,"certificate_filters":[]}`)
	if tw.Code != 201 {
		t.Fatalf("create target: %d %s", tw.Code, tw.Body.String())
	}
	tid := decode(t, tw)["id"].(string)
	if w := f.req(t, "GET", p+"/targets", "admin", ""); w.Code != 200 {
		t.Fatalf("list targets: %d", w.Code)
	}
	if w := f.req(t, "GET", p+"/targets/"+tid, "admin", ""); w.Code != 200 {
		t.Fatalf("get target: %d", w.Code)
	}
	if w := f.req(t, "POST", p+"/targets/"+tid+"/configurations", "admin", `{"configuration_ids":["`+cfgID+`"]}`); w.Code != 200 {
		t.Fatalf("attach: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "GET", p+"/targets/"+tid+"/configurations", "admin", ""); w.Code != 200 {
		t.Fatalf("list target configs: %d", w.Code)
	}

	// Deploy (direct) → 202.
	dw := f.req(t, "POST", p+"/deploy", "admin", `{"certificate_id":"cert-1","configuration_id":"`+cfgID+`"}`)
	if dw.Code != 202 {
		t.Fatalf("deploy: %d %s", dw.Code, dw.Body.String())
	}
	// Deploy to target → 202.
	if w := f.req(t, "POST", p+"/deploy/target", "admin", `{"certificate_id":"cert-1","target_id":"`+tid+`"}`); w.Code != 202 {
		t.Fatalf("deploy target: %d %s", w.Code, w.Body.String())
	}
	// Deploy to configurations → 202.
	if w := f.req(t, "POST", p+"/deploy/configurations", "admin", `{"certificate_id":"cert-1","configuration_ids":["`+cfgID+`"]}`); w.Code != 202 {
		t.Fatalf("deploy configs: %d %s", w.Code, w.Body.String())
	}
}

func TestJobsLifecycleEndpoints(t *testing.T) {
	f := newAPI(t)
	cfgID := decode(t, f.req(t, "POST", p+"/configurations", "admin", `{"name":"ep","provider_type":"dummy"}`))["id"].(string)
	jobID := decode(t, f.req(t, "POST", p+"/deploy", "admin", `{"certificate_id":"cert-1","configuration_id":"`+cfgID+`"}`))["job_id"].(string)

	if w := f.req(t, "GET", p+"/jobs", "admin", ""); w.Code != 200 {
		t.Fatalf("list jobs: %d", w.Code)
	}
	if w := f.req(t, "GET", p+"/jobs?status=pending&job_type=direct", "admin", ""); w.Code != 200 {
		t.Fatalf("list jobs filtered: %d", w.Code)
	}
	if w := f.req(t, "GET", p+"/jobs/"+jobID, "admin", ""); w.Code != 200 {
		t.Fatalf("get job: %d", w.Code)
	}
	if w := f.req(t, "GET", p+"/jobs/"+jobID+"/result", "admin", ""); w.Code != 200 {
		t.Fatalf("job result: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "POST", p+"/jobs/"+jobID+"/cancel", "admin", `{}`); w.Code != 200 {
		t.Fatalf("cancel: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "POST", p+"/jobs/"+jobID+"/retry", "admin", `{}`); w.Code != 200 {
		t.Fatalf("retry: %d %s", w.Code, w.Body.String())
	}
	// verify/rollback on a direct job (dummy supports both).
	if w := f.req(t, "POST", p+"/deploy/"+jobID+"/verify", "admin", `{}`); w.Code != 200 {
		t.Fatalf("verify: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "POST", p+"/deploy/"+jobID+"/rollback", "admin", `{}`); w.Code != 200 {
		t.Fatalf("rollback: %d %s", w.Code, w.Body.String())
	}
}

func TestStatisticsAndBackup(t *testing.T) {
	f := newAPI(t)
	f.req(t, "POST", p+"/configurations", "admin", `{"name":"ep","provider_type":"dummy","credentials":{"token":"s"}}`)

	if w := f.req(t, "GET", p+"/statistics/tenant", "admin", ""); w.Code != 200 {
		t.Fatalf("tenant stats: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "GET", p+"/statistics", "admin", ""); w.Code != 200 {
		t.Fatalf("system stats (admin): %d %s", w.Code, w.Body.String())
	}
	// Export excludes credentials by default.
	ew := f.req(t, "POST", p+"/backup/export", "admin", `{"include_credentials":false}`)
	if ew.Code != 200 || strings.Contains(ew.Body.String(), `"credentials_sealed"`) {
		t.Fatalf("export leaked or failed: %d %s", ew.Code, ew.Body.String())
	}
	// Round-trip import (skip mode) accepts the exported document.
	imp := `{"mode":"skip","backup":` + ew.Body.String() + `}`
	if w := f.req(t, "POST", p+"/backup/import", "admin", imp); w.Code != 200 {
		t.Fatalf("import: %d %s", w.Code, w.Body.String())
	}
}
