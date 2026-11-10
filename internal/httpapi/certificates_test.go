package httpapi

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/lcmclient"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
)

// fakeFinder answers lcm certificate lookups by host name.
type fakeFinder struct {
	tenant, name string
	items        []lcmclient.Match
	err          error
}

func (f *fakeFinder) FindCertificates(_ context.Context, tenantID, name string, _ int) ([]lcmclient.Match, error) {
	f.tenant, f.name = tenantID, name
	return f.items, f.err
}

// TestFindCertificates: the Deploy dialog looks certificates up by host name
// in the caller's tenant; a bad name is validation_failed, lcm being down is
// temporarily_unavailable.
func TestFindCertificates(t *testing.T) {
	f := newAPI(t)
	ff := f.finder
	ff.items = []lcmclient.Match{{ID: apiCert, Subject: "pbx.example.org", SANs: []string{"pbx.example.org"}, Serial: "5119", Status: "active", NotAfter: time.Now().Add(time.Hour), HasKey: true}}

	w := f.req(t, "GET", p+"/certificates?name=+pbx.example.org+", "admin", "")
	if w.Code != 200 || ff.tenant != apiTenant || ff.name != "pbx.example.org" {
		t.Fatalf("find: %d %s (tenant %q, name %q)", w.Code, w.Body.String(), ff.tenant, ff.name)
	}
	items := decode(t, w)["items"].([]any)
	if len(items) != 1 || items[0].(map[string]any)["id"] != apiCert || items[0].(map[string]any)["has_key"] != true {
		t.Fatalf("items = %v", items)
	}
	ff.items = []lcmclient.Match{}
	if w := f.req(t, "GET", p+"/certificates?name=nothing", "admin", ""); w.Code != 200 || !strings.Contains(w.Body.String(), `"items":[]`) {
		t.Fatalf("no match: %d %s", w.Code, w.Body.String())
	}
	for _, q := range []string{"", "?name=", "?name=" + strings.Repeat("a", 254)} {
		if w := f.req(t, "GET", p+"/certificates"+q, "admin", ""); w.Code != 422 {
			t.Fatalf("name %q: %d %s", q, w.Code, w.Body.String())
		}
	}
	ff.err = lcmclient.ErrBadName
	if w := f.req(t, "GET", p+"/certificates?name=a%20b", "admin", ""); w.Code != 422 || !strings.Contains(w.Body.String(), `"param":"name"`) {
		t.Fatalf("bad name: %d %s", w.Code, w.Body.String())
	}
	ff.err = errors.New("lcm down")
	if w := f.req(t, "GET", p+"/certificates?name=pbx", "admin", ""); w.Code != 503 || strings.Contains(w.Body.String(), "lcm down") {
		t.Fatalf("lcm down: %d %s", w.Code, w.Body.String())
	}
	if w := f.req(t, "GET", p+"/certificates?name=pbx", "", ""); w.Code != 401 {
		t.Fatalf("no token: %d", w.Code)
	}
}

// TestDeployRefusesNonCertificateID: a serial number or host name pasted as
// the certificate id is refused at once instead of becoming a job that
// retries a lookup lcm can never answer.
func TestDeployRefusesNonCertificateID(t *testing.T) {
	f := newAPI(t)
	for _, id := range []string{"511981003075791050309003028073283152729555", "pbx.example.org", ""} {
		for path, rest := range map[string]string{
			"/deploy":                `"configuration_id":"` + apiAdmin + `"`,
			"/deploy/target":         `"target_id":"` + apiAdmin + `"`,
			"/deploy/configurations": `"configuration_ids":["` + apiAdmin + `"]`,
		} {
			w := f.req(t, "POST", p+path, "admin", `{"certificate_id":"`+id+`",`+rest+`}`)
			if w.Code != 422 || !strings.Contains(w.Body.String(), `"field":"certificate_id"`) {
				t.Fatalf("%s %q: %d %s", path, id, w.Code, w.Body.String())
			}
		}
	}
	jobs, err := f.mem.ListJobs(context.Background(), apiTenant, repo.JobFilter{})
	if err != nil || len(jobs) != 0 {
		t.Fatalf("jobs created: %v %v", jobs, err)
	}
}
