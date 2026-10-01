package httpapi

import (
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// seedListData writes rows straight into the memstore: 30 configurations,
// 3 targets, a parent job with 4 children (one with 3 history entries) and
// 40 newer direct jobs.
func seedListData(t *testing.T, f *apiFixture) (parentID, childID, targetID string) {
	t.Helper()
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Second)
	for i := 0; i < 30; i++ {
		if err := f.mem.InsertConfiguration(ctx, store.TargetConfiguration{ID: store.NewID(), TenantID: apiTenant, Name: fmt.Sprintf("cfg-%02d", i),
			ProviderType: "dummy", Status: "active", Config: map[string]any{}, CreatedAt: base.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		tg := store.DeploymentTarget{ID: store.NewID(), TenantID: apiTenant, Name: fmt.Sprintf("tgt-%d", i), CreatedAt: base}
		if err := f.mem.InsertTarget(ctx, tg); err != nil {
			t.Fatal(err)
		}
		targetID = tg.ID
	}
	parentID = store.NewID()
	mustJob := func(j store.DeploymentJob) {
		if err := f.mem.InsertJob(ctx, j); err != nil {
			t.Fatal(err)
		}
	}
	mustJob(store.DeploymentJob{ID: parentID, TenantID: apiTenant, DeploymentTargetID: &targetID, CertificateID: "c", Status: "partial", TriggeredBy: "manual", CreatedAt: base})
	for i := 0; i < 4; i++ {
		id := store.NewID()
		mustJob(store.DeploymentJob{ID: id, TenantID: apiTenant, DeploymentTargetID: &targetID, ParentJobID: &parentID, CertificateID: "c", Status: "completed",
			TriggeredBy: "manual", CreatedAt: base.Add(time.Duration(i) * time.Second)})
		childID = id
	}
	for i := 0; i < 3; i++ {
		if err := f.mem.InsertHistory(ctx, store.DeploymentHistory{ID: store.NewID(), TenantID: apiTenant, JobID: childID, Action: "deploy", Result: "success",
			CreatedAt: base.Add(time.Duration(i) * time.Second)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 40; i++ {
		mustJob(store.DeploymentJob{ID: store.NewID(), TenantID: apiTenant, CertificateID: "c", Status: "failed", TriggeredBy: "event",
			CreatedAt: base.Add(time.Hour + time.Duration(i)*time.Second)})
	}
	return parentID, childID, targetID
}

func TestListContractPages(t *testing.T) {
	f := newAPI(t)
	parentID, childID, targetID := seedListData(t, f)

	w := f.req(t, "GET", p+"/configurations?page=2&page_size=10&sort=name&order=desc", "admin", "")
	if w.Code != 200 {
		t.Fatalf("configs: %d %s", w.Code, w.Body.String())
	}
	m := decode(t, w)
	items, _ := m["items"].([]any)
	if len(items) != 10 || m["total"] != float64(30) || m["page"] != float64(2) || m["page_size"] != float64(10) || m["sort"] != "name" || m["order"] != "desc" {
		t.Fatalf("configs page: %v", m)
	}
	if first := items[0].(map[string]any)["name"]; first != "cfg-19" {
		t.Fatalf("configs order: first %v", first)
	}
	// Defaults and a page beyond the end (answers the last page).
	m = decode(t, f.req(t, "GET", p+"/configurations?page=99", "admin", ""))
	if m["page"] != float64(2) || m["page_size"] != float64(25) || m["sort"] != "name" || m["order"] != "asc" || len(m["items"].([]any)) != 5 {
		t.Fatalf("configs clamp: %v", m)
	}
	// provider_type filter keeps working.
	if m = decode(t, f.req(t, "GET", p+"/configurations?provider_type=nope", "admin", "")); m["total"] != float64(0) || len(m["items"].([]any)) != 0 {
		t.Fatalf("configs filter: %v", m)
	}

	m = decode(t, f.req(t, "GET", p+"/targets?sort=created_at", "admin", ""))
	if m["total"] != float64(3) || m["order"] != "desc" || m["sort"] != "created_at" {
		t.Fatalf("targets: %v", m)
	}
	if tg := m["items"].([]any)[0].(map[string]any); tg["configuration_ids"] == nil || tg["created_at"] == nil {
		t.Fatalf("target view: %v", tg)
	}

	// Jobs: newest first by default; job_type and target_id are filtered
	// before paging (the 4 children are older than the 40 direct jobs).
	m = decode(t, f.req(t, "GET", p+"/jobs", "admin", ""))
	if m["total"] != float64(45) || m["sort"] != "created_at" || m["order"] != "desc" || len(m["items"].([]any)) != 25 {
		t.Fatalf("jobs: %v", m)
	}
	if m = decode(t, f.req(t, "GET", p+"/jobs?job_type=child&page_size=3", "admin", "")); m["total"] != float64(4) || len(m["items"].([]any)) != 3 {
		t.Fatalf("jobs child: %v", m)
	}
	if m = decode(t, f.req(t, "GET", p+"/jobs?target_id="+targetID+"&sort=job_type&order=asc", "admin", "")); m["total"] != float64(5) {
		t.Fatalf("jobs target: %v", m)
	} else if first := m["items"].([]any)[0].(map[string]any)["type"]; first != "child" {
		t.Fatalf("jobs job_type order: first %v", first)
	}

	m = decode(t, f.req(t, "GET", p+"/jobs/"+parentID+"/children?page_size=3", "admin", ""))
	if m["total"] != float64(4) || len(m["items"].([]any)) != 3 || m["sort"] != "created_at" || m["order"] != "desc" {
		t.Fatalf("children: %v", m)
	}
	m = decode(t, f.req(t, "GET", p+"/jobs/"+childID+"/history?order=asc", "admin", ""))
	if m["total"] != float64(3) || m["order"] != "asc" {
		t.Fatalf("history: %v", m)
	}
	missing := "44444444-4444-7444-8444-444444444444"
	for _, sub := range []string{"children", "history"} {
		if w := f.req(t, "GET", p+"/jobs/"+missing+"/"+sub, "admin", ""); w.Code != 404 {
			t.Fatalf("%s of missing job: %d", sub, w.Code)
		}
		if w := f.req(t, "GET", p+"/jobs/"+parentID+"/"+sub, "", ""); w.Code != 401 {
			t.Fatalf("%s unauthenticated: %d", sub, w.Code)
		}
	}
}

// TestListContractNegatives (T098): invalid list parameters are 422
// validation_failed naming the parameter, never echoing the value.
func TestListContractNegatives(t *testing.T) {
	f := newAPI(t)
	parentID, _, _ := seedListData(t, f)
	lists := []string{"/configurations", "/targets", "/jobs", "/jobs/" + parentID + "/children", "/jobs/" + parentID + "/history"}
	cases := []struct{ query, param string }{
		{"sort=zzbogus", "sort"},
		{"sort=id", "sort"},
		{"order=zzbogus", "order"},
		{"page=0", "page"},
		{"page=-1", "page"},
		{"page=zzbogus", "page"},
		{"page=99999999999", "page"},
		{"page_size=0", "page_size"},
		{"page_size=201", "page_size"},
		{"page_size=zzbogus", "page_size"},
	}
	for _, l := range lists {
		for _, c := range cases {
			w := f.req(t, "GET", p+l+"?"+c.query, "admin", "")
			if w.Code != 422 {
				t.Fatalf("%s?%s: %d %s", l, c.query, w.Code, w.Body.String())
			}
			m := decode(t, w)
			d, _ := m["detail"].(map[string]any)
			if m["reason"] != "validation_failed" || d["param"] != c.param {
				t.Fatalf("%s?%s: %s", l, c.query, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "zzbogus") || strings.Contains(w.Body.String(), "99999999999") {
				t.Fatalf("%s?%s: value echoed: %s", l, c.query, w.Body.String())
			}
		}
	}
	for _, c := range []struct{ query, param string }{
		{"job_type=zzbogus", "job_type"},
		{"target_id=zzbogus", "target_id"},
		{"parent_job_id=zzbogus", "parent_job_id"},
	} {
		w := f.req(t, "GET", p+"/jobs?"+c.query, "admin", "")
		if m := decode(t, w); w.Code != 422 || m["detail"].(map[string]any)["param"] != c.param {
			t.Fatalf("jobs?%s: %d %s", c.query, w.Code, w.Body.String())
		}
	}
	// The handler-level parser enforces the same rules (defence in depth).
	r := f.req(t, "GET", p+"/targets?page_size=200&sort=name&order=asc", "admin", "")
	if r.Code != 200 {
		t.Fatalf("max page size: %d", r.Code)
	}
}

func TestParseListDirect(t *testing.T) {
	for q, param := range map[string]string{"sort=nope": "sort", "order=up": "order", "page=0": "page", "page_size=500": "page_size"} {
		w := httptest.NewRecorder()
		if _, ok := parseList(w, httptest.NewRequest("GET", "/x?"+q, nil), store.JobList); ok || w.Code != 422 || !strings.Contains(w.Body.String(), `"param":"`+param+`"`) {
			t.Fatalf("%s: ok=%v %d %s", q, ok, w.Code, w.Body.String())
		}
	}
	w := httptest.NewRecorder()
	req, ok := parseList(w, httptest.NewRequest("GET", "/x?sort=status", nil), store.JobList)
	if !ok || req.Sort != "status" || req.Order != "asc" || req.Page != 1 || req.PageSize != 25 {
		t.Fatalf("valid: %v %+v", ok, req)
	}
}
