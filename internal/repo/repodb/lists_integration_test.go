//go:build integration

package repodb_test

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/go-tangra/go-tangra/v4/listquery"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo/repodb"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
)

// listFixture holds the same rows in the database and in the memstore.
type listFixture struct {
	db, mem repo.Store
	admin   *pgx.Conn
	// tenant A ids
	configs, targets, jobs, children, history []string
	parentID, targetID                       string
	childJobs, targetJobs                    int
}

func ptr[T any](v T) *T { return &v }

// seedLists writes rows with repeated sort keys (case-only name differences,
// equal timestamps, null completed_at) so every sort hits ties.
func seedLists(t *testing.T) *listFixture {
	t.Helper()
	ctx := context.Background()
	adminDSN, appDSN := startDB(t)
	if err := store.Migrate(ctx, adminDSN); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	st, err := store.Open(ctx, appDSN, 4)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(ctx) })
	mem := memstore.New()
	f := &listFixture{db: repodb.New(st), mem: mem, admin: admin}
	base := time.Now().UTC().Truncate(time.Second)
	at := func(table, id string, ts time.Time) {
		if _, err := admin.Exec(ctx, "UPDATE "+table+" SET created_at=$1 WHERE id=$2", ts, id); err != nil {
			t.Fatal(err)
		}
	}
	both := func(fn func(s repo.Store) error) {
		t.Helper()
		for _, s := range []repo.Store{f.db, f.mem} {
			if err := fn(s); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Configurations: 11 in A, 3 in B.
	for i := 0; i < 14; i++ {
		tenant := tenantA
		if i >= 11 {
			tenant = tenantB
		}
		c := store.TargetConfiguration{ID: store.NewID(), TenantID: tenant,
			Name:         fmt.Sprintf("%s-%02d", []string{"Edge", "edge", "core"}[i%3], i),
			ProviderType: []string{"aws_acm", "Cloudflare", "dummy"}[i%3], Status: []string{"active", "error"}[i%2],
			Config: map[string]any{}, CreatedAt: base.Add(time.Duration(i/3) * time.Minute)}
		both(func(s repo.Store) error { return s.InsertConfiguration(ctx, c) })
		at("deployer_configs", c.ID, c.CreatedAt)
		if tenant == tenantA {
			f.configs = append(f.configs, c.ID)
		}
	}
	// Targets: 9 in A (some with links), 2 in B.
	for i := 0; i < 11; i++ {
		tenant := tenantA
		if i >= 9 {
			tenant = tenantB
		}
		tg := store.DeploymentTarget{ID: store.NewID(), TenantID: tenant, Name: fmt.Sprintf("%s %d", []string{"Web", "web", "api"}[i%3], i/3),
			CertificateFilters: []store.CertificateFilter{}, ConfigOverrides: map[string]map[string]any{}, CreatedAt: base.Add(time.Duration(i/2) * time.Minute)}
		both(func(s repo.Store) error { return s.InsertTarget(ctx, tg) })
		at("deployer_targets", tg.ID, tg.CreatedAt)
		if tenant == tenantA {
			f.targets = append(f.targets, tg.ID)
			if i%2 == 0 {
				ids := f.configs[:1+i%3]
				both(func(s repo.Store) error { return s.AttachConfigurations(ctx, tenantA, tg.ID, ids) })
			}
		}
	}
	f.targetID = f.targets[0]

	// Jobs in A: 12 oldest children of one parent (target job), 1 parent, then
	// 60 newest direct jobs — more than the old LIMIT 50 window, so filtering
	// job_type/target_id after the limit would miss every child.
	insertJob := func(j store.DeploymentJob, completed bool) {
		if completed {
			j.CompletedAt = ptr(j.CreatedAt.Add(time.Duration(int(j.ID[len(j.ID)-1])%3) * time.Minute))
		}
		both(func(s repo.Store) error {
			if err := s.InsertJob(ctx, j); err != nil {
				return err
			}
			return s.UpdateJob(ctx, j)
		})
		at("deployer_jobs", j.ID, j.CreatedAt)
	}
	statuses := []string{"completed", "failed", "pending", "partial"}
	parent := store.DeploymentJob{ID: store.NewID(), TenantID: tenantA, DeploymentTargetID: ptr(f.targetID), CertificateID: "cert",
		Status: "partial", TriggeredBy: "manual", MaxRetries: 3, CreatedAt: base.Add(-2 * time.Hour)}
	insertJob(parent, true)
	f.parentID = parent.ID
	f.jobs = append(f.jobs, parent.ID)
	f.targetJobs++
	for i := 0; i < 12; i++ {
		j := store.DeploymentJob{ID: store.NewID(), TenantID: tenantA, DeploymentTargetID: ptr(f.targetID), ParentJobID: ptr(parent.ID),
			TargetConfigurationID: ptr(f.configs[i%3]), CertificateID: "cert", Status: statuses[i%4], TriggeredBy: "manual", MaxRetries: 3,
			CreatedAt: base.Add(-time.Hour + time.Duration(i/4)*time.Second)}
		insertJob(j, i%3 != 0)
		f.jobs = append(f.jobs, j.ID)
		f.children = append(f.children, j.ID)
		f.childJobs++
		f.targetJobs++
	}
	for i := 0; i < 60; i++ {
		j := store.DeploymentJob{ID: store.NewID(), TenantID: tenantA, TargetConfigurationID: ptr(f.configs[i%5]), CertificateID: "cert",
			Status: statuses[i%4], TriggeredBy: "event", MaxRetries: 3, CreatedAt: base.Add(time.Duration(i/5) * time.Second)}
		insertJob(j, i%2 == 0)
		f.jobs = append(f.jobs, j.ID)
	}
	// Tenant B: a few jobs, one of them a child.
	bParent := store.DeploymentJob{ID: store.NewID(), TenantID: tenantB, DeploymentTargetID: ptr(store.NewID()), CertificateID: "b", Status: "completed", TriggeredBy: "manual", CreatedAt: base}
	insertJob(bParent, false)
	insertJob(store.DeploymentJob{ID: store.NewID(), TenantID: tenantB, ParentJobID: ptr(bParent.ID), CertificateID: "b", Status: "completed", TriggeredBy: "manual", CreatedAt: base}, false)

	// History: 9 entries on the first child (ties on created_at), 2 in B.
	for i := 0; i < 11; i++ {
		tenant, job := tenantA, f.children[0]
		if i >= 9 {
			tenant, job = tenantB, bParent.ID
		}
		h := store.DeploymentHistory{ID: store.NewID(), TenantID: tenant, JobID: job, Action: "deploy", Result: "success",
			DurationMS: i, CreatedAt: base.Add(time.Duration(i/2) * time.Second)}
		both(func(s repo.Store) error { return s.InsertHistory(ctx, h) })
		at("deployer_history", h.ID, h.CreatedAt)
		if tenant == tenantA {
			f.history = append(f.history, h.ID)
		}
	}
	return f
}

// pager fetches one page from a store: ids, total and the applied request.
type pager func(s repo.Store, req listquery.Request) ([]string, int, listquery.Request, error)

func idsOf[T any](rows []T, id func(T) string) []string {
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, id(r))
	}
	return out
}

// walk pages through every field × direction of spec with size and checks:
// every row exactly once, the total, and the database order equals the
// memstore's (the listquery Go semantics).
func walk(t *testing.T, f *listFixture, name string, spec listquery.Spec, size int, want []string, get pager) {
	t.Helper()
	for field := range spec.Fields {
		for _, dir := range []listquery.Dir{listquery.Asc, listquery.Desc} {
			label := fmt.Sprintf("%s sort=%s order=%s", name, field, dir)
			orders := map[string][]string{}
			for sname, s := range map[string]repo.Store{"db": f.db, "mem": f.mem} {
				var seen []string
				for p := 1; ; p++ {
					req, err := listquery.New(p, size, field, dir, spec)
					if err != nil {
						t.Fatal(err)
					}
					ids, total, applied, err := get(s, req)
					if err != nil {
						t.Fatalf("%s %s page %d: %v", label, sname, p, err)
					}
					if total != len(want) {
						t.Fatalf("%s %s: total %d, want %d", label, sname, total, len(want))
					}
					if applied.Page != p || applied.Sort != field || applied.Order != dir {
						t.Fatalf("%s %s: applied %+v", label, sname, applied)
					}
					if len(ids) > size {
						t.Fatalf("%s %s: page of %d > size %d", label, sname, len(ids), size)
					}
					seen = append(seen, ids...)
					if p*size >= total {
						break
					}
				}
				if len(seen) != len(want) {
					t.Fatalf("%s %s: saw %d rows, want %d", label, sname, len(seen), len(want))
				}
				uniq := slices.Clone(seen)
				slices.Sort(uniq)
				w := slices.Clone(want)
				slices.Sort(w)
				if !slices.Equal(uniq, w) {
					t.Fatalf("%s %s: rows not exactly once", label, sname)
				}
				orders[sname] = seen
			}
			if !slices.Equal(orders["db"], orders["mem"]) {
				t.Fatalf("%s: database order differs from memstore order", label)
			}
		}
	}
	// A page beyond the end answers the last page.
	req, _ := listquery.New(1000, size, "", "", spec)
	ids, total, applied, err := get(f.db, req)
	last := (total + size - 1) / size
	if err != nil || applied.Page != last || len(ids) != total-(last-1)*size {
		t.Fatalf("%s beyond end: page %d (want %d), %d rows, %v", name, applied.Page, last, len(ids), err)
	}
}

// TestListPaging (specs 032, T097): every list × sort field × direction pages
// exactly once with the database and memstore agreeing; totals are per tenant.
func TestListPaging(t *testing.T) {
	f := seedLists(t)
	ctx := context.Background()

	walk(t, f, "configurations", store.ConfigList, 4, f.configs, func(s repo.Store, req listquery.Request) ([]string, int, listquery.Request, error) {
		rows, total, applied, err := s.PageConfigurations(ctx, tenantA, repo.ConfigFilter{}, req)
		return idsOf(rows, func(c store.TargetConfiguration) string { return c.ID }), total, applied, err
	})
	walk(t, f, "targets", store.TargetList, 4, f.targets, func(s repo.Store, req listquery.Request) ([]string, int, listquery.Request, error) {
		rows, total, applied, err := s.PageTargets(ctx, tenantA, req)
		return idsOf(rows, func(tg store.DeploymentTarget) string { return tg.ID }), total, applied, err
	})
	walk(t, f, "jobs", store.JobList, 7, f.jobs, func(s repo.Store, req listquery.Request) ([]string, int, listquery.Request, error) {
		rows, total, applied, err := s.PageJobs(ctx, tenantA, repo.JobFilter{}, req)
		return idsOf(rows, func(j store.DeploymentJob) string { return j.ID }), total, applied, err
	})
	walk(t, f, "children", store.ChildJobList, 5, f.children, func(s repo.Store, req listquery.Request) ([]string, int, listquery.Request, error) {
		rows, total, applied, err := s.PageChildJobs(ctx, tenantA, f.parentID, req)
		return idsOf(rows, func(j store.DeploymentJob) string { return j.ID }), total, applied, err
	})
	walk(t, f, "history", store.HistoryList, 4, f.history, func(s repo.Store, req listquery.Request) ([]string, int, listquery.Request, error) {
		rows, total, applied, err := s.PageHistory(ctx, tenantA, f.children[0], req)
		return idsOf(rows, func(h store.DeploymentHistory) string { return h.ID }), total, applied, err
	})

	// Filtered configurations: provider_type + status in SQL.
	rows, total, _, err := f.db.PageConfigurations(ctx, tenantA, repo.ConfigFilter{ProviderType: "dummy"}, listquery.Request{})
	if err != nil || total != 3 || len(rows) != 3 {
		t.Fatalf("configs provider filter: total %d rows %d %v", total, len(rows), err)
	}

	// Default order: names case-insensitively ascending.
	trows, _, applied, err := f.db.PageTargets(ctx, tenantA, listquery.Request{})
	if err != nil || applied.Sort != "name" || applied.Order != listquery.Asc || applied.PageSize != 25 || trows[0].Name != "api 0" {
		t.Fatalf("targets default: %+v first=%q %v", applied, trows[0].Name, err)
	}
}

// TestJobFiltersBeforeLimit is the regression for job_type/target_id being
// filtered in Go after the SQL LIMIT: the 12 child jobs are older than the 60
// newest direct jobs, yet every filtered page and total sees all of them.
func TestJobFiltersBeforeLimit(t *testing.T) {
	f := seedLists(t)
	ctx := context.Background()
	for _, c := range []struct {
		name string
		f    repo.JobFilter
		want int
	}{
		{"job_type=child", repo.JobFilter{JobType: store.JobTypeChild}, f.childJobs},
		{"job_type=parent", repo.JobFilter{JobType: store.JobTypeParent}, 1},
		{"job_type=direct", repo.JobFilter{JobType: store.JobTypeDirect}, 60},
		{"job_type=unknown", repo.JobFilter{JobType: "bogus"}, 0},
		{"target_id", repo.JobFilter{TargetID: f.targetID}, f.targetJobs},
		{"target_id+status", repo.JobFilter{TargetID: f.targetID, JobType: store.JobTypeChild, Status: "completed"}, 3},
	} {
		for sname, s := range map[string]repo.Store{"db": f.db, "mem": f.mem} {
			var seen []string
			total := -1
			for p := 1; ; p++ {
				rows, tot, _, err := s.PageJobs(ctx, tenantA, c.f, listquery.Request{Page: p, PageSize: 5})
				if err != nil {
					t.Fatalf("%s %s: %v", c.name, sname, err)
				}
				total = tot
				for _, j := range rows {
					if c.f.JobType != "" && j.JobType() != c.f.JobType || c.f.TargetID != "" && (j.DeploymentTargetID == nil || *j.DeploymentTargetID != c.f.TargetID) {
						t.Fatalf("%s %s: job %s does not match the filter", c.name, sname, j.ID)
					}
					seen = append(seen, j.ID)
				}
				if p*5 >= tot {
					break
				}
			}
			if total != c.want || len(seen) != c.want {
				t.Fatalf("%s %s: total %d, rows %d, want %d", c.name, sname, total, len(seen), c.want)
			}
		}
		// The unpaged path (default LIMIT 50) applies the filter in SQL too.
		if c.want <= 50 {
			list, err := f.db.ListJobs(ctx, tenantA, c.f)
			if err != nil || len(list) != c.want {
				t.Fatalf("%s ListJobs: %d rows, want %d (%v)", c.name, len(list), c.want, err)
			}
		}
	}
}

// TestListTenantIsolation: tenant B sees only its rows and totals.
func TestListTenantIsolation(t *testing.T) {
	f := seedLists(t)
	ctx := context.Background()
	if _, total, _, err := f.db.PageConfigurations(ctx, tenantB, repo.ConfigFilter{}, listquery.Request{}); err != nil || total != 3 {
		t.Fatalf("B configs: %d %v", total, err)
	}
	if _, total, _, err := f.db.PageTargets(ctx, tenantB, listquery.Request{}); err != nil || total != 2 {
		t.Fatalf("B targets: %d %v", total, err)
	}
	if _, total, _, err := f.db.PageJobs(ctx, tenantB, repo.JobFilter{}, listquery.Request{}); err != nil || total != 2 {
		t.Fatalf("B jobs: %d %v", total, err)
	}
	if _, total, _, err := f.db.PageJobs(ctx, tenantB, repo.JobFilter{JobType: store.JobTypeChild}, listquery.Request{}); err != nil || total != 1 {
		t.Fatalf("B child jobs: %d %v", total, err)
	}
	// Tenant A's parent and history are invisible to B.
	if _, total, _, err := f.db.PageChildJobs(ctx, tenantB, f.parentID, listquery.Request{}); err != nil || total != 0 {
		t.Fatalf("B sees A children: %d %v", total, err)
	}
	if _, total, _, err := f.db.PageHistory(ctx, tenantB, f.children[0], listquery.Request{}); err != nil || total != 0 {
		t.Fatalf("B sees A history: %d %v", total, err)
	}
	if links, err := f.db.TargetConfigurationIDsFor(ctx, tenantB, f.targets); err != nil || len(links) != 0 {
		t.Fatalf("B sees A links: %v %v", links, err)
	}
}

// TestTargetsBatchLinksAndGRPCList: the target page batch-loads configuration
// ids (equal to the per-target lookup), and the unpaged configuration list
// behind gRPC ConfigurationServer.List still returns every row, newest first.
func TestTargetsBatchLinksAndGRPCList(t *testing.T) {
	f := seedLists(t)
	ctx := context.Background()
	links, err := f.db.TargetConfigurationIDsFor(ctx, tenantA, f.targets)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range f.targets {
		one, err := f.db.ListTargetConfigurationIDs(ctx, tenantA, id)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(one, links[id]) {
			t.Fatalf("target %s: batch %v != single %v", id, links[id], one)
		}
	}
	if empty, err := f.db.TargetConfigurationIDsFor(ctx, tenantA, nil); err != nil || len(empty) != 0 {
		t.Fatalf("empty batch: %v %v", empty, err)
	}

	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	az := authz.New(f.db)
	subj := authz.Subjects{TenantID: tenantA, UserID: "u", Roles: []string{"admin"}, ActorKind: "user"}
	page, err := targets.New(f.db, az).Page(ctx, subj, listquery.Request{PageSize: 200})
	if err != nil || page.Total != len(f.targets) {
		t.Fatalf("targets page: %+v %v", page.Total, err)
	}
	for _, v := range page.Items {
		want := links[v.ID]
		if want == nil {
			want = []string{}
		}
		if !slices.Equal(v.ConfigurationIDs, want) || v.CreatedAt.IsZero() {
			t.Fatalf("target view %s: %v want %v (created %v)", v.ID, v.ConfigurationIDs, want, v.CreatedAt)
		}
	}

	all, err := configs.New(f.db, env, az).List(ctx, subj, "", "")
	if err != nil || len(all) != len(f.configs) {
		t.Fatalf("unpaged configs: %d %v", len(all), err)
	}
	for i := 1; i < len(all); i++ {
		if all[i].CreatedAt.After(all[i-1].CreatedAt) {
			t.Fatalf("unpaged configs not newest first at %d", i)
		}
	}
}
