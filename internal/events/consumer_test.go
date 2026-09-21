package events_test

import (
	"context"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/events"
	"github.com/go-freya/freya/services/deployer/internal/memstore"
	"github.com/go-freya/freya/services/deployer/internal/provider"
	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/store"
	"github.com/go-freya/freya/services/deployer/internal/targets"
)

type fakeCerts struct{ cn string }

func (f fakeCerts) FetchCertificate(_ context.Context, _ string, id string, _ bool) (provider.CertificateData, error) {
	return provider.CertificateData{ID: id, CommonName: f.cn, SANs: []string{f.cn}}, nil
}

const tenant = "00000000-0000-0000-0000-000000000001"

func admin() authz.Subjects {
	return authz.Subjects{TenantID: tenant, UserID: "u1", Roles: []string{"admin"}, ActorKind: "user"}
}

// setup makes a memstore with an auto-deploy target (filter cnPattern) + attached config.
func setup(t *testing.T, cnPattern string, attach bool) (*memstore.Mem, string) {
	t.Helper()
	m := memstore.New()
	az := authz.New(m)
	ts := targets.New(m, az)
	// A configuration to attach.
	cfgID := store.NewID()
	if attach {
		_ = m.InsertConfiguration(context.Background(), store.TargetConfiguration{ID: cfgID, TenantID: tenant, Name: "ep", ProviderType: "dummy", Status: "active"})
	}
	filters := []store.CertificateFilter{}
	if cnPattern != "" {
		filters = []store.CertificateFilter{{CommonNamePattern: cnPattern}}
	}
	tv, err := ts.Create(context.Background(), admin(), targets.Input{Name: "grp", AutoDeploy: true, Filters: filters})
	if err != nil {
		t.Fatal(err)
	}
	if attach {
		if err := ts.Attach(context.Background(), admin(), tv.ID, []string{cfgID}, nil); err != nil {
			t.Fatal(err)
		}
	}
	return m, tv.ID
}

func countJobs(m *memstore.Mem) int {
	js, _ := m.ListJobs(context.Background(), tenant, repo.JobFilter{})
	return len(js)
}

func TestAutoDeploy_MatchCreatesParentAndChild(t *testing.T) {
	m, _ := setup(t, "*.example.com", true)
	c := events.NewConsumer(m, fakeCerts{cn: "api.example.com"}, nil)
	n, err := c.Handle(context.Background(), tenant, "cert-1", false)
	if err != nil || n != 1 {
		t.Fatalf("created %d parents (err %v), want 1", n, err)
	}
	// One parent + one child.
	if got := countJobs(m); got != 2 {
		t.Fatalf("jobs = %d, want 2 (parent+child)", got)
	}
	// The child is triggered by "event".
	js, _ := m.ListJobs(context.Background(), tenant, repo.JobFilter{TriggeredBy: "event"})
	if len(js) != 2 {
		t.Fatalf("event-triggered jobs = %d", len(js))
	}
}

func TestAutoDeploy_RenewalTrigger(t *testing.T) {
	m, _ := setup(t, "*.example.com", true)
	c := events.NewConsumer(m, fakeCerts{cn: "api.example.com"}, nil)
	if _, err := c.Handle(context.Background(), tenant, "cert-1", true); err != nil {
		t.Fatal(err)
	}
	js, _ := m.ListJobs(context.Background(), tenant, repo.JobFilter{TriggeredBy: "auto_renewal"})
	if len(js) != 2 {
		t.Fatalf("auto_renewal jobs = %d, want 2", len(js))
	}
}

func TestAutoDeploy_NoMatchNoJobs(t *testing.T) {
	m, _ := setup(t, "*.other.com", true)
	c := events.NewConsumer(m, fakeCerts{cn: "api.example.com"}, nil)
	n, _ := c.Handle(context.Background(), tenant, "cert-1", false)
	if n != 0 || countJobs(m) != 0 {
		t.Fatalf("expected no jobs for non-matching cert")
	}
}

func TestAutoDeploy_NoConfigsNoJobs(t *testing.T) {
	m, _ := setup(t, "", false) // empty filter matches all, but no configs attached
	c := events.NewConsumer(m, fakeCerts{cn: "api.example.com"}, nil)
	n, _ := c.Handle(context.Background(), tenant, "cert-1", false)
	if n != 0 || countJobs(m) != 0 {
		t.Fatalf("expected no jobs when target has no configurations")
	}
}
