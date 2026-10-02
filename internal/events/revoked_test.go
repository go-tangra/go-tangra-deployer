package events_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/events"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

type fakeRevoker struct {
	mu    sync.Mutex
	calls []string
	err   error
}

func (f *fakeRevoker) MarkCertificateRevoked(_ context.Context, tenantID, certID string) (int, int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, tenantID+"/"+certID)
	return 2, 3, f.err
}

func (f *fakeRevoker) n() int { f.mu.Lock(); defer f.mu.Unlock(); return len(f.calls) }

type fakeAudit struct {
	mu     sync.Mutex
	events []audit.Event
}

func (a *fakeAudit) Record(_ context.Context, e audit.Event) error {
	if err := audit.Validate(e); err != nil {
		return err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, e)
	return nil
}

func addInventoryConfig(t *testing.T, m *memstore.Mem, status string) {
	t.Helper()
	if err := m.InsertConfiguration(context.Background(), store.TargetConfiguration{ID: store.NewID(), TenantID: tenant, Name: "hosts-" + store.NewID(),
		ProviderType: "inventory-agent", Status: status}); err != nil {
		t.Fatal(err)
	}
}

// T102: certificate.revoked is forwarded only when the tenant has an active
// inventory-agent configuration; outcomes are audited.
func TestRevokedForwardedOnlyWithInventoryAgent(t *testing.T) {
	m := memstore.New()
	rev, aud := &fakeRevoker{}, &fakeAudit{}
	c := events.NewConsumer(m, fakeCerts{cn: "x"}, nil)
	ctx := context.Background()

	// Without a forwarder nothing happens.
	if c.HandleRevoked(ctx, tenant, "cert-1") {
		t.Fatal("forwarded without a forwarder")
	}
	c.SetRevocationForwarder(rev, aud)
	if c.HandleRevoked(ctx, tenant, "cert-1") || rev.n() != 0 {
		t.Fatal("forwarded without an inventory-agent configuration")
	}
	addInventoryConfig(t, m, store.ConfigInactive)
	if c.HandleRevoked(ctx, tenant, "cert-1") || rev.n() != 0 {
		t.Fatal("forwarded for an inactive configuration")
	}
	addInventoryConfig(t, m, store.ConfigActive)
	if c.HandleRevoked(ctx, tenant, strings.Repeat("c", 129)) || c.HandleRevoked(ctx, tenant, "cert 1;x") || rev.n() != 0 {
		t.Fatal("malformed certificate id forwarded")
	}
	if c.HandleRevoked(ctx, tenant, "") || rev.n() != 0 {
		t.Fatal("forwarded without a certificate id")
	}
	if !c.HandleRevoked(ctx, tenant, "cert-1") || rev.calls[0] != tenant+"/cert-1" {
		t.Fatalf("calls = %v", rev.calls)
	}
	rev.err = errors.New("inventory down")
	if c.HandleRevoked(ctx, tenant, "cert-2") {
		t.Fatal("failure reported as success")
	}
	if len(aud.events) != 2 || aud.events[0].Outcome != audit.OutcomeOK || aud.events[0].Details["flagged_hosts"] != 3 ||
		aud.events[1].Outcome != audit.OutcomeFailed || aud.events[1].EventType != audit.CertificateRevocationForwarded {
		t.Fatalf("audit = %+v", aud.events)
	}
	// A store failure skips the forward.
	m.FailNext = errors.New("db down")
	if c.HandleRevoked(ctx, tenant, "cert-3") {
		t.Fatal("forwarded despite a store failure")
	}
}

// scriptedReader returns its entries once, then blocks until ctx ends.
type scriptedReader struct {
	mu      sync.Mutex
	entries []events.Entry
	done    bool
	cancel  context.CancelFunc
}

func (r *scriptedReader) XLast(context.Context, string) (string, error) { return "0-0", nil }
func (r *scriptedReader) XRead(ctx context.Context, _, _ string, _ time.Duration, _ int64) ([]events.Entry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done {
		r.cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
	r.done = true
	return r.entries, nil
}

// T102: the event loop forwards revocations, ignores its own events (loop
// guard) and keeps handling issued/renewed even when the forward fails.
func TestRunForwardsRevokedAndKeepsDeploying(t *testing.T) {
	m, _ := setup(t, "*.example.com", true)
	addInventoryConfig(t, m, store.ConfigActive)
	rev := &fakeRevoker{err: errors.New("inventory down")}
	c := events.NewConsumer(m, fakeCerts{cn: "api.example.com"}, nil)
	c.SetRevocationForwarder(rev, nil)
	ctx, cancel := context.WithCancel(context.Background())
	r := &scriptedReader{cancel: cancel, entries: []events.Entry{
		{ID: "1-0", Fields: map[string]string{"type": "certificate.revoked", "module": "deployer", "data": `{"certificate_id":"own"}`}},
		{ID: "2-0", Fields: map[string]string{"type": "certificate.revoked", "module": "lcm", "data": `{"certificate_id":"cert-r"}`}},
		{ID: "3-0", Fields: map[string]string{"type": "certificate.renewed", "module": "lcm", "data": `{"certificate_id":"cert-n"}`}},
	}}
	done := make(chan struct{})
	go func() {
		c.Run(ctx, r, []string{tenant}, func(t string) string { return "platform:events:" + t })
		close(done)
	}()
	<-done
	if rev.n() != 1 || rev.calls[0] != tenant+"/cert-r" {
		t.Fatalf("revocations forwarded = %v", rev.calls)
	}
	if got := countJobs(m); got != 2 {
		t.Fatalf("renewal after a failed forward: jobs = %d, want 2", got)
	}
}
