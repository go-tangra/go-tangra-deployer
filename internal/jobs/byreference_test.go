package jobs_test

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// refProvider is a delivers-by-reference provider (like inventory-agent) that
// records what the scheduler hands it. config["mode"] selects the outcome.
type refProvider struct {
	mu    sync.Mutex
	metas []provider.JobMeta
	certs []provider.CertificateData
}

var refProv = &refProvider{}

func init() { provider.Register(refProv) }

func (r *refProvider) record(ctx context.Context, cert *provider.CertificateData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m, _ := provider.JobFrom(ctx)
	r.metas = append(r.metas, m)
	r.certs = append(r.certs, *cert)
}

func (r *refProvider) last() (provider.JobMeta, provider.CertificateData, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.metas) == 0 {
		return provider.JobMeta{}, provider.CertificateData{}, 0
	}
	return r.metas[len(r.metas)-1], r.certs[len(r.certs)-1], len(r.metas)
}

func (r *refProvider) Deploy(ctx context.Context, cert *provider.CertificateData, cfg, _ map[string]any, _ provider.ProgressFn) (*provider.Result, error) {
	r.record(ctx, cert)
	details := map[string]any{"delivery_id": "d-1", "counts": map[string]any{"failed": 1}}
	switch cfg["mode"] {
	case "permanent":
		return &provider.Result{Success: false, Permanent: true, Message: "manual review required", Details: details}, nil
	case "fail":
		return &provider.Result{Success: false, Message: "failed on 1", Details: details}, nil
	}
	return &provider.Result{Success: true, Message: "installed on 1", Details: map[string]any{"delivery_id": "d-1"}}, nil
}
func (r *refProvider) Verify(ctx context.Context, cert *provider.CertificateData, _, _ map[string]any) (*provider.Result, error) {
	r.record(ctx, cert)
	return &provider.Result{Success: true, Message: "verified"}, nil
}
func (r *refProvider) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, provider.ErrUnsupported
}
func (r *refProvider) ValidateCredentials(context.Context, map[string]any, map[string]any) error {
	return nil
}
func (r *refProvider) Capabilities() provider.Capabilities {
	return provider.Capabilities{Type: "test_reference", DisplayName: "reference", SupportsVerify: true, DeliversByReference: true,
		ConfigFields: []provider.Field{
			{Key: "zone", Label: "Zone", Required: true, Overridable: true},
			{Key: "mode", Label: "Mode"},
		}}
}

// keyCerts records includeKey and returns a key only when asked.
type keyCerts struct {
	mu    sync.Mutex
	calls []bool
	err   error
}

func (k *keyCerts) FetchCertificate(_ context.Context, _ string, id string, includeKey bool) (provider.CertificateData, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.calls = append(k.calls, includeKey)
	if k.err != nil {
		return provider.CertificateData{}, k.err
	}
	c := provider.CertificateData{ID: id, SerialNumber: "0A", CommonName: "www.example.com", CertificatePEM: "CERT"}
	if includeKey {
		c.PrivateKeyPEM = "KEY"
	}
	return c, nil
}

func (k *keyCerts) count() int { k.mu.Lock(); defer k.mu.Unlock(); return len(k.calls) }
func (k *keyCerts) lastInclude() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.calls[len(k.calls)-1]
}

func refConfig(t *testing.T, m *memstore.Mem, typ string, cfg map[string]any) store.TargetConfiguration {
	t.Helper()
	c := store.TargetConfiguration{ID: store.NewID(), TenantID: adminSubj().TenantID, Name: "cfg-" + store.NewID(), ProviderType: typ,
		Config: cfg, Status: store.ConfigActive}
	if err := m.InsertConfiguration(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	return c
}

func directJob(t *testing.T, js *jobs.Service, cfgID, trigger string, retries int) string {
	t.Helper()
	id := store.NewID()
	c := cfgID
	if err := js.Create(context.Background(), store.DeploymentJob{ID: id, TenantID: adminSubj().TenantID, TargetConfigurationID: &c,
		CertificateID: "cert-1", Status: store.JobPending, MaxRetries: 3, RetryCount: retries, TriggeredBy: trigger}); err != nil {
		t.Fatal(err)
	}
	return id
}

func jobResult(t *testing.T, js *jobs.Service, id string) (jobs.Result, map[string]any) {
	t.Helper()
	r, err := js.GetResult(context.Background(), adminSubj(), id)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if len(r.Result) > 0 {
		_ = json.Unmarshal(r.Result, &m)
	}
	return r, m
}

// T026: a delivers-by-reference provider never receives a key, and sees the
// job metadata; other providers keep fetching with the key.
func TestReferenceProviderGetsNoKeyAndJobMeta(t *testing.T) {
	ctx := context.Background()
	kc := &keyCerts{}
	m, _, js := kitWith(t, kc, jobs.Config{Workers: 2, Lease: time.Minute})
	conf := refConfig(t, m, "test_reference", map[string]any{"zone": "z1"})
	id := directJob(t, js, conf.ID, store.TriggerManual, 0)
	js.Once(ctx, nil)

	meta, cert, n := refProv.last()
	if n == 0 {
		t.Fatal("provider not called")
	}
	if kc.lastInclude() || cert.PrivateKeyPEM != "" {
		t.Fatalf("reference provider got a key (include=%v)", kc.lastInclude())
	}
	want := provider.JobMeta{TenantID: adminSubj().TenantID, JobID: id, ConfigurationID: conf.ID, TargetID: "", Trigger: provider.TriggerManual}
	if meta != want {
		t.Fatalf("job meta = %+v, want %+v", meta, want)
	}
	r, res := jobResult(t, js, id)
	if r.Status != store.JobCompleted || res["details"].(map[string]any)["delivery_id"] != "d-1" {
		t.Fatalf("job = %+v result=%v", r.View, res)
	}

	// Other providers still fetch with the key.
	dummy := refConfig(t, m, "dummy", map[string]any{})
	directJob(t, js, dummy.ID, store.TriggerManual, 0)
	js.Once(ctx, nil)
	if !kc.lastInclude() {
		t.Fatal("dummy provider must fetch the certificate with its key")
	}
}

// T026/T066: target jobs carry the parent target and the auto_deploy trigger;
// a re-run job carries retry.
func TestJobMetaForTargetAndRetry(t *testing.T) {
	ctx := context.Background()
	kc := &keyCerts{}
	m, _, js := kitWith(t, kc, jobs.Config{Workers: 2, Lease: time.Minute})
	conf := refConfig(t, m, "test_reference", map[string]any{})
	tgt := store.DeploymentTarget{ID: store.NewID(), TenantID: adminSubj().TenantID, Name: "edge", AutoDeploy: true,
		ConfigOverrides: map[string]map[string]any{conf.ID: {"zone": "from-target"}}}
	if err := m.InsertTarget(ctx, tgt); err != nil {
		t.Fatal(err)
	}
	parentID, childID := store.NewID(), store.NewID()
	tid, cid := tgt.ID, conf.ID
	_ = js.Create(ctx, store.DeploymentJob{ID: parentID, TenantID: tgt.TenantID, DeploymentTargetID: &tid, CertificateID: "cert-2", Status: store.JobPending, TriggeredBy: store.TriggerAutoRenewal})
	_ = js.Create(ctx, store.DeploymentJob{ID: childID, TenantID: tgt.TenantID, TargetConfigurationID: &cid, ParentJobID: &parentID,
		CertificateID: "cert-2", Status: store.JobPending, MaxRetries: 3, TriggeredBy: store.TriggerAutoRenewal})
	js.Once(ctx, nil)
	meta, cert, _ := refProv.last()
	if meta.TargetID != tgt.ID || meta.Trigger != provider.TriggerAutoDeploy || meta.JobID != childID || cert.ID != "cert-2" {
		t.Fatalf("meta = %+v cert=%s", meta, cert.ID)
	}
	if r, _ := jobResult(t, js, childID); r.Status != store.JobCompleted {
		t.Fatalf("target-supplied field from the override: job = %+v", r.View)
	}

	for trig, want := range map[string]string{store.TriggerEvent: provider.TriggerAutoDeploy, "api": provider.TriggerManual} {
		directJob(t, js, refConfig(t, m, "test_reference", map[string]any{"zone": "z"}).ID, trig, 0)
		js.Once(ctx, nil)
		if meta, _, _ := refProv.last(); meta.Trigger != want {
			t.Fatalf("trigger %q -> %q, want %q", trig, meta.Trigger, want)
		}
	}
	directJob(t, js, refConfig(t, m, "test_reference", map[string]any{"zone": "z"}).ID, store.TriggerManual, 1)
	js.Once(ctx, nil)
	if meta, _, _ := refProv.last(); meta.Trigger != provider.TriggerRetry {
		t.Fatalf("re-run job trigger = %q", meta.Trigger)
	}
}

// T026 (research D27): a permanent failure fails at once, without retries,
// and keeps the failure details; a non-permanent one still retries and also
// keeps its details.
func TestPermanentFailureNoRetry(t *testing.T) {
	ctx := context.Background()
	m, _, js := kitWith(t, &keyCerts{}, jobs.Config{Workers: 2, Lease: time.Minute, RetryDelay: time.Hour})
	perm := directJob(t, js, refConfig(t, m, "test_reference", map[string]any{"zone": "z", "mode": "permanent"}).ID, store.TriggerManual, 0)
	js.Once(ctx, nil)
	r, res := jobResult(t, js, perm)
	if r.Status != store.JobFailed || r.RetryCount != 0 || r.StatusMessage != "manual review required" {
		t.Fatalf("permanent failure: %+v", r.View)
	}
	if d, _ := res["details"].(map[string]any); d["delivery_id"] != "d-1" || res["message"] != "manual review required" {
		t.Fatalf("failure details not kept: %v", res)
	}
	if len(r.History) != 1 || r.History[0].Result != store.ResultFailure {
		t.Fatalf("history = %+v", r.History)
	}

	soft := directJob(t, js, refConfig(t, m, "test_reference", map[string]any{"zone": "z", "mode": "fail"}).ID, store.TriggerManual, 0)
	js.Once(ctx, nil)
	r, res = jobResult(t, js, soft)
	if r.Status != store.JobRetrying || r.RetryCount != 1 {
		t.Fatalf("non-permanent failure must retry: %+v", r.View)
	}
	if d, _ := res["details"].(map[string]any); d["delivery_id"] != "d-1" {
		t.Fatalf("retrying job lost its details: %v", res)
	}
}

// T081 (job part): an incomplete effective configuration fails before the
// certificate is fetched and before the provider runs.
func TestIncompleteConfigurationFailsBeforeFetch(t *testing.T) {
	ctx := context.Background()
	kc := &keyCerts{}
	m, _, js := kitWith(t, kc, jobs.Config{Workers: 2, Lease: time.Minute})
	_, _, before := refProv.last()
	id := directJob(t, js, refConfig(t, m, "test_reference", map[string]any{"mode": "x"}).ID, store.TriggerManual, 0)
	js.Once(ctx, nil)
	r, _ := jobResult(t, js, id)
	if r.Status != store.JobFailed || r.StatusMessage != "configuration incomplete: Zone must be provided by the target" {
		t.Fatalf("job = %+v", r.View)
	}
	if kc.count() != 0 {
		t.Fatal("certificate fetched for an incomplete configuration")
	}
	if _, _, after := refProv.last(); after != before {
		t.Fatal("provider ran for an incomplete configuration")
	}
	// Verify of such a job is refused the same way.
	res, err := js.Verify(ctx, adminSubj(), id)
	if err != nil || res.Success || !strings.HasPrefix(res.Message, "configuration incomplete") || kc.count() != 0 {
		t.Fatalf("verify = %+v, %v (fetches %d)", res, err, kc.count())
	}
}

// lcm "no stored private key" fails the job at once (no retries).
func TestKeyUnavailableFailsWithoutRetry(t *testing.T) {
	ctx := context.Background()
	kc := &keyCerts{err: fmt.Errorf("lcmclient: %w", provider.ErrKeyUnavailable)}
	m, _, js := kitWith(t, kc, jobs.Config{Workers: 2, Lease: time.Minute})
	id := directJob(t, js, refConfig(t, m, "dummy", map[string]any{}).ID, store.TriggerManual, 0)
	js.Once(ctx, nil)
	r, _ := jobResult(t, js, id)
	if r.Status != store.JobFailed || r.RetryCount != 0 || r.StatusMessage != "certificate has no stored private key" {
		t.Fatalf("job = %+v", r.View)
	}
}

// Verify of a reference provider fetches without the key and passes the job
// metadata.
func TestVerifyReferenceProvider(t *testing.T) {
	ctx := context.Background()
	kc := &keyCerts{}
	m, _, js := kitWith(t, kc, jobs.Config{Workers: 2, Lease: time.Minute})
	id := directJob(t, js, refConfig(t, m, "test_reference", map[string]any{"zone": "z"}).ID, store.TriggerManual, 0)
	js.Once(ctx, nil)
	res, err := js.Verify(ctx, adminSubj(), id)
	if err != nil || !res.Success {
		t.Fatalf("verify = %+v, %v", res, err)
	}
	meta, cert, _ := refProv.last()
	if kc.lastInclude() || cert.PrivateKeyPEM != "" || meta.JobID != id || meta.TenantID != adminSubj().TenantID {
		t.Fatalf("verify: include=%v meta=%+v", kc.lastInclude(), meta)
	}
}
