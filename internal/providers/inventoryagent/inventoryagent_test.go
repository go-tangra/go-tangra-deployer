package inventoryagent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	invsdk "github.com/go-tangra/go-tangra-inventory/sdk/v4/pkg/inventoryclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

const (
	tenant = "11111111-1111-1111-1111-111111111111"
	hostA  = "0192a7c0-0000-7000-8000-00000000000a"
	hostB  = "0192a7c0-0000-7000-8000-00000000000b"
)

// fakeInventory is an in-process CertificateDeliveryService client.
type fakeInventory struct {
	mu        sync.Mutex
	creates   []invsdk.DeliveryRequest
	created   invsdk.Delivery
	createErr error
	gets      []invsdk.Delivery // returned in order; the last one repeats
	getErr    error
	getCalls  int
	preview   invsdk.Preview
	prevErr   error
	verify    invsdk.Verification
	verErr    error
	verifyReq []string
}

func (f *fakeInventory) CreateCertificateDelivery(_ context.Context, r invsdk.DeliveryRequest) (invsdk.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.creates = append(f.creates, r)
	return f.created, f.createErr
}

func (f *fakeInventory) GetCertificateDelivery(_ context.Context, tenantID, id string) (invsdk.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.getErr != nil {
		return invsdk.Delivery{}, f.getErr
	}
	if len(f.gets) == 0 {
		return f.created, nil
	}
	d := f.gets[0]
	if len(f.gets) > 1 {
		f.gets = f.gets[1:]
	}
	return d, nil
}

func (f *fakeInventory) PreviewCertificateTargets(_ context.Context, tenantID string, ids, tags []string) (invsdk.Preview, error) {
	return f.preview, f.prevErr
}

func (f *fakeInventory) VerifyHostCertificates(_ context.Context, tenantID string, ids, tags []string, name, fp string) (invsdk.Verification, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifyReq = []string{tenantID, name, fp, strings.Join(ids, ","), strings.Join(tags, ",")}
	return f.verify, f.verErr
}

// fakeClock drives the wait loop without real sleeping.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }
func (c *fakeClock) sleep(ctx context.Context, d time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
	return nil
}

func newTestProvider(inv Inventory) (*Provider, *fakeClock) {
	p := New(inv)
	// Real time, not a fixed date: context deadlines are enforced against the
	// real clock, so a deadline derived from a past fake time expires at once.
	clk := &fakeClock{t: time.Now().UTC().Truncate(time.Second)}
	p.now, p.sleep = clk.now, clk.sleep
	return p, clk
}

func jobCtx() context.Context {
	return provider.WithJob(context.Background(), provider.JobMeta{TenantID: tenant, JobID: "job-1", ConfigurationID: "cfg-1",
		TargetID: "tgt-1", Trigger: provider.TriggerAutoDeploy})
}

func readFile(t testing.TB, name string) string {
	t.Helper()
	b, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func testCert(t testing.TB) *provider.CertificateData {
	return &provider.CertificateData{ID: "cert-9", CommonName: "*.Example.COM", CertificatePEM: readFile(t, "rsa.crt") + readFile(t, "intermediate.pem")}
}

func item(host, state string, online bool) invsdk.DeliveryItem {
	return invsdk.DeliveryItem{ID: "i-" + host, HostID: host, Hostname: "h-" + host[len(host)-1:], State: state, AgentOnline: online, HookExitCode: -1}
}

func delivery(items ...invsdk.DeliveryItem) invsdk.Delivery {
	return invsdk.Delivery{ID: "d-1", CertificateID: "cert-9", Name: "wildcard.example.com", Created: true, Items: items}
}

// --- configuration (contracts/deployer-provider.md §2) ---

func TestParseConfig(t *testing.T) {
	c, err := ParseConfig(map[string]any{
		"host_ids": []any{hostA, hostB}, "host_tags": []string{"role=web", "env"}, "cert_name": "www",
		"key_policy": "certificate_only", "require_all_success": true, "wait_seconds": float64(30),
	})
	if err != nil {
		t.Fatal(err)
	}
	want := Config{HostIDs: []string{hostA, hostB}, HostTags: []string{"role=web", "env"}, CertName: "www", KeyPolicy: KeyPolicyCertificateOnly, RequireAll: true, WaitSeconds: 30}
	if !reflect.DeepEqual(c, want) {
		t.Fatalf("config = %+v", c)
	}
	// Defaults; empty selection allowed (target-supplied).
	c, err = ParseConfig(map[string]any{"key_policy": "", "wait_seconds": "", "cert_name": nil, "require_all_success": nil, "host_ids": nil})
	if err != nil || c.KeyPolicy != KeyPolicyRequire || c.WaitSeconds != DefaultWaitSeconds || c.HasSelector() {
		t.Fatalf("defaults = %+v, %v", c, err)
	}
	if c, _ := ParseConfig(map[string]any{"wait_seconds": 7}); c.WaitSeconds != 7 {
		t.Fatalf("int wait = %d", c.WaitSeconds)
	}
	if c, _ := ParseConfig(map[string]any{"wait_seconds": int64(0)}); c.WaitSeconds != 0 {
		t.Fatalf("int64 wait = %d", c.WaitSeconds)
	}
	tags17 := make([]any, 17)
	for i := range tags17 {
		tags17[i] = fmt.Sprintf("t%d", i)
	}
	ids1001 := make([]any, 1001)
	for i := range ids1001 {
		ids1001[i] = hostA
	}
	bad := []struct {
		cfg   map[string]any
		field string
		code  string
	}{
		{map[string]any{"host_ids": []any{"not-a-uuid"}}, "host_ids", "invalid_uuid"},
		{map[string]any{"host_ids": []any{hostA, hostA}}, "host_ids", "duplicate_item"},
		{map[string]any{"host_ids": ids1001}, "host_ids", "too_many_items:1000"},
		{map[string]any{"host_ids": "x"}, "host_ids", "wrong_type"},
		{map[string]any{"host_ids": []any{1}}, "host_ids", "wrong_type"},
		{map[string]any{"host_tags": tags17}, "host_tags", "too_many_items:16"},
		{map[string]any{"host_tags": []any{"=x"}}, "host_tags", "pattern"},
		{map[string]any{"host_tags": []any{"role=a\nb"}}, "host_tags", "pattern"},
		{map[string]any{"cert_name": "../x"}, "cert_name", "pattern"},
		{map[string]any{"cert_name": "a..b"}, "cert_name", "pattern"},
		{map[string]any{"cert_name": 5}, "cert_name", "wrong_type"},
		{map[string]any{"key_policy": "maybe"}, "key_policy", "not_in_options"},
		{map[string]any{"key_policy": true}, "key_policy", "wrong_type"},
		{map[string]any{"require_all_success": "yes"}, "require_all_success", "wrong_type"},
		{map[string]any{"wait_seconds": float64(241)}, "wait_seconds", "out_of_range:0..240"},
		{map[string]any{"wait_seconds": -1}, "wait_seconds", "out_of_range:0..240"},
		{map[string]any{"wait_seconds": 1.5}, "wait_seconds", "wrong_type"},
		{map[string]any{"wait_seconds": "60"}, "wait_seconds", "wrong_type"},
		{map[string]any{"client_ids": []any{hostA}}, "client_ids", "unknown_field"},
		{map[string]any{"../x": 1}, "___x", "unknown_field"},
	}
	for _, b := range bad {
		_, err := ParseConfig(b.cfg)
		var fe provider.FieldErrors
		if !errors.As(err, &fe) || fe["config."+b.field] != b.code {
			t.Errorf("%v: err = %v, want config.%s %s", b.cfg, err, b.field, b.code)
		}
	}
	p := New(nil)
	if err := p.ValidateConfig(map[string]any{"host_tags": []any{"=x"}}); err == nil {
		t.Fatal("ValidateConfig accepted a bad tag")
	}
	if safeKey(strings.Repeat("k", 70)) != strings.Repeat("k", 64) {
		t.Fatal("safeKey must clip")
	}
}

// The deployer derives and checks names like the inventory (shared vectors).
func TestNameRulesMatchInventoryVectors(t *testing.T) {
	var v struct {
		Vectors []struct {
			CN   string `json:"cn"`
			Name string `json:"name"`
			OK   bool   `json:"ok"`
		} `json:"vectors"`
		Valid   []string `json:"valid_names"`
		Invalid []string `json:"invalid_names"`
	}
	if err := json.Unmarshal([]byte(readFile(t, "default-name-vectors.json")), &v); err != nil {
		t.Fatal(err)
	}
	for _, c := range v.Vectors {
		got, ok := DefaultName(c.CN)
		if got != c.Name || ok != c.OK {
			t.Errorf("DefaultName(%q) = %q,%v want %q,%v", c.CN, got, ok, c.Name, c.OK)
		}
	}
	for _, n := range v.Valid {
		if !ValidName(n) {
			t.Errorf("ValidName(%q) = false", n)
		}
	}
	for _, n := range v.Invalid {
		if ValidName(n) {
			t.Errorf("ValidName(%q) = true", n)
		}
	}
	for _, tag := range []string{"role", "role=web", "env=prod eu", "app.kubernetes.io/name=x", "k=" + strings.Repeat("v", 255)} {
		if !ValidTag(tag) {
			t.Errorf("ValidTag(%q) = false", tag)
		}
	}
	for _, tag := range []string{"", "=x", strings.Repeat("k", 64), "ro le", "k=" + strings.Repeat("v", 256), "k=\x7f", "\xff"} {
		if ValidTag(tag) {
			t.Errorf("ValidTag(%q) = true", tag)
		}
	}
}

func TestCapabilitiesDeclaration(t *testing.T) {
	c := New(nil).Capabilities()
	if err := provider.CheckCapabilities(c); err != nil {
		t.Fatal(err)
	}
	if c.Type != Type || !c.DeliversByReference || c.SupportsRollback || !c.SupportsVerify || len(c.CredentialFields) != 0 {
		t.Fatalf("capabilities = %+v", c)
	}
	for _, f := range c.ConfigFields {
		if !f.Overridable {
			t.Errorf("%s must be overridable (research D25, Q8)", f.Key)
		}
		// Every default passes the provider's own parser.
		if f.Default != nil {
			if _, err := ParseConfig(map[string]any{f.Key: f.Default}); err != nil {
				t.Errorf("default of %s refused: %v", f.Key, err)
			}
		}
	}
	// The generic validator accepts what the parser accepts for the schema in §2.
	errs, ts := provider.ValidateInput(c, map[string]any{"cert_name": "www"}, map[string]any{}, provider.ModeConfiguration)
	if len(errs) != 0 || !reflect.DeepEqual(ts, []string{"host_ids", "host_tags"}) {
		t.Fatalf("validate = %v %v", errs, ts)
	}
}

// --- Deploy ---

func TestDeploySendsReferencesAndEvaluates(t *testing.T) {
	inv := &fakeInventory{created: delivery(item(hostA, statePending, true), item(hostB, statePending, false))}
	inv.gets = []invsdk.Delivery{delivery(item(hostA, stateInstalled, true), item(hostB, statePending, false))}
	p, _ := newTestProvider(inv)
	var progress []int
	res, err := p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}, "host_tags": []any{"role=edge"}}, nil,
		func(pct int, _ string) { progress = append(progress, pct) })
	if err != nil {
		t.Fatal(err)
	}
	want := invsdk.DeliveryRequest{TenantID: tenant, IdempotencyKey: "job-1", ConfigurationID: "cfg-1", TargetID: "tgt-1",
		Trigger: provider.TriggerAutoDeploy, CertificateID: "cert-9", Name: "wildcard.example.com", KeyPolicy: KeyPolicyRequire,
		HostIDs: []string{hostA}, HostTags: []string{"role=edge"}, RearmFailed: true}
	if len(inv.creates) != 1 || !reflect.DeepEqual(inv.creates[0], want) {
		t.Fatalf("request = %+v", inv.creates)
	}
	if !res.Success || res.Message != "Installed on 1, unchanged on 0, queued for 1, failed on 0, unsupported on 0" {
		t.Fatalf("result = %+v", res)
	}
	if res.Details["delivery_id"] != "d-1" || res.Details["counts"].(Counts).Queued != 1 {
		t.Fatalf("details = %v", res.Details)
	}
	hosts := res.Details["hosts"].([]map[string]any)
	if hosts[0]["state"] != statePending { // not-done hosts first
		t.Fatalf("hosts order = %v", hosts)
	}
	for i := 1; i < len(progress); i++ {
		if progress[i] <= progress[i-1] {
			t.Fatalf("progress not monotonic: %v", progress)
		}
	}
	if progress[0] != 10 || progress[len(progress)-1] != 100 {
		t.Fatalf("progress = %v", progress)
	}
}

// research D9 table, with and without require_all_success.
func TestDeployOutcomeMatrix(t *testing.T) {
	cases := []struct {
		name       string
		states     []string
		requireAll bool
		success    bool
	}{
		{"all installed", []string{stateInstalled, stateUnchanged}, true, true},
		{"queued only, require all", []string{statePending, stateDelivered}, true, true},
		{"one failed, require all", []string{stateInstalled, stateFailed}, true, false},
		{"one unsupported, require all", []string{stateInstalled, stateUnsupported}, true, false},
		{"one failed, partial", []string{stateInstalled, stateHookFailed}, false, true},
		{"all failed", []string{stateFailed, stateExpired, stateCancelled}, false, false},
		{"all unsupported", []string{stateUnsupported}, false, false},
		{"superseded only", []string{stateSuperseded}, false, true},
		{"superseded and unsupported", []string{stateSuperseded, stateUnsupported}, false, false},
		{"fetched counts as queued", []string{stateFetched, stateFailed}, false, true},
	}
	for _, c := range cases {
		var items []invsdk.DeliveryItem
		for i, st := range c.states {
			items = append(items, item(fmt.Sprintf("0192a7c0-0000-7000-8000-00000000000%d", i), st, true))
		}
		res := evaluate(delivery(items...), "www", c.requireAll)
		if res.Success != c.success || res.Permanent {
			t.Errorf("%s: success=%v permanent=%v (%s)", c.name, res.Success, res.Permanent, res.Message)
		}
	}
	res := evaluate(invsdk.Delivery{ID: "d", UnknownHostIDs: []string{hostB}}, "www", false)
	if res.Success || !res.Permanent || res.Message != "no hosts matched" || res.Details["unknown_host_ids"] == nil {
		t.Fatalf("no hosts: %+v", res)
	}
	if m := evaluate(delivery(item(hostA, stateSuperseded, true)), "www", false).Message; !strings.HasSuffix(m, "superseded on 1") {
		t.Fatalf("message = %q", m)
	}
}

func TestDeployNoHostsMatched(t *testing.T) {
	inv := &fakeInventory{created: invsdk.Delivery{ID: "d-0"}}
	p, _ := newTestProvider(inv)
	res, err := p.Deploy(jobCtx(), testCert(t), map[string]any{"host_tags": []any{"role=none"}}, nil, nil)
	if err != nil || res.Success || !res.Permanent || res.Message != "no hosts matched" || inv.getCalls != 0 {
		t.Fatalf("res = %+v, %v (gets %d)", res, err, inv.getCalls)
	}
}

// The wait ends at wait_seconds (bounded by the deadline minus 10 s) and an
// offline agent's pending item settles after the grace period.
func TestDeployWaitBounds(t *testing.T) {
	// Online but slow: the wait runs to wait_seconds.
	inv := &fakeInventory{created: delivery(item(hostA, stateFetched, true))}
	p, clk := newTestProvider(inv)
	start := clk.now()
	res, _ := p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}, "wait_seconds": 7}, nil, nil)
	if got := clk.now().Sub(start); got != 7*time.Second || !res.Success {
		t.Fatalf("waited %v, res %+v", got, res)
	}
	// Deadline 20 s away: clamped to 10 s.
	inv = &fakeInventory{created: delivery(item(hostA, stateFetched, true))}
	p, clk = newTestProvider(inv)
	start = clk.now()
	ctx, cancel := context.WithDeadline(jobCtx(), start.Add(20*time.Second))
	defer cancel()
	_, _ = p.Deploy(ctx, testCert(t), map[string]any{"host_ids": []any{hostA}, "wait_seconds": 240}, nil, nil)
	if got := clk.now().Sub(start); got != 10*time.Second {
		t.Fatalf("waited %v, want 10s", got)
	}
	// Offline agent: settled after the 5 s grace, not the full wait.
	inv = &fakeInventory{created: delivery(item(hostA, statePending, false))}
	p, clk = newTestProvider(inv)
	start = clk.now()
	res, _ = p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}}, nil, nil)
	if got := clk.now().Sub(start); got != 6*time.Second || !res.Success || !strings.Contains(res.Message, "queued for 1") {
		t.Fatalf("offline wait %v, res %+v", got, res)
	}
	// wait_seconds 0: no polling at all.
	inv = &fakeInventory{created: delivery(item(hostA, statePending, true))}
	p, _ = newTestProvider(inv)
	_, _ = p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}, "wait_seconds": 0}, nil, nil)
	if inv.getCalls != 0 {
		t.Fatalf("wait 0 polled %d times", inv.getCalls)
	}
	// A read error or a cancelled sleep ends the wait with the last state.
	inv = &fakeInventory{created: delivery(item(hostA, statePending, true)), getErr: errors.New("down")}
	p, _ = newTestProvider(inv)
	if res, err := p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}}, nil, nil); err != nil || !res.Success {
		t.Fatalf("read error: %+v %v", res, err)
	}
	inv = &fakeInventory{created: delivery(item(hostA, statePending, true))}
	p, _ = newTestProvider(inv)
	p.sleep = func(context.Context, time.Duration) error { return context.Canceled }
	if res, _ := p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}}, nil, nil); !res.Success {
		t.Fatalf("cancelled sleep: %+v", res)
	}
}

func TestDeployDetailsCapped(t *testing.T) {
	var items []invsdk.DeliveryItem
	for i := 0; i < 250; i++ {
		it := item(hostA, stateInstalled, true)
		it.Reason, it.Serial, it.Fingerprint = "r", "0A", "ab"
		items = append(items, it)
	}
	res := evaluate(delivery(items...), "www", false)
	if len(res.Details["hosts"].([]map[string]any)) != 200 || res.Details["hosts_truncated"] != true {
		t.Fatalf("details not capped")
	}
	if clip([]string{"a", "b"}, 1)[0] != "a" {
		t.Fatal("clip")
	}
}

func TestDeployRefusalsAndErrors(t *testing.T) {
	cfg := map[string]any{"host_ids": []any{hostA}}
	// Refusals that a retry cannot fix are permanent.
	for _, code := range []codes.Code{codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.NotFound, codes.Unauthenticated} {
		inv := &fakeInventory{createErr: status.Error(code, "certificate_not_found "+strings.Repeat("x", 300))}
		p, _ := newTestProvider(inv)
		res, err := p.Deploy(jobCtx(), testCert(t), cfg, nil, nil)
		if err != nil || res.Success || !res.Permanent || len(res.Message) > 240 || !strings.HasPrefix(res.Message, "inventory refused the delivery: certificate_not_found") {
			t.Fatalf("%v: %+v %v", code, res, err)
		}
	}
	// Transient errors return an error (the job retries).
	for _, e := range []error{status.Error(codes.Unavailable, "down"), errors.New("plain")} {
		p, _ := newTestProvider(&fakeInventory{createErr: e})
		if res, err := p.Deploy(jobCtx(), testCert(t), cfg, nil, nil); err == nil || res != nil {
			t.Fatalf("transient: %+v %v", res, err)
		}
	}
	p, _ := newTestProvider(&fakeInventory{})
	// No job context, incomplete or invalid configuration, underivable name.
	if res, _ := p.Deploy(context.Background(), testCert(t), cfg, nil, nil); !res.Permanent || res.Message != "job context missing" {
		t.Fatalf("no job: %+v", res)
	}
	noJobID := provider.WithJob(context.Background(), provider.JobMeta{TenantID: tenant})
	if res, _ := p.Deploy(noJobID, testCert(t), cfg, nil, nil); !res.Permanent {
		t.Fatalf("no job id: %+v", res)
	}
	if res, _ := p.Deploy(jobCtx(), testCert(t), map[string]any{}, nil, nil); !res.Permanent || !strings.Contains(res.Message, "must be provided by the target") {
		t.Fatalf("no selector: %+v", res)
	}
	if res, _ := p.Deploy(jobCtx(), testCert(t), map[string]any{"host_ids": []any{"bad"}}, nil, nil); !res.Permanent || !strings.HasPrefix(res.Message, "configuration invalid") {
		t.Fatalf("invalid: %+v", res)
	}
	noCN := &provider.CertificateData{ID: "c", CommonName: "***"}
	if res, _ := p.Deploy(jobCtx(), noCN, cfg, nil, nil); !res.Permanent || !strings.Contains(res.Message, "Certificate name") {
		t.Fatalf("no name: %+v", res)
	}
	// An explicit name wins over the common name; manual trigger maps through.
	inv := &fakeInventory{created: delivery(item(hostA, stateInstalled, true))}
	p, _ = newTestProvider(inv)
	manual := provider.WithJob(context.Background(), provider.JobMeta{TenantID: tenant, JobID: "j", Trigger: "something"})
	if _, err := p.Deploy(manual, noCN, map[string]any{"host_ids": []any{hostA}, "cert_name": "www"}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if inv.creates[0].Name != "www" || inv.creates[0].Trigger != provider.TriggerManual {
		t.Fatalf("request = %+v", inv.creates[0])
	}
	// Without an inventory client nothing is sent.
	if _, err := New(nil).Deploy(jobCtx(), testCert(t), cfg, nil, nil); err == nil {
		t.Fatal("deploy without client")
	}
	if trigger(provider.TriggerRetry) != provider.TriggerRetry {
		t.Fatal("retry trigger")
	}
}

// --- Verify ---

func TestVerify(t *testing.T) {
	fp, err := Fingerprint(testCert(t).CertificatePEM)
	if err != nil || len(fp) != 64 {
		t.Fatalf("fingerprint %q %v", fp, err)
	}
	st := func(statuses ...string) invsdk.Verification {
		v := invsdk.Verification{Total: len(statuses)}
		for i, s := range statuses {
			if s == "match" {
				v.Matched++
			}
			v.Hosts = append(v.Hosts, invsdk.HostCertificateStatus{HostID: fmt.Sprint(i), Status: s, Fingerprint: "ff", Reason: "r"})
		}
		return v
	}
	cases := []struct {
		v          invsdk.Verification
		requireAll bool
		success    bool
	}{
		{st("match", "match"), true, true},
		{st("match", "pending"), true, false},
		{st("match", "pending"), false, true},
		{st("match", "mismatch"), false, false},
		{st("match", "revoked"), false, false},
		{st("missing", "failed"), false, false},
		{st(), false, false},
	}
	for i, c := range cases {
		inv := &fakeInventory{verify: c.v}
		p, _ := newTestProvider(inv)
		res, err := p.Verify(jobCtx(), testCert(t), map[string]any{"host_tags": []any{"role=web"}, "require_all_success": c.requireAll}, nil)
		if err != nil || res.Success != c.success {
			t.Errorf("#%d: %+v %v", i, res, err)
		}
		if inv.verifyReq[0] != tenant || inv.verifyReq[1] != "wildcard.example.com" || inv.verifyReq[2] != fp {
			t.Errorf("#%d request = %v", i, inv.verifyReq)
		}
	}
	p, _ := newTestProvider(&fakeInventory{verErr: errors.New("down")})
	if _, err := p.Verify(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}}, nil); err == nil {
		t.Fatal("verify error swallowed")
	}
	if _, err := p.Verify(context.Background(), testCert(t), map[string]any{"host_ids": []any{hostA}}, nil); !errors.Is(err, errNoJob) {
		t.Fatalf("no job: %v", err)
	}
	for _, c := range []struct {
		cert *provider.CertificateData
		cfg  map[string]any
	}{
		{testCert(t), map[string]any{"host_ids": []any{"bad"}}},
		{testCert(t), map[string]any{}},
		{&provider.CertificateData{CommonName: "***"}, map[string]any{"host_ids": []any{hostA}}},
		{&provider.CertificateData{CommonName: "www"}, map[string]any{"host_ids": []any{hostA}}},
	} {
		if res, err := p.Verify(jobCtx(), c.cert, c.cfg, nil); err != nil || res.Success {
			t.Fatalf("verify %v: %+v %v", c.cfg, res, err)
		}
	}
	if _, err := New(nil).Verify(jobCtx(), testCert(t), map[string]any{"host_ids": []any{hostA}}, nil); err == nil {
		t.Fatal("verify without client")
	}
}

func TestFingerprint(t *testing.T) {
	if _, err := Fingerprint("junk"); err == nil {
		t.Fatal("junk accepted")
	}
	if _, err := Fingerprint("-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n"); err == nil {
		t.Fatal("bad DER accepted")
	}
	key := "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
	a, _ := Fingerprint(key + readFile(t, "ecdsa.crt"))
	b, _ := Fingerprint(readFile(t, "ecdsa.crt"))
	if a == "" || a != b {
		t.Fatal("non-certificate blocks must be skipped")
	}
}

func TestRollbackUnsupported(t *testing.T) {
	if _, err := New(nil).Rollback(context.Background(), nil, nil, nil); !errors.Is(err, provider.ErrUnsupported) {
		t.Fatalf("rollback: %v", err)
	}
}

// --- Preview / ValidateCredentials ---

func TestPreview(t *testing.T) {
	inv := &fakeInventory{preview: invsdk.Preview{Hosts: []invsdk.TargetHost{{HostID: hostA, Hostname: "web-1", OSName: "Debian",
		Tags: map[string]string{"role": "web"}, AgentOnline: true, Capability: "enabled"}}}}
	p, _ := newTestProvider(inv)
	d, err := p.Preview(jobCtx(), map[string]any{"host_ids": []any{hostA}})
	if err != nil || len(d["matched_hosts"].([]map[string]any)) != 1 || d["truncated"] != false || d["unknown_host_ids"] == nil {
		t.Fatalf("preview = %v, %v", d, err)
	}
	if err := p.ValidateCredentials(jobCtx(), nil, map[string]any{"host_ids": []any{hostA}}); err != nil {
		t.Fatal(err)
	}
	var fe *provider.FieldError
	// No host matches → field error the drawer shows on the selection.
	inv.preview = invsdk.Preview{UnknownHostIDs: []string{hostB}}
	if _, err := p.Preview(jobCtx(), map[string]any{"host_ids": []any{hostB}}); !errors.As(err, &fe) || fe.Msg != "no_hosts_matched" {
		t.Fatalf("no match: %v", err)
	}
	if _, err := p.Preview(jobCtx(), map[string]any{}); !errors.As(err, &fe) || !strings.HasPrefix(fe.Msg, "one_of_required") {
		t.Fatalf("no selector: %v", err)
	}
	if _, err := p.Preview(jobCtx(), map[string]any{"host_ids": []any{"bad"}}); err == nil {
		t.Fatal("bad config accepted")
	}
	inv.prevErr = errors.New("down")
	if _, err := p.Preview(jobCtx(), map[string]any{"host_ids": []any{hostA}}); err == nil {
		t.Fatal("preview error swallowed")
	}
	if _, err := p.Preview(context.Background(), map[string]any{"host_ids": []any{hostA}}); !errors.Is(err, errNoJob) {
		t.Fatalf("no tenant: %v", err)
	}
	if _, err := New(nil).Preview(jobCtx(), map[string]any{"host_ids": []any{hostA}}); err == nil {
		t.Fatal("preview without client")
	}
	// SetInventory rebinds the client.
	p2 := New(nil)
	p2.SetInventory(&fakeInventory{preview: invsdk.Preview{Hosts: []invsdk.TargetHost{{HostID: hostA}}}})
	if _, err := p2.Preview(jobCtx(), map[string]any{"host_ids": []any{hostA}}); err != nil {
		t.Fatal(err)
	}
}

func TestSleepCtx(t *testing.T) {
	if err := sleepCtx(context.Background(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleepCtx(ctx, time.Hour); err == nil {
		t.Fatal("cancelled sleep returned nil")
	}
}
