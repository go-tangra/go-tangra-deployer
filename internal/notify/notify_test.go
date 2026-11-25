package notify

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
	"github.com/go-tangra/go-tangra-notification/sdk/v4/pkg/notifyclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
)

const tenant = "11111111-1111-1111-1111-111111111111"

// fakeProfiles serves members (paged by pageSize) and their contacts.
type fakeProfiles struct {
	mu        sync.Mutex
	members   []*authv1.MemberDetail
	emails    map[string]string
	pageSize  int
	err       error // returned by Members while failures > 0
	failures  int
	pages     int
	contacts  int
	maxChunk  int
	tenantIDs []string
}

func (f *fakeProfiles) Members(_ context.Context, in *authv1.ListMemberDetailsRequest, _ ...grpc.CallOption) (*authv1.ListMemberDetailsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tenantIDs = append(f.tenantIDs, in.GetTenantId())
	if f.failures > 0 {
		f.failures--
		return nil, f.err
	}
	f.pages++
	size := f.pageSize
	if size == 0 {
		size = 1000
	}
	start := 0
	for i, m := range f.members {
		if m.GetUserId() == in.GetCursor() {
			start = i + 1
		}
	}
	end := min(start+size, len(f.members))
	res := &authv1.ListMemberDetailsResponse{Members: f.members[start:end]}
	if end < len(f.members) {
		res.NextCursor = f.members[end-1].GetUserId()
	}
	return res, nil
}

func (f *fakeProfiles) Contacts(_ context.Context, in *authv1.LookupContactsRequest, _ ...grpc.CallOption) (*authv1.LookupContactsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.contacts++
	f.maxChunk = max(f.maxChunk, len(in.GetUserIds()))
	res := &authv1.LookupContactsResponse{}
	for _, id := range in.GetUserIds() {
		if e, ok := f.emails[id]; ok {
			res.Contacts = append(res.Contacts, &authv1.Contact{UserId: id, Email: e})
		}
	}
	return res, nil
}

// fakeSender answers SendKey from a per-call script (default: sent).
type fakeSender struct {
	mu     sync.Mutex
	calls  []string // recipients, in order
	vars   []map[string]string
	answer func(recipient string, attempt int) (notifyclient.Result, error)
	tries  map[string]int
	notify chan struct{}
}

func (f *fakeSender) SendKey(_ context.Context, tenantID, key, recipient string, vars map[string]string, correlationID string) (notifyclient.Result, error) {
	f.mu.Lock()
	if f.tries == nil {
		f.tries = map[string]int{}
	}
	f.tries[recipient]++
	n := f.tries[recipient]
	f.calls = append(f.calls, recipient)
	f.vars = append(f.vars, vars)
	answer := f.answer
	f.mu.Unlock()
	if f.notify != nil {
		defer func() { f.notify <- struct{}{} }()
	}
	if tenantID != tenant || key != TemplateKey || correlationID == "" {
		return notifyclient.Result{}, fmt.Errorf("bad call %s %s %s", tenantID, key, correlationID)
	}
	if answer != nil {
		return answer(recipient, n)
	}
	return notifyclient.Result{Sent: true}, nil
}

type fakeAuditor struct {
	mu     sync.Mutex
	events []audit.Event
}

func (f *fakeAuditor) Record(_ context.Context, e audit.Event) error {
	if err := audit.Validate(e); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, e)
	return nil
}

func member(id string, builtin []string, roles ...string) *authv1.MemberDetail {
	return &authv1.MemberDetail{UserId: id, BuiltinRoles: builtin, Roles: roles}
}

func outcome() Outcome {
	start := time.Date(2026, 11, 25, 10, 0, 0, 0, time.UTC)
	return Outcome{
		TenantID: tenant, JobID: "job-1", TargetID: "tgt-1", TargetName: "edge", Status: "partial", TriggeredBy: "event",
		Certificate: "api.example.com", Total: 2, Succeeded: 1, Failed: 1, StartedAt: start, FinishedAt: start.Add(72 * time.Second),
		Results: []Result{{Configuration: "alpha", Status: "completed"}, {Configuration: "beta", Status: "failed", Message: "refused"}},
	}
}

type kit struct {
	n   *Notifier
	p   *fakeProfiles
	s   *fakeSender
	a   *fakeAuditor
	log *bytes.Buffer
	// waits records the backoff steps slept.
	waits []time.Duration
}

func newKit(t *testing.T, p *fakeProfiles, s *fakeSender) *kit {
	t.Helper()
	k := &kit{p: p, s: s, a: &fakeAuditor{}, log: &bytes.Buffer{}}
	k.n = New(Config{Profiles: p, Sender: s, Auditor: k.a, Log: slog.New(slog.NewJSONHandler(k.log, nil)), PortalURL: "https://portal.example.org/"})
	k.n.sleep = func(ctx context.Context, d time.Duration) bool {
		k.waits = append(k.waits, d)
		return ctx.Err() == nil
	}
	return k
}

func admins() *fakeProfiles {
	return &fakeProfiles{
		members: []*authv1.MemberDetail{
			member("u1", []string{"owner"}),
			member("u2", []string{"admin", "member"}),
			member("u3", []string{"member"}, "administrator"),
			member("u4", []string{"member"}),
			member("u5", []string{"operator"}, "operator"),
			member("u6", []string{"auditor"}, "viewer"),
			member("u7", []string{"admin"}),
		},
		emails: map[string]string{
			"u1": "owner@example.org", "u2": "Admin@Example.org", "u3": "deployer@example.org",
			"u4": "member@example.org", "u5": "operator@example.org", "u6": "viewer@example.org",
			"u7": "admin@example.org", // the same address as u2, other case
		},
	}
}

func TestAdmin(t *testing.T) {
	cases := []struct {
		m    *authv1.MemberDetail
		want bool
	}{
		{member("a", []string{"owner"}), true},
		{member("a", []string{"admin"}), true},
		{member("a", []string{"member"}, "administrator"), true},
		{member("a", []string{"member"}), false},
		{member("a", []string{"operator"}, "operator"), false},
		{member("a", []string{"auditor"}, "viewer"), false},
		{member("a", nil, "admin"), false}, // a module role named admin is not the built-in one
		{member("a", []string{"administrator"}), false},
	}
	for _, c := range cases {
		if got := Admin(c.m); got != c.want {
			t.Errorf("Admin(%v/%v) = %v, want %v", c.m.GetBuiltinRoles(), c.m.GetRoles(), got, c.want)
		}
	}
}

func TestRecipientsFilterAndDedupe(t *testing.T) {
	p := admins()
	p.pageSize = 2 // members come in four pages
	k := newKit(t, p, &fakeSender{})
	got, err := k.n.recipients(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"owner@example.org", "Admin@Example.org", "deployer@example.org"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("recipients = %v, want %v", got, want)
	}
	if p.pages != 4 {
		t.Fatalf("pages = %d, want 4", p.pages)
	}
	for _, tid := range p.tenantIDs {
		if tid != tenant {
			t.Fatalf("members asked for tenant %q", tid)
		}
	}
}

func TestRecipientsChunkedAndCapped(t *testing.T) {
	p := &fakeProfiles{emails: map[string]string{}}
	for i := range 250 {
		id := fmt.Sprintf("u%03d", i)
		p.members = append(p.members, member(id, []string{"admin"}))
		p.emails[id] = id + "@example.org"
	}
	p.members = append(p.members, member("nomail", []string{"owner"})) // no e-mail: skipped
	k := newKit(t, p, &fakeSender{})
	got, err := k.n.recipients(context.Background(), tenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != MaxRecipients {
		t.Fatalf("recipients = %d, want %d", len(got), MaxRecipients)
	}
	if p.maxChunk > contactsChunk || p.contacts != 2 {
		t.Fatalf("contacts calls = %d, largest chunk = %d", p.contacts, p.maxChunk)
	}
	if !strings.Contains(k.log.String(), "recipients capped") {
		t.Fatalf("no cap warning: %s", k.log)
	}
}

func TestProcessSendsOnePerRecipientAndAudits(t *testing.T) {
	s := &fakeSender{}
	k := newKit(t, admins(), s)
	k.n.process(context.Background(), outcome())
	if len(s.calls) != 3 {
		t.Fatalf("sent %d, want 3", len(s.calls))
	}
	v := s.vars[0]
	for _, key := range []string{"target", "status", "triggered_by", "total", "succeeded"} {
		if v[key] == "" {
			t.Fatalf("required variable %s missing: %v", key, v)
		}
	}
	if v["link"] != "https://portal.example.org/deployer/jobs?job=job-1" {
		t.Fatalf("link = %q", v["link"])
	}
	if len(k.a.events) != 1 {
		t.Fatalf("audit events = %d", len(k.a.events))
	}
	e := k.a.events[0]
	if e.EventType != audit.DeploymentNotified || e.Outcome != audit.OutcomeOK || e.SubjectID != "job-1" {
		t.Fatalf("audit = %+v", e)
	}
	if e.Details["recipients"] != 3 || e.Details["sent"] != 3 || e.Details["failed"] != 0 || e.Details["target_id"] != "tgt-1" || e.Details["status"] != "partial" {
		t.Fatalf("audit details = %v", e.Details)
	}
	// Neither the audit nor the log carries an address.
	raw, _ := json.Marshal(e)
	for _, out := range []string{string(raw), k.log.String()} {
		if strings.Contains(strings.ToLower(out), "@example.org") {
			t.Fatalf("an address leaked: %s", out)
		}
	}
}

func TestProcessRetriesRetryable(t *testing.T) {
	s := &fakeSender{answer: func(r string, attempt int) (notifyclient.Result, error) {
		if r == "owner@example.org" && attempt < 3 {
			return notifyclient.Result{Retryable: true}, nil
		}
		return notifyclient.Result{Sent: true}, nil
	}}
	k := newKit(t, admins(), s)
	k.n.process(context.Background(), outcome())
	if s.tries["owner@example.org"] != 3 || s.tries["deployer@example.org"] != 1 {
		t.Fatalf("tries = %v", s.tries)
	}
	if len(k.waits) != 2 || k.waits[0] != 5*time.Second || k.waits[1] != 30*time.Second {
		t.Fatalf("waits = %v", k.waits)
	}
	if p := k.p.pages; p != 1 {
		t.Fatalf("recipients resolved %d times, want once", p)
	}
	if e := k.a.events[0]; e.Details["sent"] != 3 || e.Details["failed"] != 0 {
		t.Fatalf("audit = %v", e.Details)
	}
}

func TestProcessGivesUp(t *testing.T) {
	s := &fakeSender{answer: func(r string, _ int) (notifyclient.Result, error) {
		if r == "owner@example.org" {
			return notifyclient.Result{Retryable: true}, nil
		}
		return notifyclient.Result{Sent: true}, nil
	}}
	k := newKit(t, admins(), s)
	k.n.process(context.Background(), outcome())
	if s.tries["owner@example.org"] != 1+len(DefaultBackoff) {
		t.Fatalf("tries = %v", s.tries)
	}
	if len(k.waits) != len(DefaultBackoff) || k.waits[2] != 2*time.Minute {
		t.Fatalf("waits = %v", k.waits)
	}
	if e := k.a.events[0]; e.Outcome != audit.OutcomeFailed || e.Details["sent"] != 2 || e.Details["failed"] != 1 {
		t.Fatalf("audit = %+v", e)
	}
}

func TestProcessStopsOnGlobalRefusal(t *testing.T) {
	s := &fakeSender{answer: func(string, int) (notifyclient.Result, error) {
		return notifyclient.Result{}, status.Error(codes.FailedPrecondition, "email not configured")
	}}
	k := newKit(t, admins(), s)
	k.n.process(context.Background(), outcome())
	if len(s.calls) != 1 || len(k.waits) != 0 {
		t.Fatalf("calls = %d, waits = %v", len(s.calls), k.waits)
	}
	if e := k.a.events[0]; e.Details["failed"] != 3 || e.Details["sent"] != 0 {
		t.Fatalf("audit = %v", e.Details)
	}
}

func TestProcessRecipientRefusalContinues(t *testing.T) {
	s := &fakeSender{answer: func(r string, _ int) (notifyclient.Result, error) {
		if r == "owner@example.org" {
			return notifyclient.Result{}, status.Error(codes.InvalidArgument, "bad recipient")
		}
		return notifyclient.Result{Sent: true}, nil
	}}
	k := newKit(t, admins(), s)
	k.n.process(context.Background(), outcome())
	if len(s.calls) != 3 {
		t.Fatalf("calls = %d", len(s.calls))
	}
	if e := k.a.events[0]; e.Details["failed"] != 1 || e.Details["sent"] != 2 {
		t.Fatalf("audit = %v", e.Details)
	}
}

func TestProcessRetriesRecipientLookup(t *testing.T) {
	p := admins()
	p.err, p.failures = status.Error(codes.Unavailable, "down"), 2
	s := &fakeSender{}
	k := newKit(t, p, s)
	k.n.process(context.Background(), outcome())
	if len(s.calls) != 3 || len(k.waits) != 2 {
		t.Fatalf("calls = %d, waits = %v", len(s.calls), k.waits)
	}
	// Auth down for good: nothing sent, the failure audited.
	p2 := admins()
	p2.err, p2.failures = status.Error(codes.Unavailable, "down"), 100
	s2 := &fakeSender{}
	k2 := newKit(t, p2, s2)
	k2.n.process(context.Background(), outcome())
	if len(s2.calls) != 0 || len(k2.waits) != len(DefaultBackoff) {
		t.Fatalf("calls = %d, waits = %v", len(s2.calls), k2.waits)
	}
	if e := k2.a.events[0]; e.Outcome != audit.OutcomeFailed || e.Reason != "recipients unavailable" {
		t.Fatalf("audit = %+v", e)
	}
	// A permanent refusal is not retried.
	p3 := admins()
	p3.err, p3.failures = status.Error(codes.PermissionDenied, "policy"), 100
	k3 := newKit(t, p3, &fakeSender{})
	k3.n.process(context.Background(), outcome())
	if len(k3.waits) != 0 {
		t.Fatalf("waits = %v", k3.waits)
	}
}

func TestNoRecipients(t *testing.T) {
	p := &fakeProfiles{members: []*authv1.MemberDetail{member("u4", []string{"member"})}, emails: map[string]string{"u4": "m@example.org"}}
	s := &fakeSender{}
	k := newKit(t, p, s)
	k.n.process(context.Background(), outcome())
	if len(s.calls) != 0 {
		t.Fatalf("sent to a non-admin")
	}
	if e := k.a.events[0]; e.Outcome != audit.OutcomeOK || e.Details["recipients"] != 0 {
		t.Fatalf("audit = %+v", e)
	}
}

func TestQueueFullDrops(t *testing.T) {
	n := New(Config{Queue: 2, Log: slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))})
	for range 2 {
		if !n.Notify(outcome()) {
			t.Fatal("enqueue refused below capacity")
		}
	}
	done := make(chan bool)
	go func() { done <- n.Notify(outcome()) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("enqueued beyond capacity")
		}
	case <-time.After(time.Second):
		t.Fatal("Notify blocked on a full queue")
	}
	if n.Dropped() != 1 {
		t.Fatalf("dropped = %d", n.Dropped())
	}
}

func TestRunSendsAndStops(t *testing.T) {
	s := &fakeSender{notify: make(chan struct{}, 10)}
	k := newKit(t, admins(), s)
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		k.n.Run(ctx)
		close(stopped)
	}()
	k.n.Notify(outcome())
	for range 3 {
		select {
		case <-s.notify:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not send")
		}
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop")
	}
}
