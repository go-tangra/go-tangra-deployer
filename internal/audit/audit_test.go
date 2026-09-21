package audit_test

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/audit"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

// fakeStore captures the audit rows written by the batch writer.
type fakeStore struct {
	mu   sync.Mutex
	rows []store.AuditRow
}

func (f *fakeStore) InsertAuditRows(_ context.Context, rows []store.AuditRow) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.rows = append(f.rows, rows...)
	return nil
}

func (f *fakeStore) all() []store.AuditRow {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]store.AuditRow(nil), f.rows...)
}

func validEvent() audit.Event {
	return audit.Event{
		TenantID: "11111111-1111-1111-1111-111111111111", EventType: audit.ConfigurationCreated,
		ActorKind: audit.ActorUser, ActorID: "u1", SubjectKind: audit.SubjectConfiguration, SubjectID: "c1",
		Outcome: audit.OutcomeOK,
	}
}

func TestValidateRejectsBadEvents(t *testing.T) {
	if audit.Validate(validEvent()) != nil {
		t.Fatal("valid event rejected")
	}
	bad := validEvent()
	bad.EventType = "nope"
	if audit.Validate(bad) == nil {
		t.Fatal("unknown event type accepted")
	}
	bad = validEvent()
	bad.TenantID = ""
	if audit.Validate(bad) == nil {
		t.Fatal("missing tenant accepted")
	}
	bad = validEvent()
	bad.Outcome = "maybe"
	if audit.Validate(bad) == nil {
		t.Fatal("bad outcome accepted")
	}
	if !audit.Known(string(audit.DeploymentCompleted)) || audit.Known("bogus") {
		t.Fatal("Known vocabulary wrong")
	}
}

// TestRecordRedactsSecrets is the security-critical check: any detail key that
// looks like key/secret/token/password/private/csr must be dropped before the
// row is persisted, at any nesting depth, and long strings truncated.
func TestRecordRedactsSecrets(t *testing.T) {
	fs := &fakeStore{}
	w := audit.NewWriter(fs, nil)
	e := validEvent()
	e.Details = map[string]any{
		"provider":    "aws_acm",
		"api_token":   "SUPER-SECRET",
		"nested":      map[string]any{"password": "p4ss", "region": "eu"},
		"private_key": "-----BEGIN PRIVATE KEY-----",
		"long":        strings.Repeat("x", 500),
	}
	if err := w.Record(context.Background(), e); err != nil {
		t.Fatalf("record: %v", err)
	}
	w.Flush(context.Background())
	w.Close()

	rows := fs.all()
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	blob := string(rows[0].Details)
	for _, forbidden := range []string{"SUPER-SECRET", "p4ss", "BEGIN PRIVATE KEY", "api_token", "password", "private_key"} {
		if strings.Contains(blob, forbidden) {
			t.Fatalf("audit detail leaked %q: %s", forbidden, blob)
		}
	}
	var m map[string]any
	if err := json.Unmarshal(rows[0].Details, &m); err != nil {
		t.Fatalf("details not JSON: %v", err)
	}
	if m["provider"] != "aws_acm" {
		t.Fatalf("non-secret field dropped: %+v", m)
	}
	if s, _ := m["long"].(string); len(s) > 256 {
		t.Fatalf("long string not truncated: %d", len(s))
	}
	if nested, ok := m["nested"].(map[string]any); !ok || nested["region"] != "eu" || nested["password"] != nil {
		t.Fatalf("nested redaction wrong: %+v", m["nested"])
	}
}

func TestRecordAfterCloseIsDropped(t *testing.T) {
	fs := &fakeStore{}
	w := audit.NewWriter(fs, nil)
	w.Close()
	if err := w.Record(context.Background(), validEvent()); err == nil {
		t.Fatal("record after close should error")
	}
	if w.Dropped() == 0 {
		t.Fatal("dropped count not incremented")
	}
}

func TestFlushIsBounded(t *testing.T) {
	fs := &fakeStore{}
	w := audit.NewWriter(fs, nil)
	defer w.Close()
	for i := 0; i < 10; i++ {
		_ = w.Record(context.Background(), validEvent())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.Flush(ctx)
	if len(fs.all()) != 10 {
		t.Fatalf("flushed %d, want 10", len(fs.all()))
	}
}
