// Package audit records every lcm operation in a closed vocabulary with a
// detail guard that keeps key material, secrets, tokens and CSRs out of the
// log. Events are buffered and written to the audit hypertable in batches by a
// background goroutine so recording never blocks the caller.
package audit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/store"
)

// EventType is the closed audit vocabulary (data-model.md).
type EventType string

// Event types.
const (
	TargetCreated          EventType = "target_created"
	TargetUpdated          EventType = "target_updated"
	TargetDeleted          EventType = "target_deleted"
	ConfigurationCreated   EventType = "configuration_created"
	ConfigurationUpdated   EventType = "configuration_updated"
	ConfigurationDeleted   EventType = "configuration_deleted"
	CredentialsValidated   EventType = "credentials_validated"
	DeploymentStarted      EventType = "deployment_started"
	DeploymentCompleted    EventType = "deployment_completed"
	DeploymentFailed       EventType = "deployment_failed"
	DeploymentVerified     EventType = "deployment_verified"
	DeploymentRolledBack   EventType = "deployment_rolled_back"
	JobCreated             EventType = "job_created"
	JobCancelled           EventType = "job_cancelled"
	JobRetried             EventType = "job_retried"
	GrantCreated           EventType = "grant_created"
	GrantRevoked           EventType = "grant_revoked"
	AccessRefused          EventType = "access_refused"
	BackupExported         EventType = "backup_exported"
	BackupExportedWithCred EventType = "backup_exported_with_credentials"
	BackupImported         EventType = "backup_imported"
)

// Subject kinds (closed set).
const (
	SubjectTarget        = "target"
	SubjectConfiguration = "configuration"
	SubjectJob           = "job"
	SubjectBackup        = "backup"
	SubjectGrant         = "grant"
	SubjectSystem        = "system"
)

// Outcomes (closed set).
const (
	OutcomeOK      = "ok"
	OutcomeRefused = "refused"
	OutcomeFailed  = "failed"
)

// Actor kinds (closed set).
const (
	ActorUser    = "user"
	ActorService = "service"
	ActorSystem  = "system"
)

var known = map[EventType]struct{}{}

func init() {
	for _, t := range []EventType{
		TargetCreated, TargetUpdated, TargetDeleted,
		ConfigurationCreated, ConfigurationUpdated, ConfigurationDeleted, CredentialsValidated,
		DeploymentStarted, DeploymentCompleted, DeploymentFailed, DeploymentVerified, DeploymentRolledBack,
		JobCreated, JobCancelled, JobRetried,
		GrantCreated, GrantRevoked, AccessRefused,
		BackupExported, BackupExportedWithCred, BackupImported,
	} {
		known[t] = struct{}{}
	}
}

// Known reports whether t is in the vocabulary.
func Known(t string) bool { _, ok := known[EventType(t)]; return ok }

// Event is one record before persistence. TS is filled by Record.
type Event struct {
	TenantID      string
	EventType     EventType
	ActorKind     string // user | service | system
	ActorID       string
	SubjectKind   string // certificate | issuer | request | job | secret | grant | webhook | bundle | backup | system
	SubjectID     string
	SubjectName   string
	Outcome       string // ok | refused | failed
	Reason        string
	CorrelationID string
	Details       map[string]any
}

// Store persists audit batches. repo.Store satisfies it, as does any test double.
type Store interface {
	InsertAuditRows(ctx context.Context, rows []store.AuditRow) error
}

// Validate checks the closed vocabulary and required fields. An unknown event
// type is a programming error and is surfaced to the caller.
func Validate(e Event) error {
	if _, ok := known[e.EventType]; !ok {
		return fmt.Errorf("audit: unknown event type %q", e.EventType)
	}
	if e.TenantID == "" {
		return errors.New("audit: tenant_id is required")
	}
	switch e.ActorKind {
	case ActorUser, ActorService, ActorSystem:
	default:
		return fmt.Errorf("audit: actor_kind %q", e.ActorKind)
	}
	switch e.SubjectKind {
	case SubjectTarget, SubjectConfiguration, SubjectJob, SubjectBackup, SubjectGrant, SubjectSystem:
	default:
		return fmt.Errorf("audit: subject_kind %q", e.SubjectKind)
	}
	switch e.Outcome {
	case OutcomeOK, OutcomeRefused, OutcomeFailed:
	default:
		return fmt.Errorf("audit: outcome %q", e.Outcome)
	}
	return nil
}

// forbidden detail-key substrings (case-insensitive).
var forbidden = []string{"key", "secret", "token", "password", "private", "csr"}

func forbiddenKey(k string) bool {
	lk := strings.ToLower(k)
	for _, f := range forbidden {
		if strings.Contains(lk, f) {
			return true
		}
	}
	return false
}

func guardValue(v any) any {
	switch x := v.(type) {
	case string:
		if len(x) > 256 {
			return x[:256]
		}
		return x
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, vv := range x {
			if forbiddenKey(k) {
				continue
			}
			out[k] = guardValue(vv)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, vv := range x {
			out[i] = guardValue(vv)
		}
		return out
	default:
		return v
	}
}

// guardDetails serialises m to JSON after dropping any key whose lowercased
// name carries key|secret|token|password|private|csr (at any depth) and
// truncating string values to 256 characters.
func guardDetails(m map[string]any) []byte {
	out := make(map[string]any, len(m))
	for k, v := range m {
		if forbiddenKey(k) {
			continue
		}
		out[k] = guardValue(v)
	}
	js, err := json.Marshal(out)
	if err != nil {
		return []byte("{}")
	}
	return js
}

// Writer buffers events and writes them in batches; Record never blocks.
type Writer struct {
	st      Store
	ch      chan store.AuditRow
	wg      sync.WaitGroup
	mu      sync.Mutex
	closed  bool
	dropped int64
	onError func(error)
	flushCh chan chan struct{}
	tick    time.Duration
}

// NewWriter starts the batch writer (queue 10k, batch 200 or 500 ms).
func NewWriter(st Store, onError func(error)) *Writer {
	w := newWriter(st, onError, 10000)
	w.start()
	return w
}

func newWriter(st Store, onError func(error), queue int) *Writer {
	w := &Writer{st: st, ch: make(chan store.AuditRow, queue), onError: onError, flushCh: make(chan chan struct{}), tick: 500 * time.Millisecond}
	if w.onError == nil {
		w.onError = func(error) {}
	}
	return w
}

func (w *Writer) start() {
	w.wg.Add(1)
	go w.run()
}

// Record validates the event, fills TS=now, guards the details and enqueues
// the row. An invalid event (unknown type, missing tenant, bad enum) returns
// an error and is not queued.
func (w *Writer) Record(_ context.Context, e Event) error {
	if err := Validate(e); err != nil {
		return err
	}
	row := store.AuditRow{
		TS:            time.Now(),
		TenantID:      e.TenantID,
		EventType:     string(e.EventType),
		ActorKind:     e.ActorKind,
		ActorID:       e.ActorID,
		SubjectKind:   e.SubjectKind,
		SubjectID:     e.SubjectID,
		SubjectName:   e.SubjectName,
		Outcome:       e.Outcome,
		Reason:        e.Reason,
		CorrelationID: e.CorrelationID,
		Details:       guardDetails(e.Details),
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		w.dropped++
		return errors.New("audit: writer closed")
	}
	select {
	case w.ch <- row:
	default:
		w.dropped++
		w.onError(errors.New("audit: queue full, event dropped"))
	}
	return nil
}

// Flush writes everything queued so far and returns when it is stored or ctx is done.
func (w *Writer) Flush(ctx context.Context) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.mu.Unlock()
	done := make(chan struct{})
	select {
	case w.flushCh <- done:
	case <-ctx.Done():
		return
	}
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// Dropped returns the number of dropped events.
func (w *Writer) Dropped() int64 { w.mu.Lock(); defer w.mu.Unlock(); return w.dropped }

func (w *Writer) run() {
	defer w.wg.Done()
	t := time.NewTicker(w.tick)
	defer t.Stop()
	buf := make([]store.AuditRow, 0, 200)
	flush := func() {
		if len(buf) == 0 {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		if err := w.st.InsertAuditRows(ctx, buf); err != nil {
			w.onError(err)
		}
		cancel()
		buf = buf[:0]
	}
	for {
		select {
		case r, ok := <-w.ch:
			if !ok {
				flush()
				return
			}
			buf = append(buf, r)
			if len(buf) >= 200 {
				flush()
			}
		case <-t.C:
			flush()
		case done := <-w.flushCh:
			for len(w.ch) > 0 {
				buf = append(buf, <-w.ch)
			}
			flush()
			close(done)
		}
	}
}

// Close drains and stops the writer.
func (w *Writer) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	close(w.ch)
	w.mu.Unlock()
	w.wg.Wait()
}
