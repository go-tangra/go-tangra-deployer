// Package notify e-mails the outcome of a target run (a parent job whose
// children all finished) to the tenant's administrators: members holding the
// built-in role owner or admin, or the deployer module role administrator.
//
// Sending never blocks or fails job processing: the scheduler hands an
// Outcome to Notify, which only enqueues it on a bounded queue (a full queue
// drops it with a warning); one worker goroutine resolves the recipients
// through auth (Profiles.Members, then Profiles.Contacts) once per outcome
// and sends one keyed notification per recipient through the notification
// module, retrying retryable failures with a short backoff. Logs and audit
// details carry counts only, never e-mail addresses or template variables.
package notify

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
	"github.com/go-tangra/go-tangra-notification/sdk/v4/pkg/notifyclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
)

// TemplateKey is the notification module's system template of a finished
// target run.
const TemplateKey = "deployer.deployment_finished"

// Defaults and bounds.
const (
	DefaultQueue = 64
	// MaxRecipients caps the addresses one outcome is sent to.
	MaxRecipients = 200
	membersPage   = 1000
	contactsChunk = 100
	maxPages      = 100 // 100k members
	callTimeout   = 10 * time.Second
	maxRecipient  = 512 // the notification module's recipient bound
)

// DefaultBackoff is the wait before each retry of retryable failures.
var DefaultBackoff = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute}

// Roles that receive status e-mails.
var (
	builtinRoles = map[string]bool{"owner": true, "admin": true}
	moduleRoles  = map[string]bool{"administrator": true}
)

// Profiles is the part of auth's Profiles service the notifier uses
// (authv1.ProfilesClient satisfies it).
type Profiles interface {
	Members(ctx context.Context, in *authv1.ListMemberDetailsRequest, opts ...grpc.CallOption) (*authv1.ListMemberDetailsResponse, error)
	Contacts(ctx context.Context, in *authv1.LookupContactsRequest, opts ...grpc.CallOption) (*authv1.LookupContactsResponse, error)
}

// Sender sends one keyed notification (notifyclient.Client satisfies it).
// Retryable outcomes come back as Result{Retryable: true} with a nil error;
// a returned error is a permanent refusal.
type Sender interface {
	SendKey(ctx context.Context, tenantID, key, recipient string, vars map[string]string, correlationID string) (notifyclient.Result, error)
}

// Auditor records audit events (audit.Writer satisfies it; nil disables).
type Auditor interface {
	Record(ctx context.Context, e audit.Event) error
}

// Result is one child job of a finished run.
type Result struct {
	Configuration string // the configuration's name (its id when deleted)
	Status        string
	Message       string // failure cause (failed children only)
}

// Outcome is a finished target run.
type Outcome struct {
	TenantID    string
	JobID       string // the parent job
	TargetID    string
	TargetName  string
	Status      string // completed | partial | failed
	TriggeredBy string // manual | event | auto_renewal
	Certificate string // certificate name, when known
	Total       int
	Succeeded   int
	Failed      int
	StartedAt   time.Time // when the run was created
	FinishedAt  time.Time
	Results     []Result
}

// Config builds a Notifier.
type Config struct {
	Profiles Profiles
	Sender   Sender
	Auditor  Auditor
	Log      *slog.Logger
	// PortalURL is the portal's https origin; empty: no link in the e-mail.
	PortalURL string
	Queue     int             // default DefaultQueue
	Backoff   []time.Duration // default DefaultBackoff
}

// Notifier queues outcomes and sends them from one worker.
type Notifier struct {
	profiles Profiles
	sender   Sender
	aud      Auditor
	log      *slog.Logger
	portal   string
	backoff  []time.Duration
	queue    chan Outcome
	dropped  atomic.Int64
	// sleep waits d or until ctx ends (false); replaced in tests.
	sleep func(ctx context.Context, d time.Duration) bool
}

// New builds the notifier; Run starts its worker.
func New(c Config) *Notifier {
	if c.Queue <= 0 {
		c.Queue = DefaultQueue
	}
	if c.Backoff == nil {
		c.Backoff = DefaultBackoff
	}
	if c.Log == nil {
		c.Log = slog.Default()
	}
	return &Notifier{
		profiles: c.Profiles, sender: c.Sender, aud: c.Auditor, log: c.Log,
		portal: strings.TrimRight(c.PortalURL, "/"), backoff: c.Backoff,
		queue: make(chan Outcome, c.Queue), sleep: sleepCtx,
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// Notify enqueues an outcome without blocking. A full queue drops it (logged
// and counted): a status e-mail never holds up a deployment.
func (n *Notifier) Notify(o Outcome) bool {
	select {
	case n.queue <- o:
		return true
	default:
		d := n.dropped.Add(1)
		n.log.Warn("deployer: status notification dropped: queue full",
			"tenant_id", o.TenantID, "job_id", o.JobID, "target_id", o.TargetID, "status", o.Status, "dropped_total", d)
		return false
	}
}

// Dropped is the number of outcomes dropped because the queue was full.
func (n *Notifier) Dropped() int64 { return n.dropped.Load() }

// Run sends queued outcomes until ctx ends.
func (n *Notifier) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case o := <-n.queue:
			n.process(ctx, o)
		}
	}
}

// process resolves the recipients once, then sends to each, retrying the
// retryable failures after each backoff step.
func (n *Notifier) process(ctx context.Context, o Outcome) {
	var (
		recipients []string
		err        error
	)
	for attempt := 0; ; attempt++ {
		recipients, err = n.recipients(ctx, o.TenantID)
		if err == nil || !retryable(err) || attempt >= len(n.backoff) || !n.sleep(ctx, n.backoff[attempt]) {
			break
		}
	}
	if err != nil {
		n.log.Warn("deployer: status notification not sent: recipients unavailable",
			"tenant_id", o.TenantID, "job_id", o.JobID, "target_id", o.TargetID, "code", status.Code(err).String())
		n.record(ctx, o, 0, 0, 0, "recipients unavailable")
		return
	}
	if len(recipients) == 0 {
		n.log.Info("deployer: status notification has no recipients", "tenant_id", o.TenantID, "job_id", o.JobID, "target_id", o.TargetID)
		n.record(ctx, o, 0, 0, 0, "no recipients")
		return
	}
	vars := Variables(o, n.portal)
	pending := recipients
	sent, failed := 0, 0
	for attempt := 0; ; attempt++ {
		var retry []string
		stop := false
		for i, r := range pending {
			if ctx.Err() != nil {
				retry = append(retry, pending[i:]...)
				break
			}
			res, serr := n.sender.SendKey(ctx, o.TenantID, TemplateKey, r, vars, o.JobID)
			switch {
			case serr != nil:
				failed++
				n.log.Warn("deployer: status notification refused", "tenant_id", o.TenantID, "job_id", o.JobID, "code", status.Code(serr).String())
				// The same refusal for everyone: the rest fails too.
				stop = global(serr)
			case res.Sent:
				sent++
			case res.Retryable:
				retry = append(retry, r)
			default:
				failed++
			}
			if stop {
				failed += len(pending) - i - 1 + len(retry)
				retry = nil
				break
			}
		}
		if len(retry) == 0 {
			break
		}
		if attempt >= len(n.backoff) || !n.sleep(ctx, n.backoff[attempt]) {
			failed += len(retry) // gave up
			break
		}
		pending = retry
	}
	lvl := slog.LevelInfo
	if failed > 0 {
		lvl = slog.LevelWarn
	}
	n.log.Log(ctx, lvl, "deployer: status notification sent", "tenant_id", o.TenantID, "job_id", o.JobID,
		"target_id", o.TargetID, "status", o.Status, "recipients", len(recipients), "sent", sent, "failed", failed)
	n.record(ctx, o, len(recipients), sent, failed, "")
}

// global reports refusals that do not depend on the recipient (unknown
// template, foreign namespace, e-mail not configured, policy).
func global(err error) bool {
	switch status.Code(err) {
	case codes.NotFound, codes.FailedPrecondition, codes.PermissionDenied, codes.Unauthenticated, codes.Unimplemented:
		return true
	}
	return false
}

func retryable(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Aborted:
		return true
	}
	return errors.Is(err, context.DeadlineExceeded)
}

// record writes the deployment_notified audit event (counts only).
func (n *Notifier) record(ctx context.Context, o Outcome, recipients, sent, failed int, reason string) {
	if n.aud == nil {
		return
	}
	outcome := audit.OutcomeOK
	if failed > 0 || (reason != "" && reason != "no recipients") {
		outcome = audit.OutcomeFailed
	}
	_ = n.aud.Record(ctx, audit.Event{
		TenantID: o.TenantID, EventType: audit.DeploymentNotified, ActorKind: audit.ActorSystem,
		SubjectKind: audit.SubjectJob, SubjectID: o.JobID, SubjectName: o.TargetName,
		Outcome: outcome, Reason: reason, CorrelationID: o.JobID,
		Details: map[string]any{"target_id": o.TargetID, "status": o.Status, "recipients": recipients, "sent": sent, "failed": failed},
	})
}

// Admin reports whether a member receives status e-mails: built-in owner or
// admin, or the deployer module role administrator.
func Admin(m *authv1.MemberDetail) bool {
	for _, r := range m.GetBuiltinRoles() {
		if builtinRoles[r] {
			return true
		}
	}
	for _, r := range m.GetRoles() {
		if moduleRoles[r] {
			return true
		}
	}
	return false
}

// recipients pages the tenant's members, keeps the administrators and looks
// their e-mail addresses up (deduplicated case-insensitively, at most
// MaxRecipients).
func (n *Notifier) recipients(ctx context.Context, tenantID string) ([]string, error) {
	if n.profiles == nil {
		return nil, status.Error(codes.Unavailable, "auth unavailable")
	}
	var ids []string
	cursor := ""
	for page := 0; page < maxPages; page++ {
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		res, err := n.profiles.Members(cctx, &authv1.ListMemberDetailsRequest{TenantId: tenantID, Cursor: cursor, Limit: membersPage})
		cancel()
		if err != nil {
			return nil, err
		}
		for _, m := range res.GetMembers() {
			if Admin(m) {
				ids = append(ids, m.GetUserId())
			}
		}
		if res.GetNextCursor() == "" || len(res.GetMembers()) == 0 {
			break
		}
		cursor = res.GetNextCursor()
	}
	seen := map[string]bool{}
	var out []string
	capped := false
	for i := 0; i < len(ids) && !capped; i += contactsChunk {
		if len(out) >= MaxRecipients { // more administrators than the cap
			capped = true
			break
		}
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		res, err := n.profiles.Contacts(cctx, &authv1.LookupContactsRequest{TenantId: tenantID, UserIds: ids[i:min(i+contactsChunk, len(ids))]})
		cancel()
		if err != nil {
			return nil, err
		}
		for _, c := range res.GetContacts() {
			e := strings.TrimSpace(c.GetEmail())
			k := strings.ToLower(e)
			if e == "" || len(e) > maxRecipient || seen[k] {
				continue
			}
			if len(out) >= MaxRecipients {
				capped = true
				break
			}
			seen[k] = true
			out = append(out, e)
		}
	}
	if capped {
		n.log.Warn("deployer: status notification recipients capped", "tenant_id", tenantID, "administrators", len(ids), "cap", MaxRecipients)
	}
	return out, nil
}
