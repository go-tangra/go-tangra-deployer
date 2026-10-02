package events

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"log/slog"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// Module is the deployer's event-source name; events it publishes carry it so
// the consumer ignores its own events (loop guard, SR-005).
const Module = "deployer"

// Cert-lifecycle event types consumed for auto-deploy, and the revocation
// forwarded to the inventory (feature 033, research D14).
const (
	EventIssued  = "certificate.issued"
	EventRenewed = "certificate.renewed"
	EventRevoked = "certificate.revoked"
)

// inventoryAgentType is the provider whose deliveries a revocation concerns
// (providers/inventoryagent.Type; not imported to keep this package free of
// provider implementations).
const inventoryAgentType = "inventory-agent"

// Revoker forwards a revoked certificate to the inventory, which cancels its
// queued deliveries and flags hosts holding it (files stay on the hosts).
type Revoker interface {
	MarkCertificateRevoked(ctx context.Context, tenantID, certificateID string) (cancelledItems, flaggedHosts int, err error)
}

// Auditor records audit events (audit.Writer satisfies it).
type Auditor interface {
	Record(ctx context.Context, e audit.Event) error
}

// CertFetcher fetches a certificate bundle from lcm (lcmclient satisfies it).
type CertFetcher interface {
	FetchCertificate(ctx context.Context, tenantID, certOrJobID string, includeKey bool) (provider.CertificateData, error)
}

// Consumer turns certificate events into deployment jobs for matching targets.
type Consumer struct {
	st   repo.Store
	cert CertFetcher
	log  *slog.Logger
	now  func() time.Time
	rev  Revoker
	aud  Auditor
}

// SetRevocationForwarder enables forwarding certificate.revoked to the
// inventory (nil disables); aud, when set, audits each forward.
func (c *Consumer) SetRevocationForwarder(r Revoker, aud Auditor) { c.rev, c.aud = r, aud }

// HandleRevoked forwards a revocation to the inventory when the tenant has an
// active inventory-agent configuration. Best effort: failures are logged and
// audited, never returned to the event loop (issued/renewed handling goes on).
// Reports whether the revocation was forwarded successfully.
func (c *Consumer) HandleRevoked(ctx context.Context, tenantID, certID string) bool {
	if c.rev == nil || certID == "" {
		return false
	}
	cfgs, err := c.st.ListConfigurations(ctx, tenantID, repo.ConfigFilter{ProviderType: inventoryAgentType, Status: store.ConfigActive})
	if err != nil {
		c.log.Warn("deployer: revocation forward skipped: configurations unavailable", "tenant", tenantID, "err", err)
		return false
	}
	if len(cfgs) == 0 {
		return false
	}
	cancelled, flagged, err := c.rev.MarkCertificateRevoked(ctx, tenantID, certID)
	ev := audit.Event{TenantID: tenantID, EventType: audit.CertificateRevocationForwarded, ActorKind: audit.ActorSystem,
		ActorID: Module, SubjectKind: audit.SubjectCertificate, SubjectID: certID, Outcome: audit.OutcomeOK}
	if err != nil {
		c.log.Warn("deployer: revocation forward to inventory failed", "tenant", tenantID, "certificate_id", certID, "err", err)
		ev.Outcome, ev.Reason = audit.OutcomeFailed, "inventory_unavailable"
	} else {
		ev.Details = map[string]any{"cancelled_items": cancelled, "flagged_hosts": flagged}
	}
	if c.aud != nil {
		_ = c.aud.Record(ctx, ev)
	}
	return err == nil
}

// NewConsumer builds the consumer.
func NewConsumer(st repo.Store, cert CertFetcher, log *slog.Logger) *Consumer {
	if log == nil {
		log = slog.Default()
	}
	return &Consumer{st: st, cert: cert, log: log, now: time.Now}
}

// SetClock injects the clock (tests).
func (c *Consumer) SetClock(now func() time.Time) { c.now = now }

// Handle processes one certificate event for a tenant: it fetches the certificate
// (fetch-to-match), matches it against the tenant's auto-deploy targets, and for
// every match creates a parent job + one child per attached configuration.
// Returns the number of parent jobs created.
func (c *Consumer) Handle(ctx context.Context, tenantID, certID string, isRenewal bool) (int, error) {
	cert, err := c.cert.FetchCertificate(ctx, tenantID, certID, false)
	if err != nil {
		return 0, err
	}
	subj := subjectFrom(cert)
	targets, err := c.st.ListAutoDeployTargets(ctx, tenantID)
	if err != nil {
		return 0, err
	}
	trigger := store.TriggerEvent
	if isRenewal {
		trigger = store.TriggerAutoRenewal
	}
	created := 0
	for _, t := range targets {
		if !MatchesAny(t.CertificateFilters, subj) {
			continue
		}
		cfgIDs, err := c.st.ListTargetConfigurationIDs(ctx, tenantID, t.ID)
		if err != nil || len(cfgIDs) == 0 {
			continue // a match needs >= 1 configuration
		}
		if err := c.spawn(ctx, tenantID, t.ID, cert.ID, trigger, cfgIDs); err != nil {
			c.log.Warn("deployer: auto-deploy job creation failed", "target", t.ID, "err", err)
			continue
		}
		created++
	}
	return created, nil
}

func (c *Consumer) spawn(ctx context.Context, tenantID, targetID, certID, trigger string, cfgIDs []string) error {
	parentID := store.NewID()
	tgt := targetID
	return c.st.Atomic(ctx, tenantID, func(tx repo.Store) error {
		if e := tx.InsertJob(ctx, store.DeploymentJob{
			ID: parentID, TenantID: tenantID, DeploymentTargetID: &tgt, CertificateID: certID,
			Status: store.JobPending, MaxRetries: 3, TriggeredBy: trigger,
		}); e != nil {
			return e
		}
		for _, cid := range cfgIDs {
			cc, pid := cid, parentID
			if e := tx.InsertJob(ctx, store.DeploymentJob{
				ID: store.NewID(), TenantID: tenantID, TargetConfigurationID: &cc, ParentJobID: &pid,
				CertificateID: certID, Status: store.JobPending, MaxRetries: 3, TriggeredBy: trigger,
			}); e != nil {
				return e
			}
		}
		return nil
	})
}

// subjectFrom builds a match subject from the fetched certificate. It uses the
// bundle's CN/SANs, and parses the leaf PEM (when present) for the issuer name
// and subject organization fields so issuer/org filters can match.
func subjectFrom(cert provider.CertificateData) CertificateSubject {
	s := CertificateSubject{CommonName: cert.CommonName, SANs: cert.SANs}
	if cert.CertificatePEM == "" {
		return s
	}
	blk, _ := pem.Decode([]byte(cert.CertificatePEM))
	if blk == nil {
		return s
	}
	leaf, err := x509.ParseCertificate(blk.Bytes)
	if err != nil {
		return s
	}
	s.IssuerName = leaf.Issuer.CommonName
	if s.CommonName == "" {
		s.CommonName = leaf.Subject.CommonName
	}
	if len(s.SANs) == 0 {
		s.SANs = leaf.DNSNames
	}
	if len(leaf.Subject.Organization) > 0 {
		s.Organization = leaf.Subject.Organization[0]
	}
	if len(leaf.Subject.OrganizationalUnit) > 0 {
		s.OrgUnit = leaf.Subject.OrganizationalUnit[0]
	}
	if len(leaf.Subject.Country) > 0 {
		s.Country = leaf.Subject.Country[0]
	}
	return s
}

// EventType classifies a raw event type; ok is false for irrelevant types.
func EventType(typ string) (isRenewal, ok bool) {
	switch strings.TrimSpace(typ) {
	case EventIssued:
		return false, true
	case EventRenewed:
		return true, true
	default:
		return false, false
	}
}

// Reader is the subset of the stream client the consumer needs.
type Reader interface {
	XRead(ctx context.Context, key, afterID string, block time.Duration, count int64) ([]Entry, error)
	XLast(ctx context.Context, key string) (string, error)
}

// Entry mirrors the stream package's entry (avoids importing it here).
type Entry struct {
	ID     string
	Fields map[string]string
}

// Run consumes the platform event bus for the given tenants, dispatching
// certificate.issued / certificate.renewed to Handle. It ignores events the
// deployer itself published (loop guard) and starts after the current tail so a
// restart does not replay old events.
func (c *Consumer) Run(ctx context.Context, r Reader, tenantIDs []string, keyFor func(string) string) {
	last := map[string]string{}
	for _, t := range tenantIDs {
		if id, err := r.XLast(ctx, keyFor(t)); err == nil && id != "" {
			last[t] = id
		} else {
			last[t] = "0-0"
		}
	}
	for ctx.Err() == nil {
		for _, t := range tenantIDs {
			key := keyFor(t)
			entries, err := r.XRead(ctx, key, last[t], 2*time.Second, 100)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				continue
			}
			for _, e := range entries {
				last[t] = e.ID
				if e.Fields["module"] == Module {
					continue // our own event (loop guard)
				}
				if strings.TrimSpace(e.Fields["type"]) == EventRevoked {
					c.HandleRevoked(ctx, t, certIDFrom(e.Fields["data"]))
					continue
				}
				isRenewal, ok := EventType(e.Fields["type"])
				if !ok {
					continue
				}
				certID := certIDFrom(e.Fields["data"])
				if certID == "" {
					continue
				}
				if _, herr := c.Handle(ctx, t, certID, isRenewal); herr != nil {
					c.log.Warn("deployer: auto-deploy failed", "tenant", t, "err", herr)
				}
			}
		}
	}
}

// certIDFrom pulls the certificate id from an lcm event payload
// {certificate_id, spiffe_id, not_after}.
func certIDFrom(data string) string {
	// Minimal, allocation-light extraction to avoid a JSON dependency here.
	const k = `"certificate_id"`
	i := strings.Index(data, k)
	if i < 0 {
		return ""
	}
	rest := data[i+len(k):]
	j := strings.Index(rest, `"`)
	if j < 0 {
		return ""
	}
	rest = rest[j+1:]
	end := strings.Index(rest, `"`)
	if end < 0 {
		return ""
	}
	return rest[:end]
}
