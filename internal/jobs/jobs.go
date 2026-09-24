// Package jobs is the deployment-job service and the distributed worker pool.
// It creates jobs, lists/gets/cancels/retries them, and — in the worker —
// claims due jobs (one winner each), loads the configuration, opens its sealed
// credentials, fetches the certificate from lcm, calls the provider, records a
// history entry, and retries with exponential backoff on failure.
package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// ErrNotFound is returned when a job is absent.
var ErrNotFound = errors.New("jobs: not found")

// ErrUnsupported is returned when a job/provider does not support the action
// (e.g. verifying a parent job, or rolling back a provider without rollback).
var ErrUnsupported = errors.New("jobs: action not supported")

// CertFetcher fetches a certificate bundle from lcm (lcmclient satisfies it).
type CertFetcher interface {
	FetchCertificate(ctx context.Context, tenantID, certOrJobID string, includeKey bool) (provider.CertificateData, error)
}

// CredOpener opens a configuration's sealed credentials (configs.Service does).
type CredOpener interface {
	OpenCredentials(c store.TargetConfiguration) (map[string]any, error)
}

// Publisher emits realtime job/deployment events (nil disables).
type Publisher interface {
	Publish(ctx context.Context, tenantID, eventType string, payload any)
}

// Config bounds the worker pool.
type Config struct {
	Workers    int
	Interval   time.Duration
	Lease      time.Duration
	JobTimeout time.Duration
	MaxRetries int
	RetryDelay time.Duration
	Backoff    float64
	Cleanup    time.Duration
}

// Service manages jobs and runs the worker pool.
type Service struct {
	st   repo.Store
	az   *authz.Authorizer
	cert CertFetcher
	cred CredOpener
	pub  Publisher
	cfg  Config
	now  func() time.Time
}

// New builds the service.
func New(st repo.Store, az *authz.Authorizer, cert CertFetcher, cred CredOpener, pub Publisher, cfg Config) *Service {
	if cfg.Workers <= 0 {
		cfg.Workers = 5
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}
	return &Service{st: st, az: az, cert: cert, cred: cred, pub: pub, cfg: cfg, now: time.Now}
}

// SetClock injects the clock (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// View is a job as returned to clients.
type View struct {
	ID                    string     `json:"id"`
	Type                  string     `json:"type"`
	DeploymentTargetID    string     `json:"deployment_target_id,omitempty"`
	TargetConfigurationID string     `json:"target_configuration_id,omitempty"`
	ParentJobID           string     `json:"parent_job_id,omitempty"`
	CertificateID         string     `json:"certificate_id"`
	CertificateSerial     string     `json:"certificate_serial,omitempty"`
	Status                string     `json:"status"`
	StatusMessage         string     `json:"status_message,omitempty"`
	Progress              int        `json:"progress"`
	RetryCount            int        `json:"retry_count"`
	MaxRetries            int        `json:"max_retries"`
	TriggeredBy           string     `json:"triggered_by"`
	CreatedAt             time.Time  `json:"created_at"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
}

func view(j store.DeploymentJob) View {
	return View{
		ID: j.ID, Type: j.JobType(), DeploymentTargetID: strp(j.DeploymentTargetID),
		TargetConfigurationID: strp(j.TargetConfigurationID), ParentJobID: strp(j.ParentJobID),
		CertificateID: j.CertificateID, CertificateSerial: j.CertificateSerial, Status: j.Status,
		StatusMessage: j.StatusMessage, Progress: j.Progress, RetryCount: j.RetryCount, MaxRetries: j.MaxRetries,
		TriggeredBy: j.TriggeredBy, CreatedAt: j.CreatedAt, CompletedAt: j.CompletedAt,
	}
}

func strp(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

// Get returns a job the caller may read.
func (s *Service) Get(ctx context.Context, subj authz.Subjects, id string) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Job, id, authz.Read); err != nil {
		return View{}, nf(err)
	}
	j, err := s.st.GetJob(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, nf(err)
	}
	return view(j), nil
}

// HistoryEntry is one deployment-history record as returned to clients.
type HistoryEntry struct {
	Action     string    `json:"action"`
	Result     string    `json:"result"`
	Message    string    `json:"message,omitempty"`
	DurationMS int       `json:"duration_ms"`
	CreatedAt  time.Time `json:"created_at"`
}

// Result is a job plus its per-action history and (for parents) its children.
type Result struct {
	View
	Result   json.RawMessage `json:"result,omitempty"`
	History  []HistoryEntry  `json:"history"`
	Children []View          `json:"children,omitempty"`
}

// GetResult returns a job with its history and, for a parent job, its children
// so a caller can see the full outcome of a deployment (spec US3).
func (s *Service) GetResult(ctx context.Context, subj authz.Subjects, id string) (Result, error) {
	if err := s.az.Check(ctx, subj, authz.Job, id, authz.Read); err != nil {
		return Result{}, nf(err)
	}
	j, err := s.st.GetJob(ctx, subj.TenantID, id)
	if err != nil {
		return Result{}, nf(err)
	}
	res := Result{View: view(j), History: []HistoryEntry{}}
	if len(j.Result) > 0 {
		res.Result = json.RawMessage(j.Result)
	}
	rows, err := s.st.ListHistory(ctx, subj.TenantID, id)
	if err != nil {
		return Result{}, err
	}
	for _, h := range rows {
		res.History = append(res.History, HistoryEntry{
			Action: h.Action, Result: h.Result, Message: h.Message,
			DurationMS: h.DurationMS, CreatedAt: h.CreatedAt,
		})
	}
	if j.JobType() == store.JobTypeParent {
		kids, err := s.st.ListChildJobs(ctx, subj.TenantID, id)
		if err != nil {
			return Result{}, err
		}
		res.Children = make([]View, 0, len(kids))
		for _, k := range kids {
			res.Children = append(res.Children, view(k))
		}
	}
	return res, nil
}

// ActionResult is the client-facing outcome of a verify/rollback.
type ActionResult struct {
	Action  string `json:"action"`
	Success bool   `json:"success"`
	Message string `json:"message,omitempty"`
}

// Verify re-checks that the certificate is correctly deployed for the job's
// configuration, applying any target config override, and records a history
// entry. Only providers advertising verify support accept it (spec US3).
func (s *Service) Verify(ctx context.Context, subj authz.Subjects, jobID string) (ActionResult, error) {
	return s.runAction(ctx, subj, jobID, store.ActionVerify)
}

// Rollback restores the previous certificate on the job's configuration where
// the provider supports it, and records a history entry (spec US3).
func (s *Service) Rollback(ctx context.Context, subj authz.Subjects, jobID string) (ActionResult, error) {
	return s.runAction(ctx, subj, jobID, store.ActionRollback)
}

func (s *Service) runAction(ctx context.Context, subj authz.Subjects, jobID, action string) (ActionResult, error) {
	if err := s.az.Check(ctx, subj, authz.Job, jobID, authz.Deploy); err != nil {
		return ActionResult{}, nf(err)
	}
	j, err := s.st.GetJob(ctx, subj.TenantID, jobID)
	if err != nil {
		return ActionResult{}, nf(err)
	}
	cfgID := strp(j.TargetConfigurationID)
	if cfgID == "" { // parent jobs have no provider to call
		return ActionResult{}, ErrUnsupported
	}
	conf, err := s.st.GetConfiguration(ctx, subj.TenantID, cfgID)
	if err != nil {
		return ActionResult{}, nf(err)
	}
	p, err := provider.Get(conf.ProviderType)
	if err != nil {
		return ActionResult{}, ErrUnsupported
	}
	caps := p.Capabilities()
	if (action == store.ActionVerify && !caps.SupportsVerify) ||
		(action == store.ActionRollback && !caps.SupportsRollback) {
		return ActionResult{}, ErrUnsupported
	}
	creds, err := s.cred.OpenCredentials(conf)
	if err != nil {
		return ActionResult{}, err
	}
	cert, err := s.cert.FetchCertificate(ctx, j.TenantID, j.CertificateID, action == store.ActionRollback)
	if err != nil {
		return ActionResult{}, err
	}
	effective := s.effectiveConfig(ctx, j, conf)
	actx, cancel := context.WithTimeout(ctx, s.jobTimeout())
	defer cancel()
	start := s.now()
	var res *provider.Result
	if action == store.ActionVerify {
		res, err = p.Verify(actx, &cert, effective, creds)
	} else {
		res, err = p.Rollback(actx, &cert, effective, creds)
	}
	dur := int(s.now().Sub(start).Milliseconds())
	success := err == nil && res != nil && res.Success
	msg := ""
	if res != nil {
		msg = res.Message
	} else if err != nil {
		msg = err.Error()
	}
	result := store.ResultFailure
	if success {
		result = store.ResultSuccess
	}
	s.history(ctx, j, action, result, msg, dur)
	return ActionResult{Action: action, Success: success, Message: msg}, nil
}

// List returns the tenant's jobs (filtered).
func (s *Service) List(ctx context.Context, subj authz.Subjects, f repo.JobFilter) ([]View, error) {
	if err := s.az.Check(ctx, subj, authz.Job, "", authz.Read); err != nil {
		return nil, err
	}
	rows, err := s.st.ListJobs(ctx, subj.TenantID, f)
	if err != nil {
		return nil, err
	}
	out := make([]View, 0, len(rows))
	for _, j := range rows {
		out = append(out, view(j))
	}
	return out, nil
}

// Create stores a job (used by the deploy and event paths).
func (s *Service) Create(ctx context.Context, j store.DeploymentJob) error {
	return s.st.InsertJob(ctx, j)
}

// Cancel fails a queued/processing job (optionally cascading to children).
func (s *Service) Cancel(ctx context.Context, subj authz.Subjects, id string, cascade bool) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Job, id, authz.Manage); err != nil {
		return View{}, nf(err)
	}
	j, err := s.st.GetJob(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, nf(err)
	}
	if j.Status == store.JobCompleted || j.Status == store.JobFailed || j.Status == store.JobCancelled {
		return view(j), nil
	}
	s.setStatus(ctx, &j, store.JobCancelled, "cancelled")
	if cascade {
		kids, _ := s.st.ListChildJobs(ctx, subj.TenantID, id)
		for _, k := range kids {
			if k.Status == store.JobPending || k.Status == store.JobProcessing || k.Status == store.JobRetrying {
				s.setStatus(ctx, &k, store.JobCancelled, "parent cancelled")
			}
		}
	}
	return view(j), nil
}

// Retry re-queues a failed job (if retries remain, or force).
func (s *Service) Retry(ctx context.Context, subj authz.Subjects, id string, force bool) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Job, id, authz.Manage); err != nil {
		return View{}, nf(err)
	}
	j, err := s.st.GetJob(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, nf(err)
	}
	if j.Status != store.JobFailed && j.Status != store.JobPartial {
		return view(j), nil
	}
	if j.RetryCount >= j.MaxRetries && !force {
		return view(j), nil
	}
	j.Status = store.JobPending
	j.NextRetryAt = nil
	j.LeaseUntil = nil
	if force {
		j.RetryCount = 0
	}
	_ = s.st.UpdateJob(ctx, j)
	return view(j), nil
}

func (s *Service) setStatus(ctx context.Context, j *store.DeploymentJob, status, msg string) {
	j.Status = status
	j.StatusMessage = msg
	if status == store.JobCompleted || status == store.JobFailed || status == store.JobCancelled || status == store.JobPartial {
		now := s.now()
		j.CompletedAt = &now
	}
	_ = s.st.UpdateJob(ctx, *j)
}

func nf(err error) error {
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, authz.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

// resultJSON marshals a small result blob.
func resultJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
