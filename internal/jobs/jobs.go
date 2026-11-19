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
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/go-tangra/go-tangra/v4/listquery"

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
	log  *slog.Logger
	// warned remembers configuration ids already warned about disallowed
	// stored settings (one WARN per configuration per process).
	warned sync.Map
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

// SetLogger sets the logger for deploy-time warnings (default slog.Default()).
func (s *Service) SetLogger(l *slog.Logger) { s.log = l }

// View is a job as returned to clients.
type View struct {
	ID                    string `json:"id"`
	Type                  string `json:"type"`
	DeploymentTargetID    string `json:"deployment_target_id,omitempty"`
	TargetConfigurationID string `json:"target_configuration_id,omitempty"`
	ParentJobID           string `json:"parent_job_id,omitempty"`
	CertificateID         string `json:"certificate_id"`
	CertificateSerial     string `json:"certificate_serial,omitempty"`
	// CertificateName is the certificate's CN (else first SAN), known once the
	// job fetched it from lcm (auto-deploy jobs know it at creation).
	CertificateName string `json:"certificate_name,omitempty"`
	// TargetName, ConfigurationName and ProviderType name where the job
	// deploys: a parent's target, a child's parent target, and the
	// configuration of a child or direct job. Empty once deleted.
	TargetName        string `json:"target_name,omitempty"`
	ConfigurationName string `json:"configuration_name,omitempty"`
	ProviderType      string `json:"provider_type,omitempty"`
	Status            string `json:"status"`
	StatusMessage     string `json:"status_message,omitempty"`
	// Error is the cause of the last failure (cleared by a success).
	Error string `json:"error,omitempty"`
	// ErrorStage is the step that failed (StageConfiguration, StageCredentials,
	// StageCertificate, StageDeploy, StageChildren) and ErrorSource who
	// reported the error: "deployer", "lcm" or "provider".
	ErrorStage  string     `json:"error_stage,omitempty"`
	ErrorSource string     `json:"error_source,omitempty"`
	Progress    int        `json:"progress"`
	RetryCount  int        `json:"retry_count"`
	MaxRetries  int        `json:"max_retries"`
	TriggeredBy string     `json:"triggered_by"`
	CreatedAt   time.Time  `json:"created_at"`
	CompletedAt *time.Time `json:"completed_at,omitempty"`
}

func view(j store.DeploymentJob) View {
	f := jobFailure(j)
	return View{
		ID: j.ID, Type: j.JobType(), DeploymentTargetID: strp(j.DeploymentTargetID),
		TargetConfigurationID: strp(j.TargetConfigurationID), ParentJobID: strp(j.ParentJobID),
		CertificateID: j.CertificateID, CertificateSerial: j.CertificateSerial, CertificateName: j.CertificateName,
		Status: j.Status, StatusMessage: j.StatusMessage, Error: f.Error, ErrorStage: f.Stage, ErrorSource: stageSource(f.Stage),
		Progress: j.Progress, RetryCount: j.RetryCount, MaxRetries: j.MaxRetries,
		TriggeredBy: j.TriggeredBy, CreatedAt: j.CreatedAt, CompletedAt: j.CompletedAt,
	}
}

// Failure stages: the step of a job that failed, recorded with the error.
const (
	StageConfiguration = "configuration" // loading or completing the configuration (deployer)
	StageCredentials   = "credentials"   // opening the sealed credentials (deployer)
	StageCertificate   = "certificate"   // fetching the certificate (lcm)
	StageDeploy        = "deploy"        // the provider and its endpoint
	StageChildren      = "children"      // a parent: the failures of its children
)

// stageSource is who reported a failure at stage: the deployer itself, lcm,
// or the provider (its endpoint's answer, or the provider's own refusal).
func stageSource(stage string) string {
	switch stage {
	case StageConfiguration, StageCredentials, StageChildren:
		return "deployer"
	case StageCertificate:
		return "lcm"
	case StageDeploy:
		return "provider"
	}
	return ""
}

// MaxErrorLen bounds the stored failure cause (it travels in live events).
const MaxErrorLen = 4096

// setError records a failure on the job's result: the step that failed
// (stage), the summary (message), the underlying cause (error, bounded) and
// the provider's details.
func setError(j *store.DeploymentJob, stage, msg, cause string, details map[string]any) {
	if len(cause) > MaxErrorLen {
		cause = strings.ToValidUTF8(cause[:MaxErrorLen], "") + "…"
	}
	r := map[string]any{"message": msg, "error": cause, "stage": stage}
	if len(details) > 0 {
		r["details"] = details
	}
	j.Result = resultJSON(r)
}

// failure is the recorded cause of a job's last failure.
type failure struct {
	Error string `json:"error"`
	Stage string `json:"stage"`
}

// jobFailure reads the failure cause from the job's result (zero when none).
// Results recorded before stages were kept have no stage.
func jobFailure(j store.DeploymentJob) failure {
	var f failure
	if len(j.Result) == 0 || json.Unmarshal(j.Result, &f) != nil {
		return failure{}
	}
	if f.Error == "" {
		f.Stage = ""
	}
	return f
}

// publish emits a realtime job event on the platform bus: the job's id, its
// parent, status, progress, message and failure cause, so an open list or
// detail drawer updates in place.
func (s *Service) publish(ctx context.Context, typ string, j store.DeploymentJob) {
	if s.pub == nil {
		return
	}
	v := view(j)
	s.pub.Publish(ctx, j.TenantID, typ, map[string]any{
		"job_id": v.ID, "type": v.Type, "parent_job_id": v.ParentJobID, "status": v.Status,
		"status_message": v.StatusMessage, "error": v.Error, "error_stage": v.ErrorStage, "error_source": v.ErrorSource,
		"progress": v.Progress, "certificate_name": v.CertificateName, "certificate_serial": v.CertificateSerial,
		"retry_count": v.RetryCount, "max_retries": v.MaxRetries, "completed_at": v.CompletedAt,
	})
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
	return s.named(ctx, subj.TenantID, []View{view(j)})[0], nil
}

// HistoryEntry is one deployment-history record as returned to clients.
type HistoryEntry struct {
	ID         string    `json:"id,omitempty"`
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
	n := s.newNames(subj.TenantID)
	res := Result{View: view(j), History: []HistoryEntry{}}
	n.fill(ctx, &res.View)
	if len(j.Result) > 0 {
		res.Result = json.RawMessage(j.Result)
	}
	rows, err := s.st.ListHistory(ctx, subj.TenantID, id)
	if err != nil {
		return Result{}, err
	}
	for _, h := range rows {
		res.History = append(res.History, historyEntry(h))
	}
	if j.JobType() == store.JobTypeParent {
		kids, err := s.st.ListChildJobs(ctx, subj.TenantID, id)
		if err != nil {
			return Result{}, err
		}
		res.Children = make([]View, 0, len(kids))
		for _, k := range kids {
			v := view(k)
			n.fill(ctx, &v)
			res.Children = append(res.Children, v)
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
	tgt, hasTarget := s.parentTarget(ctx, j)
	effective := s.effective(j, conf, tgt, hasTarget)
	if missing := provider.MissingRequired(caps, effective); len(missing) > 0 {
		msg := provider.IncompleteMessage(missing)
		s.history(ctx, j, action, store.ResultFailure, msg, 0, nil)
		return ActionResult{Action: action, Success: false, Message: msg}, nil
	}
	creds, err := s.cred.OpenCredentials(conf)
	if err != nil {
		return ActionResult{}, err
	}
	// Only a rollback needs the key, and never for a by-reference provider.
	includeKey := action == store.ActionRollback && !caps.DeliversByReference
	cert, err := s.cert.FetchCertificate(ctx, j.TenantID, j.CertificateID, includeKey)
	if err != nil {
		return ActionResult{}, err
	}
	actx, cancel := context.WithTimeout(provider.WithJob(ctx, s.jobMeta(j, tgt, hasTarget)), s.jobTimeout())
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
	var details map[string]any
	if res != nil {
		details = res.Details
	}
	s.history(ctx, j, action, result, msg, dur, details)
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
	return s.named(ctx, subj.TenantID, views(rows)), nil
}

// Page returns one page of the tenant's jobs (filtered in SQL, req's
// store.JobList order) for the jobs table and dashboard.
func (s *Service) Page(ctx context.Context, subj authz.Subjects, f repo.JobFilter, req listquery.Request) (listquery.Page[View], error) {
	if err := s.az.Check(ctx, subj, authz.Job, "", authz.Read); err != nil {
		return listquery.Page[View]{}, err
	}
	rows, total, applied, err := s.st.PageJobs(ctx, subj.TenantID, f, req)
	if err != nil {
		return listquery.Page[View]{}, err
	}
	return listquery.NewPage(s.named(ctx, subj.TenantID, views(rows)), total, applied), nil
}

// PageChildren returns one page of a job's child jobs (store.ChildJobList).
func (s *Service) PageChildren(ctx context.Context, subj authz.Subjects, id string, req listquery.Request) (listquery.Page[View], error) {
	if err := s.az.Check(ctx, subj, authz.Job, id, authz.Read); err != nil {
		return listquery.Page[View]{}, nf(err)
	}
	if _, err := s.st.GetJob(ctx, subj.TenantID, id); err != nil {
		return listquery.Page[View]{}, nf(err)
	}
	rows, total, applied, err := s.st.PageChildJobs(ctx, subj.TenantID, id, req)
	if err != nil {
		return listquery.Page[View]{}, err
	}
	return listquery.NewPage(s.named(ctx, subj.TenantID, views(rows)), total, applied), nil
}

// PageHistory returns one page of a job's deployment history
// (store.HistoryList).
func (s *Service) PageHistory(ctx context.Context, subj authz.Subjects, id string, req listquery.Request) (listquery.Page[HistoryEntry], error) {
	if err := s.az.Check(ctx, subj, authz.Job, id, authz.Read); err != nil {
		return listquery.Page[HistoryEntry]{}, nf(err)
	}
	if _, err := s.st.GetJob(ctx, subj.TenantID, id); err != nil {
		return listquery.Page[HistoryEntry]{}, nf(err)
	}
	rows, total, applied, err := s.st.PageHistory(ctx, subj.TenantID, id, req)
	if err != nil {
		return listquery.Page[HistoryEntry]{}, err
	}
	out := make([]HistoryEntry, 0, len(rows))
	for _, h := range rows {
		out = append(out, historyEntry(h))
	}
	return listquery.NewPage(out, total, applied), nil
}

func views(rows []store.DeploymentJob) []View {
	out := make([]View, 0, len(rows))
	for _, j := range rows {
		out = append(out, view(j))
	}
	return out
}

// names resolves the target, configuration and provider names of job views
// (one lookup per distinct id), so a job says where it deploys without the
// caller looking ids up. A deleted target or configuration leaves its name
// empty; the id stays.
type names struct {
	s       *Service
	tenant  string
	configs map[string]store.TargetConfiguration
	targets map[string]string
	parents map[string]string // parent job id -> its target id
}

func (s *Service) newNames(tenantID string) *names {
	return &names{s: s, tenant: tenantID, configs: map[string]store.TargetConfiguration{}, targets: map[string]string{}, parents: map[string]string{}}
}

func (n *names) config(ctx context.Context, id string) store.TargetConfiguration {
	if id == "" {
		return store.TargetConfiguration{}
	}
	c, ok := n.configs[id]
	if !ok {
		c, _ = n.s.st.GetConfiguration(ctx, n.tenant, id)
		n.configs[id] = c
	}
	return c
}

func (n *names) target(ctx context.Context, id string) string {
	if id == "" {
		return ""
	}
	name, ok := n.targets[id]
	if !ok {
		if t, err := n.s.st.GetTarget(ctx, n.tenant, id); err == nil {
			name = t.Name
		}
		n.targets[id] = name
	}
	return name
}

func (n *names) parentTarget(ctx context.Context, parentID string) string {
	tid, ok := n.parents[parentID]
	if !ok {
		if p, err := n.s.st.GetJob(ctx, n.tenant, parentID); err == nil {
			tid = strp(p.DeploymentTargetID)
		}
		n.parents[parentID] = tid
	}
	return tid
}

func (n *names) fill(ctx context.Context, v *View) {
	tid := v.DeploymentTargetID
	if tid == "" && v.ParentJobID != "" {
		tid = n.parentTarget(ctx, v.ParentJobID)
	}
	v.TargetName = n.target(ctx, tid)
	c := n.config(ctx, v.TargetConfigurationID)
	v.ConfigurationName, v.ProviderType = c.Name, c.ProviderType
}

// named resolves the names of vs in place and returns it.
func (s *Service) named(ctx context.Context, tenantID string, vs []View) []View {
	n := s.newNames(tenantID)
	for i := range vs {
		n.fill(ctx, &vs[i])
	}
	return vs
}

func historyEntry(h store.DeploymentHistory) HistoryEntry {
	return HistoryEntry{ID: h.ID, Action: h.Action, Result: h.Result, Message: h.Message, DurationMS: h.DurationMS, CreatedAt: h.CreatedAt}
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
	s.publish(ctx, "job.updated", j)
	if cascade {
		kids, _ := s.st.ListChildJobs(ctx, subj.TenantID, id)
		for _, k := range kids {
			if k.Status == store.JobPending || k.Status == store.JobProcessing || k.Status == store.JobRetrying {
				s.setStatus(ctx, &k, store.JobCancelled, "parent cancelled")
				s.publish(ctx, "job.updated", k)
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
	s.publish(ctx, "job.updated", j)
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
