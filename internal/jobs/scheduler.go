package jobs

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// Run starts the worker pool + cleanup goroutine until ctx ends. It claims due
// jobs (single-winner lease) and processes CHILD/DIRECT jobs; PARENT jobs only
// aggregate their children and are not executed here.
func (s *Service) Run(ctx context.Context, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	var wg sync.WaitGroup
	tick := time.NewTicker(s.cfg.Interval)
	defer tick.Stop()
	cleanup := time.NewTicker(6 * time.Hour)
	defer cleanup.Stop()
	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-cleanup.C:
			if s.cfg.Cleanup > 0 {
				if n, err := s.st.DeleteJobsOlderThan(ctx, s.now().Add(-s.cfg.Cleanup)); err == nil && n > 0 {
					log.Info("deployer: pruned old jobs", "count", n)
				}
			}
		case <-tick.C:
			due, err := s.st.ClaimDueJobs(ctx, s.now(), s.cfg.Lease, s.cfg.Workers)
			if err != nil {
				log.Warn("deployer: claim failed", "err", err)
				continue
			}
			for _, j := range due {
				if j.JobType() == "parent" {
					// Parents aggregate; a claimed parent just recomputes status.
					s.aggregateParent(ctx, j)
					continue
				}
				wg.Add(1)
				go func(job store.DeploymentJob) {
					defer wg.Done()
					s.process(ctx, log, job)
				}(j)
			}
		}
	}
}

// process runs one child/direct job end to end.
func (s *Service) process(ctx context.Context, log *slog.Logger, j store.DeploymentJob) {
	start := s.now()
	cfgID := strp(j.TargetConfigurationID)
	if cfgID == "" {
		s.failWith(ctx, &j, "job has no configuration", nil)
		return
	}
	conf, err := s.st.GetConfiguration(ctx, j.TenantID, cfgID)
	if err != nil {
		s.failWith(ctx, &j, "configuration unavailable", err)
		return
	}
	p, err := provider.Get(conf.ProviderType)
	if err != nil {
		s.failWith(ctx, &j, "unknown provider", err)
		return
	}
	creds, err := s.cred.OpenCredentials(conf)
	if err != nil {
		// The unseal error names no secret material.
		s.failWith(ctx, &j, "credentials unavailable", err)
		return
	}
	caps := p.Capabilities()
	tgt, hasTarget := s.parentTarget(ctx, j)
	effective := s.effective(j, conf, tgt, hasTarget)
	// A configuration missing a required value (a field left to the targets,
	// a legacy row, a provider upgrade) fails before the certificate is
	// fetched and before the provider contacts its endpoint (FR-031, FR-036).
	if missing := provider.MissingRequired(caps, effective); len(missing) > 0 {
		msg := provider.IncompleteMessage(missing)
		s.history(ctx, j, store.ActionDeploy, store.ResultFailure, msg, 0, nil)
		s.failWith(ctx, &j, msg, nil)
		return
	}
	s.publish(ctx, "deployment.started", j)
	// Providers that deliver by reference never receive the private key
	// (FR-006): the certificate is fetched without it.
	cert, err := s.cert.FetchCertificate(ctx, j.TenantID, j.CertificateID, !caps.DeliversByReference)
	if err != nil {
		if errors.Is(err, provider.ErrKeyUnavailable) {
			// lcm holds no key for this certificate: retrying cannot help.
			s.failWith(ctx, &j, "certificate has no stored private key", err)
			return
		}
		setError(&j, "certificate fetch failed", err.Error(), nil)
		s.retryOrFail(ctx, &j, "certificate fetch failed")
		return
	}
	dctx, cancel := context.WithTimeout(provider.WithJob(ctx, s.jobMeta(j, tgt, hasTarget)), s.jobTimeout())
	defer cancel()
	res, derr := p.Deploy(dctx, &cert, effective, creds, func(pct int, note string) {
		j.Progress = pct
		j.StatusMessage = note
		_ = s.st.UpdateJob(ctx, j)
		s.publish(ctx, "job.updated", j)
	})
	dur := int(s.now().Sub(start).Milliseconds())
	if derr != nil || (res != nil && !res.Success) {
		msg := "deployment failed"
		var details map[string]any
		if res != nil {
			if res.Message != "" {
				// Providers often pass an endpoint's error text through.
				msg = provider.Redact(res.Message, creds)
			}
			details = res.Details
		}
		// The provider's own error is kept with the job (an endpoint echoing
		// a credential is redacted); failure details (per-host states,
		// manual-review reasons) stay visible in the job result (research D27).
		detail := msg
		if derr != nil {
			detail = provider.Redact(derr.Error(), creds)
		}
		setError(&j, msg, detail, details)
		s.history(ctx, j, store.ActionDeploy, store.ResultFailure, historyMessage(msg, detail), dur, details)
		if res != nil && res.Permanent {
			s.fail(ctx, &j, msg)
			return
		}
		s.retryOrFail(ctx, &j, msg)
		return
	}
	// Success.
	j.Progress = 100
	j.Result = resultJSON(map[string]any{"message": res.Message, "details": res.Details})
	s.history(ctx, j, store.ActionDeploy, store.ResultSuccess, res.Message, dur, res.Details)
	s.setStatus(ctx, &j, store.JobCompleted, res.Message)
	now := s.now()
	_ = s.st.SetConfigurationStatus(ctx, j.TenantID, conf.ID, store.ConfigActive, "", &now)
	s.publish(ctx, "deployment.completed", j)
	s.aggregateFromChild(ctx, j)
}

// jobMeta is the job metadata handed to providers (provider.JobFrom).
func (s *Service) jobMeta(j store.DeploymentJob, tgt store.DeploymentTarget, hasTarget bool) provider.JobMeta {
	m := provider.JobMeta{TenantID: j.TenantID, JobID: j.ID, ConfigurationID: strp(j.TargetConfigurationID), Trigger: jobTrigger(j)}
	if hasTarget {
		m.TargetID = tgt.ID
	}
	return m
}

// jobTrigger maps the stored trigger to the provider vocabulary: automatic
// deployments (lifecycle events) are auto_deploy, a job running again after a
// failure is retry, everything else is manual.
func jobTrigger(j store.DeploymentJob) string {
	switch {
	case j.RetryCount > 0:
		return provider.TriggerRetry
	case j.TriggeredBy == store.TriggerEvent || j.TriggeredBy == store.TriggerAutoRenewal:
		return provider.TriggerAutoDeploy
	default:
		return provider.TriggerManual
	}
}

// retryOrFail schedules an exponential-backoff retry, or fails when exhausted.
func (s *Service) retryOrFail(ctx context.Context, j *store.DeploymentJob, msg string) {
	if j.RetryCount < j.MaxRetries {
		j.RetryCount++
		delay := time.Duration(float64(s.retryDelay()) * math.Pow(s.backoff(), float64(j.RetryCount-1)))
		next := s.now().Add(delay)
		j.Status = store.JobRetrying
		j.StatusMessage = msg
		j.NextRetryAt = &next
		j.LeaseUntil = nil
		_ = s.st.UpdateJob(ctx, *j)
		s.publish(ctx, "job.updated", *j)
		return
	}
	s.fail(ctx, j, msg)
}

// failWith records the failure's cause on the job (err's text, or msg when
// there is none) and fails it.
func (s *Service) failWith(ctx context.Context, j *store.DeploymentJob, msg string, err error) {
	detail := msg
	if err != nil {
		detail = err.Error()
	}
	setError(j, msg, detail, nil)
	s.fail(ctx, j, msg)
}

func (s *Service) fail(ctx context.Context, j *store.DeploymentJob, msg string) {
	s.setStatus(ctx, j, store.JobFailed, msg)
	s.publish(ctx, "deployment.failed", *j)
	s.aggregateFromChild(ctx, *j)
}

// historyMessage is the history text of a failure: the summary, plus the
// underlying error when it says more.
func historyMessage(msg, detail string) string {
	if detail == "" || detail == msg {
		return msg
	}
	return msg + ": " + detail
}

func (s *Service) history(ctx context.Context, j store.DeploymentJob, action, result, msg string, dur int, details map[string]any) {
	h := store.DeploymentHistory{
		ID: store.NewID(), TenantID: j.TenantID, JobID: j.ID, Action: action, Result: result,
		Message: msg, DurationMS: dur, CreatedAt: s.now(),
	}
	if len(details) > 0 {
		h.Details = resultJSON(details)
	}
	_ = s.st.InsertHistory(ctx, h)
}

// aggregateFromChild recomputes a child's parent status, if any.
func (s *Service) aggregateFromChild(ctx context.Context, child store.DeploymentJob) {
	if child.ParentJobID == nil {
		return
	}
	if parent, err := s.st.GetJob(ctx, child.TenantID, *child.ParentJobID); err == nil {
		s.aggregateParent(ctx, parent)
	}
}

// aggregateParent sets a parent's status from its children.
func (s *Service) aggregateParent(ctx context.Context, parent store.DeploymentJob) {
	kids, err := s.st.ListChildJobs(ctx, parent.TenantID, parent.ID)
	if err != nil || len(kids) == 0 {
		return
	}
	var done, failed, total int
	for _, k := range kids {
		total++
		switch k.Status {
		case store.JobCompleted:
			done++
		case store.JobFailed, store.JobCancelled:
			failed++
		}
	}
	parent.Progress = done * 100 / total
	switch {
	case done == total:
		s.setStatus(ctx, &parent, store.JobCompleted, "all deployments completed")
	case failed == total:
		s.setStatus(ctx, &parent, store.JobFailed, "all deployments failed")
	case done+failed == total:
		s.setStatus(ctx, &parent, store.JobPartial, "some deployments failed")
	default:
		parent.Status = store.JobProcessing
		_ = s.st.UpdateJob(ctx, parent)
	}
	s.publish(ctx, "job.updated", parent)
}

// effective layers the parent target's per-configuration override (if any)
// over the configuration's base config, so one shared credential can serve
// multiple zones/partitions (spec US4). Direct jobs have no parent, so no
// override applies. Overrides never carry credentials.
func (s *Service) effective(j store.DeploymentJob, conf store.TargetConfiguration, tgt store.DeploymentTarget, hasTarget bool) map[string]any {
	var ov map[string]any
	if hasTarget {
		ov = s.override(tgt, conf)
	}
	return s.sanitize(conf, mergeConfig(conf.Config, ov))
}

// parentTarget returns the deployment target of a child job's parent.
func (s *Service) parentTarget(ctx context.Context, j store.DeploymentJob) (store.DeploymentTarget, bool) {
	if j.ParentJobID == nil {
		return store.DeploymentTarget{}, false
	}
	parent, err := s.st.GetJob(ctx, j.TenantID, *j.ParentJobID)
	if err != nil || parent.DeploymentTargetID == nil {
		return store.DeploymentTarget{}, false
	}
	tgt, err := s.st.GetTarget(ctx, j.TenantID, *parent.DeploymentTargetID)
	if err != nil {
		return store.DeploymentTarget{}, false
	}
	return tgt, true
}

// override returns the target's override for conf, or nil. Defense in
// depth for rows stored before the save-time refusal or restored from a
// backup: only declared, overridable config fields with valid values apply —
// an override never moves the destination, switches TLS verification, sets
// headers or carries credentials (SR-015). Dropped keys are logged once by
// name, never by value.
func (s *Service) override(tgt store.DeploymentTarget, conf store.TargetConfiguration) map[string]any {
	ov := tgt.ConfigOverrides[conf.ID]
	if len(ov) == 0 {
		return ov
	}
	caps, _ := provider.Info(conf.ProviderType)
	kept, dropped := provider.FilterOverride(caps, ov)
	if len(dropped) > 0 {
		if _, seen := s.warned.LoadOrStore("ov:"+tgt.ID+":"+conf.ID, struct{}{}); !seen {
			msg := "deployer: target override carries settings that cannot be overridden; ignored"
			if len(provider.OverrideDestinationKeys(ov)) > 0 {
				msg = "deployer: target override tries to change the destination of a configuration; ignored"
			}
			s.logger().Warn(msg, "tenant_id", conf.TenantID, "target_id", tgt.ID, "configuration_id", conf.ID, "ignored_keys", dropped)
		}
	}
	return kept
}

// sanitize drops the redirect keys (provider.RedirectKeys) that rows stored
// before they were refused may still carry — they must never steer sealed
// credentials — and warns once per configuration about them and about
// credential-like custom headers (names only, never values). Stored data is
// not rewritten.
func (s *Service) sanitize(conf store.TargetConfiguration, effective map[string]any) map[string]any {
	ignored := provider.IgnoredKeys(effective)
	headers := provider.AuthHeaders(effective)
	if len(ignored) > 0 || len(headers) > 0 {
		if _, seen := s.warned.LoadOrStore(conf.ID, struct{}{}); !seen {
			s.logger().Warn("deployer: configuration carries settings that are no longer allowed",
				"tenant_id", conf.TenantID, "configuration_id", conf.ID, "provider_type", conf.ProviderType,
				"ignored_keys", ignored, "credential_header_names", headers)
		}
	}
	if len(ignored) == 0 {
		return effective
	}
	return provider.StripRedirectKeys(effective)
}

func (s *Service) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}

// mergeConfig overlays override onto base (override wins; empty override
// values mean "inherit"). Neither carries creds.
func mergeConfig(base, override map[string]any) map[string]any {
	return provider.MergeOverride(base, override)
}

func (s *Service) jobTimeout() time.Duration {
	if s.cfg.JobTimeout > 0 {
		return s.cfg.JobTimeout
	}
	return 5 * time.Minute
}
func (s *Service) retryDelay() time.Duration {
	if s.cfg.RetryDelay > 0 {
		return s.cfg.RetryDelay
	}
	return time.Minute
}
func (s *Service) backoff() float64 {
	if s.cfg.Backoff >= 1 {
		return s.cfg.Backoff
	}
	return 2.0
}

// Once claims and processes one batch of due jobs synchronously (test hook /
// single-shot). Returns the number processed.
func (s *Service) Once(ctx context.Context, log *slog.Logger) int {
	if log == nil {
		log = slog.Default()
	}
	due, err := s.st.ClaimDueJobs(ctx, s.now(), s.cfg.Lease, s.cfg.Workers)
	if err != nil {
		return 0
	}
	n := 0
	for _, j := range due {
		if j.JobType() == "parent" {
			s.aggregateParent(ctx, j)
			continue
		}
		s.process(ctx, log, j)
		n++
	}
	return n
}
