package jobs

import (
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/provider"
	"github.com/go-freya/freya/services/deployer/internal/store"
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
		s.fail(ctx, &j, "job has no configuration")
		return
	}
	conf, err := s.st.GetConfiguration(ctx, j.TenantID, cfgID)
	if err != nil {
		s.fail(ctx, &j, "configuration unavailable")
		return
	}
	p, err := provider.Get(conf.ProviderType)
	if err != nil {
		s.fail(ctx, &j, "unknown provider")
		return
	}
	creds, err := s.cred.OpenCredentials(conf)
	if err != nil {
		s.fail(ctx, &j, "credentials unavailable")
		return
	}
	s.publish(ctx, j.TenantID, "deployment.started", j)
	cert, err := s.cert.FetchCertificate(ctx, j.TenantID, j.CertificateID, true)
	if err != nil {
		s.retryOrFail(ctx, &j, "certificate fetch failed")
		return
	}
	effective := s.effectiveConfig(ctx, j, conf)
	dctx, cancel := context.WithTimeout(ctx, s.jobTimeout())
	defer cancel()
	res, derr := p.Deploy(dctx, &cert, effective, creds, func(pct int, note string) {
		j.Progress = pct
		j.StatusMessage = note
		_ = s.st.UpdateJob(ctx, j)
	})
	dur := int(s.now().Sub(start).Milliseconds())
	if derr != nil || (res != nil && !res.Success) {
		msg := "deployment failed"
		if res != nil && res.Message != "" {
			msg = res.Message
		}
		s.history(ctx, j, store.ActionDeploy, store.ResultFailure, msg, dur)
		s.retryOrFail(ctx, &j, msg)
		return
	}
	// Success.
	j.Progress = 100
	j.Result = resultJSON(map[string]any{"message": res.Message, "details": res.Details})
	s.history(ctx, j, store.ActionDeploy, store.ResultSuccess, res.Message, dur)
	s.setStatus(ctx, &j, store.JobCompleted, res.Message)
	now := s.now()
	_ = s.st.SetConfigurationStatus(ctx, j.TenantID, conf.ID, store.ConfigActive, "", &now)
	s.publish(ctx, j.TenantID, "deployment.completed", j)
	s.aggregateFromChild(ctx, j)
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
		return
	}
	s.fail(ctx, j, msg)
}

func (s *Service) fail(ctx context.Context, j *store.DeploymentJob, msg string) {
	s.setStatus(ctx, j, store.JobFailed, msg)
	s.publish(ctx, j.TenantID, "deployment.failed", *j)
	s.aggregateFromChild(ctx, *j)
}

func (s *Service) history(ctx context.Context, j store.DeploymentJob, action, result, msg string, dur int) {
	_ = s.st.InsertHistory(ctx, store.DeploymentHistory{
		ID: store.NewID(), TenantID: j.TenantID, JobID: j.ID, Action: action, Result: result,
		Message: msg, DurationMS: dur, CreatedAt: s.now(),
	})
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
}

func (s *Service) publish(ctx context.Context, tenantID, typ string, j store.DeploymentJob) {
	if s.pub != nil {
		s.pub.Publish(ctx, tenantID, typ, map[string]any{"job_id": j.ID, "status": j.Status, "progress": j.Progress})
	}
}

// effectiveConfig layers the parent target's per-configuration override (if
// any) over the configuration's base config, so one shared credential can
// serve multiple zones/partitions (spec US4). Direct jobs have no parent, so
// no override applies. Overrides never carry credentials.
func (s *Service) effectiveConfig(ctx context.Context, j store.DeploymentJob, conf store.TargetConfiguration) map[string]any {
	if j.ParentJobID == nil {
		return mergeConfig(conf.Config, nil)
	}
	parent, err := s.st.GetJob(ctx, j.TenantID, *j.ParentJobID)
	if err != nil || parent.DeploymentTargetID == nil {
		return mergeConfig(conf.Config, nil)
	}
	tgt, err := s.st.GetTarget(ctx, j.TenantID, *parent.DeploymentTargetID)
	if err != nil {
		return mergeConfig(conf.Config, nil)
	}
	return mergeConfig(conf.Config, tgt.ConfigOverrides[conf.ID])
}

// mergeConfig overlays override onto base (override wins). Neither carries creds.
func mergeConfig(base, override map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range override {
		out[k] = v
	}
	return out
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
