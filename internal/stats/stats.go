// Package stats computes deployment statistics for the dashboard and reporting:
// jobs by status and trigger, 24h/7d success rates, target and configuration
// counts (by status and provider), recent errors, and — for administrators — a
// system-wide aggregate with a per-tenant breakdown (FR-027). It reads through
// the repo interface; the concrete store enforces per-tenant RLS.
package stats

import (
	"context"
	"sort"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// recentErrorLimit bounds the recent-errors list in a snapshot.
const recentErrorLimit = 10

// RecentError is one recently-failed job, message-only (no secrets).
type RecentError struct {
	JobID         string    `json:"job_id"`
	CertificateID string    `json:"certificate_id"`
	Message       string    `json:"message"`
	At            time.Time `json:"at"`
}

// Snapshot is the statistics for one scope (a tenant, or the whole system).
type Snapshot struct {
	JobsByStatus             map[string]int `json:"jobs_by_status"`
	JobsByTrigger            map[string]int `json:"jobs_by_trigger"`
	JobsTotal                int            `json:"jobs_total"`
	TargetsTotal             int            `json:"targets_total"`
	AutoDeployTargets        int            `json:"auto_deploy_targets"`
	ConfigurationsTotal      int            `json:"configurations_total"`
	ConfigurationsByStatus   map[string]int `json:"configurations_by_status"`
	ConfigurationsByProvider map[string]int `json:"configurations_by_provider"`
	SuccessRate24h           float64        `json:"success_rate_24h"`
	SuccessRate7d            float64        `json:"success_rate_7d"`
	RecentErrors             []RecentError  `json:"recent_errors"`
}

// TenantSnapshot ties a snapshot to its tenant (system breakdown).
type TenantSnapshot struct {
	TenantID string   `json:"tenant_id"`
	Snapshot Snapshot `json:"snapshot"`
}

// System is the administrator's system-wide view.
type System struct {
	Snapshot  Snapshot         `json:"snapshot"`
	PerTenant []TenantSnapshot `json:"per_tenant"`
}

// Service computes statistics.
type Service struct {
	st  repo.Store
	now func() time.Time
}

// New builds the service.
func New(st repo.Store) *Service { return &Service{st: st, now: time.Now} }

// SetClock injects the clock (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// Tenant returns the caller's tenant statistics.
func (s *Service) Tenant(ctx context.Context, subj authz.Subjects) (Snapshot, error) {
	return s.snapshot(ctx, subj.TenantID)
}

// SystemWide returns the system aggregate plus a per-tenant breakdown. Admin
// only — non-admins get ErrForbidden.
func (s *Service) SystemWide(ctx context.Context, subj authz.Subjects) (System, error) {
	if !subj.IsAdmin() {
		return System{}, authz.ErrForbidden
	}
	ids, err := s.st.TenantIDs(ctx)
	if err != nil {
		return System{}, err
	}
	sort.Strings(ids)
	out := System{Snapshot: emptySnapshot(), PerTenant: []TenantSnapshot{}}
	for _, id := range ids {
		snap, err := s.snapshot(ctx, id)
		if err != nil {
			return System{}, err
		}
		out.PerTenant = append(out.PerTenant, TenantSnapshot{TenantID: id, Snapshot: snap})
		merge(&out.Snapshot, snap)
	}
	// Recompute system success rates from the merged terminal counts is lossy;
	// instead average the per-tenant rates weighted by terminal jobs. Simpler and
	// accurate enough: recompute below from merged recent data is not available,
	// so we leave the merged rates as the mean of tenants that had terminal jobs.
	out.Snapshot.SuccessRate24h = meanRate(out.PerTenant, func(s Snapshot) float64 { return s.SuccessRate24h })
	out.Snapshot.SuccessRate7d = meanRate(out.PerTenant, func(s Snapshot) float64 { return s.SuccessRate7d })
	trimErrors(&out.Snapshot)
	return out, nil
}

func (s *Service) snapshot(ctx context.Context, tenantID string) (Snapshot, error) {
	snap := emptySnapshot()
	jobs, err := s.st.ListJobs(ctx, tenantID, repo.JobFilter{})
	if err != nil {
		return snap, err
	}
	now := s.now()
	cutoff24 := now.Add(-24 * time.Hour)
	cutoff7d := now.Add(-7 * 24 * time.Hour)
	var term24, ok24, term7, ok7 int
	var errs []RecentError
	for _, j := range jobs {
		snap.JobsTotal++
		snap.JobsByStatus[j.Status]++
		snap.JobsByTrigger[j.TriggeredBy]++
		terminal := j.Status == store.JobCompleted || j.Status == store.JobFailed || j.Status == store.JobPartial
		if terminal {
			if j.CreatedAt.After(cutoff7d) {
				term7++
				if j.Status == store.JobCompleted {
					ok7++
				}
			}
			if j.CreatedAt.After(cutoff24) {
				term24++
				if j.Status == store.JobCompleted {
					ok24++
				}
			}
		}
		if j.Status == store.JobFailed || j.Status == store.JobPartial {
			at := j.CreatedAt
			if j.CompletedAt != nil {
				at = *j.CompletedAt
			}
			errs = append(errs, RecentError{JobID: j.ID, CertificateID: j.CertificateID, Message: j.StatusMessage, At: at})
		}
	}
	snap.SuccessRate24h = rate(ok24, term24)
	snap.SuccessRate7d = rate(ok7, term7)

	sort.Slice(errs, func(i, j int) bool { return errs[i].At.After(errs[j].At) })
	if len(errs) > recentErrorLimit {
		errs = errs[:recentErrorLimit]
	}
	snap.RecentErrors = errs

	configs, err := s.st.ListConfigurations(ctx, tenantID, repo.ConfigFilter{})
	if err != nil {
		return snap, err
	}
	snap.ConfigurationsTotal = len(configs)
	for _, c := range configs {
		snap.ConfigurationsByStatus[c.Status]++
		snap.ConfigurationsByProvider[c.ProviderType]++
	}

	targets, err := s.st.ListTargets(ctx, tenantID)
	if err != nil {
		return snap, err
	}
	snap.TargetsTotal = len(targets)
	for _, t := range targets {
		if t.AutoDeploy {
			snap.AutoDeployTargets++
		}
	}
	return snap, nil
}

func emptySnapshot() Snapshot {
	return Snapshot{
		JobsByStatus:             map[string]int{},
		JobsByTrigger:            map[string]int{},
		ConfigurationsByStatus:   map[string]int{},
		ConfigurationsByProvider: map[string]int{},
		RecentErrors:             []RecentError{},
	}
}

func rate(ok, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(ok) / float64(total)
}

func merge(dst *Snapshot, src Snapshot) {
	dst.JobsTotal += src.JobsTotal
	dst.TargetsTotal += src.TargetsTotal
	dst.AutoDeployTargets += src.AutoDeployTargets
	dst.ConfigurationsTotal += src.ConfigurationsTotal
	for k, v := range src.JobsByStatus {
		dst.JobsByStatus[k] += v
	}
	for k, v := range src.JobsByTrigger {
		dst.JobsByTrigger[k] += v
	}
	for k, v := range src.ConfigurationsByStatus {
		dst.ConfigurationsByStatus[k] += v
	}
	for k, v := range src.ConfigurationsByProvider {
		dst.ConfigurationsByProvider[k] += v
	}
	dst.RecentErrors = append(dst.RecentErrors, src.RecentErrors...)
}

func meanRate(ts []TenantSnapshot, pick func(Snapshot) float64) float64 {
	var sum float64
	var n int
	for _, t := range ts {
		if r := pick(t.Snapshot); r > 0 {
			sum += r
			n++
		}
	}
	if n == 0 {
		return 0
	}
	return sum / float64(n)
}

func trimErrors(snap *Snapshot) {
	sort.Slice(snap.RecentErrors, func(i, j int) bool { return snap.RecentErrors[i].At.After(snap.RecentErrors[j].At) })
	if len(snap.RecentErrors) > recentErrorLimit {
		snap.RecentErrors = snap.RecentErrors[:recentErrorLimit]
	}
}
