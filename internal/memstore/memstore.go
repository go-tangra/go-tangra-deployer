// Package memstore is an in-memory repo.Store for tests and single-instance dev.
// It filters by tenant like the RLS store and supports error injection.
package memstore

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// ErrNotFound is returned when a row is absent.
var ErrNotFound = errors.New("memstore: not found")

// Mem is an in-memory store.
type Mem struct {
	mu       sync.Mutex
	configs  map[string]store.TargetConfiguration
	targets  map[string]store.DeploymentTarget
	links    map[string]map[string]bool // targetID -> set(configID)
	jobs     map[string]store.DeploymentJob
	history  []store.DeploymentHistory
	audit    []store.AuditRow
	Now      func() time.Time
	FailNext error // injected error returned by the next call, then cleared
}

// New returns an empty in-memory store.
func New() *Mem {
	return &Mem{
		configs: map[string]store.TargetConfiguration{}, targets: map[string]store.DeploymentTarget{},
		links: map[string]map[string]bool{}, jobs: map[string]store.DeploymentJob{}, Now: time.Now,
	}
}

func (m *Mem) fail() error {
	if m.FailNext != nil {
		e := m.FailNext
		m.FailNext = nil
		return e
	}
	return nil
}

// Atomic runs fn against the same store (no isolation needed for the fake).
func (m *Mem) Atomic(ctx context.Context, tenantID string, fn func(tx repo.Store) error) error {
	return fn(m)
}

func (m *Mem) Close() {}

// --- configurations ---

func (m *Mem) InsertConfiguration(_ context.Context, c store.TargetConfiguration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	for _, x := range m.configs {
		if x.TenantID == c.TenantID && x.Name == c.Name {
			return errors.New("memstore: duplicate configuration name")
		}
	}
	m.configs[c.ID] = c
	return nil
}

func (m *Mem) GetConfiguration(_ context.Context, tenantID, id string) (store.TargetConfiguration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.configs[id]
	if !ok || c.TenantID != tenantID {
		return store.TargetConfiguration{}, ErrNotFound
	}
	return c, nil
}

func (m *Mem) ListConfigurations(_ context.Context, tenantID string, f repo.ConfigFilter) ([]store.TargetConfiguration, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.TargetConfiguration
	for _, c := range m.configs {
		if c.TenantID != tenantID {
			continue
		}
		if f.ProviderType != "" && c.ProviderType != f.ProviderType {
			continue
		}
		if f.Status != "" && c.Status != f.Status {
			continue
		}
		out = append(out, c)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (m *Mem) UpdateConfiguration(_ context.Context, c store.TargetConfiguration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if x, ok := m.configs[c.ID]; !ok || x.TenantID != c.TenantID {
		return ErrNotFound
	}
	m.configs[c.ID] = c
	return nil
}

func (m *Mem) DeleteConfiguration(_ context.Context, tenantID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.configs[id]; !ok || c.TenantID != tenantID {
		return ErrNotFound
	}
	delete(m.configs, id)
	for _, set := range m.links {
		delete(set, id)
	}
	return nil
}

func (m *Mem) SetConfigurationStatus(_ context.Context, tenantID, id, status, message string, lastDeploy *time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	c, ok := m.configs[id]
	if !ok || c.TenantID != tenantID {
		return ErrNotFound
	}
	c.Status, c.StatusMessage = status, message
	if lastDeploy != nil {
		c.LastDeploymentAt = lastDeploy
	}
	m.configs[id] = c
	return nil
}

// --- targets ---

func (m *Mem) InsertTarget(_ context.Context, t store.DeploymentTarget) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	for _, x := range m.targets {
		if x.TenantID == t.TenantID && x.Name == t.Name {
			return errors.New("memstore: duplicate target name")
		}
	}
	m.targets[t.ID] = t
	return nil
}

func (m *Mem) GetTarget(_ context.Context, tenantID, id string) (store.DeploymentTarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	t, ok := m.targets[id]
	if !ok || t.TenantID != tenantID {
		return store.DeploymentTarget{}, ErrNotFound
	}
	return t, nil
}

func (m *Mem) ListTargets(_ context.Context, tenantID string) ([]store.DeploymentTarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.DeploymentTarget
	for _, t := range m.targets {
		if t.TenantID == tenantID {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.After(out[j].CreatedAt) })
	return out, nil
}

func (m *Mem) ListAutoDeployTargets(_ context.Context, tenantID string) ([]store.DeploymentTarget, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.DeploymentTarget
	for _, t := range m.targets {
		if t.TenantID == tenantID && t.AutoDeploy {
			out = append(out, t)
		}
	}
	return out, nil
}

func (m *Mem) UpdateTarget(_ context.Context, t store.DeploymentTarget) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if x, ok := m.targets[t.ID]; !ok || x.TenantID != t.TenantID {
		return ErrNotFound
	}
	m.targets[t.ID] = t
	return nil
}

func (m *Mem) DeleteTarget(_ context.Context, tenantID, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.targets[id]; !ok || t.TenantID != tenantID {
		return ErrNotFound
	}
	delete(m.targets, id)
	delete(m.links, id)
	return nil
}

func (m *Mem) AttachConfigurations(_ context.Context, tenantID, targetID string, configIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.targets[targetID]; !ok || t.TenantID != tenantID {
		return ErrNotFound
	}
	if m.links[targetID] == nil {
		m.links[targetID] = map[string]bool{}
	}
	for _, id := range configIDs {
		m.links[targetID][id] = true
	}
	return nil
}

func (m *Mem) DetachConfigurations(_ context.Context, tenantID, targetID string, configIDs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.targets[targetID]; !ok || t.TenantID != tenantID {
		return ErrNotFound
	}
	for _, id := range configIDs {
		delete(m.links[targetID], id)
	}
	return nil
}

func (m *Mem) ListTargetConfigurationIDs(_ context.Context, tenantID, targetID string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.targets[targetID]; !ok || t.TenantID != tenantID {
		return nil, ErrNotFound
	}
	var out []string
	for id := range m.links[targetID] {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}

// --- jobs ---

func (m *Mem) InsertJob(_ context.Context, j store.DeploymentJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.fail(); err != nil {
		return err
	}
	m.jobs[j.ID] = j
	return nil
}

func (m *Mem) GetJob(_ context.Context, tenantID, id string) (store.DeploymentJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	j, ok := m.jobs[id]
	if !ok || j.TenantID != tenantID {
		return store.DeploymentJob{}, ErrNotFound
	}
	return j, nil
}

func (m *Mem) ListJobs(_ context.Context, tenantID string, f repo.JobFilter) ([]store.DeploymentJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.DeploymentJob
	for _, j := range m.jobs {
		if j.TenantID != tenantID {
			continue
		}
		if f.Status != "" && j.Status != f.Status {
			continue
		}
		if f.TriggeredBy != "" && j.TriggeredBy != f.TriggeredBy {
			continue
		}
		if f.CertificateID != "" && j.CertificateID != f.CertificateID {
			continue
		}
		if f.JobType != "" && j.JobType() != f.JobType {
			continue
		}
		if f.ParentJobID != "" && (j.ParentJobID == nil || *j.ParentJobID != f.ParentJobID) {
			continue
		}
		out = append(out, j)
	}
	sort.Slice(out, func(i, j2 int) bool { return out[i].CreatedAt.After(out[j2].CreatedAt) })
	if f.Limit > 0 && len(out) > f.Limit {
		out = out[:f.Limit]
	}
	return out, nil
}

func (m *Mem) ListChildJobs(_ context.Context, tenantID, parentID string) ([]store.DeploymentJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.DeploymentJob
	for _, j := range m.jobs {
		if j.TenantID == tenantID && j.ParentJobID != nil && *j.ParentJobID == parentID {
			out = append(out, j)
		}
	}
	return out, nil
}

func (m *Mem) UpdateJob(_ context.Context, j store.DeploymentJob) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if x, ok := m.jobs[j.ID]; !ok || x.TenantID != j.TenantID {
		return ErrNotFound
	}
	m.jobs[j.ID] = j
	return nil
}

func (m *Mem) DeleteJobsOlderThan(_ context.Context, cutoff time.Time) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for id, j := range m.jobs {
		if j.CreatedAt.Before(cutoff) {
			delete(m.jobs, id)
			n++
		}
	}
	return n, nil
}

// ClaimDueJobs leases due pending/retrying jobs (single-winner: sets a lease).
func (m *Mem) ClaimDueJobs(_ context.Context, now time.Time, lease time.Duration, limit int) ([]store.DeploymentJob, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.DeploymentJob
	for id, j := range m.jobs {
		if j.Status != store.JobPending && j.Status != store.JobRetrying {
			continue
		}
		if j.NextRetryAt != nil && j.NextRetryAt.After(now) {
			continue
		}
		if j.LeaseUntil != nil && j.LeaseUntil.After(now) {
			continue
		}
		until := now.Add(lease)
		j.Status = store.JobProcessing
		j.LeaseUntil = &until
		m.jobs[id] = j
		out = append(out, j)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

// --- history & audit ---

func (m *Mem) InsertHistory(_ context.Context, h store.DeploymentHistory) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.history = append(m.history, h)
	return nil
}

func (m *Mem) ListHistory(_ context.Context, tenantID, jobID string) ([]store.DeploymentHistory, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.DeploymentHistory
	for _, h := range m.history {
		if h.TenantID == tenantID && h.JobID == jobID {
			out = append(out, h)
		}
	}
	return out, nil
}

func (m *Mem) InsertAuditRows(_ context.Context, rows []store.AuditRow) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.audit = append(m.audit, rows...)
	return nil
}

// Compile-time check.
var _ repo.Store = (*Mem)(nil)

// Exists reports whether a resource exists in the tenant.
func (m *Mem) Exists(_ context.Context, tenantID, resourceType, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch resourceType {
	case "deployer_target":
		t, ok := m.targets[id]
		return ok && t.TenantID == tenantID, nil
	case "deployer_configuration":
		c, ok := m.configs[id]
		return ok && c.TenantID == tenantID, nil
	case "deployer_job":
		j, ok := m.jobs[id]
		return ok && j.TenantID == tenantID, nil
	}
	return false, nil
}

// TenantIDs returns every tenant with any config/target/job (system scope).
func (m *Mem) TenantIDs(_ context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	set := map[string]bool{}
	for _, c := range m.configs {
		set[c.TenantID] = true
	}
	for _, t := range m.targets {
		set[t.TenantID] = true
	}
	for _, j := range m.jobs {
		set[j.TenantID] = true
	}
	out := make([]string, 0, len(set))
	for id := range set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out, nil
}
