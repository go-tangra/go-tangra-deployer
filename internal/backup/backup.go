// Package backup exports and imports a tenant's deployer data (targets,
// configurations with their attachments and overrides, and job metadata) for
// backup or tenant migration (FR-028). Provider credentials are included only
// when explicitly requested, and travel as their already-sealed envelope blob
// (never plaintext); because a credential's seal is bound to its configuration
// id, import preserves entity ids so the blob unseals unchanged in the target
// tenant. Duplicate handling is per-name: skip or overwrite.
package backup

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

// SchemaVersion is the export format version.
const SchemaVersion = 1

// Import modes.
const (
	ModeSkip      = "skip"
	ModeOverwrite = "overwrite"
)

// ErrBadSchema is returned when importing an unsupported schema version.
var ErrBadSchema = errors.New("backup: unsupported schema version")

// ConfigExport is a configuration in a backup. CredentialsSealed is present only
// when the export requested credentials; it is the envelope blob, never plaintext.
type ConfigExport struct {
	ID                string         `json:"id"`
	Name              string         `json:"name"`
	Description       string         `json:"description,omitempty"`
	ProviderType      string         `json:"provider_type"`
	Config            map[string]any `json:"config,omitempty"`
	Status            string         `json:"status"`
	CredentialsSealed []byte         `json:"credentials_sealed,omitempty"`
}

// TargetExport is a target plus its attachments and per-config overrides.
type TargetExport struct {
	ID                 string                    `json:"id"`
	Name               string                    `json:"name"`
	Description        string                    `json:"description,omitempty"`
	AutoDeploy         bool                      `json:"auto_deploy"`
	CertificateFilters []store.CertificateFilter `json:"certificate_filters"`
	ConfigOverrides    map[string]map[string]any `json:"config_overrides,omitempty"`
	ConfigurationIDs   []string                  `json:"configuration_ids"`
}

// JobExport is deployment-job metadata (no certificate bytes — jobs never hold any).
type JobExport struct {
	ID                    string `json:"id"`
	DeploymentTargetID    string `json:"deployment_target_id,omitempty"`
	TargetConfigurationID string `json:"target_configuration_id,omitempty"`
	ParentJobID           string `json:"parent_job_id,omitempty"`
	CertificateID         string `json:"certificate_id"`
	Status                string `json:"status"`
	TriggeredBy           string `json:"triggered_by"`
}

// Backup is the export document.
type Backup struct {
	SchemaVersion      int            `json:"schema_version"`
	ExportedAt         time.Time      `json:"exported_at"`
	IncludeCredentials bool           `json:"include_credentials"`
	Configurations     []ConfigExport `json:"configurations"`
	Targets            []TargetExport `json:"targets"`
	Jobs               []JobExport    `json:"jobs"`
}

// ImportResult reports what an import did.
type ImportResult struct {
	ConfigurationsImported int `json:"configurations_imported"`
	ConfigurationsSkipped  int `json:"configurations_skipped"`
	TargetsImported        int `json:"targets_imported"`
	TargetsSkipped         int `json:"targets_skipped"`
	JobsImported           int `json:"jobs_imported"`
}

// Service exports and imports tenant data.
type Service struct {
	st  repo.Store
	now func() time.Time
}

// New builds the service.
func New(st repo.Store) *Service { return &Service{st: st, now: time.Now} }

// SetClock injects the clock (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// Export builds a backup of the caller's tenant. Credentials are included only
// when includeCredentials is true.
func (s *Service) Export(ctx context.Context, subj authz.Subjects, includeCredentials bool) (Backup, error) {
	b := Backup{SchemaVersion: SchemaVersion, ExportedAt: s.now(), IncludeCredentials: includeCredentials}

	configs, err := s.st.ListConfigurations(ctx, subj.TenantID, repo.ConfigFilter{})
	if err != nil {
		return b, err
	}
	for _, c := range configs {
		ce := ConfigExport{ID: c.ID, Name: c.Name, Description: c.Description, ProviderType: c.ProviderType, Config: c.Config, Status: c.Status}
		if includeCredentials && len(c.CredentialsSealed) > 0 {
			ce.CredentialsSealed = c.CredentialsSealed
		}
		b.Configurations = append(b.Configurations, ce)
	}

	targets, err := s.st.ListTargets(ctx, subj.TenantID)
	if err != nil {
		return b, err
	}
	for _, t := range targets {
		ids, err := s.st.ListTargetConfigurationIDs(ctx, subj.TenantID, t.ID)
		if err != nil {
			return b, err
		}
		b.Targets = append(b.Targets, TargetExport{
			ID: t.ID, Name: t.Name, Description: t.Description, AutoDeploy: t.AutoDeploy,
			CertificateFilters: t.CertificateFilters, ConfigOverrides: t.ConfigOverrides, ConfigurationIDs: ids,
		})
	}

	jobs, err := s.st.ListJobs(ctx, subj.TenantID, repo.JobFilter{})
	if err != nil {
		return b, err
	}
	for _, j := range jobs {
		b.Jobs = append(b.Jobs, JobExport{
			ID: j.ID, DeploymentTargetID: strp(j.DeploymentTargetID), TargetConfigurationID: strp(j.TargetConfigurationID),
			ParentJobID: strp(j.ParentJobID), CertificateID: j.CertificateID, Status: j.Status, TriggeredBy: j.TriggeredBy,
		})
	}
	return b, nil
}

// Import recreates the backup's targets and configurations in the caller's
// tenant. mode is skip (default) or overwrite for name collisions. Entity ids
// are preserved so sealed credentials remain valid.
func (s *Service) Import(ctx context.Context, subj authz.Subjects, b Backup, mode string) (ImportResult, error) {
	var res ImportResult
	if b.SchemaVersion != SchemaVersion {
		return res, fmt.Errorf("%w: %d", ErrBadSchema, b.SchemaVersion)
	}
	if mode != ModeOverwrite {
		mode = ModeSkip
	}
	err := s.st.Atomic(ctx, subj.TenantID, func(tx repo.Store) error {
		existingConfigs, err := tx.ListConfigurations(ctx, subj.TenantID, repo.ConfigFilter{})
		if err != nil {
			return err
		}
		configByName := map[string]store.TargetConfiguration{}
		for _, c := range existingConfigs {
			configByName[c.Name] = c
		}
		for _, ce := range b.Configurations {
			if old, ok := configByName[ce.Name]; ok {
				if mode == ModeSkip {
					res.ConfigurationsSkipped++
					continue
				}
				if err := tx.DeleteConfiguration(ctx, subj.TenantID, old.ID); err != nil {
					return err
				}
			}
			c := store.TargetConfiguration{
				ID: ce.ID, TenantID: subj.TenantID, Name: ce.Name, Description: ce.Description,
				ProviderType: ce.ProviderType, Config: ce.Config, CredentialsSealed: ce.CredentialsSealed,
				Status: defaultStr(ce.Status, store.ConfigActive), CreatedBy: subj.ActorID(), UpdatedBy: subj.ActorID(),
			}
			if err := tx.InsertConfiguration(ctx, c); err != nil {
				return err
			}
			res.ConfigurationsImported++
		}

		existingTargets, err := tx.ListTargets(ctx, subj.TenantID)
		if err != nil {
			return err
		}
		targetByName := map[string]store.DeploymentTarget{}
		for _, t := range existingTargets {
			targetByName[t.Name] = t
		}
		for _, te := range b.Targets {
			if old, ok := targetByName[te.Name]; ok {
				if mode == ModeSkip {
					res.TargetsSkipped++
					continue
				}
				if err := tx.DeleteTarget(ctx, subj.TenantID, old.ID); err != nil {
					return err
				}
			}
			t := store.DeploymentTarget{
				ID: te.ID, TenantID: subj.TenantID, Name: te.Name, Description: te.Description,
				AutoDeploy: te.AutoDeploy, CertificateFilters: te.CertificateFilters,
				ConfigOverrides: te.ConfigOverrides, CreatedBy: subj.ActorID(), UpdatedBy: subj.ActorID(),
			}
			if t.CertificateFilters == nil {
				t.CertificateFilters = []store.CertificateFilter{}
			}
			if t.ConfigOverrides == nil {
				t.ConfigOverrides = map[string]map[string]any{}
			}
			if err := tx.InsertTarget(ctx, t); err != nil {
				return err
			}
			if len(te.ConfigurationIDs) > 0 {
				if err := tx.AttachConfigurations(ctx, subj.TenantID, t.ID, te.ConfigurationIDs); err != nil {
					return err
				}
			}
			res.TargetsImported++
		}
		return nil
	})
	return res, err
}

func strp(p *string) string {
	if p == nil {
		return ""
	}
	return *p
}

func defaultStr(s, d string) string {
	if s == "" {
		return d
	}
	return s
}
