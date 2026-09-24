// Package targets is the deployment-target (group) service: CRUD, certificate
// filter rules, attach/detach of configurations, and per-attachment config
// overrides. Overrides carry provider CONFIG only, never credentials (FR-009).
package targets

import (
	"context"
	"errors"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// Errors.
var ErrNotFound = errors.New("targets: not found")

// ValidationError names the offending field.
type ValidationError struct{ Field, Msg string }

func (e *ValidationError) Error() string { return "targets: " + e.Field + ": " + e.Msg }
func invalid(field, msg string) error    { return &ValidationError{Field: field, Msg: msg} }

// Service manages deployment targets.
type Service struct {
	st repo.Store
	az *authz.Authorizer
}

// New builds the service.
func New(st repo.Store, az *authz.Authorizer) *Service { return &Service{st: st, az: az} }

// Input is a create/update request.
type Input struct {
	Name        string
	Description string
	AutoDeploy  bool
	Filters     []store.CertificateFilter
}

// View is a target as returned to clients.
type View struct {
	ID                 string                    `json:"id"`
	Name               string                    `json:"name"`
	Description        string                    `json:"description"`
	AutoDeploy         bool                      `json:"auto_deploy"`
	CertificateFilters []store.CertificateFilter `json:"certificate_filters"`
	ConfigurationIDs   []string                  `json:"configuration_ids"`
}

func (s *Service) view(ctx context.Context, tenantID string, t store.DeploymentTarget) View {
	ids, _ := s.st.ListTargetConfigurationIDs(ctx, tenantID, t.ID)
	if t.CertificateFilters == nil {
		t.CertificateFilters = []store.CertificateFilter{}
	}
	if ids == nil {
		ids = []string{}
	}
	return View{ID: t.ID, Name: t.Name, Description: t.Description, AutoDeploy: t.AutoDeploy, CertificateFilters: t.CertificateFilters, ConfigurationIDs: ids}
}

// Create stores a target.
func (s *Service) Create(ctx context.Context, subj authz.Subjects, in Input) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Target, "", authz.Manage); err != nil {
		return View{}, err
	}
	if l := len(in.Name); l < 1 || l > 100 {
		return View{}, invalid("name", "name must be 1-100 characters")
	}
	t := store.DeploymentTarget{
		ID: store.NewID(), TenantID: subj.TenantID, Name: in.Name, Description: in.Description,
		AutoDeploy: in.AutoDeploy, CertificateFilters: in.Filters, ConfigOverrides: map[string]map[string]any{},
		CreatedBy: subj.ActorID(), UpdatedBy: subj.ActorID(),
	}
	if err := s.st.InsertTarget(ctx, t); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return View{}, invalid("name", "a target with this name already exists")
		}
		return View{}, err
	}
	return s.view(ctx, subj.TenantID, t), nil
}

// Get returns a target the caller may read.
func (s *Service) Get(ctx context.Context, subj authz.Subjects, id string) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Read); err != nil {
		return View{}, mapNF(err)
	}
	t, err := s.st.GetTarget(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, mapNF(err)
	}
	return s.view(ctx, subj.TenantID, t), nil
}

// List returns the tenant's targets.
func (s *Service) List(ctx context.Context, subj authz.Subjects) ([]View, error) {
	if err := s.az.Check(ctx, subj, authz.Target, "", authz.Read); err != nil {
		return nil, err
	}
	rows, err := s.st.ListTargets(ctx, subj.TenantID)
	if err != nil {
		return nil, err
	}
	out := make([]View, 0, len(rows))
	for _, t := range rows {
		out = append(out, s.view(ctx, subj.TenantID, t))
	}
	return out, nil
}

// Update changes a target.
func (s *Service) Update(ctx context.Context, subj authz.Subjects, id string, in Input) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Manage); err != nil {
		return View{}, mapNF(err)
	}
	t, err := s.st.GetTarget(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, mapNF(err)
	}
	if in.Name != "" {
		t.Name = in.Name
	}
	t.Description = in.Description
	t.AutoDeploy = in.AutoDeploy
	if in.Filters != nil {
		t.CertificateFilters = in.Filters
	}
	t.UpdatedBy = subj.ActorID()
	if err := s.st.UpdateTarget(ctx, t); err != nil {
		return View{}, err
	}
	return s.view(ctx, subj.TenantID, t), nil
}

// Delete removes a target.
func (s *Service) Delete(ctx context.Context, subj authz.Subjects, id string) error {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Manage); err != nil {
		return mapNF(err)
	}
	return mapNF(s.st.DeleteTarget(ctx, subj.TenantID, id))
}

// Attach links configurations to a target, optionally with per-config overrides
// (provider config only — a credentials-shaped key is rejected).
func (s *Service) Attach(ctx context.Context, subj authz.Subjects, id string, configIDs []string, overrides map[string]map[string]any) error {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Manage); err != nil {
		return mapNF(err)
	}
	if err := rejectCredentialKeys(overrides); err != nil {
		return err
	}
	t, err := s.st.GetTarget(ctx, subj.TenantID, id)
	if err != nil {
		return mapNF(err)
	}
	if err := s.st.AttachConfigurations(ctx, subj.TenantID, id, configIDs); err != nil {
		return err
	}
	if len(overrides) > 0 {
		if t.ConfigOverrides == nil {
			t.ConfigOverrides = map[string]map[string]any{}
		}
		for cid, ov := range overrides {
			t.ConfigOverrides[cid] = ov
		}
		t.UpdatedBy = subj.ActorID()
		if err := s.st.UpdateTarget(ctx, t); err != nil {
			return err
		}
	}
	return nil
}

// Detach unlinks configurations from a target.
func (s *Service) Detach(ctx context.Context, subj authz.Subjects, id string, configIDs []string) error {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Manage); err != nil {
		return mapNF(err)
	}
	return mapNF(s.st.DetachConfigurations(ctx, subj.TenantID, id, configIDs))
}

// rejectCredentialKeys refuses overrides containing credential-shaped keys.
func rejectCredentialKeys(overrides map[string]map[string]any) error {
	bad := []string{"credential", "credentials", "secret", "password", "token", "api_token", "access_key", "secret_access_key", "private"}
	for _, ov := range overrides {
		for k := range ov {
			for _, b := range bad {
				if k == b {
					return invalid("config_overrides", "overrides must not contain credentials")
				}
			}
		}
	}
	return nil
}

func mapNF(err error) error {
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, authz.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
