// Package targets is the deployment-target (group) service: CRUD, certificate
// filter rules, attach/detach of configurations, and per-attachment config
// overrides. Overrides carry provider CONFIG only, never credentials (FR-009).
package targets

import (
	"context"
	"errors"
	"sort"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra/v4/listquery"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// Errors.
var ErrNotFound = errors.New("targets: not found")

// ValidationError names the offending field(s). Field/Msg are the first error
// (sorted by path), Fields all of them (path -> code) and ConfigurationID the
// attached configuration the override errors concern.
type ValidationError struct {
	Field, Msg      string
	Fields          map[string]string
	ConfigurationID string
}

func (e *ValidationError) Error() string { return "targets: " + e.Field + ": " + e.Msg }
func invalid(field, msg string) error {
	return &ValidationError{Field: field, Msg: msg, Fields: map[string]string{field: msg}}
}

// overrideErrors turns descriptor errors for one configuration into a
// ValidationError.
func overrideErrors(cid string, fe provider.FieldErrors) *ValidationError {
	paths := make([]string, 0, len(fe))
	for p := range fe {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return &ValidationError{Field: paths[0], Msg: fe[paths[0]], Fields: map[string]string(fe), ConfigurationID: cid}
}

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
	// ConfigOverrides holds the per-configuration overrides (provider config,
	// never credentials).
	ConfigOverrides map[string]map[string]any `json:"config_overrides"`
	// MissingRequired lists, per attached configuration, the labels of
	// required fields the merged configuration lacks (normally empty; only on
	// single reads).
	MissingRequired map[string][]string `json:"missing_required,omitempty"`
	CreatedAt       time.Time           `json:"created_at,omitzero"`
}

func (s *Service) view(ctx context.Context, tenantID string, t store.DeploymentTarget) View {
	ids, _ := s.st.ListTargetConfigurationIDs(ctx, tenantID, t.ID)
	return toView(t, ids)
}

func toView(t store.DeploymentTarget, ids []string) View {
	if t.CertificateFilters == nil {
		t.CertificateFilters = []store.CertificateFilter{}
	}
	if ids == nil {
		ids = []string{}
	}
	ov := t.ConfigOverrides
	if ov == nil {
		ov = map[string]map[string]any{}
	}
	return View{ID: t.ID, Name: t.Name, Description: t.Description, AutoDeploy: t.AutoDeploy, CertificateFilters: t.CertificateFilters,
		ConfigurationIDs: ids, ConfigOverrides: ov, CreatedAt: t.CreatedAt}
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
	v := s.view(ctx, subj.TenantID, t)
	v.MissingRequired = map[string][]string{}
	for _, cid := range v.ConfigurationIDs {
		c, cerr := s.st.GetConfiguration(ctx, subj.TenantID, cid)
		if cerr != nil {
			continue
		}
		caps, ok := provider.Info(c.ProviderType)
		if !ok {
			continue
		}
		labels := []string{}
		for _, m := range provider.MissingRequired(caps, provider.MergeOverride(c.Config, t.ConfigOverrides[cid])) {
			labels = append(labels, m.Labels...)
		}
		v.MissingRequired[cid] = labels
	}
	return v, nil
}

// List returns all the tenant's targets.
func (s *Service) List(ctx context.Context, subj authz.Subjects) ([]View, error) {
	if err := s.az.Check(ctx, subj, authz.Target, "", authz.Read); err != nil {
		return nil, err
	}
	rows, err := s.st.ListTargets(ctx, subj.TenantID)
	if err != nil {
		return nil, err
	}
	return s.views(ctx, subj.TenantID, rows)
}

// Page returns one page of the tenant's targets (req's store.TargetList
// order) for the HTTP table; the page is the one actually returned.
func (s *Service) Page(ctx context.Context, subj authz.Subjects, req listquery.Request) (listquery.Page[View], error) {
	if err := s.az.Check(ctx, subj, authz.Target, "", authz.Read); err != nil {
		return listquery.Page[View]{}, err
	}
	rows, total, applied, err := s.st.PageTargets(ctx, subj.TenantID, req)
	if err != nil {
		return listquery.Page[View]{}, err
	}
	out, err := s.views(ctx, subj.TenantID, rows)
	if err != nil {
		return listquery.Page[View]{}, err
	}
	return listquery.NewPage(out, total, applied), nil
}

// views builds the views with the attached configuration ids batch-loaded in
// one query (no per-target lookup).
func (s *Service) views(ctx context.Context, tenantID string, rows []store.DeploymentTarget) ([]View, error) {
	ids := make([]string, 0, len(rows))
	for _, t := range rows {
		ids = append(ids, t.ID)
	}
	links, err := s.st.TargetConfigurationIDsFor(ctx, tenantID, ids)
	if err != nil {
		return nil, err
	}
	out := make([]View, 0, len(rows))
	for _, t := range rows {
		out = append(out, toView(t, links[t.ID]))
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

// Attach links configurations to a target with optional per-configuration
// overrides (contracts/deployer-config-ui.md §4a). Every listed configuration
// is validated before anything is written: override keys must be declared,
// overridable config fields (never credentials), values follow their
// descriptors, and the configuration plus override must satisfy every
// required field and one_of_required group. A listed override replaces the
// stored one; a configuration listed without an override keeps (and is
// validated with) its stored override.
func (s *Service) Attach(ctx context.Context, subj authz.Subjects, id string, configIDs []string, overrides map[string]map[string]any) error {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Manage); err != nil {
		return mapNF(err)
	}
	t, err := s.st.GetTarget(ctx, subj.TenantID, id)
	if err != nil {
		return mapNF(err)
	}
	for _, cid := range attachOrder(configIDs, overrides) {
		ov, listed := overrides[cid]
		if !listed {
			ov = t.ConfigOverrides[cid]
		}
		if err := s.checkOverride(ctx, subj.TenantID, cid, ov); err != nil {
			return err
		}
	}
	// Defence in depth: credential-shaped names never reach the unsealed row.
	if err := rejectCredentialKeys(overrides); err != nil {
		return err
	}
	return s.st.Atomic(ctx, subj.TenantID, func(tx repo.Store) error {
		if len(configIDs) > 0 {
			if err := tx.AttachConfigurations(ctx, subj.TenantID, id, configIDs); err != nil {
				return err
			}
		}
		if len(overrides) == 0 {
			return nil
		}
		if t.ConfigOverrides == nil {
			t.ConfigOverrides = map[string]map[string]any{}
		}
		for cid, ov := range overrides {
			if clean := nonEmpty(ov); len(clean) > 0 {
				t.ConfigOverrides[cid] = clean
			} else {
				delete(t.ConfigOverrides, cid)
			}
		}
		t.UpdatedBy = subj.ActorID()
		return tx.UpdateTarget(ctx, t)
	})
}

// attachOrder is the deterministic validation order: the listed
// configurations, then configurations that only carry an override.
func attachOrder(configIDs []string, overrides map[string]map[string]any) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(configIDs)+len(overrides))
	for _, cid := range configIDs {
		if !seen[cid] {
			seen[cid] = true
			out = append(out, cid)
		}
	}
	extra := make([]string, 0, len(overrides))
	for cid := range overrides {
		if !seen[cid] {
			extra = append(extra, cid)
		}
	}
	sort.Strings(extra)
	return append(out, extra...)
}

// checkOverride validates one configuration's override with the provider's
// descriptors and its ConfigValidator on the merged configuration. Values are
// never echoed.
func (s *Service) checkOverride(ctx context.Context, tenantID, cid string, ov map[string]any) error {
	c, err := s.st.GetConfiguration(ctx, tenantID, cid)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			ve := invalid("configuration_ids", "not_found").(*ValidationError)
			ve.ConfigurationID = cid
			return ve
		}
		return err
	}
	caps, ok := provider.Info(c.ProviderType)
	if !ok {
		ve := invalid("configuration_ids", "provider not available").(*ValidationError)
		ve.ConfigurationID = cid
		return ve
	}
	if fe := provider.ValidateOverride(caps, c.Config, ov); len(fe) > 0 {
		return overrideErrors(cid, fe)
	}
	if verr := provider.ValidateConfig(c.ProviderType, provider.MergeOverride(c.Config, ov)); verr != nil {
		var fe *provider.FieldError
		field := "config_overrides"
		if errors.As(verr, &fe) {
			field = provider.PathOverrides + strings.TrimPrefix(fe.Field, provider.PathConfig)
			return overrideErrors(cid, provider.FieldErrors{field: fe.Msg})
		}
		return overrideErrors(cid, provider.FieldErrors{field: "rejected"})
	}
	return nil
}

func nonEmpty(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if !provider.IsEmpty(v) {
			out[k] = v
		}
	}
	return out
}

// Detach unlinks configurations from a target and drops their overrides.
func (s *Service) Detach(ctx context.Context, subj authz.Subjects, id string, configIDs []string) error {
	if err := s.az.Check(ctx, subj, authz.Target, id, authz.Manage); err != nil {
		return mapNF(err)
	}
	t, err := s.st.GetTarget(ctx, subj.TenantID, id)
	if err != nil {
		return mapNF(err)
	}
	return mapNF(s.st.Atomic(ctx, subj.TenantID, func(tx repo.Store) error {
		if err := tx.DetachConfigurations(ctx, subj.TenantID, id, configIDs); err != nil {
			return err
		}
		changed := false
		for _, cid := range configIDs {
			if _, ok := t.ConfigOverrides[cid]; ok {
				delete(t.ConfigOverrides, cid)
				changed = true
			}
		}
		if !changed {
			return nil
		}
		t.UpdatedBy = subj.ActorID()
		return tx.UpdateTarget(ctx, t)
	}))
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
