// Package configs is the target-configuration (deployment endpoint) service:
// CRUD, credential validation against the provider, and the provider catalogue.
// Credentials are sealed at rest (envelope encryption) and never returned; reads
// carry a set-marker so the UI knows a credential is present (spec FR-001..005).
package configs

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/provider"
	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/sealed"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

// SetMarker replaces a sealed credential in any read projection.
const SetMarker = "__set__"

// Errors.
var (
	ErrValidation = errors.New("configs: validation failed")
	ErrNotFound   = errors.New("configs: not found")
)

// ValidationError names the offending field.
type ValidationError struct{ Field, Msg string }

func (e *ValidationError) Error() string { return "configs: " + e.Field + ": " + e.Msg }
func invalid(field, msg string) error    { return &ValidationError{Field: field, Msg: msg} }

// Service manages target configurations.
type Service struct {
	st  repo.Store
	env *sealed.Envelope
	az  *authz.Authorizer
	now func() time.Time
}

// New builds the service.
func New(st repo.Store, env *sealed.Envelope, az *authz.Authorizer) *Service {
	return &Service{st: st, env: env, az: az, now: time.Now}
}

// SetClock injects the clock (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// Input is a create/update request. Credentials, when present, are sealed.
type Input struct {
	Name         string
	Description  string
	ProviderType string
	Config       map[string]any
	Credentials  map[string]any // nil/empty on update keeps the stored value
}

// View is a configuration as returned to clients (credentials redacted).
type View struct {
	ID               string         `json:"id"`
	Name             string         `json:"name"`
	Description      string         `json:"description"`
	ProviderType     string         `json:"provider_type"`
	Config           map[string]any `json:"config"`
	HasCredentials   bool           `json:"has_credentials"`
	Status           string         `json:"status"`
	StatusMessage    string         `json:"status_message"`
	LastDeploymentAt *time.Time     `json:"last_deployment_at,omitempty"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
}

func view(c store.TargetConfiguration) View {
	return View{
		ID: c.ID, Name: c.Name, Description: c.Description, ProviderType: c.ProviderType, Config: c.Config,
		HasCredentials: len(c.CredentialsSealed) > 0, Status: c.Status, StatusMessage: c.StatusMessage,
		LastDeploymentAt: c.LastDeploymentAt, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	}
}

// ProviderInfo is one entry of the provider catalogue.
type ProviderInfo = provider.Capabilities

// ListProviders returns the provider catalogue.
func (s *Service) ListProviders() []ProviderInfo { return provider.List() }

// Create seals credentials and stores a configuration.
func (s *Service) Create(ctx context.Context, subj authz.Subjects, in Input) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Manage); err != nil {
		return View{}, err
	}
	if l := len(in.Name); l < 1 || l > 100 {
		return View{}, invalid("name", "name must be 1-100 characters")
	}
	if !provider.Exists(in.ProviderType) {
		return View{}, invalid("provider_type", "unknown provider type")
	}
	id := store.NewID()
	sealedCreds, err := s.seal(id, in.Credentials)
	if err != nil {
		return View{}, err
	}
	row := store.TargetConfiguration{
		ID: id, TenantID: subj.TenantID, Name: in.Name, Description: in.Description, ProviderType: in.ProviderType,
		Config: in.Config, CredentialsSealed: sealedCreds, Status: store.ConfigActive, CreatedBy: subj.ActorID(), UpdatedBy: subj.ActorID(),
	}
	if err := s.st.InsertConfiguration(ctx, row); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return View{}, invalid("name", "a configuration with this name already exists")
		}
		return View{}, err
	}
	out, err := s.st.GetConfiguration(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, err
	}
	return view(out), nil
}

// Get returns a configuration the caller may read.
func (s *Service) Get(ctx context.Context, subj authz.Subjects, id string) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, id, authz.Read); err != nil {
		return View{}, mapAZ(err)
	}
	c, err := s.st.GetConfiguration(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, mapNF(err)
	}
	return view(c), nil
}

// List returns the tenant's configurations (filtered).
func (s *Service) List(ctx context.Context, subj authz.Subjects, providerType, status string) ([]View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Read); err != nil {
		return nil, err
	}
	rows, err := s.st.ListConfigurations(ctx, subj.TenantID, repo.ConfigFilter{ProviderType: providerType, Status: status})
	if err != nil {
		return nil, err
	}
	out := make([]View, 0, len(rows))
	for _, c := range rows {
		out = append(out, view(c))
	}
	return out, nil
}

// Update changes a configuration; empty Credentials keep the stored value.
func (s *Service) Update(ctx context.Context, subj authz.Subjects, id string, in Input) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, id, authz.Manage); err != nil {
		return View{}, mapAZ(err)
	}
	c, err := s.st.GetConfiguration(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, mapNF(err)
	}
	if in.Name != "" {
		c.Name = in.Name
	}
	c.Description = in.Description
	if in.Config != nil {
		c.Config = in.Config
	}
	if len(in.Credentials) > 0 {
		sealedCreds, serr := s.seal(id, in.Credentials)
		if serr != nil {
			return View{}, serr
		}
		c.CredentialsSealed = sealedCreds
	}
	c.UpdatedBy = subj.ActorID()
	if err := s.st.UpdateConfiguration(ctx, c); err != nil {
		return View{}, err
	}
	out, _ := s.st.GetConfiguration(ctx, subj.TenantID, id)
	return view(out), nil
}

// Delete removes a configuration.
func (s *Service) Delete(ctx context.Context, subj authz.Subjects, id string) error {
	if err := s.az.Check(ctx, subj, authz.Configuration, id, authz.Manage); err != nil {
		return mapAZ(err)
	}
	return mapNF(s.st.DeleteConfiguration(ctx, subj.TenantID, id))
}

// Validate tests the given credentials (and optional config) against a provider
// without persisting anything.
func (s *Service) Validate(ctx context.Context, subj authz.Subjects, providerType string, creds, config map[string]any) error {
	if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Manage); err != nil {
		return err
	}
	p, err := provider.Get(providerType)
	if err != nil {
		return invalid("provider_type", "unknown provider type")
	}
	if verr := p.ValidateCredentials(ctx, creds, config); verr != nil {
		return &ValidationError{Field: "credentials", Msg: "credentials rejected by the provider"}
	}
	return nil
}

// OpenCredentials unseals a configuration's credentials for the worker/deploy
// path (never exposed to clients).
func (s *Service) OpenCredentials(c store.TargetConfiguration) (map[string]any, error) {
	if len(c.CredentialsSealed) == 0 {
		return map[string]any{}, nil
	}
	clear, err := s.env.Open(c.CredentialsSealed, sealed.ADConfig(c.ID))
	if err != nil {
		return nil, err
	}
	var m map[string]any
	if err := json.Unmarshal(clear, &m); err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Service) seal(id string, creds map[string]any) ([]byte, error) {
	if len(creds) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(creds)
	if err != nil {
		return nil, invalid("credentials", "credentials are not encodable")
	}
	return s.env.Seal(raw, sealed.ADConfig(id))
}

func mapAZ(err error) error {
	if errors.Is(err, authz.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
func mapNF(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
