// Package configs is the target-configuration (deployment endpoint) service:
// CRUD, credential validation against the provider, and the provider catalogue.
// Credentials are sealed at rest (envelope encryption) and never returned; reads
// carry a set-marker so the UI knows a credential is present (spec FR-001..005).
//
// Feature 033 (contracts/deployer-config-ui.md): every create, update and
// validate runs the provider's field descriptors through
// provider.ValidateInput (and the provider's own ConfigValidator); errors name
// each field ("config.<key>" / "credentials.<key>") with a code that never
// carries the submitted value. Credentials are merged per field on update
// (blank keeps, a value replaces only that field, ClearCredentials removes
// optional fields) and are write-only: reads return only the names of stored
// credentials and, to managers, the values of non-secret credential fields.
package configs

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra/v4/listquery"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// SetMarker replaces a sealed credential in any read projection.
const SetMarker = "__set__"

// validateTimeout bounds a provider's ValidateCredentials/Preview call.
const validateTimeout = 20 * time.Second

// maxTargetRefs bounds the targets listed by a required_by_targets refusal.
const maxTargetRefs = 20

// Errors.
var (
	ErrValidation = errors.New("configs: validation failed")
	ErrNotFound   = errors.New("configs: not found")
	// ErrCredentialsRejected: the provider refused the entered values. The
	// provider's text is logged (values redacted), never returned.
	ErrCredentialsRejected = errors.New("configs: credentials rejected")
)

// TargetRef names a deployment target in a refusal (names are not secret).
type TargetRef struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// ValidationError names the offending field(s). Field/Msg are the first
// error (sorted by path); Fields holds all of them (path -> code); Targets
// lists the targets a required_by_targets refusal concerns.
type ValidationError struct {
	Field, Msg string
	Fields     map[string]string
	Targets    []TargetRef
}

func (e *ValidationError) Error() string { return "configs: " + e.Field + ": " + e.Msg }

func invalid(field, msg string) error {
	return &ValidationError{Field: field, Msg: msg, Fields: map[string]string{field: msg}}
}

// fieldErrors turns descriptor errors into a ValidationError (nil when none).
func fieldErrors(fe provider.FieldErrors) *ValidationError {
	if len(fe) == 0 {
		return nil
	}
	paths := make([]string, 0, len(fe))
	for p := range fe {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	return &ValidationError{Field: paths[0], Msg: fe[paths[0]], Fields: map[string]string(fe)}
}

// Auditor records audit events (audit.Writer satisfies it; nil disables).
type Auditor interface {
	Record(ctx context.Context, e audit.Event) error
}

// Service manages target configurations.
type Service struct {
	st  repo.Store
	env *sealed.Envelope
	az  *authz.Authorizer
	now func() time.Time
	log *slog.Logger
	aud Auditor
}

// New builds the service.
func New(st repo.Store, env *sealed.Envelope, az *authz.Authorizer) *Service {
	return &Service{st: st, env: env, az: az, now: time.Now}
}

// SetClock injects the clock (tests).
func (s *Service) SetClock(now func() time.Time) { s.now = now }

// SetLogger sets the logger for redacted provider rejections.
func (s *Service) SetLogger(l *slog.Logger) { s.log = l }

// SetAuditor sets the audit sink (configuration_validated).
func (s *Service) SetAuditor(a Auditor) { s.aud = a }

func (s *Service) logger() *slog.Logger {
	if s.log != nil {
		return s.log
	}
	return slog.Default()
}

// Input is a create/update request. Credentials, when present, are sealed;
// on update they are merged per field (empty values keep the stored value)
// and ClearCredentials removes optional credential fields.
type Input struct {
	Name             string
	Description      string
	ProviderType     string
	Config           map[string]any
	Credentials      map[string]any
	ClearCredentials []string
}

// View is a configuration as returned to clients (credentials redacted).
type View struct {
	ID             string         `json:"id"`
	Name           string         `json:"name"`
	Description    string         `json:"description"`
	ProviderType   string         `json:"provider_type"`
	Config         map[string]any `json:"config"`
	HasCredentials bool           `json:"has_credentials"`
	// TargetSupplied lists required configuration keys left empty for the
	// deployment targets to supply (computed; [] when complete).
	TargetSupplied []string `json:"target_supplied"`
	// CredentialsSet names the stored credential fields; CredentialsPublic
	// holds the values of non-secret credential fields. Both only for callers
	// allowed to manage the configuration (single reads); secrets never.
	CredentialsSet    []string       `json:"credentials_set,omitempty"`
	CredentialsPublic map[string]any `json:"credentials_public,omitempty"`
	// IgnoredConfigKeys names stored settings that are no longer honoured
	// (provider.RedirectKeys): they are ignored at deploy time. Names only.
	IgnoredConfigKeys []string   `json:"ignored_config_keys,omitempty"`
	Status            string     `json:"status"`
	StatusMessage     string     `json:"status_message"`
	LastDeploymentAt  *time.Time `json:"last_deployment_at,omitempty"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func view(c store.TargetConfiguration) View {
	ts := []string{}
	if caps, ok := provider.Info(c.ProviderType); ok {
		if got := provider.TargetSupplied(caps, c.Config); len(got) > 0 {
			ts = got
		}
	}
	return View{
		ID: c.ID, Name: c.Name, Description: c.Description, ProviderType: c.ProviderType, Config: c.Config,
		HasCredentials: len(c.CredentialsSealed) > 0, TargetSupplied: ts, IgnoredConfigKeys: provider.IgnoredKeys(c.Config),
		Status: c.Status, StatusMessage: c.StatusMessage,
		LastDeploymentAt: c.LastDeploymentAt, CreatedAt: c.CreatedAt, UpdatedAt: c.UpdatedAt,
	}
}

// ProviderInfo is one entry of the provider catalogue.
type ProviderInfo = provider.Capabilities

// ListProviders returns the provider catalogue (descriptors included).
func (s *Service) ListProviders() []ProviderInfo { return provider.List() }

// Providers returns the catalogue to a caller holding configurations:read
// (SR-013).
func (s *Service) Providers(ctx context.Context, subj authz.Subjects) ([]ProviderInfo, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Read); err != nil {
		return nil, err
	}
	return provider.List(), nil
}

// Create seals credentials and stores a configuration.
func (s *Service) Create(ctx context.Context, subj authz.Subjects, in Input) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Manage); err != nil {
		return View{}, err
	}
	if l := len(in.Name); l < 1 || l > 100 {
		return View{}, invalid("name", "name must be 1-100 characters")
	}
	caps, ok := provider.Info(in.ProviderType)
	if !ok {
		return View{}, invalid("provider_type", "unknown provider type")
	}
	creds := nonEmpty(in.Credentials)
	if err := checkInput(in.ProviderType, caps, in.Config, creds); err != nil {
		return View{}, err
	}
	id := store.NewID()
	sealedCreds, err := s.seal(id, creds)
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

// Get returns a configuration the caller may read. Managers also get the
// names of the stored credentials and the non-secret credential values (the
// edit drawer pre-fill, FR-028).
func (s *Service) Get(ctx context.Context, subj authz.Subjects, id string) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, id, authz.Read); err != nil {
		return View{}, mapAZ(err)
	}
	c, err := s.st.GetConfiguration(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, mapNF(err)
	}
	v := view(c)
	if len(c.CredentialsSealed) > 0 && s.az.Check(ctx, subj, authz.Configuration, id, authz.Manage) == nil {
		if creds, oerr := s.OpenCredentials(c); oerr == nil {
			v.CredentialsSet, v.CredentialsPublic = credentialProjection(c.ProviderType, creds)
		}
	}
	return v, nil
}

// credentialProjection returns the stored credential names and the values of
// the declared non-secret credential fields. Undeclared keys are named but
// never valued (they might be secrets).
func credentialProjection(providerType string, creds map[string]any) ([]string, map[string]any) {
	names := make([]string, 0, len(creds))
	for k := range creds {
		names = append(names, k)
	}
	sort.Strings(names)
	public := map[string]any{}
	if caps, ok := provider.Info(providerType); ok {
		for _, f := range caps.CredentialFields {
			if v, set := creds[f.Key]; set && !f.Secret {
				public[f.Key] = v
			}
		}
	}
	if len(public) == 0 {
		public = nil
	}
	return names, public
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

// Page returns one page of the tenant's configurations (filtered, in req's
// store.ConfigList order) for the HTTP table; the page is the one actually
// returned (clamped to the last page). List stays the unpaged gRPC path.
func (s *Service) Page(ctx context.Context, subj authz.Subjects, providerType, status string, req listquery.Request) (listquery.Page[View], error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Read); err != nil {
		return listquery.Page[View]{}, err
	}
	rows, total, applied, err := s.st.PageConfigurations(ctx, subj.TenantID, repo.ConfigFilter{ProviderType: providerType, Status: status}, req)
	if err != nil {
		return listquery.Page[View]{}, err
	}
	out := make([]View, 0, len(rows))
	for _, c := range rows {
		out = append(out, view(c))
	}
	return listquery.NewPage(out, total, applied), nil
}

// Update changes a configuration. The provider type cannot change; config
// replaces the stored config when given; credentials merge per field.
func (s *Service) Update(ctx context.Context, subj authz.Subjects, id string, in Input) (View, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, id, authz.Manage); err != nil {
		return View{}, mapAZ(err)
	}
	c, err := s.st.GetConfiguration(ctx, subj.TenantID, id)
	if err != nil {
		return View{}, mapNF(err)
	}
	if in.ProviderType != "" && in.ProviderType != c.ProviderType {
		return View{}, invalid("provider_type", "the provider of a configuration cannot be changed")
	}
	caps, ok := provider.Info(c.ProviderType)
	if !ok {
		return View{}, invalid("provider_type", "provider not available")
	}
	if in.Name != "" {
		c.Name = in.Name
	}
	c.Description = in.Description
	oldConfig := c.Config
	newConfig := c.Config
	if in.Config != nil {
		newConfig = in.Config
	}
	stored, err := s.OpenCredentials(c)
	if err != nil {
		return View{}, err
	}
	reentered := nonEmpty(in.Credentials)
	merged, cerr := mergeCredentials(caps, stored, reentered, in.ClearCredentials)
	if cerr != nil {
		return View{}, cerr
	}
	if err := checkInput(c.ProviderType, caps, newConfig, merged); err != nil {
		return View{}, err
	}
	// Moving the destination of a configuration that holds sealed
	// credentials would send them (and the private key) to the new host: the
	// credentials must be re-entered in the same request.
	if len(c.CredentialsSealed) > 0 && len(reentered) == 0 {
		if key, changed := provider.DestinationChange(c.Config, newConfig); changed {
			return View{}, invalid("config."+key, "credentials must be re-entered when the destination changes")
		}
	}
	if err := s.checkAttachedTargets(ctx, subj.TenantID, c, caps, oldConfig, newConfig); err != nil {
		return View{}, err
	}
	c.Config = newConfig
	if len(reentered) > 0 || len(in.ClearCredentials) > 0 {
		sealedCreds, serr := s.seal(id, merged)
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

// mergeCredentials overlays the re-entered values on the stored credentials
// and removes the cleared fields. Clearing a required field is refused;
// clearing an undeclared (legacy) key is allowed.
func mergeCredentials(caps provider.Capabilities, stored, reentered map[string]any, clear []string) (map[string]any, error) {
	out := make(map[string]any, len(stored)+len(reentered))
	for k, v := range stored {
		out[k] = v
	}
	for k, v := range reentered {
		out[k] = v
	}
	for _, k := range clear {
		for _, f := range caps.CredentialFields {
			if f.Key == k && f.Required {
				return nil, invalid(provider.PathCredentials+k, provider.CodeRequired)
			}
		}
		delete(out, k)
	}
	return out, nil
}

// checkAttachedTargets refuses an update that would leave a target the
// configuration is attached to without a required value it had before
// (contracts/deployer-config-ui.md §3, required_by_targets).
func (s *Service) checkAttachedTargets(ctx context.Context, tenantID string, c store.TargetConfiguration, caps provider.Capabilities, oldConfig, newConfig map[string]any) error {
	tgts, err := s.st.ListTargets(ctx, tenantID)
	if err != nil || len(tgts) == 0 {
		return err
	}
	ids := make([]string, 0, len(tgts))
	for _, t := range tgts {
		ids = append(ids, t.ID)
	}
	links, err := s.st.TargetConfigurationIDsFor(ctx, tenantID, ids)
	if err != nil {
		return err
	}
	fields := provider.FieldErrors{}
	var refs []TargetRef
	for _, t := range tgts {
		if !contains(links[t.ID], c.ID) {
			continue
		}
		ov := t.ConfigOverrides[c.ID]
		before := map[string]bool{}
		for _, m := range provider.MissingRequired(caps, provider.MergeOverride(oldConfig, ov)) {
			for _, k := range m.Keys {
				before[k] = true
			}
		}
		hit := false
		for _, m := range provider.MissingRequired(caps, provider.MergeOverride(newConfig, ov)) {
			for _, k := range m.Keys {
				if !before[k] {
					fields[provider.PathConfig+k] = provider.CodeRequiredByTargets
					hit = true
				}
			}
		}
		if hit && len(refs) < maxTargetRefs {
			refs = append(refs, TargetRef{ID: t.ID, Name: t.Name})
		}
	}
	if len(fields) == 0 {
		return nil
	}
	ve := fieldErrors(fields)
	ve.Targets = refs
	return ve
}

func contains(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// Delete removes a configuration.
func (s *Service) Delete(ctx context.Context, subj authz.Subjects, id string) error {
	if err := s.az.Check(ctx, subj, authz.Configuration, id, authz.Manage); err != nil {
		return mapAZ(err)
	}
	return mapNF(s.st.DeleteConfiguration(ctx, subj.TenantID, id))
}

// ValidateRequest is a validate/test-connection request. With ConfigurationID
// the stored credentials of that configuration are merged under the entered
// ones, so blank secrets on edit keep working.
type ValidateRequest struct {
	ProviderType    string
	ConfigurationID string
	Config          map[string]any
	Credentials     map[string]any
}

// Validation outcomes ("checked").
const (
	CheckedProbe   = "probe"   // the provider contacted its endpoint
	CheckedStatic  = "static"  // input checks only
	CheckedPartial = "partial" // target-supplied fields without defaults: probe skipped
)

// ValidateResult is a successful validation.
type ValidateResult struct {
	Valid    bool           `json:"valid"`
	Checked  string         `json:"checked"`
	Deferred []string       `json:"deferred"`
	Details  map[string]any `json:"details,omitempty"`
}

// Validate checks the entered values against the descriptors and then the
// provider (ValidateCredentials, or Preview for providers that show what the
// configuration resolves to) without persisting anything.
func (s *Service) Validate(ctx context.Context, subj authz.Subjects, req ValidateRequest) (ValidateResult, error) {
	creds := nonEmpty(req.Credentials)
	if req.ConfigurationID != "" {
		if err := s.az.Check(ctx, subj, authz.Configuration, req.ConfigurationID, authz.Manage); err != nil {
			return ValidateResult{}, mapAZ(err)
		}
		c, err := s.st.GetConfiguration(ctx, subj.TenantID, req.ConfigurationID)
		if err != nil {
			return ValidateResult{}, mapNF(err)
		}
		if req.ProviderType == "" {
			req.ProviderType = c.ProviderType
		}
		if req.ProviderType != c.ProviderType {
			return ValidateResult{}, invalid("provider_type", "the provider of a configuration cannot be changed")
		}
		stored, err := s.OpenCredentials(c)
		if err != nil {
			return ValidateResult{}, err
		}
		creds, _ = mergeCredentials(provider.Capabilities{}, stored, creds, nil)
	} else if err := s.az.Check(ctx, subj, authz.Configuration, "", authz.Manage); err != nil {
		return ValidateResult{}, err
	}
	p, err := provider.Get(req.ProviderType)
	if err != nil {
		return ValidateResult{}, invalid("provider_type", "unknown provider type")
	}
	caps := p.Capabilities()
	if err := checkInput(req.ProviderType, caps, req.Config, creds); err != nil {
		s.auditValidated(ctx, subj, req, "invalid", "")
		return ValidateResult{}, err
	}
	config, deferred := provider.FillDefaults(caps, req.Config, provider.TargetSupplied(caps, req.Config))
	res := ValidateResult{Valid: true, Checked: CheckedStatic, Deferred: []string{}}
	if caps.TestConnection {
		res.Checked = CheckedProbe
	}
	if len(deferred) > 0 {
		// Without the target-supplied values the probe cannot run.
		res.Checked, res.Deferred = CheckedPartial, deferred
		s.auditValidated(ctx, subj, req, "valid", res.Checked)
		return res, nil
	}
	vctx, cancel := context.WithTimeout(provider.WithJob(ctx, provider.JobMeta{TenantID: subj.TenantID, ConfigurationID: req.ConfigurationID}), validateTimeout)
	defer cancel()
	if pv, ok := p.(provider.Previewer); ok {
		res.Details, err = pv.Preview(vctx, config)
	} else {
		err = p.ValidateCredentials(vctx, creds, config)
	}
	if err != nil {
		var fe *provider.FieldError
		if errors.As(err, &fe) {
			// A named setting failed on the endpoint (not_found_on_endpoint,
			// no_hosts_matched): reported on the field, without a value.
			s.auditValidated(ctx, subj, req, "invalid", res.Checked)
			return ValidateResult{}, invalid(fe.Field, fe.Msg)
		}
		s.logger().Warn("deployer: provider rejected the configuration",
			"tenant_id", subj.TenantID, "provider_type", req.ProviderType, "configuration_id", req.ConfigurationID,
			"err", redact(err.Error(), req.Config, creds))
		s.auditValidated(ctx, subj, req, "rejected", res.Checked)
		return ValidateResult{}, ErrCredentialsRejected
	}
	s.auditValidated(ctx, subj, req, "valid", res.Checked)
	return res, nil
}

func (s *Service) auditValidated(ctx context.Context, subj authz.Subjects, req ValidateRequest, result, checked string) {
	if s.aud == nil {
		return
	}
	outcome := audit.OutcomeOK
	if result != "valid" {
		outcome = audit.OutcomeRefused
	}
	_ = s.aud.Record(ctx, audit.Event{
		TenantID: subj.TenantID, EventType: audit.ConfigurationValidated, ActorKind: actorKind(subj), ActorID: subj.ActorID(),
		SubjectKind: audit.SubjectConfiguration, SubjectID: req.ConfigurationID, Outcome: outcome, Reason: result,
		Details: map[string]any{"provider_type": req.ProviderType, "checked": checked},
	})
}

func actorKind(subj authz.Subjects) string {
	if subj.ActorKind == audit.ActorService || subj.ActorKind == audit.ActorSystem {
		return subj.ActorKind
	}
	return audit.ActorUser
}

// redact replaces every submitted string value (config and credentials) in a
// provider error text, so a provider that echoes its input cannot leak it
// into the log (SR-012).
func redact(msg string, maps ...map[string]any) string {
	var values []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			if len(x) >= 3 {
				values = append(values, x)
			}
		case []any:
			for _, it := range x {
				walk(it)
			}
		case map[string]any:
			for _, it := range x {
				walk(it)
			}
		}
	}
	for _, m := range maps {
		walk(m)
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	for _, v := range values {
		msg = strings.ReplaceAll(msg, v, "[redacted]")
	}
	return msg
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

// checkInput applies the provider's field descriptors (configuration mode:
// target-supplied fields may be empty) and then the provider's own
// ConfigValidator.
func checkInput(providerType string, caps provider.Capabilities, config, creds map[string]any) error {
	if fe, _ := provider.ValidateInput(caps, config, creds, provider.ModeConfiguration); len(fe) > 0 {
		return fieldErrors(fe)
	}
	err := provider.ValidateConfig(providerType, config)
	if err == nil {
		return nil
	}
	var fe *provider.FieldError
	if errors.As(err, &fe) {
		return invalid(fe.Field, fe.Msg)
	}
	var fes provider.FieldErrors
	if errors.As(err, &fes) {
		return fieldErrors(fes)
	}
	return invalid("config", "configuration rejected by the provider")
}

// nonEmpty returns m without empty values ("leave blank to keep"); never nil.
func nonEmpty(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if !provider.IsEmpty(v) {
			out[k] = v
		}
	}
	return out
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
