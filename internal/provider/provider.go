// Package provider defines the deployment-provider abstraction and a thread-safe
// registry. A provider is a pluggable backend that installs an issued
// certificate onto an infrastructure endpoint (a cloud cert store, a load
// balancer, a firewall, a DNS/WAF edge, or a generic webhook) and, where the
// backend supports it, verifies and rolls back the installation. Providers
// self-register in init() and are selected per target-configuration by their
// stable type string. The registry is write-once at init and read-only after,
// the single documented global-state exception (plan.md, Constitution VII).
package provider

import (
	"context"
	"errors"
	"net/http"
	"sort"
	"sync"
	"time"
)

// Errors returned by the registry and providers.
var (
	ErrUnknownProvider = errors.New("provider: unknown provider type")
	ErrDuplicate       = errors.New("provider: duplicate provider type")
	ErrUnsupported     = errors.New("provider: operation not supported by this provider")
	ErrCredentials     = errors.New("provider: credentials rejected")
	// ErrKeyUnavailable is returned by the certificate fetcher when lcm holds
	// no private key for a certificate (issued from a CSR, or the key was
	// already handed out). Retrying cannot help: the job fails at once.
	ErrKeyUnavailable = errors.New("provider: certificate has no stored private key")
)

// CertificateData is the material a provider installs. Fetched from lcm at deploy
// time; never persisted by the deployer.
type CertificateData struct {
	ID               string
	SerialNumber     string
	CommonName       string
	SANs             []string
	CertificatePEM   string
	PrivateKeyPEM    string
	CertificateChain string
	ExpiresAt        time.Time
}

// Result is the outcome of a Deploy/Verify/Rollback. Message is client-safe.
// Permanent marks a failure that a retry cannot fix (a missing appliance
// profile, a refused request, "no hosts matched"): the job fails without
// automatic retries and keeps Details in its result (research D27).
type Result struct {
	Success   bool
	Message   string
	Details   map[string]any
	Permanent bool
}

// ProgressFn reports 0-100 progress with a short, non-secret note.
type ProgressFn func(percent int, note string)

// Field describes one config or credential input a provider accepts
// (contracts/deployer-config-ui.md §2). The descriptors are the single source
// of truth for the configuration drawer and for save-time validation
// (ValidateInput, ValidateOverride). Optional keys are omitted when unset.
type Field struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Type     string `json:"type,omitempty"` // Type* constants; "" = string
	Secret   bool   `json:"secret"`
	Required bool   `json:"required"`
	// Overridable fields may be supplied or changed by a deployment target
	// override (stored unsealed): only non-secret config fields that do not
	// decide where credentials are sent (research D25, SR-015).
	Overridable bool     `json:"overridable,omitempty"`
	Default     any      `json:"default,omitempty"`
	Options     []Option `json:"options,omitempty"`
	Help        string   `json:"help,omitempty"`
	Placeholder string   `json:"placeholder,omitempty"`
	Group       string   `json:"group,omitempty"` // Group* constants
	Min         *int     `json:"min,omitempty"`
	Max         *int     `json:"max,omitempty"`
	MaxLength   int      `json:"max_length,omitempty"`
	Pattern     string   `json:"pattern,omitempty"` // anchored RE2
	MaxItems    int      `json:"max_items,omitempty"`
}

// Option is one choice of an enum field.
type Option struct {
	Value string `json:"value"`
	Label string `json:"label"`
}

// Capabilities declares what a provider supports and needs; drives UI + validation.
type Capabilities struct {
	Type             string `json:"type"`
	DisplayName      string `json:"display_name"`
	Description      string `json:"description,omitempty"`
	SupportsVerify   bool   `json:"supports_verify"`
	SupportsRollback bool   `json:"supports_rollback"`
	// DeliversByReference providers never receive a private key: the job
	// scheduler fetches the certificate without it and the provider passes
	// only references on (feature 033, inventory-agent).
	DeliversByReference bool `json:"delivers_by_reference"`
	// TestConnection is true when ValidateCredentials contacts the endpoint
	// ("Test connection"), false when it only checks the input.
	TestConnection   bool    `json:"test_connection,omitempty"`
	SchemaVersion    int     `json:"schema_version,omitempty"`
	ConfigFields     []Field `json:"config_fields"`
	CredentialFields []Field `json:"credential_fields"`
	// OneOfRequired lists config key groups of which at least one key must be
	// non-empty.
	OneOfRequired [][]string `json:"one_of_required,omitempty"`
}

// Job triggers passed to providers in JobMeta.Trigger.
const (
	TriggerManual     = "manual"
	TriggerAutoDeploy = "auto_deploy"
	TriggerRetry      = "retry"
)

// JobMeta identifies the deployment job a provider call belongs to. Set by the
// job scheduler around Deploy/Verify/Rollback (and with TenantID only around a
// configuration validation). Providers that do not need it ignore it.
type JobMeta struct {
	TenantID        string
	JobID           string
	ConfigurationID string
	TargetID        string // the parent job's deployment target ("" for direct jobs)
	Trigger         string // TriggerManual | TriggerAutoDeploy | TriggerRetry
}

type jobKey struct{}

// WithJob returns ctx carrying m.
func WithJob(ctx context.Context, m JobMeta) context.Context {
	return context.WithValue(ctx, jobKey{}, m)
}

// JobFrom returns the job metadata carried by ctx, if any.
func JobFrom(ctx context.Context) (JobMeta, bool) {
	m, ok := ctx.Value(jobKey{}).(JobMeta)
	return m, ok
}

// Previewer is implemented by providers whose validation can show what a
// configuration currently resolves to (inventory-agent: the matched hosts).
// The configuration validate endpoint returns the details.
type Previewer interface {
	Preview(ctx context.Context, config map[string]any) (map[string]any, error)
}

// Provider installs a certificate onto an endpoint. Implementations MUST honour
// ctx, report progress, never log/return credential or key material, and return
// a client-safe error.
type Provider interface {
	Deploy(ctx context.Context, cert *CertificateData, config map[string]any, creds map[string]any, progress ProgressFn) (*Result, error)
	Verify(ctx context.Context, cert *CertificateData, config map[string]any, creds map[string]any) (*Result, error)
	Rollback(ctx context.Context, cert *CertificateData, config map[string]any, creds map[string]any) (*Result, error)
	ValidateCredentials(ctx context.Context, creds map[string]any, config map[string]any) error
	Capabilities() Capabilities
}

var registry = struct {
	mu sync.RWMutex
	m  map[string]Provider
}{m: map[string]Provider{}}

// Register adds a provider under its capability type. Panics on empty/duplicate
// type (programmer errors surfaced at init).
func Register(p Provider) {
	typ := p.Capabilities().Type
	if typ == "" {
		panic("provider: Register with empty type")
	}
	if err := CheckCapabilities(p.Capabilities()); err != nil {
		panic(err.Error())
	}
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if _, ok := registry.m[typ]; ok {
		panic("provider: " + ErrDuplicate.Error() + ": " + typ)
	}
	registry.m[typ] = p
}

// Get returns the provider for a type, or ErrUnknownProvider.
func Get(typ string) (Provider, error) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	p, ok := registry.m[typ]
	if !ok {
		return nil, ErrUnknownProvider
	}
	return p, nil
}

// Exists reports whether a provider type is registered.
func Exists(typ string) bool {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	_, ok := registry.m[typ]
	return ok
}

// List returns every registered provider's capabilities, sorted by type.
func List() []Capabilities {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	out := make([]Capabilities, 0, len(registry.m))
	for _, p := range registry.m {
		out = append(out, p.Capabilities())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Type < out[j].Type })
	return out
}

// Info returns one provider's capabilities, or false if unknown.
func Info(typ string) (Capabilities, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	p, ok := registry.m[typ]
	if !ok {
		return Capabilities{}, false
	}
	return p.Capabilities(), true
}

func reset() {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.m = map[string]Provider{}
}

// NoRedirect is the CheckRedirect of every HTTP client that carries
// credentials or key material: a redirect is answered with the 3xx response
// itself (a failure for the provider), never followed — Go would replay a
// POST body (the private key) on 307/308 and keep custom headers (API keys)
// on a cross-host redirect.
func NoRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
