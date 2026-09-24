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
type Result struct {
	Success bool
	Message string
	Details map[string]any
}

// ProgressFn reports 0-100 progress with a short, non-secret note.
type ProgressFn func(percent int, note string)

// Field describes one config or credential input a provider needs.
type Field struct {
	Key      string `json:"key"`
	Label    string `json:"label"`
	Secret   bool   `json:"secret"`
	Required bool   `json:"required"`
}

// Capabilities declares what a provider supports and needs; drives UI + validation.
type Capabilities struct {
	Type             string  `json:"type"`
	DisplayName      string  `json:"display_name"`
	SupportsVerify   bool    `json:"supports_verify"`
	SupportsRollback bool    `json:"supports_rollback"`
	ConfigFields     []Field `json:"config_fields"`
	CredentialFields []Field `json:"credential_fields"`
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
