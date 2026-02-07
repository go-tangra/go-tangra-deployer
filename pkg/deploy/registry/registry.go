package registry

import (
	"context"
	"fmt"
	"sync"
)

// DeploymentResult represents the result of a deployment operation
type DeploymentResult struct {
	Success    bool
	Message    string
	ResourceID string
	Details    map[string]any
	DurationMs int64
}

// ProviderCapabilities describes what a provider can do
type ProviderCapabilities struct {
	SupportsVerification bool
	SupportsRollback     bool
	RequiredConfigFields []string
	RequiredCredFields   []string
}

// ProgressCallback is called to report progress during deployment
type ProgressCallback func(progress int32, message string)

// Provider defines the interface for deployment providers
type Provider interface {
	// Deploy deploys a certificate to the target
	Deploy(ctx context.Context, cert *CertificateData, config, credentials map[string]any, progressCb ProgressCallback) (*DeploymentResult, error)

	// Verify verifies that a certificate is deployed correctly
	Verify(ctx context.Context, cert *CertificateData, config, credentials map[string]any) (*DeploymentResult, error)

	// Rollback rolls back a deployment
	Rollback(ctx context.Context, cert *CertificateData, config, credentials map[string]any) (*DeploymentResult, error)

	// ValidateCredentials validates provider credentials with optional config context
	ValidateCredentials(ctx context.Context, credentials, config map[string]any) error

	// GetCapabilities returns the provider's capabilities
	GetCapabilities() *ProviderCapabilities
}

// CertificateData contains certificate data for deployment
type CertificateData struct {
	ID                string
	SerialNumber      string
	CommonName        string
	SANs              []string
	CertificatePEM    string
	PrivateKeyPEM     string
	CertificateChain  string
	ExpiresAt         int64 // Unix timestamp
}

// ProviderInfo contains information about a registered provider
type ProviderInfo struct {
	Type        string
	DisplayName string
	Description string
	Caps        *ProviderCapabilities
}

// ProviderFactory is a function that creates a new provider instance
type ProviderFactory func() Provider

// Registry manages deployment providers
type Registry struct {
	mu        sync.RWMutex
	providers map[string]ProviderFactory
	info      map[string]*ProviderInfo
}

// Global registry instance
var globalRegistry = &Registry{
	providers: make(map[string]ProviderFactory),
	info:      make(map[string]*ProviderInfo),
}

// Register registers a provider factory with the global registry
func Register(providerType string, factory ProviderFactory, info *ProviderInfo) {
	globalRegistry.mu.Lock()
	defer globalRegistry.mu.Unlock()
	globalRegistry.providers[providerType] = factory
	globalRegistry.info[providerType] = info
}

// Get retrieves a provider instance from the global registry
func Get(providerType string) (Provider, error) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	factory, ok := globalRegistry.providers[providerType]
	if !ok {
		return nil, fmt.Errorf("provider type '%s' not found", providerType)
	}
	return factory(), nil
}

// List returns information about all registered providers
func List() []*ProviderInfo {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	result := make([]*ProviderInfo, 0, len(globalRegistry.info))
	for _, info := range globalRegistry.info {
		result = append(result, info)
	}
	return result
}

// Exists checks if a provider type is registered
func Exists(providerType string) bool {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()
	_, ok := globalRegistry.providers[providerType]
	return ok
}

// GetInfo retrieves information about a specific provider
func GetInfo(providerType string) (*ProviderInfo, error) {
	globalRegistry.mu.RLock()
	defer globalRegistry.mu.RUnlock()

	info, ok := globalRegistry.info[providerType]
	if !ok {
		return nil, fmt.Errorf("provider type '%s' not found", providerType)
	}
	return info, nil
}
