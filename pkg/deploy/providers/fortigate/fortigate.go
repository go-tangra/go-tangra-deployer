package fortigate

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

const ProviderType = "fortigate"

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "FortiGate",
		Description: "Deploy SSL/TLS certificates to FortiGate firewalls via REST API",
		Caps:        capabilities(),
	})
}

// Provider implements the FortiGate deployment provider.
type Provider struct{}

func capabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     true,
		RequiredConfigFields: []string{"vdom"},
		RequiredCredFields:   []string{"host", "api_token"},
	}
}

func (p *Provider) GetCapabilities() *registry.ProviderCapabilities { return capabilities() }

// deployConfig holds the resolved provider configuration for one deployment.
type deployConfig struct {
	vdom          string
	importScope   string // "global" | "vdom"
	strategy      string // "ssl_profile" (default) | "rebind" | "delete"
	profileSuffix string // suffix for the owned profile in ssl_profile mode
	rebindRefs    bool   // rebind strategy only
	pruneOld      bool   // rebind strategy only
}

func parseConfig(config map[string]any) deployConfig {
	return deployConfig{
		vdom:          cfgString(config, "vdom", "root"),
		importScope:   cfgString(config, "import_scope", "global"),
		strategy:      cfgString(config, "replace_strategy", "ssl_profile"),
		profileSuffix: cfgString(config, "profile_suffix", defaultProfileSuffix),
		rebindRefs:    cfgBool(config, "rebind_references", true),
		pruneOld:      cfgBool(config, "prune_old", true),
	}
}

// ValidateCredentials validates FortiGate credentials by checking connectivity.
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	host, ok := credentials["host"].(string)
	if !ok || host == "" {
		return fmt.Errorf("host is required")
	}
	apiToken, ok := credentials["api_token"].(string)
	if !ok || apiToken == "" {
		return fmt.Errorf("api_token is required")
	}

	c := newClient(host, apiToken, cfgString(config, "vdom", "root"))
	r, err := c.cmdbGet(ctx, "system/global")
	if err != nil {
		return fmt.Errorf("failed to connect to FortiGate: %w", err)
	}
	if r.StatusCode == 401 || r.StatusCode == 403 {
		return fmt.Errorf("authentication failed: invalid or unauthorized API token (HTTP %d)", r.StatusCode)
	}
	if !r.ok() {
		return apiError("connectivity check", r)
	}
	return nil
}

// Deploy deploys a certificate to FortiGate.
func (p *Provider) Deploy(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	start := time.Now()

	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}
	host := credentials["host"].(string)
	token := credentials["api_token"].(string)
	cfg := parseConfig(config)
	c := newClient(host, token, cfg.vdom)

	progressCb(10, "Validating certificate data")
	if cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}

	// FortiOS local (server) cert import expects a single leaf certificate.
	// Sending the chain (leaf + intermediates) is rejected with error -145.
	leafCert := leafCertPEM(cert.CertificatePEM)

	// Derive the base name from the certificate's own subject (not external
	// metadata, which can disagree with the actual cert — e.g. an apex cert
	// recorded with a wildcard common_name).
	parsedLeaf, _ := parseLeaf(leafCert)
	base := sanitizeName(subjectBaseName(parsedLeaf, cert.CommonName))
	if base == "" {
		base = "cert_" + safeIDPrefix(cert.ID)
	}

	switch cfg.strategy {
	case "delete":
		return p.deployDelete(ctx, c, cfg, base, leafCert, cert.PrivateKeyPEM, host, start, progressCb)
	case "rebind":
		return p.deployRebind(ctx, c, cfg, base, leafCert, cert.PrivateKeyPEM, host, start, progressCb)
	default: // "ssl_profile" — production-safe default
		return p.deploySSLProfile(ctx, c, cfg, base, leafCert, cert.PrivateKeyPEM, host, start, progressCb)
	}
}

// deployRebind imports the renewed certificate under a unique dated name,
// repoints existing references to it, then prunes superseded family members.
// This avoids FortiOS rejecting the replacement of a referenced certificate.
func (p *Provider) deployRebind(ctx context.Context, c *fgClient, cfg deployConfig, base, fullCert, keyPEM, host string, start time.Time, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	date := time.Now().UTC().Format("20060102")
	newName := versionedName(base, date)

	progressCb(20, "Checking for existing certificate")
	exists, err := c.certExists(ctx, newName)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing certificate: %w", err)
	}

	imported := false
	if !exists {
		progressCb(40, "Importing certificate "+newName)
		if err := c.importCert(ctx, newName, fullCert, keyPEM, cfg.importScope); err != nil {
			return nil, fmt.Errorf("failed to import certificate: %w", err)
		}
		ok, verr := c.certExists(ctx, newName)
		if verr != nil || !ok {
			return nil, fmt.Errorf("verification failed: certificate %s not found after import (%v)", newName, verr)
		}
		imported = true
	}

	inFamily := familyMatcher(base)

	var rb rebindResult
	if cfg.rebindRefs {
		progressCb(65, "Rebinding references to "+newName)
		rb = rebindReferences(ctx, c, inFamily, newName)
	}

	// Prune is safe regardless of rebind outcome: FortiOS refuses to delete a
	// still-referenced certificate, so a failed rebind cannot lead to a broken
	// reference here — the old cert simply survives the prune.
	var pruned, pruneErrs []string
	if cfg.pruneOld {
		progressCb(85, "Pruning superseded certificates")
		pruned, pruneErrs = pruneFamily(ctx, c, inFamily, newName)
	}

	progressCb(100, "Deployment complete")

	success := len(rb.Errors) == 0
	msg := fmt.Sprintf("Certificate %s deployed (%d reference(s) rebound, %d superseded cert(s) pruned)", newName, len(rb.Rebound), len(pruned))
	if !success {
		msg = fmt.Sprintf("Certificate %s imported but %d reference rebind(s) failed: %s", newName, len(rb.Errors), strings.Join(rb.Errors, "; "))
	}

	return &registry.DeploymentResult{
		Success:    success,
		Message:    msg,
		ResourceID: newName,
		Details: map[string]any{
			"host":             host,
			"vdom":             cfg.vdom,
			"certificate_name": newName,
			"strategy":         "rebind",
			"imported":         imported,
			"rebound":          rb.Rebound,
			"rebind_errors":    rb.Errors,
			"pruned":           pruned,
			"prune_errors":     pruneErrs,
		},
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// deployDelete is the legacy replace strategy: delete the existing certificate
// then re-import under the same name. It only works when the certificate is not
// referenced by any other object; otherwise FortiOS blocks the delete. Kept as
// an opt-in escape hatch via replace_strategy=delete.
func (p *Provider) deployDelete(ctx context.Context, c *fgClient, cfg deployConfig, base, fullCert, keyPEM, host string, start time.Time, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	certName := base

	progressCb(20, "Checking for existing certificate")
	exists, err := c.certExists(ctx, certName)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing certificate: %w", err)
	}

	if exists {
		progressCb(50, "Deleting existing certificate")
		if err := c.deleteCert(ctx, certName); err != nil {
			return nil, fmt.Errorf("failed to replace certificate %s: %w; if it is referenced by an SSL profile or other object, set replace_strategy=rebind", certName, err)
		}
		time.Sleep(1 * time.Second)
	}

	progressCb(60, "Importing certificate")
	if err := c.importCert(ctx, certName, fullCert, keyPEM, cfg.importScope); err != nil {
		return nil, fmt.Errorf("failed to import certificate: %w", err)
	}

	progressCb(90, "Verifying deployment")
	ok, err := c.certExists(ctx, certName)
	if err != nil || !ok {
		return nil, fmt.Errorf("verification failed: certificate not found after import")
	}

	progressCb(100, "Deployment complete")
	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate deployed successfully to FortiGate",
		ResourceID: certName,
		Details: map[string]any{
			"host":             host,
			"vdom":             cfg.vdom,
			"certificate_name": certName,
			"strategy":         "delete",
			"was_update":       exists,
		},
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// pruneFamily deletes family certificates other than keep. In-use members are
// left in place (FortiOS rejects the delete) and reported as non-fatal errors.
func pruneFamily(ctx context.Context, c *fgClient, inFamily func(string) bool, keep string) (pruned, errs []string) {
	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		return nil, []string{err.Error()}
	}
	for _, n := range names {
		if n == keep || !inFamily(n) {
			continue
		}
		if err := c.deleteCert(ctx, n); err != nil {
			errs = append(errs, fmt.Sprintf("%s: %v", n, err))
			continue
		}
		pruned = append(pruned, n)
	}
	return pruned, errs
}

// Verify checks that a member of the certificate family exists on the device.
func (p *Provider) Verify(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	start := time.Now()
	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}
	cfg := parseConfig(config)
	c := newClient(credentials["host"].(string), credentials["api_token"].(string), cfg.vdom)

	base := sanitizeName(cert.CommonName)
	if base == "" {
		base = "cert_" + safeIDPrefix(cert.ID)
	}
	inFamily := familyMatcher(base)

	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		return &registry.DeploymentResult{Success: false, Message: fmt.Sprintf("Failed to verify: %v", err), DurationMs: time.Since(start).Milliseconds()}, nil
	}
	var found []string
	for _, n := range names {
		if inFamily(n) {
			found = append(found, n)
		}
	}
	if len(found) == 0 {
		return &registry.DeploymentResult{Success: false, Message: "Certificate not found on FortiGate", DurationMs: time.Since(start).Milliseconds()}, nil
	}
	sort.Strings(found)
	current := found[len(found)-1] // newest dated version sorts last

	return &registry.DeploymentResult{
		Success:    true,
		Message:    "Certificate verified on FortiGate",
		ResourceID: current,
		Details: map[string]any{
			"host":             credentials["host"],
			"vdom":             cfg.vdom,
			"certificate_name": current,
			"family_members":   found,
		},
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// Rollback removes deployed family certificates that are no longer referenced.
// Referenced members are left in place (FortiOS rejects the delete), so a
// rollback cannot break a live configuration.
func (p *Provider) Rollback(ctx context.Context, cert *registry.CertificateData, config, credentials map[string]any) (*registry.DeploymentResult, error) {
	start := time.Now()
	if err := p.ValidateCredentials(ctx, credentials, config); err != nil {
		return nil, err
	}
	cfg := parseConfig(config)
	c := newClient(credentials["host"].(string), credentials["api_token"].(string), cfg.vdom)

	base := sanitizeName(cert.CommonName)
	if base == "" {
		base = "cert_" + safeIDPrefix(cert.ID)
	}
	inFamily := familyMatcher(base)

	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		return &registry.DeploymentResult{Success: false, Message: fmt.Sprintf("Rollback failed: %v", err), DurationMs: time.Since(start).Milliseconds()}, nil
	}
	var deleted, skipped []string
	for _, n := range names {
		if !inFamily(n) {
			continue
		}
		if err := c.deleteCert(ctx, n); err != nil {
			skipped = append(skipped, fmt.Sprintf("%s (%v)", n, err))
			continue
		}
		deleted = append(deleted, n)
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    fmt.Sprintf("Rollback removed %d certificate(s); %d still referenced", len(deleted), len(skipped)),
		DurationMs: time.Since(start).Milliseconds(),
		Details: map[string]any{
			"deleted": deleted,
			"skipped": skipped,
		},
	}, nil
}

func cfgString(m map[string]any, key, def string) string {
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

func cfgBool(m map[string]any, key string, def bool) bool {
	switch v := m[key].(type) {
	case bool:
		return v
	case string:
		switch strings.ToLower(strings.TrimSpace(v)) {
		case "true", "1", "yes", "on":
			return true
		case "false", "0", "no", "off":
			return false
		}
	}
	return def
}

func safeIDPrefix(id string) string {
	if len(id) >= 8 {
		return id[:8]
	}
	return id
}
