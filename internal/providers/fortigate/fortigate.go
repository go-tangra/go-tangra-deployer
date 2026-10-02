// Package fortigate is a deployment provider that installs an issued
// certificate + private key onto a FortiGate firewall via the FortiOS REST API.
//
// It is a 1:1 port of the go-tangra v3 FortiGate provider (v3.5.1): the
// certificate (leaf only — FortiOS rejects a chain with -145) is imported via
// POST /api/v2/monitor/vpn-certificate/local/import (type=regular) and every
// call is scoped to a VDOM (?vdom=) and authenticated with a bearer API token.
// Three replace strategies are selected by replace_strategy:
//
//   - "ssl_profile" (default): import under a dated name (or reuse the
//     identical certificate), bind it into the provider-owned SSL/SSH
//     inspection profile "{base}{profile_suffix}" (cloned from an existing
//     replace-mode profile when missing) and, optionally, into the shared
//     default_ssl_profile. A reference outside those profiles stops the
//     deployment for manual review. Nothing is ever deleted.
//   - "rebind": import under a dated name, repoint every reference to the
//     certificate family (SSL/SSH profiles, SSL-VPN, admin GUI, VIPs) and
//     prune superseded family members.
//   - "delete": the legacy delete-then-import under the bare name.
//
// v4 keeps these safety properties on top of v3: redirects are never followed,
// the API token and private key never appear in a message, error or details,
// reuse of a device certificate requires identical DER (not only the serial),
// and profile names are refused when they could form a URL path segment.
package fortigate

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func init() { provider.Register(Provider{}) }

// Replace strategies.
const (
	strategySSLProfile = "ssl_profile"
	strategyRebind     = "rebind"
	strategyDelete     = "delete"
)

// settle is the pause after deleting a certificate before re-importing under
// the same name (delete strategy, v3: one second); tests replace it.
var settle = func() { time.Sleep(time.Second) }

// Provider installs certificates onto a FortiGate firewall via the FortiOS REST API.
type Provider struct{}

// Capabilities describes the FortiGate provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "fortigate",
		DisplayName:      "FortiGate",
		Description:      "Imports the certificate and key as a local certificate on a FortiGate (FortiOS REST API) and binds it into SSL/SSH inspection profiles.",
		SupportsVerify:   true,
		SupportsRollback: true,
		TestConnection:   true,
		SchemaVersion:    1,
		ConfigFields: []provider.Field{
			{Key: "vdom", Label: "VDOM", Type: provider.TypeString, Required: true, Overridable: true, Group: provider.GroupConnection,
				Default: "root", Pattern: `^[A-Za-z0-9_-]{1,31}$`, MaxLength: 31, Placeholder: "root",
				Help: "Virtual domain the certificate is managed in."},
			{Key: "import_scope", Label: "Import scope", Type: provider.TypeEnum, Overridable: true, Group: provider.GroupOptions,
				Default: "global", Options: []provider.Option{{Value: "global", Label: "Global"}, {Value: "vdom", Label: "VDOM"}},
				Help: "Import the certificate globally or into the VDOM only."},
			{Key: "replace_strategy", Label: "Replace strategy", Type: provider.TypeEnum, Group: provider.GroupOptions,
				Default: strategySSLProfile, Options: []provider.Option{
					{Value: strategySSLProfile, Label: "SSL profile (import dated, bind into profiles, never delete)"},
					{Value: strategyRebind, Label: "Rebind (import dated, repoint all references, prune old)"},
					{Value: strategyDelete, Label: "Delete (delete and re-import under the same name)"}},
				Help: "How a renewal replaces the certificate. SSL profile stops for manual review when the certificate is used outside its profiles; Rebind repoints SSL/SSH profiles, SSL-VPN, admin GUI and VIPs; Delete fails while the certificate is in use."},
			{Key: "profile_suffix", Label: "Profile suffix", Type: provider.TypeString, Overridable: true, Group: provider.GroupOptions,
				Default: defaultProfileSuffix, Pattern: `^[^\x00-\x1f"\\/]{1,35}$`, MaxLength: 35, Placeholder: defaultProfileSuffix,
				Help: "SSL profile strategy: suffix of the provider-owned SSL/SSH inspection profile <certificate name><suffix>, created by cloning an existing replace-mode profile when missing."},
			{Key: "default_ssl_profile", Label: "Default SSL profile", Type: provider.TypeString, Overridable: true, Group: provider.GroupOptions,
				Pattern: `^[^\x00-\x1f"\\/.][^\x00-\x1f"\\/]{0,34}$`, MaxLength: 35, Placeholder: "inbound-www",
				Help: "SSL profile strategy: existing SSL/SSH inspection profile (server certificate mode replace) whose server certificate list is also updated in place; other domains' certificates are kept."},
			{Key: "rebind_references", Label: "Rebind references", Type: provider.TypeBool, Group: provider.GroupOptions, Default: true,
				Help: "Rebind strategy: repoint SSL/SSH profiles, SSL-VPN, admin GUI and VIPs from older certificates of the same name family to the new one."},
			{Key: "prune_old", Label: "Prune old certificates", Type: provider.TypeBool, Group: provider.GroupOptions, Default: true,
				Help: "Rebind strategy: delete superseded certificates of the same name family; certificates still in use are kept and reported."},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "Host", Type: provider.TypeString, Required: true, Group: provider.GroupConnection,
				Pattern: provider.HostPattern, MaxLength: 270, Placeholder: "fortigate.example.com",
				Help: "Management address of the FortiGate (host or host:port)."},
			{Key: "api_token", Label: "API token", Type: provider.TypeString, Secret: true, Required: true, Group: provider.GroupCredentials,
				MaxLength: 256, Help: "Token of a REST API administrator allowed to manage certificates."},
		},
	}
}

// deployConfig holds the resolved provider configuration for one deployment.
type deployConfig struct {
	vdom           string
	importScope    string // "global" | "vdom"
	strategy       string // "ssl_profile" (default) | "rebind" | "delete"
	profileSuffix  string // suffix for the owned profile in ssl_profile mode
	defaultProfile string // optional production-bound ssl-ssh-profile updated in addition to {base}_ssl_profile
	rebindRefs     bool   // rebind strategy only
	pruneOld       bool   // rebind strategy only
}

func parseConfig(config map[string]any) deployConfig {
	return deployConfig{
		vdom:           cfgString(config, "vdom", "root"),
		importScope:    cfgString(config, "import_scope", "global"),
		strategy:       cfgString(config, "replace_strategy", strategySSLProfile),
		profileSuffix:  cfgString(config, "profile_suffix", defaultProfileSuffix),
		defaultProfile: cfgString(config, "default_ssl_profile", ""),
		rebindRefs:     cfgBool(config, "rebind_references", true),
		pruneOld:       cfgBool(config, "prune_old", true),
	}
}

// usesProfiles reports whether the strategy is ssl_profile (any value other
// than rebind/delete falls back to it, as in v3).
func (cfg deployConfig) usesProfiles() bool {
	return cfg.strategy != strategyRebind && cfg.strategy != strategyDelete
}

// ValidateCredentials ("Test connection") checks the required credentials and
// probes the device. An unreachable device is not fatal at save time (v4
// contract shared by the appliance providers); 401/403 and any other non-2xx
// answer are (v3). In the ssl_profile strategy a configured
// default_ssl_profile must exist on the device (not_found_on_endpoint).
func (p Provider) ValidateCredentials(ctx context.Context, creds, config map[string]any) error {
	c, err := clientFrom(creds, config)
	if err != nil {
		return err
	}
	r, err := c.cmdbGet(ctx, "system/global")
	if err != nil {
		return nil // unreachable at validation time is not fatal
	}
	if err := checkProbe(r); err != nil {
		return err
	}
	cfg := parseConfig(config)
	if !cfg.usesProfiles() || strings.TrimSpace(cfg.defaultProfile) == "" {
		return nil
	}
	if !profileNamePattern.MatchString(cfg.defaultProfile) {
		return &provider.FieldError{Field: provider.PathConfig + "default_ssl_profile", Msg: provider.CodePattern}
	}
	if _, found, err := c.getSSLSSHProfile(ctx, cfg.defaultProfile); err == nil && !found {
		return &provider.FieldError{Field: provider.PathConfig + "default_ssl_profile", Msg: provider.CodeNotFoundOnEndpoint}
	}
	return nil
}

// clientFrom checks the required credentials and builds the client.
func clientFrom(creds, config map[string]any) (*fgClient, error) {
	host := credString(creds, "host")
	if host == "" {
		return nil, fmt.Errorf("host is required")
	}
	token := credString(creds, "api_token")
	if token == "" {
		return nil, fmt.Errorf("api_token is required")
	}
	return newClient(host, token, cfgString(config, "vdom", "root")), nil
}

func checkProbe(r *apiResponse) error {
	if r.StatusCode == http.StatusUnauthorized || r.StatusCode == http.StatusForbidden {
		return fmt.Errorf("authentication failed: invalid or unauthorized API token (HTTP %d)", r.StatusCode)
	}
	if !r.ok() {
		return apiError("connectivity check", r)
	}
	return nil
}

// connect is v3's ValidateCredentials as run before every Deploy, Verify and
// Rollback: the device must answer the connectivity check.
func connect(ctx context.Context, creds, config map[string]any) (*fgClient, error) {
	c, err := clientFrom(creds, config)
	if err != nil {
		return nil, err
	}
	r, err := c.cmdbGet(ctx, "system/global")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to FortiGate: %w", err)
	}
	if err := checkProbe(r); err != nil {
		return nil, err
	}
	return c, nil
}

// baseName derives the base object name from the certificate's own subject
// (not external metadata, which can disagree with the actual cert — e.g. an
// apex cert recorded with a wildcard common_name), falling back to the
// metadata common name and then to "cert_<id prefix>".
func baseName(leafPEM string, cert *provider.CertificateData) string {
	parsedLeaf, _ := parseLeaf(leafPEM)
	base := sanitizeName(subjectBaseName(parsedLeaf, cert.CommonName))
	if base == "" {
		base = "cert_" + safeIDPrefix(cert.ID)
	}
	return base
}

// Deploy deploys a certificate to FortiGate.
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	c, err := connect(ctx, creds, config)
	if err != nil {
		return nil, err
	}
	cfg := parseConfig(config)

	note(progress, 10, "Validating certificate data")
	if cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}

	// FortiOS local (server) cert import expects a single leaf certificate.
	// Sending the chain (leaf + intermediates) is rejected with error -145.
	leafCert := leafCertPEM(cert.CertificatePEM)
	base := baseName(leafCert, cert)

	switch cfg.strategy {
	case strategyDelete:
		return p.deployDelete(ctx, c, cfg, base, leafCert, cert.PrivateKeyPEM, progress)
	case strategyRebind:
		return p.deployRebind(ctx, c, cfg, base, leafCert, cert.PrivateKeyPEM, progress)
	default: // "ssl_profile" — production-safe default
		// v4: names that could form a URL path segment are refused before
		// anything is sent (stored rows predating the descriptor patterns).
		if cfg.defaultProfile != "" && !profileNamePattern.MatchString(cfg.defaultProfile) {
			return &provider.Result{Success: false, Permanent: true, Message: "default_ssl_profile is not a valid SSL/SSH profile name"}, nil
		}
		if !profileSuffixPattern.MatchString(cfg.profileSuffix) {
			return &provider.Result{Success: false, Permanent: true, Message: "profile_suffix is not a valid SSL/SSH profile name suffix"}, nil
		}
		return p.deploySSLProfile(ctx, c, cfg, base, leafCert, cert.PrivateKeyPEM, progress)
	}
}

// deployRebind imports the renewed certificate under a unique dated name,
// repoints existing references to it, then prunes superseded family members.
// This avoids FortiOS rejecting the replacement of a referenced certificate.
func (p Provider) deployRebind(ctx context.Context, c *fgClient, cfg deployConfig, base, fullCert, keyPEM string, progress provider.ProgressFn) (*provider.Result, error) {
	date := now().UTC().Format("20060102")

	// Idempotency by CONTENT, not by name: reuse the identical certificate,
	// suffix-probe on a dated-name collision, and import a single leaf so
	// FortiOS doesn't reject the import with -145.
	leaf := leafCertPEM(fullCert)
	parsed, err := parseLeaf(leaf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}
	note(progress, 15, "Checking whether the certificate is already on the device")
	existingName, err := c.findLocalCertBySerial(ctx, parsed)
	if err != nil {
		return nil, fmt.Errorf("failed to look up existing certificate: %w", err)
	}

	var newName string
	imported := false
	sameDayCollision := false
	if existingName != "" {
		newName = existingName // reuse — the same certificate is already on the device
	} else {
		name, collided, rerr := c.resolveFreeImportName(ctx, base, date)
		if rerr != nil {
			return nil, fmt.Errorf("failed to resolve import name: %w", rerr)
		}
		newName = name
		sameDayCollision = collided
		note(progress, 40, "Importing certificate "+newName)
		if err := c.importCert(ctx, newName, leaf, keyPEM, cfg.importScope); err != nil {
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
		note(progress, 65, "Rebinding references to "+newName)
		rb = rebindReferences(ctx, c, inFamily, newName)
	}

	// Prune is safe regardless of rebind outcome: FortiOS refuses to delete a
	// still-referenced certificate, so a failed rebind cannot lead to a broken
	// reference here — the old cert simply survives the prune.
	var pruned, pruneErrs []string
	if cfg.pruneOld {
		note(progress, 85, "Pruning superseded certificates")
		pruned, pruneErrs = pruneFamily(ctx, c, inFamily, newName)
	}

	note(progress, 100, "Deployment complete")

	success := len(rb.Errors) == 0
	msg := fmt.Sprintf("Certificate %s deployed (%d reference(s) rebound, %d superseded cert(s) pruned)", newName, len(rb.Rebound), len(pruned))
	if !success {
		msg = fmt.Sprintf("Certificate %s imported but %d reference rebind(s) failed: %s", newName, len(rb.Errors), strings.Join(rb.Errors, "; "))
	}

	details := map[string]any{
		"host":             c.host,
		"vdom":             cfg.vdom,
		"certificate_name": newName,
		"strategy":         strategyRebind,
		"imported":         imported,
		"rebound":          rb.Rebound,
		"rebind_errors":    rb.Errors,
		"pruned":           pruned,
		"prune_errors":     pruneErrs,
	}
	if sameDayCollision {
		details["same_day_collision"] = true
	}
	return &provider.Result{
		Success: success,
		Message: msg,
		Details: details,
	}, nil
}

// deployDelete is the legacy replace strategy: delete the existing certificate
// then re-import under the same name. It only works when the certificate is not
// referenced by any other object; otherwise FortiOS blocks the delete. Kept as
// an opt-in escape hatch via replace_strategy=delete.
func (p Provider) deployDelete(ctx context.Context, c *fgClient, cfg deployConfig, base, fullCert, keyPEM string, progress provider.ProgressFn) (*provider.Result, error) {
	certName := base

	note(progress, 20, "Checking for existing certificate")
	exists, err := c.certExists(ctx, certName)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing certificate: %w", err)
	}

	if exists {
		note(progress, 50, "Deleting existing certificate")
		if err := c.deleteCert(ctx, certName); err != nil {
			return nil, fmt.Errorf("failed to replace certificate %s: %w; if it is referenced by an SSL profile or other object, set replace_strategy=rebind", certName, err)
		}
		settle()
	}

	note(progress, 60, "Importing certificate")
	if err := c.importCert(ctx, certName, fullCert, keyPEM, cfg.importScope); err != nil {
		return nil, fmt.Errorf("failed to import certificate: %w", err)
	}

	note(progress, 90, "Verifying deployment")
	ok, err := c.certExists(ctx, certName)
	if err != nil || !ok {
		return nil, fmt.Errorf("verification failed: certificate not found after import")
	}

	note(progress, 100, "Deployment complete")
	return &provider.Result{
		Success: true,
		Message: "Certificate deployed successfully to FortiGate",
		Details: map[string]any{
			"host":             c.host,
			"vdom":             cfg.vdom,
			"certificate_name": certName,
			"strategy":         strategyDelete,
			"was_update":       exists,
		},
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
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	c, err := connect(ctx, creds, config)
	if err != nil {
		return nil, err
	}
	cfg := parseConfig(config)
	inFamily := familyMatcher(baseName(leafCertPEM(cert.CertificatePEM), cert))

	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("Failed to verify: %v", err)}, nil
	}
	var found []string
	for _, n := range names {
		if inFamily(n) {
			found = append(found, n)
		}
	}
	if len(found) == 0 {
		return &provider.Result{Success: false, Message: "Certificate not found on FortiGate"}, nil
	}
	sort.Strings(found)
	current := found[len(found)-1] // newest dated version sorts last

	return &provider.Result{
		Success: true,
		Message: "Certificate verified on FortiGate",
		Details: map[string]any{
			"host":             c.host,
			"vdom":             cfg.vdom,
			"certificate_name": current,
			"family_members":   found,
		},
	}, nil
}

// Rollback removes deployed family certificates that are no longer referenced.
// Referenced members are left in place (FortiOS rejects the delete), so a
// rollback cannot break a live configuration.
func (p Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	c, err := connect(ctx, creds, config)
	if err != nil {
		return nil, err
	}
	inFamily := familyMatcher(baseName(leafCertPEM(cert.CertificatePEM), cert))

	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("Rollback failed: %v", err)}, nil
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

	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("Rollback removed %d certificate(s); %d still referenced", len(deleted), len(skipped)),
		Details: map[string]any{
			"deleted": deleted,
			"skipped": skipped,
		},
	}, nil
}

// ---- helpers ----------------------------------------------------------------

func credString(m map[string]any, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func cfgString(m map[string]any, key, def string) string {
	if v, ok := m[key].(string); ok && v != "" {
		return v
	}
	return def
}

// cfgBool reads a bool option; stored v3 configurations may hold it as a
// string ("true"/"1"/"yes"/"on", "false"/"0"/"no"/"off"). Anything else is
// the default.
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

func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}
