// Package bigip is a deployment provider for F5 BIG-IP load balancers.
//
// It is a 1:1 port of the go-tangra v3 BIG-IP provider (v3.5.1). Over the
// iControl REST API (HTTP Basic auth) it uploads the certificate, the private
// key and — when present — the chain to the file-transfer endpoint, installs
// them as sys/crypto objects /<partition>/<name>.crt, /<partition>/<name>.key
// and /<partition>/<name>_chain.crt (overwriting an existing object of the
// same name) and, when ssl_profile is set, creates that client-SSL profile
// (cert, key, chain none, ciphers DEFAULT) or, when it already exists,
// points its cert and key at the deployed objects. Every Deploy, Verify and
// Rollback first checks that the appliance answers /mgmt/tm/sys/version.
//
// v4 keeps these safety properties on top of v3: redirects are never
// followed, the password and the private key never appear in a message, an
// error or details (appliance responses are scrubbed), partition and profile
// names that could form a path segment are refused before anything is sent,
// and a failure is returned as a Result carrying v3's message so the job
// history shows it.
package bigip

import (
	"context"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func init() { provider.Register(Provider{}) }

// defaultPartition is v3's partition when none is configured.
const defaultPartition = "Common"

// namePattern is a BIG-IP partition or object name segment: no slash, no
// tilde, no leading dot (so never "." or "..").
const namePattern = `[A-Za-z0-9_][A-Za-z0-9_.-]{0,63}`

// partitionPattern is the descriptor and runtime pattern of partition.
var partitionPattern = regexp.MustCompile(`^` + namePattern + `$`)

// Provider installs certificates onto an F5 BIG-IP via iControl REST.
type Provider struct{}

// Capabilities describes the BIG-IP provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "bigip",
		DisplayName:      "F5 BIG-IP",
		Description:      "Deploys the certificate, key and chain to an F5 BIG-IP via the iControl REST API and, optionally, binds them into a client-SSL profile.",
		SupportsVerify:   true,
		SupportsRollback: true,
		TestConnection:   true,
		SchemaVersion:    1,
		ConfigFields: []provider.Field{
			{Key: "partition", Label: "Partition", Type: provider.TypeString, Required: true, Overridable: true, Group: provider.GroupConnection,
				Default: defaultPartition, Pattern: partitionPattern.String(), MaxLength: 64, Placeholder: defaultPartition,
				Help: "Administrative partition the certificate, key, chain and client-SSL profile are created in (default Common)."},
			{Key: "ssl_profile", Label: "SSL profile", Type: provider.TypeString, Overridable: true, Group: provider.GroupOptions,
				Pattern: sslProfilePattern.String(), MaxLength: 321, Placeholder: "www_clientssl",
				Help: "Client-SSL profile to bind the certificate into (name in the partition or /Partition/name). Created (chain none, ciphers DEFAULT) when it does not exist; otherwise its certificate and key are updated. Rollback deletes it. Empty: no profile is touched."},
		},
		CredentialFields: []provider.Field{
			{Key: "host", Label: "Host", Type: provider.TypeString, Required: true, Group: provider.GroupConnection,
				Pattern: provider.HostPattern, MaxLength: 270, Placeholder: "bigip.example.com",
				Help: "Management address of the BIG-IP (host or host:port)."},
			{Key: "username", Label: "Username", Type: provider.TypeString, Required: true, Group: provider.GroupCredentials,
				MaxLength: 128, Placeholder: "deployer", Help: "iControl REST user allowed to manage certificates and SSL profiles."},
			{Key: "password", Label: "Password", Type: provider.TypeString, Secret: true, Required: true, Group: provider.GroupCredentials,
				MaxLength: 256, Help: "Password of the iControl REST user."},
		},
	}
}

// settings is the resolved configuration of one call.
type settings struct {
	partition  string
	sslProfile string // as configured (v3 details); "" = no profile
	profile    string // resolved /Partition/name; "" = no profile
}

// parseSettings resolves partition (default Common) and ssl_profile. v4: a
// value that could form a path segment is refused here, before any request.
func parseSettings(config map[string]any) (settings, *provider.FieldError) {
	s := settings{partition: strings.Trim(strings.TrimSpace(cfgString(config, "partition", "")), "/")}
	if s.partition == "" {
		s.partition = defaultPartition
	}
	if !partitionPattern.MatchString(s.partition) {
		return s, &provider.FieldError{Field: provider.PathConfig + "partition", Msg: provider.CodePattern}
	}
	s.sslProfile = strings.TrimSpace(cfgString(config, "ssl_profile", ""))
	if s.sslProfile == "" {
		return s, nil
	}
	full, ok := resolveProfilePath(s.sslProfile, s.partition)
	if !ok {
		return s, &provider.FieldError{Field: provider.PathConfig + "ssl_profile", Msg: provider.CodePattern}
	}
	s.profile = full
	return s, nil
}

// invalidSettingResult refuses a stored value outside the descriptor pattern
// (legacy row or bad override); the value itself is not echoed.
func invalidSettingResult(fe *provider.FieldError) *provider.Result {
	what := "partition is not a valid BIG-IP partition name"
	if fe.Field == provider.PathConfig+"ssl_profile" {
		what = "ssl_profile is not a valid client-SSL profile name or /Partition/name path"
	}
	return &provider.Result{Success: false, Permanent: true, Message: what}
}

// ValidateCredentials ("Test connection") checks the required credentials and
// probes /mgmt/tm/sys/version. An unreachable appliance is not fatal at save
// time (v4 contract shared by the appliance providers); 401 and any other
// non-200 answer are (v3). A missing ssl_profile is fine: Deploy creates it.
func (p Provider) ValidateCredentials(ctx context.Context, creds, config map[string]any) error {
	c, err := clientFrom(creds)
	if err != nil {
		return err
	}
	if _, fe := parseSettings(config); fe != nil {
		return fe
	}
	r, err := c.do(ctx, http.MethodGet, "/mgmt/tm/sys/version", nil)
	if err != nil {
		if isBuildError(err) {
			return err
		}
		return nil // unreachable at validation time is not fatal
	}
	return checkProbe(r)
}

// clientFrom checks the required credentials (v3 order and messages) and
// builds the client.
func clientFrom(creds map[string]any) (*client, error) {
	host := hostString(creds)
	if host == "" {
		return nil, fmt.Errorf("host is required")
	}
	username := credString(creds, "username")
	if username == "" {
		return nil, fmt.Errorf("username is required")
	}
	password := credString(creds, "password")
	if password == "" {
		return nil, fmt.Errorf("password is required")
	}
	return newClient(host, username, password), nil
}

// checkProbe maps the sys/version answer (v3 messages).
func checkProbe(r *response) error {
	if r.status == http.StatusUnauthorized {
		return fmt.Errorf("authentication failed: invalid username or password")
	}
	if r.status != http.StatusOK {
		return fmt.Errorf("BIG-IP API error (HTTP %d): %s", r.status, r.snippet())
	}
	return nil
}

// connect is v3's ValidateCredentials as run before every Deploy, Verify and
// Rollback: the appliance must answer the version probe.
func (c *client) connect(ctx context.Context) error {
	r, err := c.do(ctx, http.MethodGet, "/mgmt/tm/sys/version", nil)
	if err != nil {
		if isBuildError(err) {
			return err
		}
		return fmt.Errorf("failed to connect to BIG-IP: %w", err)
	}
	return checkProbe(r)
}

// prepare runs the shared preamble of Deploy, Verify and Rollback: required
// credentials (Go error, v3), settings (permanent Result, v4) and the
// connectivity probe (failed Result carrying v3's message).
func prepare(ctx context.Context, config, creds map[string]any) (*client, settings, *provider.Result, error) {
	c, err := clientFrom(creds)
	if err != nil {
		return nil, settings{}, nil, err
	}
	s, fe := parseSettings(config)
	if fe != nil {
		return nil, s, invalidSettingResult(fe), nil
	}
	if err := c.connect(ctx); err != nil {
		return nil, s, failed("%v", err), nil
	}
	return c, s, nil, nil
}

// objectNames returns the v3 object paths of a certificate in a partition.
func objectNames(partition, base string) (certFull, keyFull, chainFull string) {
	return fmt.Sprintf("/%s/%s.crt", partition, base),
		fmt.Sprintf("/%s/%s.key", partition, base),
		fmt.Sprintf("/%s/%s_chain.crt", partition, base)
}

// Deploy deploys a certificate to BIG-IP.
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	c, s, refused, err := prepare(ctx, config, creds)
	if err != nil || refused != nil {
		return refused, err
	}

	note(progress, 10, "Validating certificate data")
	if cert == nil || cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}
	c.addSecret(cert.PrivateKeyPEM)

	base := certBaseName(cert)
	certFull, keyFull, chainFull := objectNames(s.partition, base)

	// Step 1: upload and install the certificate.
	note(progress, 20, "Uploading certificate to BIG-IP")
	if err := c.installCrypto(ctx, "cert", "certificate", certFull, cert.CertificatePEM); err != nil {
		return failed("failed to upload certificate: %v", err), nil
	}

	// Step 2: upload and install the private key.
	note(progress, 40, "Uploading private key to BIG-IP")
	if err := c.installCrypto(ctx, "key", "key", keyFull, cert.PrivateKeyPEM); err != nil {
		return failed("failed to upload private key: %v", err), nil
	}

	// Step 3: upload and install the CA chain if provided (non-fatal).
	if cert.CertificateChain != "" {
		note(progress, 55, "Uploading certificate chain to BIG-IP")
		if err := c.installCrypto(ctx, "cert", "certificate", chainFull, cert.CertificateChain); err != nil {
			note(progress, 60, "Warning: failed to upload certificate chain")
		}
	}

	// Step 4: create or update the client-SSL profile if specified.
	if s.profile != "" {
		note(progress, 70, "Creating/updating SSL profile")
		if err := c.createOrUpdateSSLProfile(ctx, s.profile, certFull, keyFull); err != nil {
			return failed("failed to create/update SSL profile: %v", err), nil
		}
	}

	note(progress, 90, "Verifying deployment")
	if err := c.verifyCertExists(ctx, certFull); err != nil {
		return failed("verification failed: %v", err), nil
	}

	note(progress, 100, "Deployment complete")
	details := map[string]any{
		"host":             c.host,
		"partition":        s.partition,
		"certificate_name": certFull,
		"key_name":         keyFull,
	}
	if s.sslProfile != "" {
		details["ssl_profile"] = s.sslProfile
	}
	return &provider.Result{
		Success: true,
		Message: "Certificate deployed successfully to F5 BIG-IP",
		Details: details,
	}, nil
}

// Verify checks that the certificate object exists in the partition.
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	c, s, refused, err := prepare(ctx, config, creds)
	if err != nil || refused != nil {
		return refused, err
	}
	certFull, _, _ := objectNames(s.partition, certBaseName(cert))
	if err := c.verifyCertExists(ctx, certFull); err != nil {
		return failed("Certificate not found: %v", err), nil
	}
	return &provider.Result{
		Success: true,
		Message: "Certificate verified on BIG-IP",
		Details: map[string]any{
			"host":             c.host,
			"partition":        s.partition,
			"certificate_name": certFull,
		},
	}, nil
}

// Rollback removes the deployed objects: the client-SSL profile named by
// ssl_profile first (best effort — it references the certificate), then the
// certificate and key (failures reported) and the optional chain (ignored).
// A 404 counts as already removed.
func (p Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	c, s, refused, err := prepare(ctx, config, creds)
	if err != nil || refused != nil {
		return refused, err
	}
	certFull, keyFull, chainFull := objectNames(s.partition, certBaseName(cert))

	if s.profile != "" {
		_ = c.deleteResource(ctx, "ltm/profile/client-ssl", s.profile) // best effort
	}

	var errs []string
	if err := c.deleteResource(ctx, "sys/crypto/cert", certFull); err != nil {
		errs = append(errs, fmt.Sprintf("certificate: %v", err))
	}
	if err := c.deleteResource(ctx, "sys/crypto/key", keyFull); err != nil {
		errs = append(errs, fmt.Sprintf("key: %v", err))
	}
	_ = c.deleteResource(ctx, "sys/crypto/cert", chainFull) // optional: may not exist

	if len(errs) > 0 {
		return failed("Rollback partially failed: %s", strings.Join(errs, "; ")), nil
	}
	return &provider.Result{
		Success: true,
		Message: "Certificate and key removed from BIG-IP",
		Details: map[string]any{
			"deleted_cert": certFull,
			"deleted_key":  keyFull,
		},
	}, nil
}

// failed is a retryable failure carrying v3's message.
func failed(format string, args ...any) *provider.Result {
	return &provider.Result{Success: false, Message: fmt.Sprintf(format, args...)}
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

// hostString returns the host credential with surrounding space, an
// http(s):// scheme and trailing slashes removed.
func hostString(creds map[string]any) string {
	h := strings.TrimSpace(credString(creds, "host"))
	h = strings.TrimPrefix(h, "https://")
	h = strings.TrimPrefix(h, "http://")
	return strings.TrimRight(h, "/")
}

// certBaseName is v3's object base name: the sanitised common name, else
// "cert-" and the first eight characters of the certificate ID. v4: a short
// or empty ID does not panic (serial, then "freya_cert", as fallbacks) and the
// ID prefix keeps only object-name characters.
func certBaseName(cert *provider.CertificateData) string {
	if cert == nil {
		return "freya_cert"
	}
	if name := sanitizeName(cert.CommonName); name != "" {
		return name
	}
	id := sanitizeID(cert.ID)
	if len(id) > 8 {
		id = id[:8]
	}
	if id == "" {
		id = sanitizeName(cert.SerialNumber)
	}
	if id == "" {
		return "freya_cert"
	}
	return "cert-" + id
}

// sanitizeID drops the characters not allowed in an object name.
func sanitizeID(id string) string {
	return strings.Map(func(r rune) rune {
		if isNameRune(r) {
			return r
		}
		return -1
	}, id)
}

func isNameRune(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-'
}

// sanitizeName converts a domain/common name to a valid BIG-IP resource name
// (v3): the first "*" becomes "star", dots and every other character outside
// [A-Za-z0-9_-] become "_", runs of "_" collapse, leading/trailing "_" are
// trimmed and a leading digit gets a "cert_" prefix.
//
//	"www.example.com" → "www_example_com", "*.example.com" → "star_example_com"
func sanitizeName(name string) string {
	result := strings.Replace(name, "*", "star", 1)
	result = strings.ReplaceAll(result, ".", "_")
	result = strings.Map(func(r rune) rune {
		if isNameRune(r) {
			return r
		}
		return '_'
	}, result)
	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}
	result = strings.Trim(result, "_")
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "cert_" + result
	}
	return result
}

// note guards an optional progress callback.
func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}
