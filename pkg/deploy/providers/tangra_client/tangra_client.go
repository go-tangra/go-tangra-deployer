package tangra_client

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// ProviderType is the registry key for this provider.
const ProviderType = "tangra-client"

func init() {
	registry.Register(ProviderType, func() registry.Provider {
		return &Provider{
			log: log.NewHelper(log.DefaultLogger),
		}
	}, &registry.ProviderInfo{
		Type:        ProviderType,
		DisplayName: "Tangra Client",
		Description: "Deploy certificates to one or more registered go-tangra-client agents via the LCM streaming channel",
		Caps: &registry.ProviderCapabilities{
			SupportsVerification: true,
			SupportsRollback:     false,
			RequiredConfigFields: []string{},
			RequiredCredFields:   []string{},
		},
	})
}

// Config is the parsed shape of TargetConfiguration.config for this provider.
//
// Either ClientIDs or Labels (or both) must be provided. ClientIDs always
// takes precedence — labels are only consulted when no explicit IDs are set.
type Config struct {
	// ClientIDs is the explicit list of registered tangra-client client_ids
	// to deliver to. When set, label resolution is skipped.
	ClientIDs []string
	// Labels is a metadata selector matched against each registered client's
	// metadata. All key/value pairs must match (AND semantics). Resolution
	// requires the LCM ListLcmClients admin RPC.
	Labels map[string]string
	// CertName overrides the certificate name advertised to the agent. When
	// empty, the certificate's CommonName is used. The agent uses this as
	// the directory name under live/<cert-name>/.
	CertName string
	// RequireAllSuccess, when true, fails the deployment if any single
	// client push fails. Otherwise partial success is reported as success
	// with a summary message.
	RequireAllSuccess bool
}

// Provider implements registry.Provider for the tangra-client target type.
type Provider struct {
	log *log.Helper
}

// GetCapabilities returns the provider capabilities.
func (p *Provider) GetCapabilities() *registry.ProviderCapabilities {
	return &registry.ProviderCapabilities{
		SupportsVerification: true,
		SupportsRollback:     false,
		RequiredConfigFields: []string{},
		RequiredCredFields:   []string{},
	}
}

// ValidateCredentials validates that the supplied configuration has at least
// one resolvable target (explicit IDs or labels). Credentials are unused —
// the underlying Pusher uses ambient mTLS established by the deployer.
func (p *Provider) ValidateCredentials(ctx context.Context, credentials, config map[string]any) error {
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}
	if len(cfg.ClientIDs) == 0 && len(cfg.Labels) == 0 {
		return fmt.Errorf("tangra-client config requires either 'client_ids' or 'labels'")
	}
	return nil
}

// Deploy resolves the target client list and pushes the certificate to each.
func (p *Provider) Deploy(
	ctx context.Context,
	cert *registry.CertificateData,
	config map[string]any,
	credentials map[string]any,
	progressCb registry.ProgressCallback,
) (*registry.DeploymentResult, error) {
	start := time.Now()

	cfg, err := parseConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if cert == nil || cert.CertificatePEM == "" {
		return nil, fmt.Errorf("certificate PEM is required for tangra-client deployment")
	}

	pu, err := getPusher()
	if err != nil {
		return nil, err
	}

	progress(progressCb, 10, "Resolving target tangra-clients")

	clientIDs, err := pu.ResolveClients(ctx, cfg.ClientIDs, cfg.Labels)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %w", err)
	}
	if len(clientIDs) == 0 {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    "no tangra-clients matched the target configuration",
			DurationMs: time.Since(start).Milliseconds(),
		}, nil
	}

	certName := cfg.CertName
	if certName == "" {
		certName = cert.CommonName
	}

	p.log.Infof("[tangra-client] Pushing certificate %s (cn=%s) to %d client(s)", cert.ID, cert.CommonName, len(clientIDs))

	progress(progressCb, 40, fmt.Sprintf("Publishing certificate to %d client(s)", len(clientIDs)))

	results, err := pu.PushCertificate(ctx, clientIDs, certName, cert)
	if err != nil {
		return nil, fmt.Errorf("push certificate: %w", err)
	}

	successCount := 0
	var failedSummary []string
	for _, r := range results {
		if r.Success {
			successCount++
			continue
		}
		failedSummary = append(failedSummary, fmt.Sprintf("%s: %s", r.ClientID, r.Message))
	}

	progress(progressCb, 100, "Deployment complete")

	details := map[string]any{
		"provider":    ProviderType,
		"total":       len(results),
		"succeeded":   successCount,
		"failed":      len(results) - successCount,
		"client_ids":  clientIDs,
		"cert_name":   certName,
	}

	if successCount == len(results) {
		return &registry.DeploymentResult{
			Success:    true,
			Message:    fmt.Sprintf("Certificate delivered to %d tangra-client(s)", successCount),
			ResourceID: strings.Join(clientIDs, ","),
			Details:    details,
			DurationMs: time.Since(start).Milliseconds(),
		}, nil
	}

	if cfg.RequireAllSuccess || successCount == 0 {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Failed to deliver to %d/%d client(s): %s", len(results)-successCount, len(results), strings.Join(failedSummary, "; ")),
			Details:    details,
			DurationMs: time.Since(start).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    fmt.Sprintf("Partial success: delivered to %d/%d client(s)", successCount, len(results)),
		ResourceID: strings.Join(clientIDs, ","),
		Details:    details,
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// Verify confirms that each target tangra-client reported back a successful
// install of `cert` via LCM's ReportInstalledCertificate RPC. The check is
// twofold: the agent must have reported status=INSTALLED, and (when both sides
// have a fingerprint) the reported SHA-256 fingerprint must match the cert
// the provider intended to deliver. A target with no report is treated as
// pending — Verify returns success=false but no hard error, since the agent
// may not have processed the push yet.
func (p *Provider) Verify(
	ctx context.Context,
	cert *registry.CertificateData,
	config map[string]any,
	credentials map[string]any,
) (*registry.DeploymentResult, error) {
	start := time.Now()

	cfg, err := parseConfig(config)
	if err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}
	if cert == nil || cert.CertificatePEM == "" {
		return nil, fmt.Errorf("certificate PEM is required for tangra-client verification")
	}

	pu, err := getPusher()
	if err != nil {
		return nil, err
	}

	clientIDs, err := pu.ResolveClients(ctx, cfg.ClientIDs, cfg.Labels)
	if err != nil {
		return nil, fmt.Errorf("resolve clients: %w", err)
	}
	if len(clientIDs) == 0 {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    "no tangra-clients matched the target configuration",
			DurationMs: time.Since(start).Milliseconds(),
		}, nil
	}

	certName := cfg.CertName
	if certName == "" {
		certName = cert.CommonName
	}

	expectedFingerprint, err := fingerprintFromCertPEM(cert.CertificatePEM)
	if err != nil {
		// Fingerprint comparison is a soft assertion; fall back to status-only.
		p.log.Warnf("[tangra-client] Failed to derive expected fingerprint, verifying status only: %v", err)
		expectedFingerprint = ""
	}

	statuses, err := pu.VerifyInstalled(ctx, clientIDs, certName)
	if err != nil {
		return nil, fmt.Errorf("verify installed: %w", err)
	}

	installed := 0
	pending := 0
	failed := 0
	mismatch := 0
	var problems []string

	for _, st := range statuses {
		switch st.Status {
		case "INSTALLED":
			if expectedFingerprint != "" && st.FingerprintSHA256 != "" &&
				!strings.EqualFold(st.FingerprintSHA256, expectedFingerprint) {
				mismatch++
				problems = append(problems, fmt.Sprintf("%s: fingerprint mismatch (got %s)", st.ClientID, st.FingerprintSHA256))
				continue
			}
			installed++
		case "FAILED":
			failed++
			msg := st.Message
			if msg == "" {
				msg = "agent reported install failure"
			}
			problems = append(problems, fmt.Sprintf("%s: %s", st.ClientID, msg))
		case "REMOVED":
			failed++
			problems = append(problems, fmt.Sprintf("%s: certificate was removed", st.ClientID))
		default:
			pending++
			problems = append(problems, fmt.Sprintf("%s: no install report yet", st.ClientID))
		}
	}

	details := map[string]any{
		"provider":             ProviderType,
		"total":                len(statuses),
		"installed":            installed,
		"failed":               failed,
		"pending":              pending,
		"fingerprint_mismatch": mismatch,
		"cert_name":            certName,
		"expected_fingerprint": expectedFingerprint,
	}

	if installed == len(statuses) {
		return &registry.DeploymentResult{
			Success:    true,
			Message:    fmt.Sprintf("Verified %d tangra-client(s) installed certificate %q", installed, certName),
			Details:    details,
			DurationMs: time.Since(start).Milliseconds(),
		}, nil
	}

	if cfg.RequireAllSuccess || installed == 0 {
		return &registry.DeploymentResult{
			Success:    false,
			Message:    fmt.Sprintf("Verification incomplete: %d installed, %d failed, %d pending, %d fingerprint mismatch — %s", installed, failed, pending, mismatch, strings.Join(problems, "; ")),
			Details:    details,
			DurationMs: time.Since(start).Milliseconds(),
		}, nil
	}

	return &registry.DeploymentResult{
		Success:    true,
		Message:    fmt.Sprintf("Partial verification: %d/%d installed", installed, len(statuses)),
		Details:    details,
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// fingerprintFromCertPEM returns the lowercase hex SHA-256 fingerprint of the
// first certificate block found in certPEM. Used to compare against the agent-
// reported fingerprint during verification.
func fingerprintFromCertPEM(certPEM string) (string, error) {
	block, _ := pem.Decode([]byte(certPEM))
	if block == nil {
		return "", fmt.Errorf("no PEM block found in certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return "", fmt.Errorf("parse certificate: %w", err)
	}
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:]), nil
}

// Rollback is not implemented. Rolling back a pushed certificate would
// require asking the agent to re-install the previous cert, which the agent
// does not currently expose.
func (p *Provider) Rollback(
	ctx context.Context,
	cert *registry.CertificateData,
	config map[string]any,
	credentials map[string]any,
) (*registry.DeploymentResult, error) {
	return &registry.DeploymentResult{
		Success: false,
		Message: "rollback is not supported by the tangra-client provider",
	}, nil
}

// parseConfig coerces the loose map produced by the proto Struct into a typed
// Config. Unknown fields are ignored.
func parseConfig(raw map[string]any) (*Config, error) {
	cfg := &Config{}
	if raw == nil {
		return cfg, nil
	}

	if v, ok := raw["client_ids"]; ok {
		ids, err := toStringSlice(v)
		if err != nil {
			return nil, fmt.Errorf("client_ids: %w", err)
		}
		cfg.ClientIDs = ids
	}
	if v, ok := raw["client_id"].(string); ok && v != "" {
		cfg.ClientIDs = append(cfg.ClientIDs, v)
	}

	if v, ok := raw["labels"]; ok {
		labels, err := toStringMap(v)
		if err != nil {
			return nil, fmt.Errorf("labels: %w", err)
		}
		cfg.Labels = labels
	}

	if v, ok := raw["cert_name"].(string); ok {
		cfg.CertName = v
	}
	switch v := raw["require_all_success"].(type) {
	case bool:
		cfg.RequireAllSuccess = v
	case string:
		cfg.RequireAllSuccess = strings.EqualFold(v, "true") || v == "1"
	}

	return cfg, nil
}

func toStringSlice(v any) ([]string, error) {
	switch x := v.(type) {
	case []string:
		return x, nil
	case []any:
		out := make([]string, 0, len(x))
		for i, item := range x {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("element %d is not a string", i)
			}
			if s != "" {
				out = append(out, s)
			}
		}
		return out, nil
	case string:
		// UI forms only render single-line inputs, so client_ids arrives
		// as a comma/whitespace-separated string. Trim and drop empties
		// so trailing commas or stray whitespace don't produce blank
		// entries that the LCM rejects.
		parts := strings.FieldsFunc(x, func(r rune) bool {
			return r == ',' || r == ' ' || r == '\t' || r == '\n'
		})
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if p != "" {
				out = append(out, p)
			}
		}
		return out, nil
	default:
		return nil, fmt.Errorf("expected array or comma-separated string, got %T", v)
	}
}

func toStringMap(v any) (map[string]string, error) {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]string, len(x))
		for k, val := range x {
			s, ok := val.(string)
			if !ok {
				return nil, fmt.Errorf("value for %q is not a string", k)
			}
			out[k] = s
		}
		return out, nil
	case string:
		// UI form sends labels as a JSON object literal in a text input
		// (e.g. {"env":"prod","region":"eu"}). Tolerate an empty string
		// for the case where the user creates the config without
		// labels — parseConfig caller decides whether that's allowed.
		trimmed := strings.TrimSpace(x)
		if trimmed == "" {
			return nil, nil
		}
		var parsed map[string]string
		if err := json.Unmarshal([]byte(trimmed), &parsed); err != nil {
			return nil, fmt.Errorf("labels JSON: %w", err)
		}
		return parsed, nil
	default:
		return nil, fmt.Errorf("expected object or JSON string, got %T", v)
	}
}

func progress(cb registry.ProgressCallback, pct int32, msg string) {
	if cb != nil {
		cb(pct, msg)
	}
}
