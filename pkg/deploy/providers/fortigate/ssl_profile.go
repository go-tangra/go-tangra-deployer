package fortigate

import (
	"context"
	"fmt"
	"time"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// defaultProfileSuffix is appended to the base certificate name to form the
// SSL-inspection profile this provider owns, e.g. "star_factory_bg" →
// "star_factory_bg_ssl_profile".
const defaultProfileSuffix = "_ssl_profile"

func profileNameFor(base, suffix string) string {
	if suffix == "" {
		suffix = defaultProfileSuffix
	}
	return base + suffix
}

// deploySSLProfile is the production-safe default strategy. It:
//   - imports the renewed cert under a timestamped name (never overwrites),
//   - binds it through exactly one convention-named SSL-inspection profile
//     "{base}_ssl_profile" (created if missing, edited if present),
//   - refuses to touch anything if the cert is referenced outside that single
//     convention (more than one reference, or a differently-named holder),
//     instead leaving the uploaded cert for manual review and signalling it,
//   - never deletes any certificate or profile.
func (p *Provider) deploySSLProfile(ctx context.Context, c *fgClient, cfg deployConfig, base, fullCert, keyPEM, host string, start time.Time, progressCb registry.ProgressCallback) (*registry.DeploymentResult, error) {
	date := time.Now().UTC().Format("20060102")
	newCert := versionedName(base, date)
	profileName := profileNameFor(base, cfg.profileSuffix)

	// 1. Import the renewed cert under a unique timestamped name.
	progressCb(20, "Checking for existing certificate")
	exists, err := c.certExists(ctx, newCert)
	if err != nil {
		return nil, fmt.Errorf("failed to check existing certificate: %w", err)
	}
	imported := false
	if !exists {
		progressCb(40, "Importing certificate "+newCert)
		if err := c.importCert(ctx, newCert, fullCert, keyPEM, cfg.importScope); err != nil {
			return nil, fmt.Errorf("failed to import certificate: %w", err)
		}
		ok, verr := c.certExists(ctx, newCert)
		if verr != nil || !ok {
			return nil, fmt.Errorf("verification failed: certificate %s not found after import (%v)", newCert, verr)
		}
		imported = true
	}

	inFamily := familyMatcher(base)

	// 2. Determine whether auto-binding is safe (convention check).
	progressCb(60, "Scanning certificate references")
	hits, err := scanReferences(ctx, c, inFamily)
	if err != nil {
		return manualReviewResult(host, cfg, newCert, profileName, imported,
			fmt.Sprintf("could not scan references: %v", err), nil, start), nil
	}
	var foreign []referenceHit
	for _, h := range hits {
		if h.Holder == holderSSLSSHProfile && h.Object == profileName {
			continue // the one reference we own
		}
		foreign = append(foreign, h)
	}
	if len(foreign) > 0 {
		reason := fmt.Sprintf("certificate is referenced by %d object(s) outside the expected profile %q", len(foreign), profileName)
		return manualReviewResult(host, cfg, newCert, profileName, imported, reason, foreign, start), nil
	}

	// 3. Create or edit the convention-named profile to point at the new cert.
	prof, found, err := c.getSSLSSHProfile(ctx, profileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile %s: %w", profileName, err)
	}

	var action string
	if !found {
		progressCb(85, "Creating SSL inspection profile "+profileName)
		if err := c.createSSLSSHProfile(ctx, profileName, newCert); err != nil {
			return nil, fmt.Errorf("failed to create profile %s: %w", profileName, err)
		}
		action = "created"
	} else {
		progressCb(85, "Updating SSL inspection profile "+profileName)
		newList, _ := replaceInList(prof.ServerCert, inFamily, newCert)
		if !containsName(newList, newCert) {
			newList = append(newList, namedRef{Name: newCert})
		}
		if err := c.setSSLSSHProfileServerCert(ctx, profileName, newList); err != nil {
			return nil, fmt.Errorf("failed to update profile %s: %w", profileName, err)
		}
		action = "updated"
	}

	progressCb(100, "Deployment complete")
	return &registry.DeploymentResult{
		Success:    true,
		Message:    fmt.Sprintf("Certificate %s deployed; SSL inspection profile %q %s (no certificates or profiles deleted)", newCert, profileName, action),
		ResourceID: newCert,
		Details: map[string]any{
			"host":             host,
			"vdom":             cfg.vdom,
			"certificate_name": newCert,
			"strategy":         "ssl_profile",
			"profile":          profileName,
			"profile_action":   action,
			"imported":         imported,
		},
		DurationMs: time.Since(start).Milliseconds(),
	}, nil
}

// manualReviewResult signals that the cert was uploaded but auto-binding was
// intentionally skipped because the convention was not met. It is reported as a
// non-successful result so the deployment surfaces as an attention-needed job
// (the deployer has no separate push-notification channel); the Details payload
// carries the full reason and the offending references for the operator.
func manualReviewResult(host string, cfg deployConfig, newCert, profileName string, imported bool, reason string, foreign []referenceHit, start time.Time) *registry.DeploymentResult {
	refs := make([]string, 0, len(foreign))
	for _, h := range foreign {
		refs = append(refs, fmt.Sprintf("%s:%s", h.Holder, h.Object))
	}
	return &registry.DeploymentResult{
		Success:    false,
		Message:    fmt.Sprintf("MANUAL REVIEW REQUIRED: certificate %s was uploaded but NOT auto-bound — %s. No references, profiles, or certificates were modified or deleted.", newCert, reason),
		ResourceID: newCert,
		Details: map[string]any{
			"host":                   host,
			"vdom":                   cfg.vdom,
			"certificate_name":       newCert,
			"strategy":               "ssl_profile",
			"manual_review_required": true,
			"expected_profile":       profileName,
			"foreign_references":     refs,
			"reason":                 reason,
			"imported":               imported,
		},
		DurationMs: time.Since(start).Milliseconds(),
	}
}
