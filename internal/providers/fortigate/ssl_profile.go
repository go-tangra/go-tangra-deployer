package fortigate

// The ssl_profile replace strategy (the default), ported 1:1 from the v3
// provider's ssl_profile.go. v4 differences: a manual-review result is
// Permanent (a retry cannot change the convention check) except when the
// reference scan itself failed, and the profile names are checked against the
// descriptor patterns before anything is sent (see Deploy).

import (
	"context"
	"fmt"
	"regexp"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// defaultProfileSuffix is appended to the base certificate name to form the
// SSL-inspection profile this provider owns, e.g. "star_factory_bg" →
// "star_factory_bg_ssl_profile".
const defaultProfileSuffix = "_ssl_profile"

// profileNamePattern is the descriptor pattern of default_ssl_profile. No
// leading dot: "." and ".." would be path segments of the FortiOS URL; no
// slash, backslash, quote or control character.
var profileNamePattern = regexp.MustCompile(`^[^\x00-\x1f"\\/.][^\x00-\x1f"\\/]{0,34}$`)

// profileSuffixPattern is the descriptor pattern of profile_suffix (the suffix
// follows a sanitized base, so a dot is harmless here).
var profileSuffixPattern = regexp.MustCompile(`^[^\x00-\x1f"\\/]{1,35}$`)

func profileNameFor(base, suffix string) string {
	if suffix == "" {
		suffix = defaultProfileSuffix
	}
	return base + suffix
}

// deploySSLProfile is the production-safe default strategy. It:
//   - imports the renewed cert under a timestamped name (never overwrites),
//   - maintains a per-cert audit profile "{base}_ssl_profile" (created if
//     missing, edited if present),
//   - optionally also updates a single shared production-bound profile named
//     by cfg.defaultProfile (the profile attached to firewall policies; one
//     policy can attach only one ssl-ssh-profile, so the production profile
//     is the binding point for a whole SNI-multiplexed multi-domain setup),
//   - refuses to touch anything if the cert is referenced outside the two
//     known profiles, instead leaving the uploaded cert for manual review,
//   - never deletes any certificate or profile.
func (p Provider) deploySSLProfile(ctx context.Context, c *fgClient, cfg deployConfig, base, leafCert, keyPEM string, progress provider.ProgressFn) (*provider.Result, error) {
	profileName := profileNameFor(base, cfg.profileSuffix)
	// defaultProfile is only treated as a second target when it is set AND
	// distinct from the audit profile — equality collapses to a single PUT.
	hasDefault := cfg.defaultProfile != "" && cfg.defaultProfile != profileName

	// 1. Resolve the on-device certificate name idempotently. FortiOS rejects
	//    re-importing identical content (-145), so if the same certificate is
	//    already present, reuse it instead of importing.
	leaf, err := parseLeaf(leafCert)
	if err != nil {
		return nil, fmt.Errorf("failed to parse certificate: %w", err)
	}

	note(progress, 15, "Checking whether the certificate is already on the device")
	existingName, err := c.findLocalCertBySerial(ctx, leaf)
	if err != nil {
		return nil, fmt.Errorf("failed to look up existing certificate: %w", err)
	}

	var resolvedName string
	imported := false
	sameDayCollision := false
	if existingName != "" {
		resolvedName = existingName // reuse — idempotent
	} else {
		// Same-day reissue protection: if a different cert (already confirmed
		// above) was imported earlier today under the preferred dated name,
		// fall back to a sequence-suffixed variant so we don't collide with it.
		name, collided, rerr := c.resolveFreeImportName(ctx, base, now().UTC().Format("20060102"))
		if rerr != nil {
			return nil, fmt.Errorf("failed to resolve import name: %w", rerr)
		}
		resolvedName = name
		sameDayCollision = collided
		note(progress, 30, "Importing certificate "+resolvedName)
		if err := c.importCert(ctx, resolvedName, leafCert, keyPEM, cfg.importScope); err != nil {
			return nil, fmt.Errorf("failed to import certificate: %w", err)
		}
		ok, verr := c.certExists(ctx, resolvedName)
		if verr != nil || !ok {
			return nil, fmt.Errorf("verification failed: certificate %s not found after import (%v)", resolvedName, verr)
		}
		imported = true
	}

	// The cert set to protect: the resolved cert plus its dated name-family.
	famMatch := familyMatcher(base)
	inFamily := func(n string) bool { return n == resolvedName || famMatch(n) }

	// 2. Convention check: the cert may only be referenced by profiles we know
	//    about (the audit profile and, if configured, the production-bound
	//    profile). Anything else (VIP, SSL-VPN, admin GUI, other inspection
	//    profiles) → manual review so a human can decide.
	note(progress, 50, "Scanning certificate references")
	hits, err := scanReferences(ctx, c, inFamily)
	if err != nil {
		res := manualReviewResult(c, cfg, resolvedName, profileName, imported,
			fmt.Sprintf("could not scan references: %v", err), nil)
		res.Permanent = false // the scan failed; a retry can succeed
		return res, nil
	}
	var foreign []referenceHit
	for _, h := range hits {
		if h.Holder == holderSSLSSHProfile && (h.Object == profileName || (hasDefault && h.Object == cfg.defaultProfile)) {
			continue // a reference we own
		}
		foreign = append(foreign, h)
	}
	if len(foreign) > 0 {
		reason := fmt.Sprintf("certificate is referenced by %d object(s) outside the expected profile(s)", len(foreign))
		return manualReviewResult(c, cfg, resolvedName, profileName, imported, reason, foreign), nil
	}

	// 3. Pre-validate the default profile (if configured) BEFORE any write so
	//    a misconfiguration doesn't leave the audit profile updated and the
	//    production profile stale. An existing default profile must be in
	//    server-cert-mode=replace (the only mode where the server-cert list is
	//    actually presented to clients); a missing one is created in step 5.
	var defaultProf *sslSSHProfile
	if hasDefault {
		note(progress, 60, "Validating default SSL inspection profile "+cfg.defaultProfile)
		dp, dfound, derr := c.getSSLSSHProfile(ctx, cfg.defaultProfile)
		if derr != nil {
			return nil, fmt.Errorf("failed to read default profile %s: %w", cfg.defaultProfile, derr)
		}
		// A missing default profile is created in step 5 (v4; v3 stopped for
		// manual review), after every check has passed.
		if dfound && dp.ServerCertMode != "" && dp.ServerCertMode != "replace" {
			// FortiOS only honours the server-cert list when mode=replace. An
			// empty string is allowed and treated as "use device default",
			// which in practice means replace once a server-cert is set.
			return manualReviewResult(c, cfg, resolvedName, profileName, imported,
				fmt.Sprintf("default_ssl_profile %q has server-cert-mode=%q; only \"replace\" supports a server-cert list, so PUTting the cert into this profile would be a silent no-op — change the mode or point default_ssl_profile at a different profile", cfg.defaultProfile, dp.ServerCertMode),
				nil), nil
		}
		if dfound {
			defaultProf = dp
		}
	}

	// 4. Create or edit the convention-named audit profile to point at the
	//    cert. This profile is per-cert and not typically attached to a
	//    firewall policy — it exists for audit and rollback purposes.
	prof, found, err := c.getSSLSSHProfile(ctx, profileName)
	if err != nil {
		return nil, fmt.Errorf("failed to read profile %s: %w", profileName, err)
	}

	var auditAction string
	if !found {
		// Cloned from a replace-mode template when one exists (v3), else
		// created from FortiOS defaults (v4; v3 stopped for manual review).
		note(progress, 75, "Creating SSL inspection profile "+profileName)
		action, cerr := c.ensureSSLSSHProfile(ctx, profileName, resolvedName, cfg.defaultProfile)
		if cerr != nil {
			return nil, cerr
		}
		auditAction = action
	} else {
		note(progress, 75, "Updating SSL inspection profile "+profileName)
		newList, _ := replaceInList(prof.ServerCert, inFamily, resolvedName)
		if !containsName(newList, resolvedName) {
			newList = append(newList, namedRef{Name: resolvedName})
		}
		if err := c.setSSLSSHProfileServerCert(ctx, profileName, newList); err != nil {
			return nil, fmt.Errorf("failed to update profile %s: %w", profileName, err)
		}
		auditAction = "updated"
	}

	// 5. Update the production-bound default profile in place (PUT, never
	//    delete+recreate), preserving sibling cert entries for other domains
	//    that share the same multi-SNI inspection profile. Drift fallback:
	//    if no family member is present (operator removed it manually), we
	//    append rather than no-op so future renewals re-converge.
	var (
		defaultAction string
		boundPolicies []string
	)
	if hasDefault {
		note(progress, 90, "Updating default SSL inspection profile "+cfg.defaultProfile)
		if defaultProf == nil {
			// Missing: created bound to the certificate (v4; v3 required the
			// operator to pre-create it).
			action, cerr := c.ensureSSLSSHProfile(ctx, cfg.defaultProfile, resolvedName, profileName)
			if cerr != nil {
				return nil, cerr
			}
			defaultAction = action
		} else {
			newList, changed := replaceInList(defaultProf.ServerCert, inFamily, resolvedName)
			if !changed && !containsName(newList, resolvedName) {
				newList = append(newList, namedRef{Name: resolvedName})
				changed = true
				defaultAction = "appended (no family member was present)"
			} else if changed {
				defaultAction = "updated"
			} else {
				defaultAction = "no-change (already current)"
			}
			if changed {
				if err := c.setSSLSSHProfileServerCert(ctx, cfg.defaultProfile, newList); err != nil {
					return nil, fmt.Errorf("failed to update default profile %s: %w", cfg.defaultProfile, err)
				}
			}
		}
		// Best-effort blast-radius enumeration. A failure here does not break
		// the deploy — the cert and both profiles are already correct.
		boundPolicies, _ = c.findPoliciesBoundToProfile(ctx, cfg.defaultProfile)
	}

	note(progress, 100, "Deployment complete")
	verb := "imported"
	if !imported {
		verb = "reused (already present)"
	}
	msg := fmt.Sprintf("Certificate %s %s; audit profile %q %s", resolvedName, verb, profileName, auditAction)
	if hasDefault {
		msg += fmt.Sprintf("; default profile %q %s", cfg.defaultProfile, defaultAction)
	}
	msg += " (no certificates or profiles deleted)"

	details := map[string]any{
		"host":             c.host,
		"vdom":             cfg.vdom,
		"certificate_name": resolvedName,
		"strategy":         "ssl_profile",
		"profile":          profileName,
		"profile_action":   auditAction,
		"imported":         imported,
	}
	if sameDayCollision {
		// Surface the unusual case so operators notice a same-day reissue.
		details["same_day_collision"] = true
	}
	if hasDefault {
		details["default_profile"] = cfg.defaultProfile
		details["default_profile_action"] = defaultAction
		details["bound_policies"] = boundPolicies
	}
	return &provider.Result{
		Success: true,
		Message: msg,
		Details: details,
	}, nil
}

// manualReviewResult signals that the cert was uploaded but auto-binding was
// intentionally skipped because the convention was not met. It is reported as a
// non-successful, permanent result so the deployment surfaces as an
// attention-needed job without automatic retries; the Details payload carries
// the full reason and the offending references for the operator.
func manualReviewResult(c *fgClient, cfg deployConfig, newCert, profileName string, imported bool, reason string, foreign []referenceHit) *provider.Result {
	refs := make([]string, 0, len(foreign))
	for _, h := range foreign {
		refs = append(refs, fmt.Sprintf("%s:%s", h.Holder, h.Object))
	}
	details := map[string]any{
		"host":                   c.host,
		"vdom":                   cfg.vdom,
		"certificate_name":       newCert,
		"strategy":               "ssl_profile",
		"manual_review_required": true,
		"expected_profile":       profileName,
		"foreign_references":     refs,
		"reason":                 reason,
		"imported":               imported,
	}
	if cfg.defaultProfile != "" {
		details["default_profile"] = cfg.defaultProfile
	}
	return &provider.Result{
		Success:   false,
		Permanent: true,
		Message:   fmt.Sprintf("MANUAL REVIEW REQUIRED: certificate %s was uploaded but NOT auto-bound — %s. No references, profiles, or certificates were modified or deleted.", newCert, reason),
		Details:   details,
	}
}
