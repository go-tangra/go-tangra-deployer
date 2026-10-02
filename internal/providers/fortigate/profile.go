package fortigate

// Deploy/Verify/Rollback in default_ssl_profile mode (feature 033 US8,
// research D27, SR-016). Every pre-check (profile exists, server-cert-mode
// replace, no foreign reference to the certificate family) runs before the
// first write; a failed pre-check is a permanent "manual review" result with
// nothing imported, bound, changed or deleted. Deploy never deletes or
// overwrites a certificate and never deletes or recreates a profile.

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// profileNamePattern is the descriptor pattern of default_ssl_profile.
var profileNamePattern = regexp.MustCompile(`^[^\x00-\x1f"\\/]{1,35}$`)

// Profile actions reported in the job details.
const (
	actionUpdated   = "updated"
	actionAppended  = "appended"
	actionUnchanged = "unchanged"
)

// profileJob carries what the profile-mode operations share.
type profileJob struct {
	c       *fgClient
	profile string
	base    string
	serial  string
}

func newProfileJob(c *fgClient, profile string, cert *provider.CertificateData) (*profileJob, *provider.Result, error) {
	if !profileNamePattern.MatchString(profile) {
		return nil, &provider.Result{Success: false, Permanent: true, Message: "default_ssl_profile is not a valid SSL/SSH profile name"}, nil
	}
	leafPEM := leafCertPEM(cert.CertificatePEM)
	leaf, err := parseLeaf(leafPEM)
	if err != nil {
		return nil, nil, fmt.Errorf("parse certificate: %w", err)
	}
	return &profileJob{c: c, profile: profile, base: certName(leafPEM, cert), serial: certSerial(leaf)}, nil, nil
}

func (j *profileJob) details() map[string]any {
	return map[string]any{"host": j.c.host, "vdom": j.c.vdom, "ssl_profile": j.profile}
}

// manualReview is the permanent "manual review required" result: the reason
// and the referencing objects are in the details; nothing was changed.
func (j *profileJob) manualReview(certName string, imported bool, reason string, foreign []referenceHit) *provider.Result {
	refs := make([]string, 0, len(foreign))
	for _, h := range foreign {
		refs = append(refs, h.Holder+":"+h.Object)
	}
	d := j.details()
	d["manual_review_required"] = true
	d["reason"] = reason
	d["foreign_references"] = refs
	d["imported"] = imported
	if certName != "" {
		d["certificate_name"] = certName
	}
	return &provider.Result{
		Success:   false,
		Permanent: true,
		Message: fmt.Sprintf("MANUAL REVIEW REQUIRED: certificate was not bound into SSL/SSH profile %q — %s. No profile, reference or certificate was changed or deleted.",
			j.profile, reason),
		Details: d,
	}
}

// deployProfile imports (or reuses) the certificate under a dated name and
// replaces the family entry in the profile's server-cert list in place.
func (j *profileJob) deploy(ctx context.Context, cert *provider.CertificateData, scope string, progress provider.ProgressFn) (*provider.Result, error) {
	c := j.c
	note(progress, 15, "checking whether the certificate is already on the device")
	certs, err := c.listLocalCerts(ctx)
	if err != nil {
		return nil, fmt.Errorf("list certificates: %w", err)
	}
	existing := c.findLocalCertBySerial(ctx, certs, j.serial)
	famMatch := familyMatcher(j.base)
	inFamily := func(n string) bool { return famMatch(n) || (existing != "" && n == existing) }

	note(progress, 30, "validating SSL/SSH profile "+j.profile)
	prof, found, err := c.getSSLSSHProfile(ctx, j.profile)
	if err != nil {
		return nil, fmt.Errorf("read SSL/SSH profile: %w", err)
	}
	if !found {
		return j.manualReview(existing, false, fmt.Sprintf("SSL/SSH profile %q does not exist on the device; create it in server certificate mode replace", j.profile), nil), nil
	}
	if prof.ServerCertMode != "" && prof.ServerCertMode != "replace" {
		return j.manualReview(existing, false, fmt.Sprintf("SSL/SSH profile %q has server-cert-mode %q; only \"replace\" presents a server certificate list", j.profile, prof.ServerCertMode), nil), nil
	}

	note(progress, 45, "scanning certificate references")
	hits, err := scanReferences(ctx, c, inFamily)
	if err != nil {
		return j.manualReview(existing, false, "could not scan certificate references: "+err.Error(), nil), nil
	}
	var foreign []referenceHit
	for _, h := range hits {
		if h.Holder == holderSSLSSHProfile && h.Object == j.profile {
			continue
		}
		foreign = append(foreign, h)
	}
	if len(foreign) > 0 {
		return j.manualReview(existing, false, fmt.Sprintf("the certificate is referenced by %d object(s) outside the profile", len(foreign)), foreign), nil
	}

	name, imported, collided := existing, false, false
	if name == "" {
		name, collided, err = c.resolveFreeImportName(ctx, j.base, now().UTC().Format("20060102"))
		if err != nil {
			return nil, fmt.Errorf("resolve import name: %w", err)
		}
		note(progress, 60, "importing certificate "+name)
		if err := c.importCert(ctx, name, leafCertPEM(cert.CertificatePEM), cert.PrivateKeyPEM, scope); err != nil {
			return nil, fmt.Errorf("import certificate: %w", err)
		}
		if ok, verr := c.certExists(ctx, name); verr != nil || !ok {
			return nil, fmt.Errorf("verification failed: certificate %s not found after import", name)
		}
		imported = true
	}

	note(progress, 80, "updating SSL/SSH profile "+j.profile)
	newList, changed := replaceInList(prof.ServerCert, func(n string) bool { return n == name || famMatch(n) }, name)
	action := actionUpdated
	switch {
	case !containsName(newList, name):
		newList = append(newList, namedRef{Name: name})
		action = actionAppended
	case !changed:
		action = actionUnchanged
	}
	if action != actionUnchanged {
		if err := c.setSSLSSHProfileServerCert(ctx, j.profile, newList); err != nil {
			return nil, fmt.Errorf("update SSL/SSH profile: %w", err)
		}
	}
	bound, perr := c.findPoliciesBoundToProfile(ctx, j.profile)
	if perr != nil {
		bound = nil // best effort: the certificate and profile are already correct
	}

	note(progress, 100, "deployment complete")
	d := j.details()
	d["certificate_name"] = name
	d["imported"] = imported
	d["profile_action"] = action
	d["bound_policies"] = bound
	if collided {
		d["same_day_collision"] = true
	}
	verb := "imported"
	if !imported {
		verb = "reused (already on the device)"
	}
	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("Certificate %s %s; SSL/SSH profile %q %s (no certificate or profile deleted)", name, verb, j.profile, action),
		Details: d,
	}, nil
}

// verify succeeds when a certificate with the deployed serial exists and the
// profile lists it.
func (j *profileJob) verify(ctx context.Context) (*provider.Result, error) {
	c := j.c
	d := j.details()
	certs, err := c.listLocalCerts(ctx)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("verify failed: %v", err), Details: d}, nil
	}
	name := c.findLocalCertBySerial(ctx, certs, j.serial)
	if name == "" {
		return &provider.Result{Success: false, Message: "certificate not found on FortiGate", Details: d}, nil
	}
	d["certificate_name"] = name
	prof, found, err := c.getSSLSSHProfile(ctx, j.profile)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("verify failed: %v", err), Details: d}, nil
	}
	if !found {
		return &provider.Result{Success: false, Message: fmt.Sprintf("SSL/SSH profile %q not found", j.profile), Details: d}, nil
	}
	if !containsName(prof.ServerCert, name) {
		return &provider.Result{Success: false, Message: fmt.Sprintf("certificate %s is not listed in SSL/SSH profile %q", name, j.profile), Details: d}, nil
	}
	return &provider.Result{Success: true, Message: fmt.Sprintf("Certificate %s verified in SSL/SSH profile %q", name, j.profile), Details: d}, nil
}

// rollback points the profile back to the newest other family certificate on
// the device and then deletes the deployed certificate when nothing else
// references it. Without a previous certificate nothing is changed.
func (j *profileJob) rollback(ctx context.Context) (*provider.Result, error) {
	c := j.c
	d := j.details()
	certs, err := c.listLocalCerts(ctx)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("rollback failed: %v", err), Details: d}, nil
	}
	deployed := c.findLocalCertBySerial(ctx, certs, j.serial)
	if deployed == "" {
		return &provider.Result{Success: false, Message: "deployed certificate not found on FortiGate; profile unchanged", Details: d}, nil
	}
	d["certificate_name"] = deployed
	famMatch := familyMatcher(j.base)
	var others []string
	for _, lc := range certs {
		if lc.Name != deployed && famMatch(lc.Name) {
			others = append(others, lc.Name)
		}
	}
	if len(others) == 0 {
		return &provider.Result{Success: false, Permanent: true, Message: "no previous certificate to restore; profile unchanged", Details: d}, nil
	}
	sort.Strings(others) // dated names sort chronologically; the bare base first
	previous := others[len(others)-1]
	d["restored_certificate"] = previous

	prof, found, err := c.getSSLSSHProfile(ctx, j.profile)
	if err != nil {
		return &provider.Result{Success: false, Message: fmt.Sprintf("rollback failed: %v", err), Details: d}, nil
	}
	if !found {
		return &provider.Result{Success: false, Permanent: true, Message: fmt.Sprintf("SSL/SSH profile %q not found; nothing changed", j.profile), Details: d}, nil
	}
	newList, changed := replaceInList(prof.ServerCert, func(n string) bool { return n == deployed }, previous)
	if changed {
		if err := c.setSSLSSHProfileServerCert(ctx, j.profile, newList); err != nil {
			return &provider.Result{Success: false, Message: fmt.Sprintf("rollback failed: %v", err), Details: d}, nil
		}
	}
	d["profile_action"] = map[bool]string{true: actionUpdated, false: actionUnchanged}[changed]

	if err := c.deleteCert(ctx, deployed); err != nil {
		d["certificate_deleted"] = false
		reason := "delete refused"
		if strings.Contains(err.Error(), "referenced") {
			reason = "still referenced"
		}
		return &provider.Result{
			Success: true,
			Message: fmt.Sprintf("SSL/SSH profile %q points to %s again; certificate %s left in place (%s)", j.profile, previous, deployed, reason),
			Details: d,
		}, nil
	}
	d["certificate_deleted"] = true
	return &provider.Result{
		Success: true,
		Message: fmt.Sprintf("SSL/SSH profile %q points to %s again; certificate %s removed", j.profile, previous, deployed),
		Details: d,
	}, nil
}
