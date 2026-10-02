package fortigate

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// Port of the v3 provider's deploy_test.go (v3.5.1) onto the v4 fake FortiOS:
// same scenarios, same assertions; the clock is fixed instead of read.

const testDate = "20261002"

// genTestCert returns a self-signed cert PEM and key PEM for the given CN and
// serial, so the provider's real cert parsing (subject + serial) works.
func genTestCert(t *testing.T, cn string, serial int64) (certPEM, keyPEM string) {
	t.Helper()
	return certWithSerial(t, cn, serial)
}

// newRebindFake seeds the rebind/delete scenarios (v3 newFakeForti). The
// pre-existing certs are never parsed by those strategies, so placeholder
// PEMs are fine.
func newRebindFake() *fakeFortiOS {
	f := newFakeFortiOS()
	f.addCert("star_factory_bg", "placeholder")
	f.addCert("star_jobs_bg_2025", "placeholder")
	f.addProfile("Jobs-Tech SSL Inspection", "replace", "star_jobs_bg_2025", "star_factory_bg")
	f.vpnCert = "jobs-bg-vpn"
	f.adminCert = "self-sign"
	return f
}

// newProfileFake is the v3 ssl_profile fixture: an empty device with a
// replace-mode template profile.
func newProfileFake(certs map[string]string, profiles map[string][]string) *fakeFortiOS {
	f := newFakeFortiOS()
	names := make([]string, 0, len(certs))
	for n := range certs {
		names = append(names, n)
	}
	slices.Sort(names)
	for _, n := range names {
		f.addCert(n, certs[n])
	}
	for n, cs := range profiles {
		f.addProfile(n, "replace", cs...)
	}
	f.vpnCert, f.adminCert = "x", "y"
	return f
}

// deployForTest runs Deploy against a fresh test server wrapping fake.
func deployForTest(t *testing.T, f *fakeFortiOS, cfg map[string]any, commonName, certPEM, keyPEM string) *provider.Result {
	t.Helper()
	fixedClock(t, testDate)
	noSettle(t)
	srv := f.serve(t)
	cd := &provider.CertificateData{CommonName: commonName, CertificatePEM: certPEM, PrivateKeyPEM: keyPEM}
	res, err := (Provider{}).Deploy(context.Background(), cd, cfg, fgCreds(srv), func(int, string) {})
	noSecrets(t, cd, res, err)
	if err != nil {
		t.Fatalf("Deploy error: %v", err)
	}
	return res
}

// ssl_profile (default): fresh cert (not on device) → import + create profile.
func TestDeploySSLProfile_CreatesProfile(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 1001)
	f := newProfileFake(nil, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	base := sanitizeName("test.example.com")
	newCert := versionedName(base, testDate)
	prof := profileNameFor(base, "")
	if _, ok := f.certs[newCert]; !ok {
		t.Errorf("expected import of %q; certs=%v", newCert, f.order)
	}
	assertList(t, f.serverCerts(prof), newCert)
}

// ssl_profile idempotency: cert already on device → reuse, no import.
func TestDeploySSLProfile_ReusesExistingBySerial(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 2002)
	f := newProfileFake(map[string]string{"preexisting_cert": certPEM}, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	prof := profileNameFor(sanitizeName("test.example.com"), "")
	if len(f.certs) != 1 {
		t.Errorf("expected NO new import (idempotent), certs=%v", f.order)
	}
	assertList(t, f.serverCerts(prof), "preexisting_cert")
	if res.Details["certificate_name"] != "preexisting_cert" {
		t.Errorf("certificate_name = %v, want preexisting_cert", res.Details["certificate_name"])
	}
}

// v4 (T110): a device certificate that only shares the serial (another key or
// issuer) is NOT reused — v3 matched by serial alone.
func TestDeploySSLProfile_SameSerialOtherCertificateIsImported(t *testing.T) {
	lookalike, _ := genTestCert(t, "test.example.com", 2003)
	certPEM, keyPEM := genTestCert(t, "test.example.com", 2003)
	f := newProfileFake(map[string]string{"lookalike": lookalike}, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success || res.Details["imported"] != true || res.Details["certificate_name"] != "test_example_com_"+testDate {
		t.Fatalf("result = %+v", res)
	}
}

// #2: base name derived from the cert SUBJECT, not the metadata common_name.
func TestDeploySSLProfile_UsesCertSubjectNotMetadata(t *testing.T) {
	// Subject is apex.example.com; metadata lies and says *.example.com.
	certPEM, keyPEM := genTestCert(t, "apex.example.com", 4004)
	f := newProfileFake(nil, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "*.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	wantProf := "apex_example_com_ssl_profile" // from subject, NOT star_example_com
	if _, ok := f.profiles[wantProf]; !ok {
		t.Errorf("expected profile from cert subject %q; profiles=%v", wantProf, f.profiles)
	}
}

// ssl_profile: cert referenced by a differently-named profile → bail.
func TestDeploySSLProfile_BailsOnForeignReference(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 3003)
	f := newProfileFake(map[string]string{"preexisting_cert": certPEM}, map[string][]string{"Other Inspection": {"preexisting_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected manual-review (non-success) on foreign reference")
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected Details.manual_review_required=true")
	}
	prof := profileNameFor(sanitizeName("test.example.com"), "")
	if _, exists := f.profiles[prof]; exists {
		t.Error("must not create the conventional profile when bailing")
	}
	assertList(t, f.serverCerts("Other Inspection"), "preexisting_cert")
}

// ssl_profile: profile missing AND no template to clone → bail (but cert still imported).
func TestDeploySSLProfile_BailsWhenNoTemplate(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 7007)
	f := newProfileFake(nil, nil)
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected bail when no template profile exists to clone")
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected manual_review_required")
	}
	newCert := versionedName(sanitizeName("test.example.com"), testDate)
	if _, ok := f.certs[newCert]; !ok {
		t.Error("cert should still be imported even when bailing on missing template")
	}
}

// ssl_profile + default_ssl_profile: production-bound profile contains an
// old family member and a sibling cert; deploy swaps the family member in
// place and preserves the sibling so multi-domain inspection keeps working.
func TestDeploySSLProfile_DefaultProfile_SwapsExistingFamily(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app1.example.com", 8001)
	base := sanitizeName("app1.example.com")
	auditProf := profileNameFor(base, "")
	defaultProf := "multi_domain_ssl_profile"
	oldFamily := "app1_example_com_20260101"
	sibling := "app2_example_com_20260101"
	f := newProfileFake(map[string]string{oldFamily: "placeholder"}, map[string][]string{defaultProf: {oldFamily, sibling}})
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, f, cfg, "app1.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s — details=%v", res.Message, res.Details)
	}
	newCert := versionedName(base, testDate)
	got := f.serverCerts(defaultProf)
	if len(got) != 2 || !slices.Contains(got, newCert) || !slices.Contains(got, sibling) {
		t.Errorf("default profile = %v, want [%s, %s] (any order)", got, newCert, sibling)
	}
	if slices.Contains(got, oldFamily) {
		t.Errorf("default profile still contains old family member %q: %v", oldFamily, got)
	}
	if _, ok := f.profiles[auditProf]; !ok {
		t.Errorf("audit profile %q must also be created/updated", auditProf)
	}
	if act, _ := res.Details["default_profile_action"].(string); !strings.Contains(act, "updated") {
		t.Errorf("Details.default_profile_action = %q, want \"updated\"", act)
	}
}

// ssl_profile + default_ssl_profile: family is absent from the production
// profile (e.g. operator manually removed it). Deploy must APPEND so future
// renewals re-converge instead of silently no-op'ing.
func TestDeploySSLProfile_DefaultProfile_AppendsWhenAbsent(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app3.example.com", 8002)
	defaultProf := "multi_domain_ssl_profile"
	sibling := "app2_example_com_20260101"
	f := newProfileFake(nil, map[string][]string{defaultProf: {sibling}})
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, f, cfg, "app3.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	newCert := versionedName(sanitizeName("app3.example.com"), testDate)
	got := f.serverCerts(defaultProf)
	if len(got) != 2 || !slices.Contains(got, sibling) || !slices.Contains(got, newCert) {
		t.Errorf("default profile = %v, want sibling+new (any order)", got)
	}
	if act, _ := res.Details["default_profile_action"].(string); !strings.Contains(act, "appended") {
		t.Errorf("Details.default_profile_action = %q, want \"appended\"", act)
	}
}

// ssl_profile + default_ssl_profile: profile is missing → manual review, no
// writes to either profile. The cert is still imported (idempotent).
func TestDeploySSLProfile_DefaultProfileMissing_ManualReview(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 8003)
	// A replace-mode template must exist or we'd bail for a different reason.
	f := newProfileFake(nil, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": "nonexistent_profile"}
	res := deployForTest(t, f, cfg, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected manual review when default_ssl_profile is missing")
	}
	if mr, _ := res.Details["manual_review_required"].(bool); !mr {
		t.Error("expected Details.manual_review_required=true")
	}
	if !strings.Contains(res.Message, "does not exist") {
		t.Errorf("expected message to explain the missing profile, got: %s", res.Message)
	}
	auditProf := profileNameFor(sanitizeName("test.example.com"), "")
	if _, ok := f.profiles[auditProf]; ok {
		t.Error("audit profile must NOT be created when bailing on missing default profile")
	}
	newCert := versionedName(sanitizeName("test.example.com"), testDate)
	if _, ok := f.certs[newCert]; !ok {
		t.Error("cert should still be imported (idempotent) even when bailing")
	}
}

// ssl_profile + default_ssl_profile: profile exists but mode is not "replace"
// → manual review, because PUTting server-cert would be a silent no-op.
func TestDeploySSLProfile_DefaultProfileWrongMode_ManualReview(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 8004)
	defaultProf := "deep-inspection-resign"
	f := newProfileFake(nil, map[string][]string{defaultProf: {"some_ca"}, "deep-inspection": {"tmpl_cert"}})
	f.profiles[defaultProf].ServerCertMode = "re-sign"
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, f, cfg, "test.example.com", certPEM, keyPEM)
	if res.Success {
		t.Fatal("expected manual review when default profile is not in replace mode")
	}
	if !strings.Contains(res.Message, "server-cert-mode") {
		t.Errorf("expected message to call out server-cert-mode, got: %s", res.Message)
	}
	auditProf := profileNameFor(sanitizeName("test.example.com"), "")
	if _, ok := f.profiles[auditProf]; ok {
		t.Error("audit profile must NOT be created when bailing on wrong-mode default profile")
	}
	assertList(t, f.serverCerts(defaultProf), "some_ca")
}

// ssl_profile + default_ssl_profile: when default_ssl_profile equals the
// audit profile name, the deploy must only PUT once (not twice).
func TestDeploySSLProfile_DefaultEqualsAudit_SinglePUT(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "test.example.com", 8005)
	base := sanitizeName("test.example.com")
	auditProf := profileNameFor(base, "")
	old := "test_example_com_20260101"
	f := newProfileFake(map[string]string{old: "placeholder"}, map[string][]string{auditProf: {old}})
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": auditProf}
	res := deployForTest(t, f, cfg, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	if got := len(f.bodies("PUT", "cmdb/firewall/ssl-ssh-profile/"+auditProf)); got != 1 {
		t.Errorf("expected exactly 1 PUT to %q, got %d", auditProf, got)
	}
	if _, ok := res.Details["default_profile"]; ok {
		t.Error("Details.default_profile must be omitted when default collapses to audit")
	}
}

// ssl_profile + default_ssl_profile: the production profile is one of two
// references to the cert; this must NOT trigger the foreign-reference bail.
func TestDeploySSLProfile_DefaultProfileReference_NotFlaggedAsForeign(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app1.example.com", 8006)
	base := sanitizeName("app1.example.com")
	auditProf := profileNameFor(base, "")
	defaultProf := "multi_domain_ssl_profile"
	pre := "preexisting_app1_cert"
	f := newProfileFake(map[string]string{pre: certPEM}, map[string][]string{defaultProf: {pre, "app2_cert"}, auditProf: {pre}})
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, f, cfg, "app1.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success (not foreign), got: %s — details=%v", res.Message, res.Details)
	}
	if res.Details["certificate_name"] != pre {
		t.Errorf("certificate_name = %v, want preexisting %q (idempotent)", res.Details["certificate_name"], pre)
	}
}

// ssl_profile + default_ssl_profile: bound_policies enumeration surfaces the
// firewall policies that will start serving the new cert.
func TestDeploySSLProfile_DefaultProfile_ReportsBoundPolicies(t *testing.T) {
	certPEM, keyPEM := genTestCert(t, "app1.example.com", 8007)
	defaultProf := "multi_domain_ssl_profile"
	f := newProfileFake(nil, map[string][]string{defaultProf: {"sibling_cert"}})
	f.policies = []map[string]any{
		{"name": "WAN-to-DMZ", "ssl-ssh-profile": defaultProf},
		{"name": "Internal-to-DMZ", "ssl-ssh-profile": defaultProf},
		{"name": "Other-Policy", "ssl-ssh-profile": "different_profile"},
	}
	cfg := map[string]any{"vdom": "root", "default_ssl_profile": defaultProf}
	res := deployForTest(t, f, cfg, "app1.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	bp, _ := res.Details["bound_policies"].([]string)
	if len(bp) != 2 || !slices.Contains(bp, "WAN-to-DMZ") || !slices.Contains(bp, "Internal-to-DMZ") {
		t.Errorf("Details.bound_policies = %v, want both WAN-to-DMZ and Internal-to-DMZ", bp)
	}
	if slices.Contains(bp, "Other-Policy") {
		t.Error("Details.bound_policies must not include policies bound to a different profile")
	}
}

// ssl_profile + same-day reissue: morning cert occupies <base>_YYYYMMDD;
// afternoon deploys a different serial under the same base on the same day.
// The deploy must NOT collide on the dated name — instead it suffix-probes
// to <base>_YYYYMMDD_01.
func TestDeploySSLProfile_SameDayCollision_UsesSequenceSuffix(t *testing.T) {
	base := sanitizeName("test.example.com")
	morningName := versionedName(base, testDate)
	morningPEM, _ := genTestCert(t, "test.example.com", 9001)
	afternoonPEM, afternoonKey := genTestCert(t, "test.example.com", 9002)
	f := newProfileFake(map[string]string{morningName: morningPEM}, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", afternoonPEM, afternoonKey)
	if !res.Success {
		t.Fatalf("expected success, got: %s — details=%v", res.Message, res.Details)
	}
	wantNew := suffixedVersionedName(base, testDate, 1)
	if _, ok := f.certs[wantNew]; !ok {
		t.Errorf("expected afternoon import as %q, certs=%v", wantNew, f.order)
	}
	if _, ok := f.certs[morningName]; !ok {
		t.Errorf("morning cert %q must NOT be removed or overwritten", morningName)
	}
	if collided, _ := res.Details["same_day_collision"].(bool); !collided {
		t.Error("Details.same_day_collision must be true on suffix-probed import")
	}
}

// ssl_profile + same-day reissue: morning AND first afternoon names both
// taken; the deploy must probe past _01 to _02.
func TestDeploySSLProfile_SameDayCollision_ProbesUntilFree(t *testing.T) {
	base := sanitizeName("test.example.com")
	morning := versionedName(base, testDate)
	midday := suffixedVersionedName(base, testDate, 1)
	morningPEM, _ := genTestCert(t, "test.example.com", 9101)
	middayPEM, _ := genTestCert(t, "test.example.com", 9102)
	afternoonPEM, afternoonKey := genTestCert(t, "test.example.com", 9103)
	f := newProfileFake(map[string]string{morning: morningPEM, midday: middayPEM}, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", afternoonPEM, afternoonKey)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	want := suffixedVersionedName(base, testDate, 2)
	if _, ok := f.certs[want]; !ok {
		t.Errorf("expected import as %q, certs=%v", want, f.order)
	}
}

// ssl_profile + same-day re-trigger (same certificate): the morning cert is
// reused — no suffix probe, no new import.
func TestDeploySSLProfile_SameDaySameSerial_StaysIdempotent(t *testing.T) {
	base := sanitizeName("test.example.com")
	morningName := versionedName(base, testDate)
	certPEM, keyPEM := genTestCert(t, "test.example.com", 9200)
	f := newProfileFake(map[string]string{morningName: certPEM}, map[string][]string{"deep-inspection": {"tmpl_cert"}})
	res := deployForTest(t, f, map[string]any{"vdom": "root"}, "test.example.com", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	if len(f.certs) != 1 {
		t.Errorf("expected no new import (idempotent), certs=%v", f.order)
	}
	if collided, _ := res.Details["same_day_collision"].(bool); collided {
		t.Error("Details.same_day_collision must be false on idempotent reuse")
	}
	if res.Details["certificate_name"] != morningName {
		t.Errorf("certificate_name = %v, want %q", res.Details["certificate_name"], morningName)
	}
}

// rebind strategy (opt-in): repoints references and prunes old family members.
func TestDeployRebind_EndToEnd(t *testing.T) {
	f := newRebindFake()
	certPEM, keyPEM := genTestCert(t, "*.factory.bg", 5005)
	res := deployForTest(t, f, map[string]any{"vdom": "root", "replace_strategy": "rebind"}, "*.factory.bg", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("Deploy not successful: %s", res.Message)
	}
	newName := versionedName("star_factory_bg", testDate)
	if _, ok := f.certs[newName]; !ok {
		t.Errorf("expected new cert %q; certs=%v", newName, f.order)
	}
	if _, ok := f.certs["star_factory_bg"]; ok {
		t.Errorf("expected old cert star_factory_bg pruned; certs=%v", f.certs)
	}
	if _, ok := f.certs["star_jobs_bg_2025"]; !ok {
		t.Error("unrelated cert star_jobs_bg_2025 must not be pruned")
	}
	assertList(t, f.serverCerts("Jobs-Tech SSL Inspection"), "star_jobs_bg_2025", newName)
}

// rebind same-day reissue: morning cert occupies <base>_YYYYMMDD; afternoon
// deploys a different serial under the same base on the same day. The fix
// imports as <base>_YYYYMMDD_01, rebinds refs there, and prunes the morning
// cert (it's in family and no longer referenced after the rebind).
func TestDeployRebind_SameDayCollision_UsesSequenceSuffixAndPrunesMorning(t *testing.T) {
	base := sanitizeName("*.factory.bg")
	morningName := versionedName(base, testDate)
	morningPEM, _ := genTestCert(t, "*.factory.bg", 7501)
	afternoonPEM, afternoonKey := genTestCert(t, "*.factory.bg", 7502)
	f := newProfileFake(map[string]string{morningName: morningPEM}, map[string][]string{"Jobs-Tech SSL Inspection": {morningName}})
	res := deployForTest(t, f, map[string]any{"vdom": "root", "replace_strategy": "rebind"}, "*.factory.bg", afternoonPEM, afternoonKey)
	if !res.Success {
		t.Fatalf("expected success, got: %s — details=%v", res.Message, res.Details)
	}
	wantNew := suffixedVersionedName(base, testDate, 1)
	if _, ok := f.certs[wantNew]; !ok {
		t.Errorf("expected afternoon import as %q, certs=%v", wantNew, f.order)
	}
	if _, ok := f.certs[morningName]; ok {
		t.Errorf("expected morning cert %q to be pruned", morningName)
	}
	assertList(t, f.serverCerts("Jobs-Tech SSL Inspection"), wantNew)
	if collided, _ := res.Details["same_day_collision"].(bool); !collided {
		t.Error("Details.same_day_collision must be true on suffix-probed rebind")
	}
}

// rebind same-day re-trigger (same certificate): the existing name is reused;
// no new import, no suffix probe.
func TestDeployRebind_SameDaySameSerial_StaysIdempotent(t *testing.T) {
	base := sanitizeName("*.factory.bg")
	morningName := versionedName(base, testDate)
	certPEM, keyPEM := genTestCert(t, "*.factory.bg", 7600)
	f := newProfileFake(map[string]string{morningName: certPEM}, map[string][]string{"Jobs-Tech SSL Inspection": {morningName}})
	res := deployForTest(t, f, map[string]any{"vdom": "root", "replace_strategy": "rebind"}, "*.factory.bg", certPEM, keyPEM)
	if !res.Success {
		t.Fatalf("expected success, got: %s", res.Message)
	}
	if len(f.certs) != 1 {
		t.Errorf("expected no new import on same-serial retry, certs=%v", f.order)
	}
	if res.Details["certificate_name"] != morningName {
		t.Errorf("certificate_name = %v, want %q", res.Details["certificate_name"], morningName)
	}
	if collided, _ := res.Details["same_day_collision"].(bool); collided {
		t.Error("Details.same_day_collision must be false on idempotent reuse")
	}
}

// delete strategy (legacy): referenced cert returns an actionable error.
func TestDeployDelete_ReferencedReturnsHelpfulError(t *testing.T) {
	f := newRebindFake()
	noSettle(t)
	certPEM, keyPEM := genTestCert(t, "*.factory.bg", 6006)
	srv := f.serve(t)
	cd := &provider.CertificateData{CommonName: "*.factory.bg", CertificatePEM: certPEM, PrivateKeyPEM: keyPEM}
	res, err := (Provider{}).Deploy(context.Background(), cd, map[string]any{"vdom": "root", "replace_strategy": "delete"}, fgCreds(srv), func(int, string) {})
	noSecrets(t, cd, res, err)
	if err == nil {
		t.Fatal("expected delete strategy to fail on referenced cert")
	}
	if !strings.Contains(err.Error(), "replace_strategy=rebind") {
		t.Errorf("expected actionable hint, got: %v", err)
	}
}
