package fortigate

import (
	"context"
	"os"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// TestDeviceReadPaths exercises the provider's read paths and rebind planning
// against a real FortiGate, without performing any writes. It runs only when
// FG_HOST and FG_TOKEN are set, e.g.:
//
//	FG_HOST=192.168.100.98 FG_TOKEN=xxx go test -run TestDeviceReadPaths -v ./pkg/deploy/providers/fortigate/
func TestDeviceReadPaths(t *testing.T) {
	host := os.Getenv("FG_HOST")
	token := os.Getenv("FG_TOKEN")
	if host == "" || token == "" {
		t.Skip("set FG_HOST and FG_TOKEN to run device validation")
	}
	ctx := context.Background()
	c := newClient(host, token, "root")

	// 1. Connectivity / credentials.
	p := &Provider{}
	creds := map[string]any{"host": host, "api_token": token}
	cfg := map[string]any{"vdom": "root"}
	if err := p.ValidateCredentials(ctx, creds, cfg); err != nil {
		t.Fatalf("ValidateCredentials: %v", err)
	}

	// 2. List certs and confirm the family detection sees the deployer cert.
	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		t.Fatalf("listLocalCertNames: %v", err)
	}
	base := sanitizeName("*.factory.bg")
	inFamily := familyMatcher(base)
	var fam []string
	for _, n := range names {
		if inFamily(n) {
			fam = append(fam, n)
		}
	}
	t.Logf("local certs: %d; family(%s): %v", len(names), base, fam)
	if len(fam) == 0 {
		t.Errorf("expected at least one family member for base %q", base)
	}

	// 3. Dry-run rebind plan against real ssl-ssh-profiles (no PUT performed).
	newName := versionedName(base, "20260527")
	r, err := c.cmdbGet(ctx, "firewall/ssl-ssh-profile")
	if err != nil || !r.ok() {
		t.Fatalf("get ssl-ssh-profile: %v (resp ok=%v)", err, r != nil && r.ok())
	}
	var profiles []struct {
		Name       string     `json:"name"`
		ServerCert []namedRef `json:"server-cert"`
	}
	if err := jsonResults(r, &profiles); err != nil {
		t.Fatalf("decode profiles: %v", err)
	}
	plannedChanges := 0
	for _, pr := range profiles {
		newList, changed := replaceInList(pr.ServerCert, inFamily, newName)
		if !changed {
			continue
		}
		plannedChanges++
		var before, after []string
		for _, e := range pr.ServerCert {
			before = append(before, e.Name)
		}
		for _, e := range newList {
			after = append(after, e.Name)
		}
		t.Logf("PLAN profile %q: %v -> %v", pr.Name, before, after)
	}
	t.Logf("planned profile rebinds: %d (target new name=%s)", plannedChanges, newName)

	// 3b. ssl_profile (default) decision against the real device, read-only.
	hits, err := scanReferences(ctx, c, inFamily)
	if err != nil {
		t.Fatalf("scanReferences: %v", err)
	}
	conventional := profileNameFor(base, defaultProfileSuffix)
	var foreign []string
	for _, h := range hits {
		if h.Holder == holderSSLSSHProfile && h.Object == conventional {
			continue
		}
		foreign = append(foreign, h.Holder+":"+h.Object+"→"+h.Cert)
	}
	if len(foreign) > 0 {
		t.Logf("ssl_profile DECISION: BAIL (manual review). conventional=%q, foreign refs=%v", conventional, foreign)
	} else {
		t.Logf("ssl_profile DECISION: safe to create/edit conventional profile %q", conventional)
	}

	// 4. Provider Verify (read-only) should find the family.
	vres, err := p.Verify(ctx, &registry.CertificateData{CommonName: "*.factory.bg"}, cfg, creds)
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	t.Logf("Verify: success=%v msg=%q resource=%q", vres.Success, vres.Message, vres.ResourceID)
	if !vres.Success {
		t.Errorf("expected Verify success")
	}
}
