package fortigate

// Read-only reference scan and the in-place SSL/SSH profile update of the
// default_ssl_profile mode (feature 033 US8, research D27), ported from the
// v3 provider's references.go without the rebind/create machinery (Q9): the
// provider never rebinds a reference it does not own and never creates,
// deletes or recreates a profile.

import (
	"context"
	"net/http"
	"strconv"
)

// namedRef matches FortiOS table entries shaped like {"name": "..."}.
type namedRef struct {
	Name string `json:"name"`
}

// Holder identifiers for reference scanning.
const (
	holderSSLSSHProfile = "ssl-ssh-profile"
	holderSSLVPN        = "vpn.ssl/settings.servercert"
	holderAdminGUI      = "system/global.admin-server-cert"
	holderVIP           = "firewall/vip.ssl-certificate"
)

// referenceHit records one object that references a certificate.
type referenceHit struct {
	Holder string // one of the holder* constants
	Object string // the referencing object's name
	Cert   string // the referenced certificate name
}

// cmdbGet reads a cmdb object and fails on any non-2xx status.
func (c *fgClient) cmdbGet(ctx context.Context, path string, out any) error {
	r, err := c.do(ctx, http.MethodGet, "cmdb/"+path, nil)
	if err != nil {
		return err
	}
	if !r.ok() {
		return apiError("get "+path, r)
	}
	return jsonResults(r, out)
}

// scanReferences finds, read-only, every object that references a member of
// the certificate family: SSL/SSH profiles, the SSL-VPN server certificate,
// the administrator GUI certificate and VIPs. Any API failure is returned so
// the caller stops for manual review rather than guess.
func scanReferences(ctx context.Context, c *fgClient, inFamily func(string) bool) ([]referenceHit, error) {
	var hits []referenceHit

	var profiles []sslSSHProfile
	if err := c.cmdbGet(ctx, "firewall/ssl-ssh-profile", &profiles); err != nil {
		return nil, err
	}
	for _, p := range profiles {
		for _, sc := range p.ServerCert {
			if inFamily(sc.Name) {
				hits = append(hits, referenceHit{holderSSLSSHProfile, p.Name, sc.Name})
			}
		}
	}

	for _, s := range []struct{ path, field, holder string }{
		{"vpn.ssl/settings", "servercert", holderSSLVPN},
		{"system/global", "admin-server-cert", holderAdminGUI},
	} {
		var obj map[string]any
		if err := c.cmdbGet(ctx, s.path, &obj); err != nil {
			return nil, err
		}
		if cur, _ := obj[s.field].(string); cur != "" && inFamily(cur) {
			hits = append(hits, referenceHit{s.holder, s.path, cur})
		}
	}

	var vips []map[string]any
	if err := c.cmdbGet(ctx, "firewall/vip", &vips); err != nil {
		return nil, err
	}
	for _, v := range vips {
		name, _ := v["name"].(string)
		switch sc := v["ssl-certificate"].(type) {
		case string:
			if sc != "" && inFamily(sc) {
				hits = append(hits, referenceHit{holderVIP, name, sc})
			}
		case []any:
			for _, e := range sc {
				if m, ok := e.(map[string]any); ok {
					if n, _ := m["name"].(string); inFamily(n) {
						hits = append(hits, referenceHit{holderVIP, name, n})
					}
				}
			}
		}
	}
	return hits, nil
}

// sslSSHProfile is the subset of an ssl-ssh-profile read and written here.
type sslSSHProfile struct {
	Name           string     `json:"name"`
	ServerCert     []namedRef `json:"server-cert"`
	ServerCertMode string     `json:"server-cert-mode"`
}

// getSSLSSHProfile fetches a profile by name; found=false on 404.
func (c *fgClient) getSSLSSHProfile(ctx context.Context, name string) (*sslSSHProfile, bool, error) {
	r, err := c.do(ctx, http.MethodGet, "cmdb/firewall/ssl-ssh-profile/"+escapeMkey(name), nil)
	if err != nil {
		return nil, false, err
	}
	if r.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if !r.ok() {
		return nil, false, apiError("get ssl-ssh-profile "+name, r)
	}
	var list []sslSSHProfile
	if err := jsonResults(r, &list); err != nil {
		return nil, false, err
	}
	if len(list) == 0 {
		return nil, false, nil
	}
	return &list[0], true, nil
}

// setSSLSSHProfileServerCert updates a profile in place, sending only its
// server-cert list (every other profile setting is untouched).
func (c *fgClient) setSSLSSHProfileServerCert(ctx context.Context, name string, list []namedRef) error {
	r, err := c.do(ctx, http.MethodPut, "cmdb/firewall/ssl-ssh-profile/"+escapeMkey(name), map[string]any{"server-cert": toNameList(list)})
	if err != nil {
		return err
	}
	if !r.ok() {
		return apiError("update ssl-ssh-profile "+name, r)
	}
	return nil
}

// findPoliciesBoundToProfile lists the firewall policies using the profile
// (the blast radius of the update); best effort, reported in details only.
func (c *fgClient) findPoliciesBoundToProfile(ctx context.Context, name string) ([]string, error) {
	var pols []map[string]any
	if err := c.cmdbGet(ctx, "firewall/policy", &pols); err != nil {
		return nil, err
	}
	out := []string{}
	for _, p := range pols {
		if prof, _ := p["ssl-ssh-profile"].(string); prof == name {
			pname, _ := p["name"].(string)
			if pname == "" {
				if id, ok := p["policyid"].(float64); ok {
					pname = "#" + strconv.Itoa(int(id))
				}
			}
			if pname != "" {
				out = append(out, pname)
			}
		}
	}
	return out, nil
}

// replaceInList swaps every family member for newName and removes duplicates,
// keeping order and every unrelated entry. It reports whether anything changed.
func replaceInList(list []namedRef, inFamily func(string) bool, newName string) ([]namedRef, bool) {
	out := make([]namedRef, 0, len(list))
	seen := make(map[string]bool, len(list))
	changed := false
	for _, e := range list {
		name := e.Name
		if inFamily(name) && name != newName {
			changed = true
			name = newName
		}
		if seen[name] {
			changed = true // a duplicate produced by the swap
			continue
		}
		seen[name] = true
		out = append(out, namedRef{Name: name})
	}
	return out, changed
}

func containsName(list []namedRef, name string) bool {
	for _, e := range list {
		if e.Name == name {
			return true
		}
	}
	return false
}

func toNameList(list []namedRef) []map[string]string {
	out := make([]map[string]string, 0, len(list))
	for _, e := range list {
		out = append(out, map[string]string{"name": e.Name})
	}
	return out
}
