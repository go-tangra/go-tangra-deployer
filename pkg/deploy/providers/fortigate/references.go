package fortigate

import (
	"context"
	"fmt"
)

// rebindResult summarizes the reference repointing performed during a deploy.
type rebindResult struct {
	Rebound []string // human-readable descriptions of each change applied
	Errors  []string // non-fatal failures encountered per holder
}

// namedRef matches FortiOS table entries shaped like {"name": "...", ...}.
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
	Holder string // holder type/path (one of the holder* constants)
	Object string // the referencing object's name
	Cert   string // the referenced certificate name
}

// scanReferences finds, read-only, every object that references a member of the
// certificate family. It is used by the ssl_profile strategy to decide whether
// a renewal is safe to auto-bind. Any API failure is returned so the caller can
// fall back to a manual-review notification rather than guess.
func scanReferences(ctx context.Context, c *fgClient, inFamily func(string) bool) ([]referenceHit, error) {
	var hits []referenceHit

	r, err := c.cmdbGet(ctx, "firewall/ssl-ssh-profile")
	if err != nil {
		return nil, err
	}
	if !r.ok() {
		return nil, apiError("list ssl-ssh-profiles", r)
	}
	var profiles []struct {
		Name       string     `json:"name"`
		ServerCert []namedRef `json:"server-cert"`
	}
	if err := jsonResults(r, &profiles); err != nil {
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
		hit, err := scanScalar(ctx, c, s.path, s.field, s.holder, inFamily)
		if err != nil {
			return nil, err
		}
		if hit != nil {
			hits = append(hits, *hit)
		}
	}

	vipHits, err := scanVIPs(ctx, c, inFamily)
	if err != nil {
		return nil, err
	}
	hits = append(hits, vipHits...)

	return hits, nil
}

func scanScalar(ctx context.Context, c *fgClient, path, field, holder string, inFamily func(string) bool) (*referenceHit, error) {
	r, err := c.cmdbGet(ctx, path)
	if err != nil {
		return nil, err
	}
	if !r.ok() {
		return nil, apiError("get "+path, r)
	}
	var obj map[string]any
	if err := jsonResults(r, &obj); err != nil {
		return nil, err
	}
	cur, _ := obj[field].(string)
	if cur != "" && inFamily(cur) {
		return &referenceHit{Holder: holder, Object: path, Cert: cur}, nil
	}
	return nil, nil
}

func scanVIPs(ctx context.Context, c *fgClient, inFamily func(string) bool) ([]referenceHit, error) {
	r, err := c.cmdbGet(ctx, "firewall/vip")
	if err != nil {
		return nil, err
	}
	if !r.ok() {
		return nil, apiError("list vips", r)
	}
	var vips []map[string]any
	if err := jsonResults(r, &vips); err != nil {
		return nil, err
	}
	var hits []referenceHit
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

// sslSSHProfile is the subset of an ssl-ssh-profile we read/write.
type sslSSHProfile struct {
	Name       string     `json:"name"`
	ServerCert []namedRef `json:"server-cert"`
}

// getSSLSSHProfile fetches a profile by name; found=false on 404.
func (c *fgClient) getSSLSSHProfile(ctx context.Context, name string) (*sslSSHProfile, bool, error) {
	r, err := c.cmdbGet(ctx, "firewall/ssl-ssh-profile/"+escapeMkey(name))
	if err != nil {
		return nil, false, err
	}
	if r.StatusCode == 404 {
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

// createSSLSSHProfile creates a "protecting SSL server" inspection profile that
// references cert (server-cert-mode=replace).
func (c *fgClient) createSSLSSHProfile(ctx context.Context, name, cert string) error {
	body := map[string]any{
		"name":             name,
		"server-cert-mode": "replace",
		"server-cert":      []map[string]string{{"name": cert}},
	}
	r, err := c.cmdbPost(ctx, "firewall/ssl-ssh-profile", body)
	if err != nil {
		return err
	}
	if !r.ok() {
		return apiError("create ssl-ssh-profile "+name, r)
	}
	return nil
}

// setSSLSSHProfileServerCert replaces a profile's server-cert list.
func (c *fgClient) setSSLSSHProfileServerCert(ctx context.Context, name string, list []namedRef) error {
	r, err := c.cmdbPut(ctx, "firewall/ssl-ssh-profile/"+escapeMkey(name), map[string]any{"server-cert": toNameList(list)})
	if err != nil {
		return err
	}
	if !r.ok() {
		return apiError("update ssl-ssh-profile "+name, r)
	}
	return nil
}

func containsName(list []namedRef, name string) bool {
	for _, e := range list {
		if e.Name == name {
			return true
		}
	}
	return false
}

// rebindReferences repoints every reference that currently points at a member
// of the certificate family (per inFamily) to newName. It scans the holder
// types that can bind a local certificate: SSL/SSH inspection profiles
// (server-cert list), the SSL-VPN server cert, the admin GUI cert, and VIP
// SSL certificates. Failures are collected per-holder and never abort the
// scan, so a renewal still completes even if one object can't be updated.
func rebindReferences(ctx context.Context, c *fgClient, inFamily func(string) bool, newName string) rebindResult {
	var res rebindResult
	rebindSSLSSHProfiles(ctx, c, inFamily, newName, &res)
	rebindScalar(ctx, c, "vpn.ssl/settings", "servercert", inFamily, newName, &res)
	rebindScalar(ctx, c, "system/global", "admin-server-cert", inFamily, newName, &res)
	rebindVIPs(ctx, c, inFamily, newName, &res)
	return res
}

// rebindSSLSSHProfiles updates firewall/ssl-ssh-profile.server-cert lists.
func rebindSSLSSHProfiles(ctx context.Context, c *fgClient, inFamily func(string) bool, newName string, res *rebindResult) {
	r, err := c.cmdbGet(ctx, "firewall/ssl-ssh-profile")
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("list ssl-ssh-profiles: %v", err))
		return
	}
	if !r.ok() {
		res.Errors = append(res.Errors, apiError("list ssl-ssh-profiles", r).Error())
		return
	}
	var profiles []struct {
		Name       string     `json:"name"`
		ServerCert []namedRef `json:"server-cert"`
	}
	if err := jsonResults(r, &profiles); err != nil {
		res.Errors = append(res.Errors, err.Error())
		return
	}

	for _, p := range profiles {
		newList, changed := replaceInList(p.ServerCert, inFamily, newName)
		if !changed {
			continue
		}
		body := map[string]any{"server-cert": toNameList(newList)}
		put, err := c.cmdbPut(ctx, "firewall/ssl-ssh-profile/"+escapeMkey(p.Name), body)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("update ssl-ssh-profile %q: %v", p.Name, err))
			continue
		}
		if !put.ok() {
			res.Errors = append(res.Errors, apiError(fmt.Sprintf("update ssl-ssh-profile %q", p.Name), put).Error())
			continue
		}
		res.Rebound = append(res.Rebound, fmt.Sprintf("ssl-ssh-profile %q server-cert → %s", p.Name, newName))
	}
}

// rebindScalar handles a complex singleton object with a single scalar cert
// field (e.g. vpn.ssl/settings.servercert, system/global.admin-server-cert).
func rebindScalar(ctx context.Context, c *fgClient, path, field string, inFamily func(string) bool, newName string, res *rebindResult) {
	r, err := c.cmdbGet(ctx, path)
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("get %s: %v", path, err))
		return
	}
	if !r.ok() {
		res.Errors = append(res.Errors, apiError("get "+path, r).Error())
		return
	}
	var obj map[string]any
	if err := jsonResults(r, &obj); err != nil {
		res.Errors = append(res.Errors, err.Error())
		return
	}
	cur, _ := obj[field].(string)
	if cur == "" || cur == newName || !inFamily(cur) {
		return
	}
	put, err := c.cmdbPut(ctx, path, map[string]any{field: newName})
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("update %s.%s: %v", path, field, err))
		return
	}
	if !put.ok() {
		res.Errors = append(res.Errors, apiError(fmt.Sprintf("update %s.%s", path, field), put).Error())
		return
	}
	res.Rebound = append(res.Rebound, fmt.Sprintf("%s.%s %s → %s", path, field, cur, newName))
}

// rebindVIPs handles firewall/vip.ssl-certificate, which is a scalar string on
// older FortiOS and a list of {name} on newer versions.
func rebindVIPs(ctx context.Context, c *fgClient, inFamily func(string) bool, newName string, res *rebindResult) {
	r, err := c.cmdbGet(ctx, "firewall/vip")
	if err != nil {
		res.Errors = append(res.Errors, fmt.Sprintf("list vips: %v", err))
		return
	}
	if !r.ok() {
		res.Errors = append(res.Errors, apiError("list vips", r).Error())
		return
	}
	var vips []map[string]any
	if err := jsonResults(r, &vips); err != nil {
		res.Errors = append(res.Errors, err.Error())
		return
	}

	for _, v := range vips {
		name, _ := v["name"].(string)
		body, desc := vipRebindBody(v["ssl-certificate"], inFamily, newName)
		if body == nil {
			continue
		}
		put, err := c.cmdbPut(ctx, "firewall/vip/"+escapeMkey(name), body)
		if err != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("update vip %q: %v", name, err))
			continue
		}
		if !put.ok() {
			res.Errors = append(res.Errors, apiError(fmt.Sprintf("update vip %q", name), put).Error())
			continue
		}
		res.Rebound = append(res.Rebound, fmt.Sprintf("vip %q ssl-certificate %s → %s", name, desc, newName))
	}
}

// vipRebindBody builds the PUT body for a VIP's ssl-certificate field if it
// references the family. Returns (nil, "") when no change is needed.
func vipRebindBody(raw any, inFamily func(string) bool, newName string) (map[string]any, string) {
	switch sc := raw.(type) {
	case string:
		if sc == "" || sc == newName || !inFamily(sc) {
			return nil, ""
		}
		return map[string]any{"ssl-certificate": newName}, sc
	case []any:
		list := make([]namedRef, 0, len(sc))
		for _, e := range sc {
			if m, ok := e.(map[string]any); ok {
				n, _ := m["name"].(string)
				list = append(list, namedRef{Name: n})
			}
		}
		newList, changed := replaceInList(list, inFamily, newName)
		if !changed {
			return nil, ""
		}
		return map[string]any{"ssl-certificate": toNameList(newList)}, "[list]"
	default:
		return nil, ""
	}
}

// replaceInList swaps every family member in the list for newName and removes
// duplicates, preserving order and all unrelated entries. It reports whether
// any change was made.
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
			changed = true // dropped a duplicate produced by the swap
			continue
		}
		seen[name] = true
		out = append(out, namedRef{Name: name})
	}
	return out, changed
}

func toNameList(list []namedRef) []map[string]string {
	out := make([]map[string]string, 0, len(list))
	for _, e := range list {
		out = append(out, map[string]string{"name": e.Name})
	}
	return out
}
