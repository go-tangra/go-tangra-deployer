package fortigate

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"regexp"
	"strings"
)

// fortiNameMaxLen is FortiGate's certificate-name length limit.
const fortiNameMaxLen = 35

// dateVersionLen is the length of the "_YYYYMMDD" suffix used for versioning.
const dateVersionLen = 9

// certExists reports whether a local certificate with the given name exists.
func (c *fgClient) certExists(ctx context.Context, name string) (bool, error) {
	r, err := c.cmdbGet(ctx, "certificate/local/"+escapeMkey(name))
	if err != nil {
		return false, err
	}
	if r.StatusCode == 404 {
		return false, nil
	}
	if !r.ok() {
		return false, apiError("check certificate", r)
	}
	return true, nil
}

// listLocalCertNames returns the names of all local certificates.
func (c *fgClient) listLocalCertNames(ctx context.Context) ([]string, error) {
	r, err := c.cmdbGet(ctx, "certificate/local")
	if err != nil {
		return nil, err
	}
	if !r.ok() {
		return nil, apiError("list certificates", r)
	}
	var results []struct {
		Name string `json:"name"`
	}
	if err := jsonResults(r, &results); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(results))
	for _, x := range results {
		names = append(names, x.Name)
	}
	return names, nil
}

// importCert imports a new local certificate. scope is "global" or "vdom".
func (c *fgClient) importCert(ctx context.Context, name, certPEM, keyPEM, scope string) error {
	payload := map[string]any{
		"type":             "regular",
		"certname":         name,
		"file_content":     base64.StdEncoding.EncodeToString([]byte(certPEM)),
		"key_file_content": base64.StdEncoding.EncodeToString([]byte(keyPEM)),
		"scope":            scope,
	}
	r, err := c.monitorPost(ctx, "vpn-certificate/local/import", payload)
	if err != nil {
		return err
	}
	if !r.ok() {
		return apiError("import certificate "+name, r)
	}
	return nil
}

// deleteCert deletes a local certificate. A 404 is treated as success.
func (c *fgClient) deleteCert(ctx context.Context, name string) error {
	r, err := c.cmdbDelete(ctx, "certificate/local/"+escapeMkey(name))
	if err != nil {
		return err
	}
	if r.StatusCode == 404 {
		return nil
	}
	if !r.ok() {
		return apiError("delete certificate "+name, r)
	}
	return nil
}

// jsonResults decodes the FortiOS "results" array into out.
func jsonResults(r *apiResponse, out any) error {
	if len(r.Env.Results) == 0 {
		return nil
	}
	if err := json.Unmarshal(r.Env.Results, out); err != nil {
		return fmt.Errorf("fortigate: decode results: %w", err)
	}
	return nil
}

// sanitizeName converts a common name to a valid FortiGate resource name.
// Uses the same rules as the BIG-IP provider so the same certificate ends up
// with a consistent identifier across devices:
//   - Wildcard "*" is replaced with "star"
//   - Dots "." are replaced with underscores "_"
//   - All other non-alphanumeric characters are replaced with underscores
//   - Multiple underscores are collapsed
//   - Leading/trailing underscores are trimmed
//   - A leading digit is prefixed with "cert_"
//   - The result is truncated to FortiGate's 35-character limit (with a
//     trailing-underscore trim afterwards so we don't leave a dangling "_")
//
// Examples:
//
//	"www.example.com"  → "www_example_com"
//	"*.example.com"    → "star_example_com"
//	"123.example.com"  → "cert_123_example_com"
func sanitizeName(name string) string {
	// Replace wildcard with "star" (first occurrence only; mirrors bigip).
	result := strings.Replace(name, "*", "star", 1)

	// Replace dots with underscores.
	result = strings.ReplaceAll(result, ".", "_")

	// Replace any remaining invalid characters with underscores.
	result = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '_' || r == '-' {
			return r
		}
		return '_'
	}, result)

	// Collapse multiple underscores.
	for strings.Contains(result, "__") {
		result = strings.ReplaceAll(result, "__", "_")
	}

	// Remove leading/trailing underscores.
	result = strings.Trim(result, "_")

	// Ensure it doesn't start with a number.
	if len(result) > 0 && result[0] >= '0' && result[0] <= '9' {
		result = "cert_" + result
	}

	// FortiGate-specific constraint: 35-character limit.
	if len(result) > fortiNameMaxLen {
		result = strings.TrimRight(result[:fortiNameMaxLen], "_")
	}

	return result
}

// parseLeaf parses the first certificate block of a PEM bundle into an x509
// certificate.
func parseLeaf(pemData string) (*x509.Certificate, error) {
	block, _ := pem.Decode([]byte(leafCertPEM(pemData)))
	if block == nil {
		return nil, fmt.Errorf("fortigate: no certificate PEM block found")
	}
	return x509.ParseCertificate(block.Bytes)
}

// normalizeSerial renders a hex serial uppercase with leading zeros stripped,
// so values like "033643883…" and "33643883…" compare equal.
func normalizeSerial(hexStr string) string {
	s := strings.ToUpper(strings.TrimLeft(hexStr, "0"))
	if s == "" {
		return "0"
	}
	return s
}

// certSerial returns the normalized hex serial of a parsed certificate.
func certSerial(c *x509.Certificate) string {
	return normalizeSerial(fmt.Sprintf("%X", c.SerialNumber))
}

// subjectBaseName derives the logical name to base FortiGate objects on from
// the certificate ITSELF (subject CN, then first DNS SAN), falling back to the
// supplied value. This avoids trusting external metadata (e.g. LCM's stored
// common_name), which can disagree with the actual certificate subject.
func subjectBaseName(c *x509.Certificate, fallback string) string {
	if c != nil {
		if cn := strings.TrimSpace(c.Subject.CommonName); cn != "" {
			return cn
		}
		if len(c.DNSNames) > 0 {
			return c.DNSNames[0]
		}
	}
	return fallback
}

// getLocalCertPEM returns the PEM of a local certificate's `certificate` field.
func (c *fgClient) getLocalCertPEM(ctx context.Context, name string) (string, error) {
	r, err := c.cmdbGet(ctx, "certificate/local/"+escapeMkey(name))
	if err != nil {
		return "", err
	}
	if r.StatusCode == 404 {
		return "", nil
	}
	if !r.ok() {
		return "", apiError("get certificate "+name, r)
	}
	var list []struct {
		Certificate string `json:"certificate"`
	}
	if err := jsonResults(r, &list); err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", nil
	}
	return list[0].Certificate, nil
}

// findLocalCertBySerial returns the name of the local certificate whose serial
// matches, or "" if none is present. This makes deployment idempotent: FortiOS
// rejects re-importing identical content (error -145), so an already-present
// certificate is reused rather than re-imported.
func (c *fgClient) findLocalCertBySerial(ctx context.Context, serial string) (string, error) {
	names, err := c.listLocalCertNames(ctx)
	if err != nil {
		return "", err
	}
	for _, name := range names {
		pemStr, err := c.getLocalCertPEM(ctx, name)
		if err != nil || !strings.Contains(pemStr, "BEGIN CERTIFICATE") {
			continue
		}
		leaf, err := parseLeaf(pemStr)
		if err != nil {
			continue
		}
		if certSerial(leaf) == serial {
			return name, nil
		}
	}
	return "", nil
}

// leafCertPEM returns only the first CERTIFICATE block from a PEM bundle.
// FortiOS's local (server) certificate import (type=regular) expects a single
// leaf certificate paired with the key; passing the full chain (leaf +
// intermediates) makes it reject the import with error -145 "the imported
// local certificate is invalid". Intermediates are not part of the local cert
// object on FortiGate.
func leafCertPEM(pemData string) string {
	rest := []byte(pemData)
	for {
		block, remainder := pem.Decode(rest)
		if block == nil {
			break
		}
		if block.Type == "CERTIFICATE" {
			return string(pem.EncodeToMemory(block))
		}
		rest = remainder
	}
	return pemData // no PEM block found; return as-is
}

// versionedName builds a unique, dated certificate name for a renewal, e.g.
// "star_factory_bg" + "20260527" → "star_factory_bg_20260527". The base is
// truncated if needed so the full name fits FortiGate's 35-char limit.
func versionedName(base, date string) string {
	vbase := base
	if len(vbase)+dateVersionLen > fortiNameMaxLen {
		vbase = strings.TrimRight(vbase[:fortiNameMaxLen-dateVersionLen], "_")
	}
	return vbase + "_" + date
}

// familyMatcher returns a predicate that matches the legacy bare base name and
// any dated version "<vbase>_YYYYMMDD" of it. This lets a renewal find prior
// deployments of the same logical certificate to rebind away from and prune,
// while never matching unrelated certs or manually-suffixed ones (e.g.
// "star_jobs_bg_2025" has a 4-digit suffix and is not matched).
func familyMatcher(base string) func(string) bool {
	vbase := base
	if len(vbase)+dateVersionLen > fortiNameMaxLen {
		vbase = strings.TrimRight(vbase[:fortiNameMaxLen-dateVersionLen], "_")
	}
	re := regexp.MustCompile("^" + regexp.QuoteMeta(vbase) + `_\d{8}$`)
	return func(name string) bool {
		return name == base || re.MatchString(name)
	}
}
