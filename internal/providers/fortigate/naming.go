package fortigate

// Versioned certificate names and serial lookup for the default_ssl_profile
// mode (feature 033 US8, research D27), ported from the v3 provider's
// certificate.go. A renewal is imported under a new dated name instead of
// deleting and re-importing under the same name, because FortiOS refuses to
// delete a certificate an SSL/SSH profile still references (F13).

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// dateVersionLen is the length of the "_YYYYMMDD" suffix used for versioning.
const dateVersionLen = 9

// sequenceVersionLen is the length of the "_NN" suffix that disambiguates two
// different certificates imported on the same day.
const sequenceVersionLen = 3

// maxSameDaySequence caps the same-day suffix at "_99"; more renewals of one
// certificate in a single UTC day indicate an upstream loop.
const maxSameDaySequence = 99

// now is the clock used for dated names; tests replace it.
var now = time.Now

// versionedName builds a dated name, e.g. "www_example_com_20260527". The
// base is truncated so the name fits FortiGate's 35-character limit.
func versionedName(base, date string) string {
	vbase := base
	if len(vbase)+dateVersionLen > fortiNameMaxLen {
		vbase = strings.TrimRight(vbase[:fortiNameMaxLen-dateVersionLen], "_")
	}
	return vbase + "_" + date
}

// suffixedVersionedName builds a same-day variant, e.g. "<base>_20260527_01".
func suffixedVersionedName(base, date string, seq int) string {
	vbase := base
	overhead := dateVersionLen + sequenceVersionLen
	if len(vbase)+overhead > fortiNameMaxLen {
		vbase = strings.TrimRight(vbase[:fortiNameMaxLen-overhead], "_")
	}
	return fmt.Sprintf("%s_%s_%02d", vbase, date, seq)
}

// familyMatcher matches the bare base name, its dated versions
// "<base>_YYYYMMDD" and same-day variants "<base>_YYYYMMDD_NN", for both
// truncations of a long base, and nothing else (a manual "<base>_2025" or
// "<base>_LE" is not part of the family).
func familyMatcher(base string) func(string) bool {
	vbase := base
	if len(vbase)+dateVersionLen > fortiNameMaxLen {
		vbase = strings.TrimRight(vbase[:fortiNameMaxLen-dateVersionLen], "_")
	}
	sbase := base
	if len(sbase)+dateVersionLen+sequenceVersionLen > fortiNameMaxLen {
		sbase = strings.TrimRight(sbase[:fortiNameMaxLen-dateVersionLen-sequenceVersionLen], "_")
	}
	reDated := regexp.MustCompile("^" + regexp.QuoteMeta(vbase) + `_\d{8}$`)
	reSeq := regexp.MustCompile("^" + regexp.QuoteMeta(sbase) + `_\d{8}_\d{2}$`)
	return func(name string) bool {
		return name == base || reDated.MatchString(name) || reSeq.MatchString(name)
	}
}

// resolveFreeImportName returns a name free to import to: the dated name, or
// the first free "_NN" variant when a different certificate already took it
// today (same-serial reuse is handled before). collided reports a suffix.
func (c *fgClient) resolveFreeImportName(ctx context.Context, base, date string) (name string, collided bool, err error) {
	preferred := versionedName(base, date)
	exists, err := c.certExists(ctx, preferred)
	if err != nil {
		return "", false, fmt.Errorf("check certificate name %s: %w", preferred, err)
	}
	if !exists {
		return preferred, false, nil
	}
	for seq := 1; seq <= maxSameDaySequence; seq++ {
		candidate := suffixedVersionedName(base, date, seq)
		exists, err := c.certExists(ctx, candidate)
		if err != nil {
			return "", false, fmt.Errorf("check certificate name %s: %w", candidate, err)
		}
		if !exists {
			return candidate, true, nil
		}
	}
	return "", false, fmt.Errorf("no free import name for %s on %s after %d same-day suffixes", base, date, maxSameDaySequence)
}

// normalizeSerial renders a hex serial uppercase without leading zeros.
func normalizeSerial(hexStr string) string {
	s := strings.ToUpper(strings.TrimLeft(hexStr, "0"))
	if s == "" {
		return "0"
	}
	return s
}

// certSerial returns the normalized hex serial of a certificate.
func certSerial(c *x509.Certificate) string {
	return normalizeSerial(fmt.Sprintf("%X", c.SerialNumber))
}

// jsonResults decodes the FortiOS "results" member into out.
func jsonResults(r *apiResponse, out any) error {
	if len(r.Env.Results) == 0 {
		return nil
	}
	if err := json.Unmarshal(r.Env.Results, out); err != nil {
		return fmt.Errorf("fortigate: decode results: %w", err)
	}
	return nil
}

// localCert is the subset of a certificate/local entry read here.
type localCert struct {
	Name        string `json:"name"`
	Certificate string `json:"certificate"`
}

// listLocalCerts returns every local certificate (name and PEM).
func (c *fgClient) listLocalCerts(ctx context.Context) ([]localCert, error) {
	r, err := c.do(ctx, http.MethodGet, "cmdb/certificate/local", nil)
	if err != nil {
		return nil, err
	}
	if !r.ok() {
		return nil, apiError("list certificates", r)
	}
	var list []localCert
	if err := jsonResults(r, &list); err != nil {
		return nil, err
	}
	return list, nil
}

// getLocalCertPEM returns the PEM of one local certificate ("" when absent).
func (c *fgClient) getLocalCertPEM(ctx context.Context, name string) (string, error) {
	r, err := c.do(ctx, http.MethodGet, "cmdb/certificate/local/"+escapeMkey(name), nil)
	if err != nil {
		return "", err
	}
	if r.StatusCode == http.StatusNotFound {
		return "", nil
	}
	if !r.ok() {
		return "", apiError("get certificate "+name, r)
	}
	var list []localCert
	if err := jsonResults(r, &list); err != nil {
		return "", err
	}
	if len(list) == 0 {
		return "", nil
	}
	return list[0].Certificate, nil
}

// findLocalCertBySerial returns the name of the local certificate with this
// serial, or "" when none is present. FortiOS rejects re-importing identical
// content (-145), so an already present certificate is reused. The PEM comes
// from the listing when FortiOS includes it, otherwise from a per-name read
// (v3 behaviour); unreadable entries are skipped.
func (c *fgClient) findLocalCertBySerial(ctx context.Context, list []localCert, serial string) string {
	for _, lc := range list {
		pemStr := lc.Certificate
		if pemStr == "" {
			var err error
			if pemStr, err = c.getLocalCertPEM(ctx, lc.Name); err != nil {
				continue
			}
		}
		if !strings.Contains(pemStr, "BEGIN CERTIFICATE") {
			continue
		}
		leaf, err := parseLeaf(pemStr)
		if err != nil {
			continue
		}
		if certSerial(leaf) == serial {
			return lc.Name
		}
	}
	return ""
}

// escapeMkey escapes a FortiOS object key for a URL path segment.
func escapeMkey(mkey string) string { return url.PathEscape(mkey) }
