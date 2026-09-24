// Package events drives auto-deployment: it consumes lcm certificate lifecycle
// events from the platform bus and matches them against deployment targets'
// certificate-filter rules. This file holds the matcher, which is pure and
// security-sensitive: filter patterns are untrusted input, so pattern
// evaluation is length- and complexity-bounded to prevent ReDoS (spec SR-004).
package events

import (
	"regexp"
	"strings"
	"sync"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// MaxPatternLen bounds a single filter pattern; longer patterns never compile.
const MaxPatternLen = 256

// CertificateSubject is the material a filter matches against — read from the
// certificate the deployer fetches for an event (fetch-to-match, research §4).
type CertificateSubject struct {
	IssuerName   string
	CommonName   string
	SANs         []string
	Organization string
	OrgUnit      string
	Country      string
}

// compiled caches compiled patterns so a hot event stream does not recompile.
var compiled sync.Map // string -> *regexp.Regexp (or a sentinel for invalid)

var errPattern = regexp.MustCompile(`\x00`) // never-matching sentinel for bad patterns

// compile turns a filter pattern into a bounded, anchored regexp. It accepts a
// literal, a leading-`*` wildcard (e.g. "*.example.com"), or a plain regex, in
// that tolerance order, and rejects anything over MaxPatternLen. A pattern that
// will not compile yields a never-matching regexp so a bad rule matches nothing
// rather than erroring the whole event.
func compile(pat string) *regexp.Regexp {
	if pat == "" {
		return nil
	}
	if len(pat) > MaxPatternLen {
		return errPattern
	}
	if v, ok := compiled.Load(pat); ok {
		return v.(*regexp.Regexp)
	}
	re := build(pat)
	compiled.Store(pat, re)
	return re
}

func build(pat string) *regexp.Regexp {
	// A leading-wildcard glob like *.example.com — the common cert CN/SAN case.
	if strings.HasPrefix(pat, "*.") {
		g := "^" + regexp.QuoteMeta(pat[1:]) + "$" // ".example.com" quoted, matches a single left label via the star below
		g = "^[^.]+" + regexp.QuoteMeta(pat[1:]) + "$"
		if re, err := regexp.Compile(g); err == nil {
			return re
		}
	}
	// Try as a regex (anchored). RE2 (Go's regexp) is linear-time, so this is
	// inherently ReDoS-safe once length-bounded.
	anchored := pat
	if !strings.HasPrefix(anchored, "^") {
		anchored = "^" + anchored
	}
	if !strings.HasSuffix(anchored, "$") {
		anchored = anchored + "$"
	}
	if re, err := regexp.Compile(anchored); err == nil {
		return re
	}
	// Fall back to a literal match.
	if re, err := regexp.Compile("^" + regexp.QuoteMeta(pat) + "$"); err == nil {
		return re
	}
	return errPattern
}

// matchPattern reports whether any of values matches the (possibly empty) pattern.
// An empty pattern is ignored (matches). A bad pattern matches nothing.
func matchPattern(pat string, values ...string) bool {
	re := compile(pat)
	if re == nil {
		return true // no criterion
	}
	if re == errPattern {
		return false
	}
	for _, v := range values {
		if re.MatchString(v) {
			return true
		}
	}
	return false
}

// Matches reports whether the certificate subject satisfies the filter with AND
// logic. Exact fields (issuer/org/OU/country) compare literally; CN and SAN use
// the bounded pattern matcher. An empty filter matches all.
func Matches(f store.CertificateFilter, s CertificateSubject) bool {
	if f.Empty() {
		return true
	}
	if f.IssuerName != "" && f.IssuerName != s.IssuerName {
		return false
	}
	if f.SubjectOrganization != "" && f.SubjectOrganization != s.Organization {
		return false
	}
	if f.SubjectOrgUnit != "" && f.SubjectOrgUnit != s.OrgUnit {
		return false
	}
	if f.SubjectCountry != "" && f.SubjectCountry != s.Country {
		return false
	}
	if f.CommonNamePattern != "" && !matchPattern(f.CommonNamePattern, s.CommonName) {
		return false
	}
	if f.SANPattern != "" && !matchPattern(f.SANPattern, s.SANs...) {
		return false
	}
	return true
}

// MatchesAny reports whether any of the target's filters matches (OR across the
// filter list; a target with no filters matches all).
func MatchesAny(filters []store.CertificateFilter, s CertificateSubject) bool {
	if len(filters) == 0 {
		return true
	}
	for _, f := range filters {
		if Matches(f, s) {
			return true
		}
	}
	return false
}
