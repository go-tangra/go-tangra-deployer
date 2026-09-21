package events

import (
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/store"
)

func subj() CertificateSubject {
	return CertificateSubject{
		IssuerName: "Pebble Intermediate CA", CommonName: "api.example.com",
		SANs: []string{"api.example.com", "www.example.com"}, Organization: "Acme",
		OrgUnit: "Eng", Country: "US",
	}
}

func TestEmptyFilterMatchesAll(t *testing.T) {
	if !Matches(store.CertificateFilter{}, subj()) {
		t.Fatal("empty filter must match all")
	}
	if !MatchesAny(nil, subj()) {
		t.Fatal("no filters must match all")
	}
}

func TestExactFields(t *testing.T) {
	s := subj()
	if !Matches(store.CertificateFilter{IssuerName: "Pebble Intermediate CA", SubjectOrganization: "Acme", SubjectCountry: "US"}, s) {
		t.Fatal("exact match should pass")
	}
	if Matches(store.CertificateFilter{IssuerName: "Other CA"}, s) {
		t.Fatal("wrong issuer must fail")
	}
	if Matches(store.CertificateFilter{SubjectOrgUnit: "Sales"}, s) {
		t.Fatal("wrong OU must fail")
	}
}

func TestPatterns(t *testing.T) {
	s := subj()
	for _, p := range []string{"api.example.com", "*.example.com", "^api\\..*", "api\\.example\\.com"} {
		if !Matches(store.CertificateFilter{CommonNamePattern: p}, s) {
			t.Fatalf("CN pattern %q should match %q", p, s.CommonName)
		}
	}
	if Matches(store.CertificateFilter{CommonNamePattern: "*.other.com"}, s) {
		t.Fatal("*.other.com must not match api.example.com")
	}
	if !Matches(store.CertificateFilter{SANPattern: "www.example.com"}, s) {
		t.Fatal("SAN exact should match one of the SANs")
	}
	if !Matches(store.CertificateFilter{SANPattern: "*.example.com"}, s) {
		t.Fatal("SAN wildcard should match a SAN")
	}
}

func TestANDLogic(t *testing.T) {
	s := subj()
	// All criteria satisfied → match.
	if !Matches(store.CertificateFilter{IssuerName: "Pebble Intermediate CA", CommonNamePattern: "*.example.com", SubjectCountry: "US"}, s) {
		t.Fatal("all-satisfied AND should match")
	}
	// One criterion off → no match (AND).
	if Matches(store.CertificateFilter{IssuerName: "Pebble Intermediate CA", CommonNamePattern: "*.nope.com"}, s) {
		t.Fatal("one-off AND must fail")
	}
}

func TestOverlongPatternMatchesNothing(t *testing.T) {
	long := strings.Repeat("a", MaxPatternLen+1)
	if Matches(store.CertificateFilter{CommonNamePattern: long}, subj()) {
		t.Fatal("overlong pattern must not match (bounded, SR-004)")
	}
}

func TestBadRegexMatchesNothingNotError(t *testing.T) {
	// An invalid regex falls back to a literal; a truly unmatchable pattern
	// simply does not match — it never panics or hangs.
	if Matches(store.CertificateFilter{CommonNamePattern: "([unterminated"}, subj()) {
		t.Fatal("literal fallback should not match api.example.com")
	}
}

func FuzzMatcher(f *testing.F) {
	f.Add("*.example.com", "api.example.com")
	f.Add("(a+)+$", "aaaaaaaaaaaaaaaaaaaa!")
	f.Add(strings.Repeat("(a*)*", 50), "aaaaaaaaaa")
	f.Fuzz(func(t *testing.T, pattern, value string) {
		// Must never panic or hang regardless of pattern (RE2 is linear-time and
		// patterns are length-bounded). We only assert termination.
		_ = Matches(store.CertificateFilter{CommonNamePattern: pattern, SANPattern: pattern}, CertificateSubject{CommonName: value, SANs: []string{value}})
	})
}
