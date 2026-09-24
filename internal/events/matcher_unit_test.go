package events

import (
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// These white-box unit tests target the unexported compile/build/matchPattern
// helpers and the Matches/MatchesAny branches that matcher_test.go and
// FuzzMatcher do not already exercise (notably the empty-pattern "no criterion"
// path and the compiled-pattern cache hit). They intentionally avoid
// duplicating assertions already present in matcher_test.go.

func TestCompileEmptyPatternIsNil(t *testing.T) {
	if re := compile(""); re != nil {
		t.Fatalf("compile(\"\") = %v, want nil (no criterion)", re)
	}
}

func TestCompileOverlongIsSentinel(t *testing.T) {
	long := strings.Repeat("a", MaxPatternLen+1)
	if re := compile(long); re != errPattern {
		t.Fatalf("compile(overlong) = %v, want the never-matching sentinel", re)
	}
}

func TestCompileCacheHit(t *testing.T) {
	// A distinctive pattern that no other test compiles, so the first call
	// stores it and the second must return the identical cached *Regexp.
	const pat = "cache-hit-probe\\.example\\.test"
	first := compile(pat)
	second := compile(pat)
	if first == nil || first != second {
		t.Fatalf("compile cache miss: first=%p second=%p", first, second)
	}
}

func TestMatchPatternEmptyMatches(t *testing.T) {
	// An empty pattern is "no criterion" and matches regardless of the values,
	// including with no values at all.
	if !matchPattern("") {
		t.Fatal("empty pattern with no values must match (no criterion)")
	}
	if !matchPattern("", "anything", "at", "all") {
		t.Fatal("empty pattern must match any values (no criterion)")
	}
}

func TestMatchPatternOverlongMatchesNothing(t *testing.T) {
	long := strings.Repeat("a", MaxPatternLen+1)
	if matchPattern(long, strings.Repeat("a", MaxPatternLen+1)) {
		t.Fatal("overlong pattern must match nothing (ReDoS guard, SR-004)")
	}
}

func TestMatchPatternMultipleValues(t *testing.T) {
	// The matcher scans all values and reports true on the first hit.
	if !matchPattern("*.example.com", "not-a-match", "host.example.com") {
		t.Fatal("a later value that matches should still count")
	}
	// None of the values match → false.
	if matchPattern("*.example.com", "a.other.com", "b.other.net") {
		t.Fatal("no matching value should yield false")
	}
}

func TestBuildLeadingWildcardAnchoring(t *testing.T) {
	// A leading-wildcard glob matches exactly one left label and is anchored:
	// it must not match a bare apex or a multi-label left side.
	re := build("*.example.com")
	cases := []struct {
		in   string
		want bool
	}{
		{"api.example.com", true},
		{"example.com", false},          // no left label
		{"a.b.example.com", false},      // more than one left label ([^.]+)
		{"api.example.com.evil", false}, // trailing suffix rejected by the $ anchor
		{"xapi.example.com", true},      // any single non-dot label is fine
	}
	for _, c := range cases {
		if got := re.MatchString(c.in); got != c.want {
			t.Fatalf("build(*.example.com).MatchString(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestBuildPlainRegexAnchored(t *testing.T) {
	// A plain regex is anchored on both ends, so a partial match is rejected.
	re := build("api\\..*")
	if !re.MatchString("api.example.com") {
		t.Fatal("anchored regex should match api.example.com")
	}
	if re.MatchString("xapi.example.com") {
		t.Fatal("anchored regex must not match a non-prefix occurrence")
	}
}

func TestBuildAlreadyAnchoredPattern(t *testing.T) {
	// A pattern that already carries ^ and $ must not be double-anchored.
	re := build("^host\\.example\\.com$")
	if !re.MatchString("host.example.com") {
		t.Fatal("already-anchored pattern should match its literal")
	}
	if re.MatchString("host.example.com.extra") {
		t.Fatal("already-anchored pattern must stay anchored")
	}
}

func TestBuildLiteralFallback(t *testing.T) {
	// An invalid regex falls back to a literal, exact match.
	re := build("([unterminated")
	if !re.MatchString("([unterminated") {
		t.Fatal("literal fallback must match the pattern text exactly")
	}
	if re.MatchString("prefix([unterminated") {
		t.Fatal("literal fallback must be anchored (exact match only)")
	}
}

func TestMatchesCommonNameMismatch(t *testing.T) {
	// Exercises the CommonNamePattern != "" && !matchPattern(...) → false branch
	// in Matches (matcher_test.go covers CN matches but not a CN-only miss).
	s := CertificateSubject{CommonName: "api.example.com", IssuerName: "CA"}
	if Matches(store.CertificateFilter{CommonNamePattern: "*.other.com"}, s) {
		t.Fatal("non-matching CN pattern must fail the filter")
	}
}

func TestMatchesSANMismatch(t *testing.T) {
	// Exercises the SANPattern != "" && !matchPattern(...) → false branch.
	s := CertificateSubject{SANs: []string{"api.example.com"}}
	if Matches(store.CertificateFilter{SANPattern: "*.other.com"}, s) {
		t.Fatal("non-matching SAN pattern must fail the filter")
	}
}

func TestMatchesOrgAndCountryMismatch(t *testing.T) {
	s := CertificateSubject{Organization: "Acme", Country: "US"}
	if Matches(store.CertificateFilter{SubjectOrganization: "Other"}, s) {
		t.Fatal("wrong organization must fail")
	}
	if Matches(store.CertificateFilter{SubjectCountry: "DE"}, s) {
		t.Fatal("wrong country must fail")
	}
}

func TestMatchesAnyTrueFalseAndEmptyList(t *testing.T) {
	s := CertificateSubject{IssuerName: "CA-2", CommonName: "api.example.com"}
	filters := []store.CertificateFilter{
		{IssuerName: "CA-1"}, // does not match
		{IssuerName: "CA-2"}, // matches → MatchesAny true
	}
	if !MatchesAny(filters, s) {
		t.Fatal("MatchesAny must be true when any filter matches")
	}
	none := []store.CertificateFilter{{IssuerName: "CA-1"}, {IssuerName: "CA-3"}}
	if MatchesAny(none, s) {
		t.Fatal("MatchesAny must be false when no filter matches")
	}
	// Empty (non-nil) filter list matches all.
	if !MatchesAny([]store.CertificateFilter{}, s) {
		t.Fatal("empty filter list must match all")
	}
}
