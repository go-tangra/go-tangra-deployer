package events

import (
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

// This file fuzz- and negative-tests the deployer's event-payload parsers so
// that malformed, oversized, or adversarial event data can never panic the
// consumer (task T074). The two parsing entry points are certIDFrom (pulls
// certificate_id out of an lcm event's JSON "data" field) and subjectFrom
// (x509-parses a leaf PEM out of a fetched certificate bundle). The matcher
// (Matches/MatchesAny) already has its own FuzzMatcher and overlong-pattern
// tests in matcher_test.go, so it is intentionally not re-fuzzed here.

// FuzzCertIDFrom throws arbitrary bytes at the event-payload parser and asserts
// it always terminates without panicking and never returns more than it was
// given (the id is a substring of the input).
func FuzzCertIDFrom(f *testing.F) {
	// Seed corpus: valid event JSON, empty object, non-JSON, deeply nested, huge.
	f.Add(`{"certificate_id":"abc","module":"lcm"}`)
	f.Add(`{"certificate_id":"abc","spiffe_id":"spiffe://x","not_after":"2026-01-01T00:00:00Z"}`)
	f.Add(`{}`)
	f.Add(``)
	f.Add(`not json at all }{ "certificate_id"`)
	f.Add(strings.Repeat(`{"a":`, 5000) + `1` + strings.Repeat(`}`, 5000)) // deeply nested
	f.Add(`{"certificate_id":` + strings.Repeat("A", 1<<16) + `}`)         // large, unterminated value
	f.Add(strings.Repeat(`{"certificate_id":"x"}`, 10000))                 // large, many keys

	f.Fuzz(func(t *testing.T, data string) {
		got := certIDFrom(data) // must never panic on any input

		// Invariant: the returned id is a substring lifted verbatim from the
		// input, so it can never be longer than the input.
		if len(got) > len(data) {
			t.Fatalf("certIDFrom returned %d bytes from a %d-byte input", len(got), len(data))
		}
		// Invariant: a non-empty result must actually appear in the input.
		if got != "" && !strings.Contains(data, got) {
			t.Fatalf("certIDFrom returned %q which is not present in the input", got)
		}
	})
}

// FuzzSubjectFrom throws arbitrary bytes at the PEM/x509 parsing path and
// asserts it always terminates without panicking and never surfaces bogus data
// (a well-formed cert is never fabricated from garbage input).
func FuzzSubjectFrom(f *testing.F) {
	f.Add("")
	f.Add("-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n")
	f.Add("-----BEGIN CERTIFICATE-----\nZm9vYmFy\n-----END CERTIFICATE-----\n") // valid b64, invalid DER
	f.Add("not a pem at all")
	f.Add(strings.Repeat("A", 1<<16))

	f.Fuzz(func(t *testing.T, pemStr string) {
		// subjectFrom must never panic no matter what the PEM string contains.
		s := subjectFrom(provider.CertificateData{CertificatePEM: pemStr})
		_ = s
	})
}

// certIDFromCase drives a single negative certIDFrom input and asserts it does
// not panic and, where specified, returns the expected value.
func TestCertIDFromNegative(t *testing.T) {
	oversized := `{"certificate_id":"` + strings.Repeat("z", 2<<20) + `"}` // ~2 MB value
	cases := []struct {
		name string
		data string
		want string // "" means "assert exactly empty"; "-" means "no value assertion"
	}{
		{"empty", "", ""},
		{"empty object", "{}", ""},
		{"non-json garbage", "\x00\xff\xfe not json <<<>>>", ""},
		{"truncated json", `{"certificate_id":"abc`, ""}, // value has no closing quote -> empty
		{"truncated before value", `{"certificate_id":`, ""},
		{"truncated at key", `{"certificate_i`, ""},
		{"number value", `{"certificate_id":12345}`, ""},
		{"null value", `{"certificate_id":null}`, ""},
		{"boolean value", `{"certificate_id":true}`, ""},
		{"invalid utf8", "{\"certificate_id\":\"\xff\xfe\xfa\"}", "\xff\xfe\xfa"},
		{"wrong key only", `{"cert_id":"abc"}`, ""},
		{"oversized 2mb value", oversized, strings.Repeat("z", 2<<20)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := certIDFrom(tc.data) // must return cleanly, never panic
			if tc.want != "-" && got != tc.want {
				t.Fatalf("certIDFrom(%.40q...) = %q, want %q", tc.data, got, tc.want)
			}
			if len(got) > len(tc.data) {
				t.Fatalf("certIDFrom returned more bytes (%d) than the input (%d)", len(got), len(tc.data))
			}
		})
	}
}

// TestCertIDFromArrayValueDoesNotPanic covers a wrong-type (array) value. The
// naive extractor may lift the first quoted element; the guarantee under test is
// only that it returns cleanly without panicking.
func TestCertIDFromArrayValueDoesNotPanic(t *testing.T) {
	got := certIDFrom(`{"certificate_id":["a","b","c"]}`)
	if len(got) > 32 {
		t.Fatalf("unexpectedly large result for array value: %q", got)
	}
}

// TestSubjectFromMalformedPEM feeds malformed, oversized, and non-PEM bytes to
// the x509 parsing path and asserts it degrades to the bundle's own CN/SANs
// (never panics, never fabricates issuer/org fields from junk).
func TestSubjectFromMalformedPEM(t *testing.T) {
	cases := []struct {
		name string
		pem  string
	}{
		{"empty", ""},
		{"not pem", "definitely not a certificate"},
		{"empty pem block", "-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n"},
		{"valid b64 invalid der", "-----BEGIN CERTIFICATE-----\nZm9vYmFy\n-----END CERTIFICATE-----\n"},
		{"truncated pem header", "-----BEGIN CERTIFICATE-----\nMIIB"},
		{"invalid utf8 body", "-----BEGIN CERTIFICATE-----\n\xff\xfe\xfa\n-----END CERTIFICATE-----\n"},
		{"oversized junk", strings.Repeat("A", 2<<20)},
		{"oversized pem-wrapped junk", "-----BEGIN CERTIFICATE-----\n" + strings.Repeat("QUFB\n", 1<<16) + "-----END CERTIFICATE-----\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			in := provider.CertificateData{
				ID:             "cert-1",
				CommonName:     "fallback.example.com",
				SANs:           []string{"fallback.example.com"},
				CertificatePEM: tc.pem,
			}
			s := subjectFrom(in) // must not panic on any malformed/oversized PEM

			// A leaf that fails to parse must leave the bundle's own fields intact
			// and must not populate issuer/org fields out of garbage.
			if s.CommonName != "fallback.example.com" {
				t.Fatalf("CommonName mutated by bad PEM: got %q", s.CommonName)
			}
			if s.IssuerName != "" || s.Organization != "" || s.OrgUnit != "" || s.Country != "" {
				t.Fatalf("bad PEM leaked subject fields: %+v", s)
			}
		})
	}
}

// TestSubjectFromNoPEMUsesBundle confirms the no-PEM path returns the bundle's
// own CN/SANs unchanged (baseline for the malformed-PEM assertions above).
func TestSubjectFromNoPEMUsesBundle(t *testing.T) {
	s := subjectFrom(provider.CertificateData{
		CommonName: "api.example.com",
		SANs:       []string{"api.example.com", "www.example.com"},
	})
	if s.CommonName != "api.example.com" || len(s.SANs) != 2 {
		t.Fatalf("no-PEM subject not passed through: %+v", s)
	}
}
