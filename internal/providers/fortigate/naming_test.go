package fortigate

import (
	"math/big"
	"testing"
)

// T107: vectors ported from the v3 provider's naming_test.go.

func TestVersionedName(t *testing.T) {
	tests := []struct{ name, base, date, want string }{
		{"short", "star_factory_bg", "20260527", "star_factory_bg_20260527"},
		{"apex", "factory_bg", "20260527", "factory_bg_20260527"},
		{"truncates to 35", "abcdefghijklmnopqrstuvwxyz012345", "20260527", "abcdefghijklmnopqrstuvwxyz_20260527"},
	}
	for _, tt := range tests {
		got := versionedName(tt.base, tt.date)
		if got != tt.want || len(got) > fortiNameMaxLen {
			t.Errorf("%s: versionedName(%q,%q) = %q, want %q", tt.name, tt.base, tt.date, got, tt.want)
		}
	}
}

func TestSuffixedVersionedName(t *testing.T) {
	cases := []struct {
		base, date string
		seq        int
		want       string
	}{
		{"star_factory_bg", "20260527", 1, "star_factory_bg_20260527_01"},
		{"app", "20260527", 99, "app_20260527_99"},
		{"verylongbase_aaaaaaaaaaaaaaaaaaaaa", "20260527", 1, "verylongbase_aaaaaaaaaa_20260527_01"},
	}
	for _, tc := range cases {
		got := suffixedVersionedName(tc.base, tc.date, tc.seq)
		if got != tc.want || len(got) > fortiNameMaxLen {
			t.Errorf("suffixedVersionedName(%q,%q,%d) = %q, want %q", tc.base, tc.date, tc.seq, got, tc.want)
		}
	}
}

func TestFamilyMatcher(t *testing.T) {
	inFamily := familyMatcher("star_factory_bg")
	for _, n := range []string{
		"star_factory_bg", "star_factory_bg_20260527", "star_factory_bg_20250101",
		"star_factory_bg_20260527_01", "star_factory_bg_20260527_99",
	} {
		if !inFamily(n) {
			t.Errorf("expected %q in family", n)
		}
	}
	for _, n := range []string{
		"star_factory_bg_2025", "star_factory_bg_LE", "star_jobs_bg_2025", "star_factory_bg_2026052",
		"factory_bg", "star_factory_bg_x20260527", "star_factory_bg_20260527_1",
		"star_factory_bg_20260527_AB", "star_factory_bg_20260527_100",
	} {
		if inFamily(n) {
			t.Errorf("expected %q NOT in family", n)
		}
	}
	// A long base matches both truncations.
	long := "verylongbase_aaaaaaaaaaaaaaaaaaaaa"
	lf := familyMatcher(long)
	for _, n := range []string{long, versionedName(long, "20260527"), suffixedVersionedName(long, "20260527", 3)} {
		if !lf(n) {
			t.Errorf("long family misses %q", n)
		}
	}
}

func TestReplaceInList(t *testing.T) {
	inFamily := familyMatcher("star_factory_bg")
	out, changed := replaceInList([]namedRef{{"star_hire_bg_LE"}, {"star_jobs_bg_2025"}, {"star_factory_bg"}}, inFamily, "star_factory_bg_20260527")
	assertNames(t, out, "star_hire_bg_LE", "star_jobs_bg_2025", "star_factory_bg_20260527")
	if !changed {
		t.Fatal("expected changed")
	}
	out, changed = replaceInList([]namedRef{{"star_factory_bg"}, {"star_factory_bg_20260527"}, {"other"}}, inFamily, "star_factory_bg_20260527")
	assertNames(t, out, "star_factory_bg_20260527", "other")
	if !changed {
		t.Fatal("expected changed (duplicate dropped)")
	}
	if _, changed = replaceInList([]namedRef{{"unrelated"}, {"star_factory_bg_20260527"}}, inFamily, "star_factory_bg_20260527"); changed {
		t.Fatal("expected unchanged")
	}
}

func assertNames(t *testing.T, got []namedRef, want ...string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("names = %v, want %v", got, want)
	}
	for i := range want {
		if got[i].Name != want[i] {
			t.Fatalf("names = %v, want %v", got, want)
		}
	}
}

func TestNormalizeSerial(t *testing.T) {
	if normalizeSerial("00ab") != "AB" || normalizeSerial("000") != "0" {
		t.Fatal("normalizeSerial")
	}
	if s := normalizeSerial(big.NewInt(4242).Text(16)); s != "1092" {
		t.Fatalf("serial = %s", s)
	}
}
