package fortigate

import (
	"strings"
	"testing"
)

func TestVersionedName(t *testing.T) {
	tests := []struct {
		name, base, date, want string
	}{
		{"short", "star_factory_bg", "20260527", "star_factory_bg_20260527"},
		{"apex", "factory_bg", "20260527", "factory_bg_20260527"},
		{"truncates to 35", "abcdefghijklmnopqrstuvwxyz012345", "20260527", "abcdefghijklmnopqrstuvwxyz_20260527"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := versionedName(tt.base, tt.date)
			if got != tt.want {
				t.Errorf("versionedName(%q,%q) = %q, want %q", tt.base, tt.date, got, tt.want)
			}
			if len(got) > fortiNameMaxLen {
				t.Errorf("name %q exceeds %d chars", got, fortiNameMaxLen)
			}
		})
	}
}

func TestFamilyMatcher(t *testing.T) {
	inFamily := familyMatcher("star_factory_bg")
	match := []string{
		"star_factory_bg",
		"star_factory_bg_20260527",
		"star_factory_bg_20250101",
		"star_factory_bg_20260527_01", // same-day suffix #1
		"star_factory_bg_20260527_99", // same-day suffix #99
	}
	noMatch := []string{
		"star_factory_bg_2025",        // 4-digit manual suffix, not a date
		"star_factory_bg_LE",          // label suffix
		"star_jobs_bg_2025",           // different base
		"star_factory_bg_2026052",     // 7 digits
		"factory_bg",                  // shorter, different base
		"star_factory_bg_x20260527",   // garbage prefix on date
		"star_factory_bg_20260527_1",  // single-digit seq, not the format
		"star_factory_bg_20260527_AB", // non-numeric seq
		"star_factory_bg_20260527_100", // 3-digit seq, not the format
	}
	for _, n := range match {
		if !inFamily(n) {
			t.Errorf("expected %q to be in family", n)
		}
	}
	for _, n := range noMatch {
		if inFamily(n) {
			t.Errorf("expected %q NOT to be in family", n)
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
		// 33-char base + "_YYYYMMDD_NN" overhead 12 → truncates to 23-char base.
		{"verylongbase_aaaaaaaaaaaaaaaaaaaaa", "20260527", 1, "verylongbase_aaaaaaaaaa_20260527_01"},
	}
	for _, tc := range cases {
		got := suffixedVersionedName(tc.base, tc.date, tc.seq)
		if got != tc.want {
			t.Errorf("suffixedVersionedName(%q,%q,%d) = %q, want %q", tc.base, tc.date, tc.seq, got, tc.want)
		}
		if len(got) > fortiNameMaxLen {
			t.Errorf("name %q exceeds %d chars", got, fortiNameMaxLen)
		}
	}
}

func TestReplaceInList(t *testing.T) {
	inFamily := familyMatcher("star_factory_bg")
	in := []namedRef{
		{Name: "star_hire_bg_LE"},
		{Name: "star_jobs_bg_2025"},
		{Name: "star_factory_bg"}, // family member → replace
	}
	out, changed := replaceInList(in, inFamily, "star_factory_bg_20260527")
	if !changed {
		t.Fatal("expected changed=true")
	}
	want := []string{"star_hire_bg_LE", "star_jobs_bg_2025", "star_factory_bg_20260527"}
	if len(out) != len(want) {
		t.Fatalf("len = %d, want %d (%v)", len(out), len(want), out)
	}
	for i, w := range want {
		if out[i].Name != w {
			t.Errorf("out[%d] = %q, want %q", i, out[i].Name, w)
		}
	}
}

func TestReplaceInList_DedupesAfterSwap(t *testing.T) {
	inFamily := familyMatcher("star_factory_bg")
	// Both the old and the already-new name present → result must have one.
	in := []namedRef{
		{Name: "star_factory_bg"},
		{Name: "star_factory_bg_20260527"},
		{Name: "other"},
	}
	out, changed := replaceInList(in, inFamily, "star_factory_bg_20260527")
	if !changed {
		t.Fatal("expected changed=true (duplicate dropped)")
	}
	want := []string{"star_factory_bg_20260527", "other"}
	if len(out) != len(want) {
		t.Fatalf("got %v, want %v", out, want)
	}
	for i, w := range want {
		if out[i].Name != w {
			t.Errorf("out[%d] = %q, want %q", i, out[i].Name, w)
		}
	}
}

func TestReplaceInList_NoChange(t *testing.T) {
	inFamily := familyMatcher("star_factory_bg")
	in := []namedRef{{Name: "unrelated"}, {Name: "star_factory_bg_20260527"}}
	_, changed := replaceInList(in, inFamily, "star_factory_bg_20260527")
	if changed {
		t.Error("expected changed=false when nothing in family except the target")
	}
}

func TestLeafCertPEM(t *testing.T) {
	leaf := "-----BEGIN CERTIFICATE-----\nLEAFLEAFLEAF\n-----END CERTIFICATE-----\n"
	inter := "-----BEGIN CERTIFICATE-----\nINTERINTER\n-----END CERTIFICATE-----\n"
	root := "-----BEGIN CERTIFICATE-----\nROOTROOT\n-----END CERTIFICATE-----\n"

	// A multi-cert bundle must collapse to just the first (leaf) block.
	got := leafCertPEM(leaf + inter + root)
	if c := countBlocks(got); c != 1 {
		t.Fatalf("expected 1 cert block, got %d: %q", c, got)
	}
	if !strings.Contains(got, "LEAFLEAFLEAF") || strings.Contains(got, "INTERINTER") {
		t.Errorf("leaf extraction wrong: %q", got)
	}

	// A single leaf is returned unchanged (one block).
	if c := countBlocks(leafCertPEM(leaf)); c != 1 {
		t.Errorf("single leaf should stay 1 block, got %d", c)
	}
}

func countBlocks(s string) int { return strings.Count(s, "BEGIN CERTIFICATE") }

func TestCfgBool(t *testing.T) {
	m := map[string]any{"a": true, "b": "false", "c": "yes", "d": "garbage"}
	if !cfgBool(m, "a", false) {
		t.Error("a should be true")
	}
	if cfgBool(m, "b", true) {
		t.Error("b should be false")
	}
	if !cfgBool(m, "c", false) {
		t.Error("c should be true")
	}
	if !cfgBool(m, "d", true) {
		t.Error("d (garbage) should fall back to default true")
	}
	if cfgBool(m, "missing", false) {
		t.Error("missing should fall back to default false")
	}
}
