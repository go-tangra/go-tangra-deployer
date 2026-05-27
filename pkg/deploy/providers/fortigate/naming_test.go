package fortigate

import "testing"

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
	match := []string{"star_factory_bg", "star_factory_bg_20260527", "star_factory_bg_20250101"}
	noMatch := []string{
		"star_factory_bg_2025",    // 4-digit manual suffix, not a date
		"star_factory_bg_LE",      // label suffix
		"star_jobs_bg_2025",       // different base
		"star_factory_bg_2026052", // 7 digits
		"factory_bg",              // shorter, different base
		"star_factory_bg_x20260527",
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
