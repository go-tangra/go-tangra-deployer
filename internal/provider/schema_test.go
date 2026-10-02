package provider

import (
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type vectorFile struct {
	Providers map[string]Capabilities `json:"providers"`
	Cases     []struct {
		Name           string            `json:"name"`
		Provider       string            `json:"provider"`
		Mode           string            `json:"mode"`
		Config         map[string]any    `json:"config"`
		Credentials    map[string]any    `json:"credentials"`
		Override       map[string]any    `json:"override"`
		Errors         map[string]string `json:"errors"`
		TargetSupplied []string          `json:"target_supplied"`
	} `json:"cases"`
}

func loadVectors(t testing.TB) vectorFile {
	t.Helper()
	raw, err := os.ReadFile("../../api/testdata/provider-field-vectors.json")
	if err != nil {
		t.Fatal(err)
	}
	var v vectorFile
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// T079: the shared vectors (also run by the UI fieldsToZod tests).
func TestValidatorVectors(t *testing.T) {
	v := loadVectors(t)
	for name, caps := range v.Providers {
		if err := CheckCapabilities(caps); err != nil {
			t.Fatalf("vector provider %s: %v", name, err)
		}
	}
	for _, c := range v.Cases {
		caps, ok := v.Providers[c.Provider]
		if !ok {
			t.Fatalf("%s: unknown provider %q", c.Name, c.Provider)
		}
		var got FieldErrors
		var ts []string
		switch c.Mode {
		case "configuration":
			got, ts = ValidateInput(caps, c.Config, c.Credentials, ModeConfiguration)
		case "effective":
			got, ts = ValidateInput(caps, c.Config, c.Credentials, ModeEffective)
		case "override":
			got = ValidateOverride(caps, c.Config, c.Override)
		default:
			t.Fatalf("%s: unknown mode %q", c.Name, c.Mode)
		}
		if len(got) == 0 {
			got = FieldErrors{}
		}
		want := FieldErrors(c.Errors)
		if want == nil {
			want = FieldErrors{}
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%s: errors = %v, want %v", c.Name, map[string]string(got), map[string]string(want))
		}
		if c.Mode == "configuration" {
			sort.Strings(ts)
			wts := append([]string{}, c.TargetSupplied...)
			sort.Strings(wts)
			if len(ts) == 0 {
				ts = []string{}
			}
			if !reflect.DeepEqual(ts, wts) {
				t.Errorf("%s: target_supplied = %v, want %v", c.Name, ts, wts)
			}
		}
	}
}

func TestCheckCapabilitiesRejects(t *testing.T) {
	ok := Field{Key: "k", Label: "K"}
	cases := map[string]Capabilities{
		"bad key":              {Type: "x", ConfigFields: []Field{{Key: "Bad-Key", Label: "B"}}},
		"duplicate key":        {Type: "x", ConfigFields: []Field{ok}, CredentialFields: []Field{ok}},
		"no label":             {Type: "x", ConfigFields: []Field{{Key: "k"}}},
		"long help":            {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Help: strings.Repeat("h", 301)}}},
		"unknown type":         {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Type: "float"}}},
		"unknown group":        {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Group: "misc"}}},
		"secret in config":     {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Secret: true}}},
		"secret non-string":    {Type: "x", CredentialFields: []Field{{Key: "k", Label: "K", Type: TypeInt, Secret: true}}},
		"overridable cred":     {Type: "x", CredentialFields: []Field{{Key: "k", Label: "K", Overridable: true}}},
		"enum without options": {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Type: TypeEnum}}},
		"min > max":            {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Type: TypeInt, Min: IntPtr(5), Max: IntPtr(1)}}},
		"negative bound":       {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", MaxLength: -1}}},
		"unanchored pattern":   {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Pattern: "abc"}}},
		"bad pattern":          {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Pattern: "^(a$"}}},
		"secret default":       {Type: "x", CredentialFields: []Field{{Key: "k", Label: "K", Secret: true, Default: "x"}}},
		"wrong default type":   {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Type: TypeInt, Default: "60"}}},
		"default out of range": {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Type: TypeInt, Max: IntPtr(5), Default: 6}}},
		"default vs pattern":   {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Pattern: "^a$", Default: "b"}}},
		"short group":          {Type: "x", ConfigFields: []Field{ok}, OneOfRequired: [][]string{{"k"}}},
		"group unknown key":    {Type: "x", ConfigFields: []Field{ok}, OneOfRequired: [][]string{{"k", "zz"}}},
		"group required key": {Type: "x", ConfigFields: []Field{{Key: "k", Label: "K", Required: true}, {Key: "j", Label: "J"}},
			OneOfRequired: [][]string{{"k", "j"}}},
		"bad credential": {Type: "x", CredentialFields: []Field{{Key: "k"}}},
	}
	for name, c := range cases {
		if err := CheckCapabilities(c); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	good := Capabilities{Type: "x",
		ConfigFields: []Field{{Key: "a", Label: "A", Group: GroupConnection, Type: TypeEnum, Options: []Option{{Value: "v", Label: "V"}}, Default: "v"},
			{Key: "b", Label: "B", Overridable: true}, {Key: "c", Label: "C", Overridable: true}},
		CredentialFields: []Field{{Key: "s", Label: "S", Type: TypeText, Secret: true}},
		OneOfRequired:    [][]string{{"b", "c"}}}
	if err := CheckCapabilities(good); err != nil {
		t.Fatalf("good declaration refused: %v", err)
	}
}

func TestCheckValueTypes(t *testing.T) {
	cases := []struct {
		f    Field
		v    any
		want string
	}{
		{Field{Type: TypeInt}, 5, ""},
		{Field{Type: TypeInt}, int32(5), ""},
		{Field{Type: TypeInt}, int64(5), ""},
		{Field{Type: TypeInt}, json.Number("7"), ""},
		{Field{Type: TypeInt}, json.Number("7.5"), CodeWrongType},
		{Field{Type: TypeInt}, 1e300, CodeWrongType},
		{Field{Type: TypeInt}, true, CodeWrongType},
		{Field{Type: TypeInt, Min: IntPtr(3)}, 1, "out_of_range:3.."},
		{Field{Type: TypeInt, Max: IntPtr(3)}, 4, "out_of_range:..3"},
		{Field{Type: TypeInt}, "", ""},
		{Field{Type: TypeInt}, []any{}, CodeWrongType},
		{Field{Type: TypeString}, 3, CodeWrongType},
		{Field{Type: TypeURL}, 3, CodeWrongType},
		{Field{Type: TypeURL, MaxLength: 5}, "https://long.example", "too_long:5"},
		{Field{Type: TypeURL}, "https://", CodeInvalidURL},
		{Field{Type: TypeURL}, "http://ok.example/x", ""},
		{Field{Type: TypeBool}, false, ""},
		{Field{Type: TypeEnum, Options: []Option{{Value: "a"}}}, 1, CodeWrongType},
		{Field{Type: TypeEnum, Options: []Option{{Value: "a"}}}, "a", ""},
		{Field{Type: TypeStringList}, []string{"a"}, ""},
		{Field{Type: TypeStringList}, []string{}, ""},
		{Field{Type: TypeStringList}, []any{1}, CodeWrongType},
		{Field{Type: TypeStringList}, "a", CodeWrongType},
		{Field{Type: TypeStringList, MaxLength: 2}, []any{"abc"}, "too_long:2"},
		{Field{Type: TypeHostSelector, MaxItems: 1}, []any{"a", "b"}, "too_many_items:1"},
		{Field{Type: TypeHostSelector}, map[string]any{}, CodeWrongType},
		{Field{Type: TypeKeyValue}, map[string]string{"a": "b"}, ""},
		{Field{Type: TypeKeyValue}, map[string]string{}, ""},
		{Field{Type: TypeKeyValue}, map[string]any{"a": 1}, CodeWrongType},
		{Field{Type: TypeKeyValue}, []any{"a"}, CodeWrongType},
		{Field{Type: TypeKeyValue}, []any{}, CodeWrongType},
		{Field{Type: TypeKeyValue, MaxLength: 1}, map[string]any{"a": "bb"}, "too_long:1"},
		{Field{Key: "headers", Type: TypeKeyValue}, map[string]any{"X-Api-Key": "v"}, "forbidden_header:X-Api-Key"},
		{Field{Key: "headers", Type: TypeKeyValue}, map[string]any{"X Source\x00": "v"}, ""},
		{Field{Type: "nope"}, "x", CodeWrongType},
	}
	for i, c := range cases {
		if got := checkValue(c.f, c.v); got != c.want {
			t.Errorf("#%d checkValue(%+v, %#v) = %q, want %q", i, c.f, c.v, got, c.want)
		}
	}
	// A pattern that does not compile never matches (declarations are checked
	// at registration; this is defence in depth).
	if got := checkString(Field{Pattern: "^(x$"}, "x", true); got != CodePattern {
		t.Fatalf("broken pattern = %q", got)
	}
}

func TestIsEmpty(t *testing.T) {
	for _, v := range []any{nil, "", "  ", []any{}, []string{}, map[string]any{}, map[string]string{}} {
		if !IsEmpty(v) {
			t.Errorf("IsEmpty(%#v) = false", v)
		}
	}
	for _, v := range []any{"x", 0, false, []any{"a"}, map[string]any{"a": 1}} {
		if IsEmpty(v) {
			t.Errorf("IsEmpty(%#v) = true", v)
		}
	}
}

func hostsCaps() Capabilities {
	return Capabilities{Type: "h",
		ConfigFields: []Field{
			{Key: "zone", Label: "Zone", Required: true, Overridable: true, Default: "root"},
			{Key: "host_ids", Label: "Hosts", Type: TypeHostSelector, Overridable: true},
			{Key: "host_tags", Label: "Host tags", Type: TypeStringList, Overridable: true},
			{Key: "partition", Label: "Partition", Required: true},
		},
		OneOfRequired: [][]string{{"host_ids", "host_tags"}}}
}

func TestMissingRequiredAndMessage(t *testing.T) {
	c := hostsCaps()
	if got := MissingRequired(c, map[string]any{"zone": "z", "partition": "p", "host_tags": []any{"a"}}); len(got) != 0 {
		t.Fatalf("complete config reported missing: %+v", got)
	}
	got := MissingRequired(c, map[string]any{"partition": ""})
	if len(got) != 3 {
		t.Fatalf("missing = %+v", got)
	}
	if msg := IncompleteMessage(got); msg != "configuration incomplete: Zone must be provided by the target" {
		t.Fatalf("message = %q", msg)
	}
	if msg := IncompleteMessage(got[1:]); msg != "configuration incomplete: Partition" {
		t.Fatalf("message = %q", msg)
	}
	if msg := IncompleteMessage(got[2:]); msg != "configuration incomplete: Hosts or Host tags must be provided by the target" {
		t.Fatalf("message = %q", msg)
	}
	if IncompleteMessage(nil) != "" {
		t.Fatal("empty message expected")
	}
}

func TestTargetSuppliedLabelsDefaults(t *testing.T) {
	c := hostsCaps()
	ts := TargetSupplied(c, map[string]any{"partition": "p"})
	if !reflect.DeepEqual(ts, []string{"zone", "host_ids", "host_tags"}) {
		t.Fatalf("TargetSupplied = %v", ts)
	}
	if l := Labels(c, ts); !reflect.DeepEqual(l, []string{"Zone", "Hosts", "Host tags"}) {
		t.Fatalf("Labels = %v", l)
	}
	filled, left := FillDefaults(c, map[string]any{"partition": "p"}, ts)
	if filled["zone"] != "root" || !reflect.DeepEqual(left, []string{"host_ids", "host_tags"}) {
		t.Fatalf("FillDefaults = %v %v", filled, left)
	}
	_, left = FillDefaults(c, map[string]any{"zone": "z"}, []string{"zone"})
	if len(left) != 0 {
		t.Fatalf("set field reported left: %v", left)
	}
}

func TestValidateOverrideNonOverridableRequired(t *testing.T) {
	c := hostsCaps()
	errs := ValidateOverride(c, map[string]any{"zone": "z", "host_tags": []any{"a"}}, nil)
	if !reflect.DeepEqual(errs, FieldErrors{"config.partition": CodeRequired}) {
		t.Fatalf("errs = %v", errs)
	}
	// A one-of group with a non-overridable member is reported on config.<key>.
	m := Capabilities{Type: "m", ConfigFields: []Field{{Key: "a", Label: "A", Overridable: true}, {Key: "b", Label: "B"}}, OneOfRequired: [][]string{{"a", "b"}}}
	errs = ValidateOverride(m, nil, map[string]any{})
	if errs["config_overrides.a"] != "one_of_required:a,b" || errs["config.b"] != "one_of_required:a,b" {
		t.Fatalf("errs = %v", errs)
	}
}

func TestFieldErrorsError(t *testing.T) {
	fe := FieldErrors{"config.b": "required", "config.a": "pattern"}
	if fe.Error() != "validation failed: config.a: pattern; config.b: required" {
		t.Fatalf("Error() = %q", fe.Error())
	}
}

func TestSafeKey(t *testing.T) {
	if got := safeKey("a b\x00/" + strings.Repeat("x", 70)); len(got) != 64 || strings.ContainsAny(got, " \x00/") {
		t.Fatalf("safeKey = %q", got)
	}
	errs, _ := ValidateInput(Capabilities{Type: "x"}, map[string]any{"../evil": 1}, nil, ModeConfiguration)
	if errs["config.___evil"] != CodeUnknownField {
		t.Fatalf("errs = %v", errs)
	}
}
