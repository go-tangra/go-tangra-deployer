package provider

import (
	"encoding/json"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Field types (contracts/deployer-config-ui.md §2). An empty Type is a plain
// string (declarations written before 033).
const (
	TypeString       = "string"
	TypeText         = "text"
	TypeURL          = "url"
	TypeInt          = "int"
	TypeBool         = "bool"
	TypeEnum         = "enum"
	TypeStringList   = "string_list"
	TypeKeyValue     = "key_value"
	TypeHostSelector = "host_selector"
)

// Display groups.
const (
	GroupConnection  = "connection"
	GroupCredentials = "credentials"
	GroupOptions     = "options"
)

// Validation codes (contracts/deployer-config-ui.md §3). They are built only
// from the descriptor and never contain a submitted value (SR-012).
const (
	CodeRequired           = "required"
	CodeOneOfRequired      = "one_of_required"
	CodeWrongType          = "wrong_type"
	CodePattern            = "pattern"
	CodeTooLong            = "too_long"
	CodeOutOfRange         = "out_of_range"
	CodeTooManyItems       = "too_many_items"
	CodeNotInOptions       = "not_in_options"
	CodeInvalidURL         = "invalid_url"
	CodeUnknownField       = "unknown_field"
	CodeForbiddenHeader    = "forbidden_header"
	CodeNotOverridable     = "not_overridable"
	CodeRequiredByTargets  = "required_by_targets"
	CodeNotFoundOnEndpoint = "not_found_on_endpoint"
)

// Field-path prefixes of validation errors.
const (
	PathConfig      = "config."
	PathCredentials = "credentials."
	PathOverrides   = "config_overrides."
)

// DefaultMaxLength bounds string/text/url values and list items without an
// explicit max_length.
const DefaultMaxLength = 1024

// Mode selects how ValidateInput treats missing required fields.
type Mode int

const (
	// ModeConfiguration validates a stored configuration: a missing required
	// field (or an empty one_of_required group) is accepted when every field
	// concerned is overridable and returned as target-supplied (research D25).
	ModeConfiguration Mode = iota
	// ModeEffective validates a merged configuration (configuration plus
	// target override): every required field and group must be satisfied.
	ModeEffective
)

// FieldErrors maps a field path ("config.<key>", "credentials.<key>",
// "config_overrides.<key>") to a validation code. Codes never carry values.
type FieldErrors map[string]string

func (fe FieldErrors) Error() string {
	keys := make([]string, 0, len(fe))
	for k := range fe {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, k+": "+fe[k])
	}
	return "validation failed: " + strings.Join(parts, "; ")
}

// IntPtr returns a pointer to n (descriptor bounds).
func IntPtr(n int) *int { return &n }

var (
	keyPattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)
	reCache    sync.Map // pattern -> *regexp.Regexp
)

func compiled(p string) (*regexp.Regexp, error) {
	if re, ok := reCache.Load(p); ok {
		return re.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(p)
	if err != nil {
		return nil, err
	}
	reCache.Store(p, re)
	return re, nil
}

func fieldType(f Field) string {
	if f.Type == "" {
		return TypeString
	}
	return f.Type
}

// CheckCapabilities validates a provider declaration (contracts/deployer-
// config-ui.md §2). Register panics on an invalid one: a declaration error is a
// programming error surfaced at start.
func CheckCapabilities(c Capabilities) error {
	seen := map[string]bool{}
	check := func(f Field, credential bool) error {
		where := c.Type + "." + f.Key
		if !keyPattern.MatchString(f.Key) {
			return fmt.Errorf("provider %s: invalid field key %q", c.Type, f.Key)
		}
		if seen[f.Key] {
			return fmt.Errorf("provider %s: duplicate field key", where)
		}
		seen[f.Key] = true
		if strings.TrimSpace(f.Label) == "" {
			return fmt.Errorf("provider %s: label is required", where)
		}
		if len(f.Help) > 300 {
			return fmt.Errorf("provider %s: help longer than 300 characters", where)
		}
		typ := fieldType(f)
		switch typ {
		case TypeString, TypeText, TypeURL, TypeInt, TypeBool, TypeEnum, TypeStringList, TypeKeyValue, TypeHostSelector:
		default:
			return fmt.Errorf("provider %s: unknown type %q", where, f.Type)
		}
		switch f.Group {
		case "", GroupConnection, GroupCredentials, GroupOptions:
		default:
			return fmt.Errorf("provider %s: unknown group %q", where, f.Group)
		}
		if f.Secret && (!credential || (typ != TypeString && typ != TypeText)) {
			return fmt.Errorf("provider %s: secret only for string/text credential fields", where)
		}
		if f.Overridable && (credential || f.Secret) {
			return fmt.Errorf("provider %s: credential and secret fields are never overridable", where)
		}
		if typ == TypeEnum && len(f.Options) == 0 {
			return fmt.Errorf("provider %s: enum without options", where)
		}
		if f.Min != nil && f.Max != nil && *f.Min > *f.Max {
			return fmt.Errorf("provider %s: min > max", where)
		}
		if f.MaxLength < 0 || f.MaxItems < 0 {
			return fmt.Errorf("provider %s: negative bound", where)
		}
		if f.Pattern != "" {
			if !strings.HasPrefix(f.Pattern, "^") || !strings.HasSuffix(f.Pattern, "$") {
				return fmt.Errorf("provider %s: pattern must be anchored", where)
			}
			if _, err := compiled(f.Pattern); err != nil {
				return fmt.Errorf("provider %s: pattern does not compile: %w", where, err)
			}
		}
		if f.Default != nil {
			if f.Secret {
				return fmt.Errorf("provider %s: a secret has no default", where)
			}
			if code := checkValue(f, f.Default); code != "" {
				return fmt.Errorf("provider %s: default violates its rule (%s)", where, code)
			}
		}
		return nil
	}
	for _, f := range c.ConfigFields {
		if err := check(f, false); err != nil {
			return err
		}
	}
	for _, f := range c.CredentialFields {
		if err := check(f, true); err != nil {
			return err
		}
	}
	for _, g := range c.OneOfRequired {
		if len(g) < 2 {
			return fmt.Errorf("provider %s: one_of_required group needs at least two keys", c.Type)
		}
		for _, k := range g {
			f, ok := configField(c, k)
			if !ok {
				return fmt.Errorf("provider %s: one_of_required names unknown config field %q", c.Type, k)
			}
			if f.Required {
				return fmt.Errorf("provider %s: one_of_required member %q must not be required on its own", c.Type, k)
			}
		}
	}
	return nil
}

func configField(c Capabilities, key string) (Field, bool) {
	for _, f := range c.ConfigFields {
		if f.Key == key {
			return f, true
		}
	}
	return Field{}, false
}

func credentialField(c Capabilities, key string) (Field, bool) {
	for _, f := range c.CredentialFields {
		if f.Key == key {
			return f, true
		}
	}
	return Field{}, false
}

// IsEmpty reports whether v counts as missing for a required field: nil, an
// empty (or blank) string, an empty list or an empty map (v3 isEmptyValue).
func IsEmpty(v any) bool {
	switch x := v.(type) {
	case nil:
		return true
	case string:
		return strings.TrimSpace(x) == ""
	case []any:
		return len(x) == 0
	case []string:
		return len(x) == 0
	case map[string]any:
		return len(x) == 0
	case map[string]string:
		return len(x) == 0
	}
	return false
}

// ValidateInput checks a configuration and its credentials against the
// provider's descriptors: required fields and one_of_required groups, types,
// patterns, bounds, lengths, item counts, enum options, URL schemes, refused
// authentication headers and undeclared keys. In ModeConfiguration a missing
// overridable required field (or a fully overridable, empty group) is not an
// error; those keys are returned as targetSupplied. A nil creds map skips the
// credential checks (callers that validate configuration only).
func ValidateInput(c Capabilities, config, creds map[string]any, mode Mode) (errs FieldErrors, targetSupplied []string) {
	errs = FieldErrors{}
	checkValues(c.ConfigFields, config, PathConfig, errs)
	missingConfig(c, config, mode, PathConfig, errs, &targetSupplied)
	if creds != nil {
		checkValues(c.CredentialFields, creds, PathCredentials, errs)
		for _, f := range c.CredentialFields {
			if f.Required && IsEmpty(creds[f.Key]) {
				errs[PathCredentials+f.Key] = CodeRequired
			}
		}
	}
	return errs, targetSupplied
}

// checkValues validates every present key of m against fields (unknown keys
// are refused).
func checkValues(fields []Field, m map[string]any, prefix string, errs FieldErrors) {
	byKey := make(map[string]Field, len(fields))
	for _, f := range fields {
		byKey[f.Key] = f
	}
	for k, v := range m {
		f, ok := byKey[k]
		if !ok {
			errs[prefix+safeKey(k)] = CodeUnknownField
			continue
		}
		if code := checkValue(f, v); code != "" {
			errs[prefix+k] = code
		}
	}
}

// missingConfig records missing required config fields and unsatisfied groups.
func missingConfig(c Capabilities, config map[string]any, mode Mode, prefix string, errs FieldErrors, supplied *[]string) {
	for _, f := range c.ConfigFields {
		if !f.Required || !IsEmpty(config[f.Key]) {
			continue
		}
		if mode == ModeConfiguration && f.Overridable {
			*supplied = append(*supplied, f.Key)
			continue
		}
		if _, set := errs[prefix+f.Key]; !set {
			errs[prefix+f.Key] = CodeRequired
		}
	}
	for _, g := range c.OneOfRequired {
		if !groupEmpty(g, config) {
			continue
		}
		if mode == ModeConfiguration && groupOverridable(c, g) {
			*supplied = append(*supplied, g...)
			continue
		}
		for _, k := range g {
			if _, set := errs[prefix+k]; !set {
				errs[prefix+k] = groupCode(g)
			}
		}
	}
}

func groupEmpty(g []string, m map[string]any) bool {
	for _, k := range g {
		if !IsEmpty(m[k]) {
			return false
		}
	}
	return true
}

func groupOverridable(c Capabilities, g []string) bool {
	for _, k := range g {
		if f, _ := configField(c, k); !f.Overridable {
			return false
		}
	}
	return true
}

func groupCode(g []string) string { return CodeOneOfRequired + ":" + strings.Join(g, ",") }

// TargetSupplied returns the configuration keys that every target attaching
// this configuration must supply: required (or one_of_required) fields that are
// empty here and overridable. Declaration order.
func TargetSupplied(c Capabilities, config map[string]any) []string {
	_, ts := ValidateInput(c, config, nil, ModeConfiguration)
	return ts
}

// MergeOverride overlays a target override on a configuration; empty override
// values are dropped (they mean "inherit").
func MergeOverride(config, override map[string]any) map[string]any {
	out := make(map[string]any, len(config)+len(override))
	for k, v := range config {
		out[k] = v
	}
	for k, v := range override {
		if !IsEmpty(v) {
			out[k] = v
		}
	}
	return out
}

// ValidateOverride checks a target override for one attached configuration
// (contracts/deployer-config-ui.md §4a): every key must be a declared,
// overridable config field (credential keys and every non-overridable field →
// not_overridable, undeclared → unknown_field), every non-empty value must
// follow its descriptor, and the merged configuration must satisfy every
// required field and group. Paths are "config_overrides.<key>" — or
// "config.<key>" for a required field the target cannot supply.
func ValidateOverride(c Capabilities, config, override map[string]any) FieldErrors {
	errs := FieldErrors{}
	for k, v := range override {
		f, ok := configField(c, k)
		switch {
		case !ok:
			if _, cred := credentialField(c, k); cred {
				errs[PathOverrides+k] = CodeNotOverridable
			} else {
				errs[PathOverrides+safeKey(k)] = CodeUnknownField
			}
		case !f.Overridable:
			errs[PathOverrides+k] = CodeNotOverridable
		case !IsEmpty(v):
			if code := checkValue(f, v); code != "" {
				errs[PathOverrides+k] = code
			}
		}
	}
	merged := MergeOverride(config, override)
	for _, f := range c.ConfigFields {
		if !f.Required || !IsEmpty(merged[f.Key]) {
			continue
		}
		errs[requiredPath(f)] = CodeRequired
	}
	for _, g := range c.OneOfRequired {
		if !groupEmpty(g, merged) {
			continue
		}
		for _, k := range g {
			f, _ := configField(c, k)
			if _, set := errs[requiredPath(f)]; !set {
				errs[requiredPath(f)] = groupCode(g)
			}
		}
	}
	return errs
}

func requiredPath(f Field) string {
	if f.Overridable {
		return PathOverrides + f.Key
	}
	return PathConfig + f.Key
}

// Missing is a required field (or a one_of_required group) an effective
// configuration lacks. Labels only — never values.
type Missing struct {
	Keys   []string
	Labels []string
	// TargetSupplied is true when the field(s) are overridable: a deployment
	// target was expected to provide them.
	TargetSupplied bool
}

// MissingRequired lists the required config fields and one_of_required groups
// the effective (merged) configuration lacks. Checked at job start, before the
// certificate is fetched and before the provider runs (FR-031, FR-036).
func MissingRequired(c Capabilities, effective map[string]any) []Missing {
	var out []Missing
	for _, f := range c.ConfigFields {
		if f.Required && IsEmpty(effective[f.Key]) {
			out = append(out, Missing{Keys: []string{f.Key}, Labels: []string{f.Label}, TargetSupplied: f.Overridable})
		}
	}
	for _, g := range c.OneOfRequired {
		if !groupEmpty(g, effective) {
			continue
		}
		m := Missing{Keys: append([]string(nil), g...), TargetSupplied: groupOverridable(c, g)}
		for _, k := range g {
			f, _ := configField(c, k)
			m.Labels = append(m.Labels, f.Label)
		}
		out = append(out, m)
	}
	return out
}

// IncompleteMessage is the job failure text for missing required fields:
// "configuration incomplete: <label>[ must be provided by the target]".
func IncompleteMessage(missing []Missing) string {
	if len(missing) == 0 {
		return ""
	}
	m := missing[0]
	msg := "configuration incomplete: " + strings.Join(m.Labels, " or ")
	if m.TargetSupplied {
		msg += " must be provided by the target"
	}
	return msg
}

// Labels returns the labels of the given config keys (declaration order).
func Labels(c Capabilities, keys []string) []string {
	want := map[string]bool{}
	for _, k := range keys {
		want[k] = true
	}
	var out []string
	for _, f := range c.ConfigFields {
		if want[f.Key] {
			out = append(out, f.Label)
		}
	}
	return out
}

// FillDefaults returns a copy of config where each of keys that is empty is
// set to its descriptor default (when one is declared), plus the keys that
// stayed empty. Used to probe a configuration with target-supplied fields.
func FillDefaults(c Capabilities, config map[string]any, keys []string) (map[string]any, []string) {
	out := make(map[string]any, len(config)+len(keys))
	for k, v := range config {
		out[k] = v
	}
	var left []string
	for _, k := range keys {
		f, _ := configField(c, k)
		if f.Default != nil && IsEmpty(out[k]) {
			out[k] = f.Default
			continue
		}
		if IsEmpty(out[k]) {
			left = append(left, k)
		}
	}
	return out, left
}

// checkValue returns the code of the first rule v violates, or "". Empty
// values pass (required-ness is checked separately).
func checkValue(f Field, v any) string {
	if IsEmpty(v) {
		if _, isStr := v.(string); isStr || v == nil {
			return ""
		}
		switch fieldType(f) {
		case TypeStringList, TypeHostSelector:
			if isList(v) {
				return ""
			}
		case TypeKeyValue:
			if isMap(v) {
				return ""
			}
		}
		return CodeWrongType
	}
	switch fieldType(f) {
	case TypeString, TypeText:
		s, ok := v.(string)
		if !ok {
			return CodeWrongType
		}
		return checkString(f, s, fieldType(f) == TypeString)
	case TypeURL:
		s, ok := v.(string)
		if !ok {
			return CodeWrongType
		}
		if code := checkString(f, s, false); code != "" {
			return code
		}
		u, err := url.Parse(s)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil {
			return CodeInvalidURL
		}
		return ""
	case TypeInt:
		n, ok := toInt(v)
		if !ok {
			return CodeWrongType
		}
		if (f.Min != nil && n < int64(*f.Min)) || (f.Max != nil && n > int64(*f.Max)) {
			return rangeCode(f)
		}
		return ""
	case TypeBool:
		if _, ok := v.(bool); !ok {
			return CodeWrongType
		}
		return ""
	case TypeEnum:
		s, ok := v.(string)
		if !ok {
			return CodeWrongType
		}
		for _, o := range f.Options {
			if o.Value == s {
				return ""
			}
		}
		return CodeNotInOptions
	case TypeStringList, TypeHostSelector:
		items, ok := toStrings(v)
		if !ok {
			return CodeWrongType
		}
		if f.MaxItems > 0 && len(items) > f.MaxItems {
			return CodeTooManyItems + ":" + strconv.Itoa(f.MaxItems)
		}
		for _, it := range items {
			if code := checkString(f, it, true); code != "" {
				return code
			}
		}
		return ""
	case TypeKeyValue:
		m, ok := toStringMap(v)
		if !ok {
			return CodeWrongType
		}
		if f.MaxItems > 0 && len(m) > f.MaxItems {
			return CodeTooManyItems + ":" + strconv.Itoa(f.MaxItems)
		}
		names := make([]string, 0, len(m))
		for k := range m {
			names = append(names, k)
		}
		sort.Strings(names)
		for _, k := range names {
			if f.Key == "headers" && IsAuthHeader(k) {
				return CodeForbiddenHeader + ":" + safeKey(k)
			}
			if code := checkString(Field{MaxLength: f.MaxLength}, m[k], false); code != "" {
				return code
			}
		}
		return ""
	}
	return CodeWrongType
}

func checkString(f Field, s string, withPattern bool) string {
	limit := f.MaxLength
	if limit <= 0 {
		limit = DefaultMaxLength
	}
	if len(s) > limit {
		return CodeTooLong + ":" + strconv.Itoa(limit)
	}
	if withPattern && f.Pattern != "" {
		re, err := compiled(f.Pattern)
		if err != nil || !re.MatchString(s) {
			return CodePattern
		}
	}
	return ""
}

func rangeCode(f Field) string {
	lo, hi := "", ""
	if f.Min != nil {
		lo = strconv.Itoa(*f.Min)
	}
	if f.Max != nil {
		hi = strconv.Itoa(*f.Max)
	}
	return CodeOutOfRange + ":" + lo + ".." + hi
}

func toInt(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case float64:
		if x != math.Trunc(x) || math.IsInf(x, 0) || math.Abs(x) > 1<<53 {
			return 0, false
		}
		return int64(x), true
	case json.Number:
		n, err := x.Int64()
		return n, err == nil
	}
	return 0, false
}

func isList(v any) bool {
	switch v.(type) {
	case []any, []string:
		return true
	}
	return false
}

func isMap(v any) bool {
	switch v.(type) {
	case map[string]any, map[string]string:
		return true
	}
	return false
}

func toStrings(v any) ([]string, bool) {
	switch x := v.(type) {
	case []string:
		return x, true
	case []any:
		out := make([]string, 0, len(x))
		for _, it := range x {
			s, ok := it.(string)
			if !ok {
				return nil, false
			}
			out = append(out, s)
		}
		return out, true
	}
	return nil, false
}

func toStringMap(v any) (map[string]string, bool) {
	switch x := v.(type) {
	case map[string]string:
		return x, true
	case map[string]any:
		out := make(map[string]string, len(x))
		for k, it := range x {
			s, ok := it.(string)
			if !ok {
				return nil, false
			}
			out[k] = s
		}
		return out, true
	}
	return nil, false
}

// safeKey makes a submitted key safe for an error path or code: at most 64
// bytes, characters outside [A-Za-z0-9_-] replaced by "_".
func safeKey(k string) string {
	if len(k) > 64 {
		k = k[:64]
	}
	b := []byte(k)
	for i, c := range b {
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-') {
			b[i] = '_'
		}
	}
	return string(b)
}
