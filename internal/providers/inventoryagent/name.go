package inventoryagent

import (
	"strings"
	"unicode/utf8"
)

// The certificate-name and host-tag rules are the same as the inventory's
// (go-tangra-inventory internal/certmaterial, tested against its vectors in
// testdata/default-name-vectors.json): the deployer refuses what the
// inventory and the agent would refuse.

// MaxNameLen bounds a certificate name.
const (
	MaxNameLen     = 64
	maxTagKeyLen   = 63
	maxTagValueLen = 255
)

func isAlnum(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

func isNameByte(c byte) bool { return isAlnum(c) || c == '.' || c == '_' || c == '-' }

// ValidName reports whether s is a certificate name:
// ^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$ without "..". Such a name is a single
// path component that never starts with a dot.
func ValidName(s string) bool {
	if s == "" || len(s) > MaxNameLen || !isAlnum(s[0]) || strings.Contains(s, "..") {
		return false
	}
	for i := 1; i < len(s); i++ {
		if !isNameByte(s[i]) {
			return false
		}
	}
	return true
}

// DefaultName derives a certificate name from a common name: lowercased, a
// leading "*." becomes "wildcard.", other characters outside [a-z0-9._-]
// become "_", runs of dots collapse, leading non-alphanumerics are dropped and
// the result is cut to 64 characters. ok is false when nothing valid remains.
func DefaultName(cn string) (string, bool) {
	s := strings.ToLower(strings.TrimSpace(cn))
	if strings.HasPrefix(s, "*.") {
		s = "wildcard." + s[2:]
	}
	s = strings.Map(func(r rune) rune {
		if r < utf8.RuneSelf && isNameByte(byte(r)) { // #nosec G115 -- r < utf8.RuneSelf (0x80) fits a byte
			return r
		}
		return '_'
	}, s)
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '.' && i > 0 && s[i-1] == '.' {
			continue
		}
		b.WriteByte(s[i])
	}
	out := strings.TrimLeftFunc(b.String(), func(r rune) bool { return r >= utf8.RuneSelf || !isAlnum(byte(r)) }) // #nosec G115 -- byte(r) only when r < utf8.RuneSelf
	if len(out) > MaxNameLen {
		out = out[:MaxNameLen]
	}
	if !ValidName(out) {
		return "", false
	}
	return out, true
}

// ValidTag reports whether s is a host tag selector "key" or "key=value": the
// key 1-63 characters of [A-Za-z0-9_.:/-], the value at most 255 bytes of
// valid UTF-8 without control characters.
func ValidTag(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	k, v, _ := strings.Cut(s, "=")
	if k == "" || len(k) > maxTagKeyLen || len(v) > maxTagValueLen {
		return false
	}
	for i := 0; i < len(k); i++ {
		c := k[i]
		if !isAlnum(c) && !strings.ContainsRune("_.:/-", rune(c)) {
			return false
		}
	}
	for _, r := range v {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r < 0xa0) {
			return false
		}
	}
	return true
}
