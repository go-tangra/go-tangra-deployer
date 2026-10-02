// Package providertest holds test helpers for deployment providers (feature
// 033): it checks that every configuration or credential key a provider's
// source reads is declared in its field descriptors, so the schema-driven
// configuration drawer and the save-time validator never lose a setting.
package providertest

import (
	"os"
	"regexp"
	"sort"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// readPatterns match the idioms the providers use to read config/credential
// keys: m["key"], helper(m, "key"[, default]) and the webhook probe URL key.
var readPatterns = []*regexp.Regexp{
	regexp.MustCompile(`\b(?:config|creds|credsMap|cfg|m)\["([a-z_]+)"\]`),
	regexp.MustCompile(`\b(?:str|strFrom|cfgString|cfgBool|credString|urlFrom)\(\s*\w+,\s*"([a-z_]+)"`),
	regexp.MustCompile(`\bprobe\(\s*\w+,\s*"[a-z]+",\s*"([a-z_]+)"`),
}

// KeysRead returns the sorted, de-duplicated keys the Go source file reads.
func KeysRead(t testing.TB, file string) []string {
	t.Helper()
	src, err := os.ReadFile(file) // #nosec G304 -- test helper, fixed paths
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, re := range readPatterns {
		for _, m := range re.FindAllSubmatch(src, -1) {
			seen[string(m[1])] = true
		}
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// AssertDeclaredKeysCoverReads fails when a key read by the source file is
// neither a config nor a credential field of caps.
func AssertDeclaredKeysCoverReads(t testing.TB, caps provider.Capabilities, file string) {
	t.Helper()
	declared := map[string]bool{}
	for _, f := range caps.ConfigFields {
		declared[f.Key] = true
	}
	for _, f := range caps.CredentialFields {
		declared[f.Key] = true
	}
	read := KeysRead(t, file)
	if len(read) == 0 {
		t.Fatalf("%s: no key reads found (patterns out of date?)", file)
	}
	for _, k := range read {
		if !declared[k] {
			t.Errorf("%s reads %q, which %s does not declare", file, k, caps.Type)
		}
	}
}
