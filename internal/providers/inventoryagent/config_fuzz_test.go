package inventoryagent

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// FuzzInventoryAgentConfig (T039): the parser never panics, accepted
// configurations satisfy every rule of contracts/deployer-provider.md §2, and
// no error echoes a submitted value.
func FuzzInventoryAgentConfig(f *testing.F) {
	f.Add(`{"host_ids":["0192a7c0-0000-7000-8000-00000000000a"],"host_tags":["role=web"],"cert_name":"www","key_policy":"require","require_all_success":true,"wait_seconds":60}`)
	f.Add(`{"host_tags":["=bad-tag-value"],"cert_name":"../escape-attempt"}`)
	f.Add(`{"wait_seconds":241,"key_policy":"secret-policy-value"}`)
	f.Add(`{"unknown":"value"}`)
	f.Fuzz(func(t *testing.T, raw string) {
		var m map[string]any
		if json.Unmarshal([]byte(raw), &m) != nil {
			return
		}
		c, err := ParseConfig(m)
		if err != nil {
			var fe provider.FieldErrors
			if !errors.As(err, &fe) {
				t.Fatalf("error type %T", err)
			}
			text := fe.Error()
			for _, v := range m {
				if s, ok := v.(string); ok && len(s) > 3 && strings.Contains(text, s) && !strings.Contains("validation failed: config. wrong_type pattern not_in_options unknown_field invalid_uuid duplicate_item out_of_range:0..240 too_many_items:1000 too_many_items:16 host_ids host_tags cert_name key_policy require_all_success wait_seconds", s) {
					t.Fatalf("error echoes %q: %s", s, text)
				}
			}
			return
		}
		if len(c.HostIDs) > MaxHostIDs || len(c.HostTags) > MaxHostTags || c.WaitSeconds < 0 || c.WaitSeconds > MaxWaitSeconds {
			t.Fatalf("bounds violated: %+v", c)
		}
		if c.CertName != "" && (!ValidName(c.CertName) || strings.ContainsAny(c.CertName, "/\\") || strings.Contains(c.CertName, "..")) {
			t.Fatalf("unsafe name accepted: %q", c.CertName)
		}
		if c.KeyPolicy != KeyPolicyRequire && c.KeyPolicy != KeyPolicyCertificateOnly {
			t.Fatalf("key policy %q", c.KeyPolicy)
		}
		for _, tag := range c.HostTags {
			if !ValidTag(tag) {
				t.Fatalf("tag %q", tag)
			}
		}
	})
}
