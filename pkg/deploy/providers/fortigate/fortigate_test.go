package fortigate

import (
	"strings"
	"testing"
)

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain domain", "www.example.com", "www_example_com"},
		{"wildcard", "*.example.com", "star_example_com"},
		{"multiple dots", "a.b.c.example.com", "a_b_c_example_com"},
		{"collapse underscores", "weird__name..com", "weird_name_com"},
		{"leading digit", "1.example.com", "cert_1_example_com"},
		{"trailing junk", "example.com--", "example_com--"},
		{
			name: "over 35 chars truncates without trailing underscore",
			in:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.example.com",
			want: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		{"empty", "", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sanitizeName(tc.in)
			if got != tc.want {
				t.Fatalf("sanitizeName(%q) = %q, want %q", tc.in, got, tc.want)
			}
			if len(got) > 35 {
				t.Fatalf("result %q exceeds FortiGate's 35-char limit", got)
			}
			if strings.HasSuffix(got, "_") {
				t.Fatalf("result %q has a trailing underscore", got)
			}
		})
	}
}
