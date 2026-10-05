package provider

import (
	"sort"
	"strings"
)

// Redact replaces every string value (of at least three characters) found in
// the maps — config, credentials, nested lists and objects — in msg, so a
// provider or endpoint that echoes its input cannot leak it into a log or a
// stored job error (SR-012). Longer values are replaced first.
func Redact(msg string, maps ...map[string]any) string {
	var values []string
	var walk func(v any)
	walk = func(v any) {
		switch x := v.(type) {
		case string:
			if len(x) >= 3 {
				values = append(values, x)
			}
		case []any:
			for _, it := range x {
				walk(it)
			}
		case map[string]any:
			for _, it := range x {
				walk(it)
			}
		}
	}
	for _, m := range maps {
		walk(m)
	}
	sort.Slice(values, func(i, j int) bool { return len(values[i]) > len(values[j]) })
	for _, v := range values {
		msg = strings.ReplaceAll(msg, v, "[redacted]")
	}
	return msg
}
