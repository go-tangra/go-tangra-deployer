package httpapi

import (
	"strings"
	"testing"
)

// Status e-mail settings round-trip through POST/PUT /targets: omitted on
// create they default to off/always, omitted on update they are kept, and an
// unknown notify_on is refused naming the field.
func TestTargetNotificationSettings(t *testing.T) {
	f := newAPI(t)
	w := f.req(t, "POST", p+"/targets", "admin", `{"name":"plain","certificate_filters":[]}`)
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	if m := decode(t, w); m["notify_enabled"] != false || m["notify_on"] != "always" {
		t.Fatalf("defaults: %v", m)
	}

	w = f.req(t, "POST", p+"/targets", "admin", `{"name":"edge","notify_enabled":true,"notify_on":"failure"}`)
	if w.Code != 201 {
		t.Fatalf("create: %d %s", w.Code, w.Body.String())
	}
	m := decode(t, w)
	id, _ := m["id"].(string)
	if m["notify_enabled"] != true || m["notify_on"] != "failure" {
		t.Fatalf("created: %v", m)
	}
	// An update that does not name them keeps them (an older client).
	if w := f.req(t, "PUT", p+"/targets/"+id, "admin", `{"name":"edge","description":"x"}`); w.Code != 200 {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
	if m := decode(t, f.req(t, "GET", p+"/targets/"+id, "admin", "")); m["notify_enabled"] != true || m["notify_on"] != "failure" {
		t.Fatalf("kept: %v", m)
	}
	if w := f.req(t, "PUT", p+"/targets/"+id, "admin", `{"name":"edge","notify_enabled":false,"notify_on":"always"}`); w.Code != 200 {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
	if m := decode(t, f.req(t, "GET", p+"/targets/"+id, "admin", "")); m["notify_enabled"] != false || m["notify_on"] != "always" {
		t.Fatalf("updated: %v", m)
	}
	for _, body := range []string{`{"name":"bad","notify_on":"sometimes"}`, `{"name":"bad","notify_on":""}`} {
		if w := f.req(t, "POST", p+"/targets", "admin", body); w.Code != 422 || !strings.Contains(w.Body.String(), "notify_on") {
			t.Fatalf("create %s: %d %s", body, w.Code, w.Body.String())
		}
	}
	if w := f.req(t, "PUT", p+"/targets/"+id, "admin", `{"name":"edge","notify_on":"never"}`); w.Code != 422 || !strings.Contains(w.Body.String(), "notify_on") {
		t.Fatalf("update: %d %s", w.Code, w.Body.String())
	}
}
