package httpapi

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// A well-formed but absent UUID (passes the OpenAPI id/job_id maxLength check but
// matches nothing in the store).
const absentID = "44444444-4444-4444-4444-444444444444"

// status asserts the HTTP status and, when reason != "", the JSON "reason".
func status(t *testing.T, f *apiFixture, method, path, body string, want int, reason string) {
	t.Helper()
	w := f.req(t, method, path, "admin", body)
	if w.Code != want {
		t.Fatalf("%s %s: status = %d, want %d (%s)", method, path, w.Code, want, w.Body.String())
	}
	if reason != "" {
		if got := decode(t, w)["reason"]; got != reason {
			t.Fatalf("%s %s: reason = %v, want %q (%s)", method, path, got, reason, w.Body.String())
		}
	}
}

// TestValidationFailures exercises the failSvc validation_failed branch and the
// service-level validators the happy path never trips.
func TestValidationFailures(t *testing.T) {
	f := newAPI(t)

	// Unknown provider type on create → configs.ValidationError → 422.
	status(t, f, "POST", p+"/configurations", `{"name":"x","provider_type":"nope"}`, 422, "validation_failed")

	// ValidateCredentials for an unknown provider → configs.ValidationError → 422.
	status(t, f, "POST", p+"/configurations/validate", `{"provider_type":"nope","credentials":{}}`, 422, "validation_failed")

	// A real config + target, then attach an override carrying a credential-shaped
	// key → targets.ValidationError → 422 (rejectCredentialKeys).
	cfgID := decode(t, f.req(t, "POST", p+"/configurations", "admin", `{"name":"ep","provider_type":"dummy"}`))["id"].(string)
	tid := decode(t, f.req(t, "POST", p+"/targets", "admin", `{"name":"t","certificate_filters":[]}`))["id"].(string)
	status(t, f, "POST", p+"/targets/"+tid+"/configurations",
		`{"configuration_ids":["`+cfgID+`"],"config_overrides":{"`+cfgID+`":{"password":"x"}}}`, 422, "validation_failed")
}

// TestNotFoundFailures exercises the not_found branch across every resource
// handler with a well-formed but absent UUID.
func TestNotFoundFailures(t *testing.T) {
	f := newAPI(t)

	// Configurations.
	status(t, f, "GET", p+"/configurations/"+absentID, "", 404, "not_found")
	status(t, f, "PUT", p+"/configurations/"+absentID, `{"name":"x"}`, 404, "not_found")
	status(t, f, "POST", p+"/configurations/"+absentID+"/remove", `{}`, 404, "not_found")

	// Targets.
	status(t, f, "GET", p+"/targets/"+absentID, "", 404, "not_found")
	status(t, f, "PUT", p+"/targets/"+absentID, `{"name":"x","certificate_filters":[]}`, 404, "not_found")
	status(t, f, "POST", p+"/targets/"+absentID+"/remove", `{}`, 404, "not_found")
	status(t, f, "GET", p+"/targets/"+absentID+"/configurations", "", 404, "not_found")
	status(t, f, "POST", p+"/targets/"+absentID+"/configurations", `{"configuration_ids":[]}`, 404, "not_found")
	status(t, f, "POST", p+"/targets/"+absentID+"/configurations/remove", `{"configuration_ids":[]}`, 404, "not_found")

	// Jobs.
	status(t, f, "GET", p+"/jobs/"+absentID, "", 404, "not_found")
	status(t, f, "GET", p+"/jobs/"+absentID+"/result", "", 404, "not_found")
	status(t, f, "POST", p+"/jobs/"+absentID+"/cancel", `{}`, 404, "not_found")
	status(t, f, "POST", p+"/jobs/"+absentID+"/retry", `{}`, 404, "not_found")

	// Deploy to absent config / target / job.
	status(t, f, "POST", p+"/deploy", `{"certificate_id":"c","configuration_id":"`+absentID+`"}`, 404, "not_found")
	status(t, f, "POST", p+"/deploy/target", `{"certificate_id":"c","target_id":"`+absentID+`"}`, 404, "not_found")
	status(t, f, "POST", p+"/deploy/configurations", `{"certificate_id":"c","configuration_ids":["`+absentID+`"]}`, 404, "not_found")
	status(t, f, "POST", p+"/deploy/"+absentID+"/verify", `{}`, 404, "not_found")
	status(t, f, "POST", p+"/deploy/"+absentID+"/rollback", `{}`, 404, "not_found")
}

// TestUnsupportedParentJob confirms that verify/rollback on a PARENT job (which
// has no provider) map jobs.ErrUnsupported → 409.
func TestUnsupportedParentJob(t *testing.T) {
	f := newAPI(t)
	cfgID := decode(t, f.req(t, "POST", p+"/configurations", "admin", `{"name":"ep","provider_type":"dummy"}`))["id"].(string)
	tid := decode(t, f.req(t, "POST", p+"/targets", "admin", `{"name":"t","auto_deploy":true,"certificate_filters":[]}`))["id"].(string)
	if w := f.req(t, "POST", p+"/targets/"+tid+"/configurations", "admin", `{"configuration_ids":["`+cfgID+`"]}`); w.Code != 200 {
		t.Fatalf("attach: %d %s", w.Code, w.Body.String())
	}
	// Deploy to the target → PARENT job id in the 202 response.
	dw := f.req(t, "POST", p+"/deploy/target", "admin", `{"certificate_id":"cert-1","target_id":"`+tid+`"}`)
	if dw.Code != 202 {
		t.Fatalf("deploy target: %d %s", dw.Code, dw.Body.String())
	}
	parentID := decode(t, dw)["job_id"].(string)

	status(t, f, "POST", p+"/deploy/"+parentID+"/verify", `{}`, 409, "unsupported")
	status(t, f, "POST", p+"/deploy/"+parentID+"/rollback", `{}`, 409, "unsupported")
}

// TestDeployPreconditions covers the deploy.ErrInactive and deploy.ErrNoConfigs
// mappings (both → 422 validation_failed per deps.go).
func TestDeployPreconditions(t *testing.T) {
	f := newAPI(t)

	// ErrInactive: create a config, flip it inactive in the store, then deploy.
	cfgID := decode(t, f.req(t, "POST", p+"/configurations", "admin", `{"name":"ep","provider_type":"dummy"}`))["id"].(string)
	if err := f.mem.SetConfigurationStatus(context.Background(), apiTenant, cfgID, store.ConfigInactive, "", nil); err != nil {
		t.Fatalf("set inactive: %v", err)
	}
	status(t, f, "POST", p+"/deploy", `{"certificate_id":"cert-1","configuration_id":"`+cfgID+`"}`, 422, "validation_failed")

	// ErrNoConfigs: a target with nothing attached.
	tid := decode(t, f.req(t, "POST", p+"/targets", "admin", `{"name":"empty","certificate_filters":[]}`))["id"].(string)
	status(t, f, "POST", p+"/deploy/target", `{"certificate_id":"cert-1","target_id":"`+tid+`"}`, 422, "validation_failed")
}

// TestBackupBadSchema maps backup.ErrBadSchema → 422 validation_failed.
func TestBackupBadSchema(t *testing.T) {
	f := newAPI(t)
	status(t, f, "POST", p+"/backup/import",
		`{"mode":"skip","backup":{"schema_version":999,"configurations":[],"targets":[],"jobs":[]}}`, 422, "validation_failed")
}

// TestMalformedBody covers the handler DecodeJSON error path → 400 malformed_body
// (unknown field, and syntactically invalid JSON).
func TestMalformedBody(t *testing.T) {
	f := newAPI(t)
	status(t, f, "POST", p+"/configurations", `{"unexpected_field":true}`, 400, "malformed_body")
	status(t, f, "POST", p+"/configurations", `{not-json`, 400, "malformed_body")
}

// TestCSRFMissing builds a raw mutating request WITHOUT the X-CSRF-Token header:
// the OpenAPI request validation (openapi3filter) rejects it before the handler.
func TestCSRFMissing(t *testing.T) {
	f := newAPI(t)
	r := httptest.NewRequest("POST", "https://localhost"+p+"/configurations",
		strings.NewReader(`{"name":"x","provider_type":"dummy"}`))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Authorization", "Bearer admin")
	// Deliberately no X-CSRF-Token header.
	w := httptest.NewRecorder()
	f.s.Handler().ServeHTTP(w, r)
	if w.Code < 400 || w.Code >= 500 {
		t.Fatalf("csrf missing: status = %d (%s), want 4xx", w.Code, w.Body.String())
	}
	if got := decode(t, w)["reason"]; got != "validation_failed" {
		t.Fatalf("csrf missing: reason = %v, want validation_failed (%s)", got, w.Body.String())
	}
}
