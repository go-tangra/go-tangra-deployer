package provider

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
)

type fake struct{ caps Capabilities }

func (f fake) Deploy(context.Context, *CertificateData, map[string]any, map[string]any, ProgressFn) (*Result, error) {
	return &Result{Success: true}, nil
}
func (f fake) Verify(context.Context, *CertificateData, map[string]any, map[string]any) (*Result, error) {
	return &Result{Success: true}, nil
}
func (f fake) Rollback(context.Context, *CertificateData, map[string]any, map[string]any) (*Result, error) {
	return &Result{Success: true}, nil
}
func (f fake) ValidateCredentials(context.Context, map[string]any, map[string]any) error { return nil }
func (f fake) Capabilities() Capabilities                                                { return f.caps }

func TestRegisterGetListInfo(t *testing.T) {
	reset()
	t.Cleanup(reset)
	Register(fake{caps: Capabilities{Type: "beta", DisplayName: "Beta", SupportsVerify: true}})
	Register(fake{caps: Capabilities{Type: "alpha", DisplayName: "Alpha", SupportsRollback: true}})

	if !Exists("alpha") || Exists("nope") {
		t.Fatal("Exists wrong")
	}
	if _, err := Get("nope"); err != ErrUnknownProvider {
		t.Fatalf("Get(nope) = %v, want ErrUnknownProvider", err)
	}
	if _, err := Get("alpha"); err != nil {
		t.Fatalf("Get(alpha): %v", err)
	}
	list := List()
	if len(list) != 2 || list[0].Type != "alpha" || list[1].Type != "beta" {
		t.Fatalf("List not sorted deterministically: %+v", list)
	}
	c, ok := Info("beta")
	if !ok || !c.SupportsVerify {
		t.Fatalf("Info(beta) = %+v, %v", c, ok)
	}
	if _, ok := Info("nope"); ok {
		t.Fatal("Info(nope) should be false")
	}
}

func TestRegisterEmptyTypePanics(t *testing.T) {
	reset()
	t.Cleanup(reset)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on empty type")
		}
	}()
	Register(fake{caps: Capabilities{Type: ""}})
}

func TestRegisterDuplicatePanics(t *testing.T) {
	reset()
	t.Cleanup(reset)
	Register(fake{caps: Capabilities{Type: "x"}})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate type")
		}
	}()
	Register(fake{caps: Capabilities{Type: "x"}})
}

// T025: the field descriptor serialises every key of
// contracts/deployer-config-ui.md §2 when set and omits the optional ones when
// not, so a bare field keeps its pre-033 JSON.
func TestFieldJSON(t *testing.T) {
	bare, err := json.Marshal(Field{Key: "zone_id", Label: "Zone ID", Required: true})
	if err != nil {
		t.Fatal(err)
	}
	if string(bare) != `{"key":"zone_id","label":"Zone ID","secret":false,"required":true}` {
		t.Fatalf("bare field JSON = %s", bare)
	}
	full := Field{
		Key: "wait_seconds", Label: "Wait", Type: TypeInt, Required: true, Overridable: true, Default: 60,
		Options: []Option{{Value: "a", Label: "A"}}, Help: "h", Placeholder: "p", Group: GroupOptions,
		Min: IntPtr(0), Max: IntPtr(240), MaxLength: 10, Pattern: "^x$", MaxItems: 3,
	}
	raw, err := json.Marshal(full)
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	for _, k := range []string{"key", "label", "type", "secret", "required", "overridable", "default", "options", "help",
		"placeholder", "group", "min", "max", "max_length", "pattern", "max_items"} {
		if _, ok := m[k]; !ok {
			t.Errorf("descriptor key %q missing from %s", k, raw)
		}
	}
	if m["min"] != float64(0) {
		t.Fatalf("min 0 must be serialised: %s", raw)
	}
	// A false default is a value, not "unset".
	raw, _ = json.Marshal(Field{Key: "b", Label: "B", Type: TypeBool, Default: false})
	if !strings.Contains(string(raw), `"default":false`) {
		t.Fatalf("false default dropped: %s", raw)
	}
}

func TestCapabilitiesJSON(t *testing.T) {
	raw, _ := json.Marshal(Capabilities{Type: "x", DisplayName: "X"})
	var m map[string]any
	_ = json.Unmarshal(raw, &m)
	for _, k := range []string{"description", "test_connection", "schema_version", "one_of_required"} {
		if _, ok := m[k]; ok {
			t.Errorf("%q must be omitted when unset: %s", k, raw)
		}
	}
	if v, ok := m["delivers_by_reference"]; !ok || v != false {
		t.Fatalf("delivers_by_reference must always be serialised: %s", raw)
	}
	raw, _ = json.Marshal(Capabilities{Type: "x", Description: "d", TestConnection: true, SchemaVersion: 1,
		DeliversByReference: true, OneOfRequired: [][]string{{"a", "b"}}})
	_ = json.Unmarshal(raw, &m)
	if m["description"] != "d" || m["test_connection"] != true || m["schema_version"] != float64(1) ||
		m["delivers_by_reference"] != true || m["one_of_required"] == nil {
		t.Fatalf("capabilities JSON = %s", raw)
	}
}

func TestJobMetaRoundTrip(t *testing.T) {
	if _, ok := JobFrom(context.Background()); ok {
		t.Fatal("JobFrom on a bare context must report absent")
	}
	want := JobMeta{TenantID: "t", JobID: "j", ConfigurationID: "c", TargetID: "g", Trigger: TriggerAutoDeploy}
	got, ok := JobFrom(WithJob(context.Background(), want))
	if !ok || got != want {
		t.Fatalf("JobFrom = %+v, %v", got, ok)
	}
}

func TestRegisterRejectsInvalidDeclaration(t *testing.T) {
	reset()
	t.Cleanup(reset)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on an invalid declaration")
		}
	}()
	Register(fake{caps: Capabilities{Type: "bad", CredentialFields: []Field{{Key: "token", Label: "Token", Secret: true, Overridable: true}}}})
}

// TestNoRedirect (T110): credential-bearing clients never follow a redirect.
func TestNoRedirect(t *testing.T) {
	if err := NoRedirect(nil, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("NoRedirect = %v", err)
	}
}
