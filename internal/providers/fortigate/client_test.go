package fortigate

import (
	"strings"
	"testing"
)

// Port of the v3 provider's client_test.go.

func TestIsReferenced(t *testing.T) {
	tests := []struct {
		name string
		r    *apiResponse
		want bool
	}{
		{"http 424", &apiResponse{StatusCode: 424}, true},
		{"cli error -23", &apiResponse{StatusCode: 500, Env: fortiEnvelope{Error: -23}}, true},
		{"body in use", &apiResponse{StatusCode: 500, Body: []byte(`{"cli_error":"entry is used by ..."}`), Env: fortiEnvelope{CLIError: "entry is used by ssl-ssh-profile"}}, true},
		{"body being used", &apiResponse{StatusCode: 500, Body: []byte("currently being used")}, true},
		{"message datasource conflict", &apiResponse{StatusCode: 500, Env: fortiEnvelope{Message: "Datasource conflict"}}, true},
		{"plain 403 not referenced", &apiResponse{StatusCode: 403, Body: []byte(`{"status":"error"}`)}, false},
		{"ok", &apiResponse{StatusCode: 200}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isReferenced(tt.r); got != tt.want {
				t.Errorf("isReferenced = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestApiError_Annotations(t *testing.T) {
	inUse := apiError("delete certificate x", &apiResponse{
		StatusCode: 424,
		Body:       []byte(`{"status":"error","http_status":424}`),
		Env:        fortiEnvelope{Status: "error", HTTPStatus: 424},
	})
	if !strings.Contains(inUse.Error(), "referenced/in-use") {
		t.Errorf("expected in-use annotation, got: %v", inUse)
	}

	forbidden := apiError("import", &apiResponse{
		StatusCode: 403,
		Body:       []byte(`{"status":"error"}`),
		Env:        fortiEnvelope{Status: "error"},
	})
	if !strings.Contains(forbidden.Error(), "write permission") {
		t.Errorf("expected forbidden hint, got: %v", forbidden)
	}

	full := apiError("op", &apiResponse{
		StatusCode: 500,
		Body:       []byte(strings.Repeat("x", 600)),
		Env:        fortiEnvelope{Status: "error", Error: -651, CLIError: "bad", Message: "msg"},
	})
	want := `fortigate: op failed: HTTP 500, status=error, error=-651, cli_error="bad", message="msg": ` + strings.Repeat("x", 512) + "…"
	if full.Error() != want {
		t.Errorf("apiError = %q", full.Error())
	}
}

func TestScrub(t *testing.T) {
	if got := string(scrub([]byte("a TOKEN b TOKEN"), "TOKEN", "")); got != "a "+redacted+" b "+redacted {
		t.Fatalf("scrub = %q", got)
	}
	if got := string(scrub([]byte("clean"), "TOKEN")); got != "clean" {
		t.Fatalf("scrub = %q", got)
	}
}
