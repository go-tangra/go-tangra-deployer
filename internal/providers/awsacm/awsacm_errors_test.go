package awsacm_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

// TestValidateMissingSecretOnly exercises the credsFrom branch where the access
// key is present but the secret access key is missing.
func TestValidateMissingSecretOnly(t *testing.T) {
	p, _ := provider.Get("aws_acm")
	if err := p.ValidateCredentials(context.Background(),
		map[string]any{"access_key_id": "AKID"}, map[string]any{"region": "us-east-1"}); err == nil {
		t.Fatal("expected error when secret_access_key missing")
	}
}

// TestDeployMissingInputs covers Deploy's three guard clauses: bad credentials,
// missing region, and missing certificate material.
func TestDeployMissingInputs(t *testing.T) {
	p, _ := provider.Get("aws_acm")
	ctx := context.Background()

	if _, err := p.Deploy(ctx, &provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K"},
		map[string]any{"region": "us-east-1"}, map[string]any{}, nil); err == nil {
		t.Fatal("expected credentials error")
	}
	if _, err := p.Deploy(ctx, &provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K"},
		map[string]any{}, creds(), nil); err == nil {
		t.Fatal("expected region error")
	}
	if _, err := p.Deploy(ctx, &provider.CertificateData{},
		map[string]any{"region": "us-east-1"}, creds(), nil); err == nil {
		t.Fatal("expected certificate-material error")
	}
}

// TestDeployReimportWithSessionToken drives the certificate_arn reimport branch,
// the certificate-chain branch, and the session-token signing branch together.
func TestDeployReimportWithSessionToken(t *testing.T) {
	var gotBody map[string]string
	var gotSecTok string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSecTok = r.Header.Get("X-Amz-Security-Token")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		_, _ = io.WriteString(w, `{"CertificateArn":"arn:aws:acm:us-east-1:1:certificate/xyz"}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("aws_acm")
	cr := map[string]any{"access_key_id": "AKIDEXAMPLE", "secret_access_key": secret, "session_token": "SESSION-Tok"}
	res, err := p.Deploy(context.Background(),
		&provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K", CertificateChain: "CH"},
		map[string]any{"region": "us-east-1", "endpoint": srv.URL, "certificate_arn": "arn:existing"}, cr, nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success {
		t.Fatalf("result: %+v", res)
	}
	if gotBody["CertificateArn"] != "arn:existing" {
		t.Fatalf("reimport arn not sent: %+v", gotBody)
	}
	if gotBody["CertificateChain"] == "" {
		t.Fatalf("chain not sent: %+v", gotBody)
	}
	if gotSecTok != "SESSION-Tok" {
		t.Fatalf("session-token header = %q", gotSecTok)
	}
}

// TestDeployErrorMessageVariants covers acmError's Message/type/default branches
// and confirms the secret never appears in the failure message.
func TestDeployErrorMessageVariants(t *testing.T) {
	cases := []struct {
		name, body, want string
	}{
		{"capital Message field", `{"Message":"capital message"}`, "capital message"},
		{"type only", `{"__type":"ThrottlingException"}`, "ThrottlingException"},
		{"empty body", ``, "unknown error"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()

			p, _ := provider.Get("aws_acm")
			res, err := p.Deploy(context.Background(),
				&provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K"},
				map[string]any{"region": "us-east-1", "endpoint": srv.URL}, creds(), nil)
			if err != nil {
				t.Fatalf("unexpected transport error: %v", err)
			}
			if res.Success || !strings.Contains(res.Message, tc.want) {
				t.Fatalf("want %q in message, got %+v", tc.want, res)
			}
			if strings.Contains(res.Message, secret) {
				t.Fatal("secret leaked into failure message")
			}
		})
	}
}

// TestVerifyErrorPaths covers Verify's credential guard, missing-arn guard, the
// transport-error path, and the non-ISSUED status path.
func TestVerifyErrorPaths(t *testing.T) {
	p, _ := provider.Get("aws_acm")
	ctx := context.Background()

	if _, err := p.Verify(ctx, &provider.CertificateData{},
		map[string]any{"region": "us-east-1", "certificate_arn": "a"}, map[string]any{}); err == nil {
		t.Fatal("expected credentials error")
	}

	res, err := p.Verify(ctx, &provider.CertificateData{},
		map[string]any{"region": "us-east-1"}, creds())
	if err != nil || res.Success {
		t.Fatalf("expected clean missing-arn failure: res=%+v err=%v", res, err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"message":"nope"}`)
	}))
	defer srv.Close()
	res, err = p.Verify(ctx, &provider.CertificateData{},
		map[string]any{"region": "us-east-1", "endpoint": srv.URL, "certificate_arn": "arn:x"}, creds())
	if err != nil || res.Success {
		t.Fatalf("expected clean transport failure: res=%+v err=%v", res, err)
	}

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"Certificate":{"Serial":"0b","Status":"PENDING_VALIDATION"}}`)
	}))
	defer srv2.Close()
	res, err = p.Verify(ctx, &provider.CertificateData{},
		map[string]any{"region": "us-east-1", "endpoint": srv2.URL, "certificate_arn": "arn:x"}, creds())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if res.Success {
		t.Fatalf("expected non-ISSUED status to fail verification: %+v", res)
	}
}
