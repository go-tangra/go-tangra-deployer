package awsacm_test

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/provider"

	_ "github.com/go-freya/freya/services/deployer/internal/providers/awsacm"
)

const secret = "wJalrXUtnFEMI-super-secret-key"

func creds() map[string]any {
	return map[string]any{"access_key_id": "AKIDEXAMPLE", "secret_access_key": secret}
}

func TestRegistrationAndCapabilities(t *testing.T) {
	p, err := provider.Get("aws_acm")
	if err != nil {
		t.Fatalf("aws_acm not registered: %v", err)
	}
	c := p.Capabilities()
	if c.Type != "aws_acm" || c.DisplayName == "" || !c.SupportsVerify || c.SupportsRollback {
		t.Fatalf("capabilities: %+v", c)
	}
}

func TestDeploySignsAndImports(t *testing.T) {
	var gotAuth, gotTarget, gotContentType string
	var gotBody map[string]string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotTarget = r.Header.Get("X-Amz-Target")
		gotContentType = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(raw, &gotBody)
		w.Header().Set("Content-Type", "application/x-amz-json-1.1")
		_, _ = io.WriteString(w, `{"CertificateArn":"arn:aws:acm:us-east-1:123:certificate/abc"}`)
	}))
	defer srv.Close()

	p, _ := provider.Get("aws_acm")
	config := map[string]any{"region": "us-east-1", "endpoint": srv.URL}
	res, err := p.Deploy(context.Background(), &provider.CertificateData{
		ID: "c1", CertificatePEM: "CERT", PrivateKeyPEM: "KEY", CertificateChain: "CHAIN",
	}, config, creds(), nil)
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if !res.Success {
		t.Fatalf("deploy result: %+v", res)
	}
	if res.Details["certificate_arn"] != "arn:aws:acm:us-east-1:123:certificate/abc" {
		t.Fatalf("arn not mapped: %+v", res.Details)
	}
	if gotTarget != "CertificateManager.ImportCertificate" {
		t.Fatalf("target = %q", gotTarget)
	}
	if gotContentType != "application/x-amz-json-1.1" {
		t.Fatalf("content-type = %q", gotContentType)
	}
	if !strings.HasPrefix(gotAuth, "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/") {
		t.Fatalf("auth header shape: %q", gotAuth)
	}
	if strings.Contains(gotAuth, secret) {
		t.Fatal("secret access key leaked into Authorization header")
	}
	// Blobs are base64-encoded on the wire.
	if got, _ := base64.StdEncoding.DecodeString(gotBody["Certificate"]); string(got) != "CERT" {
		t.Fatalf("Certificate blob = %q", gotBody["Certificate"])
	}
	if got, _ := base64.StdEncoding.DecodeString(gotBody["PrivateKey"]); string(got) != "KEY" {
		t.Fatalf("PrivateKey blob = %q", gotBody["PrivateKey"])
	}
}

func TestDeployFailureDoesNotLeakSecret(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = io.WriteString(w, `{"__type":"ValidationException","message":"bad cert"}`)
	}))
	defer srv.Close()
	p, _ := provider.Get("aws_acm")
	res, err := p.Deploy(context.Background(), &provider.CertificateData{CertificatePEM: "C", PrivateKeyPEM: "K"},
		map[string]any{"region": "us-east-1", "endpoint": srv.URL}, creds(), nil)
	if err != nil {
		t.Fatalf("unexpected transport error: %v", err)
	}
	if res.Success || !strings.Contains(res.Message, "bad cert") {
		t.Fatalf("expected failure with acm message: %+v", res)
	}
	if strings.Contains(res.Message, secret) {
		t.Fatal("secret leaked into failure message")
	}
}

func TestVerifyReadsBack(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tgt := r.Header.Get("X-Amz-Target"); tgt != "CertificateManager.DescribeCertificate" {
			t.Errorf("verify target = %q", tgt)
		}
		_, _ = io.WriteString(w, `{"Certificate":{"Serial":"0a","Status":"ISSUED"}}`)
	}))
	defer srv.Close()
	p, _ := provider.Get("aws_acm")
	res, err := p.Verify(context.Background(), &provider.CertificateData{},
		map[string]any{"region": "us-east-1", "endpoint": srv.URL, "certificate_arn": "arn:aws:acm:x"}, creds())
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !res.Success {
		t.Fatalf("verify result: %+v", res)
	}
}

func TestRollbackUnsupported(t *testing.T) {
	p, _ := provider.Get("aws_acm")
	res, _ := p.Rollback(context.Background(), &provider.CertificateData{}, nil, nil)
	if res.Success {
		t.Fatal("rollback must not report success for ACM")
	}
}

func TestValidateCredentials(t *testing.T) {
	p, _ := provider.Get("aws_acm")
	if err := p.ValidateCredentials(context.Background(), map[string]any{}, map[string]any{"region": "us-east-1"}); err == nil {
		t.Fatal("expected missing-credential error")
	}
	if err := p.ValidateCredentials(context.Background(), creds(), map[string]any{}); err == nil {
		t.Fatal("expected missing-region error")
	}
	if err := p.ValidateCredentials(context.Background(), creds(), map[string]any{"region": "us-east-1"}); err != nil {
		t.Fatalf("valid creds rejected: %v", err)
	}
}
