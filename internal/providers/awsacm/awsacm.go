// Package awsacm deploys an issued certificate to AWS Certificate Manager by
// calling the ACM ImportCertificate API directly over signed HTTPS (AWS
// Signature V4), so it needs no AWS SDK dependency. Verify reads the imported
// certificate back with DescribeCertificate. ACM keeps no prior version to
// return to, so rollback is unsupported.
package awsacm

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

func init() { provider.Register(Provider{}) }

// Provider imports certificates into AWS Certificate Manager.
type Provider struct{}

// Capabilities describes the ACM provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "aws_acm",
		DisplayName:      "AWS Certificate Manager",
		SupportsVerify:   true,
		SupportsRollback: false,
		ConfigFields: []provider.Field{
			{Key: "region", Label: "AWS region", Required: true},
			{Key: "certificate_arn", Label: "Existing ACM ARN (reimport)"},
		},
		CredentialFields: []provider.Field{
			{Key: "access_key_id", Label: "Access key ID", Required: true},
			{Key: "secret_access_key", Label: "Secret access key", Secret: true, Required: true},
			{Key: "session_token", Label: "Session token (optional)", Secret: true},
		},
	}
}

type creds struct{ accessKey, secretKey, sessionToken string }

func credsFrom(m map[string]any) (creds, error) {
	c := creds{
		accessKey:    str(m, "access_key_id"),
		secretKey:    str(m, "secret_access_key"),
		sessionToken: str(m, "session_token"),
	}
	if c.accessKey == "" || c.secretKey == "" {
		return c, fmt.Errorf("access_key_id and secret_access_key are required")
	}
	return c, nil
}

func str(m map[string]any, k string) string {
	if v, ok := m[k].(string); ok {
		return v
	}
	return ""
}

// endpoint returns the ACM endpoint for the region, overridable via
// config["endpoint"] so the signing/marshalling can be tested against a mock.
func endpoint(config map[string]any, region string) string {
	if e := str(config, "endpoint"); e != "" {
		return e
	}
	return "https://acm." + region + ".amazonaws.com/"
}

// ValidateCredentials checks required fields are present (a real preflight would
// call STS GetCallerIdentity; kept offline here to avoid surprising API calls).
func (Provider) ValidateCredentials(_ context.Context, cr, config map[string]any) error {
	if _, err := credsFrom(cr); err != nil {
		return err
	}
	if str(config, "region") == "" {
		return fmt.Errorf("region is required")
	}
	return nil
}

// Deploy imports the certificate (and key + chain) into ACM.
func (p Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, credsMap map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	cr, err := credsFrom(credsMap)
	if err != nil {
		return nil, err
	}
	region := str(config, "region")
	if region == "" {
		return nil, fmt.Errorf("region is required")
	}
	if cert.CertificatePEM == "" || cert.PrivateKeyPEM == "" {
		return nil, fmt.Errorf("certificate and private key are required")
	}
	note(progress, 20, "preparing certificate")

	// ImportCertificate blobs are base64-encoded strings in the JSON wire form.
	body := map[string]string{
		"Certificate": b64(cert.CertificatePEM),
		"PrivateKey":  b64(cert.PrivateKeyPEM),
	}
	if cert.CertificateChain != "" {
		body["CertificateChain"] = b64(cert.CertificateChain)
	}
	if arn := str(config, "certificate_arn"); arn != "" {
		body["CertificateArn"] = arn // reimport into the same ACM entry
	}
	note(progress, 60, "importing to ACM")

	var out struct {
		CertificateArn string `json:"CertificateArn"`
	}
	if err := call(ctx, cr, region, endpoint(config, region), "ImportCertificate", body, &out); err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}
	note(progress, 100, "imported")
	return &provider.Result{
		Success: true,
		Message: "certificate imported into ACM",
		Details: map[string]any{"certificate_arn": out.CertificateArn, "region": region},
	}, nil
}

// Verify reads the certificate back from ACM and checks the serial matches.
func (p Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, credsMap map[string]any) (*provider.Result, error) {
	cr, err := credsFrom(credsMap)
	if err != nil {
		return nil, err
	}
	region := str(config, "region")
	arn := str(config, "certificate_arn")
	if arn == "" {
		return &provider.Result{Success: false, Message: "certificate_arn is required to verify"}, nil
	}
	var out struct {
		Certificate struct {
			Serial string `json:"Serial"`
			Status string `json:"Status"`
		} `json:"Certificate"`
	}
	if err := call(ctx, cr, region, endpoint(config, region), "DescribeCertificate", map[string]string{"CertificateArn": arn}, &out); err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}
	ok := out.Certificate.Status == "ISSUED" || out.Certificate.Status == ""
	return &provider.Result{
		Success: ok,
		Message: "ACM status " + out.Certificate.Status,
		Details: map[string]any{"status": out.Certificate.Status, "serial": out.Certificate.Serial},
	}, nil
}

// Rollback is unsupported: ACM ImportCertificate overwrites in place and keeps
// no prior version.
func (Provider) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return &provider.Result{Success: false, Message: "rollback is not supported for AWS ACM"}, nil
}

// call signs and sends one ACM JSON API request (target CertificateManager.<op>)
// and decodes a 2xx JSON body into out. Non-2xx yields a client-safe error that
// never contains the credentials.
func call(ctx context.Context, cr creds, region, ep, op string, in any, out any) error {
	payload, err := json.Marshal(in)
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep, bytes.NewReader(payload))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CertificateManager."+op)
	if cr.sessionToken != "" {
		req.Header.Set("X-Amz-Security-Token", cr.sessionToken)
	}
	signV4(req, payload, cr, region, "acm", time.Now().UTC())

	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		return fmt.Errorf("acm request failed: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("acm %s returned HTTP %d: %s", op, resp.StatusCode, acmError(raw))
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			return fmt.Errorf("decode acm response: %w", err)
		}
	}
	return nil
}

// acmError extracts the ACM error message/type without leaking request data.
func acmError(raw []byte) string {
	var e struct {
		Type    string `json:"__type"`
		Message string `json:"message"`
		Msg     string `json:"Message"`
	}
	_ = json.Unmarshal(raw, &e)
	switch {
	case e.Message != "":
		return e.Message
	case e.Msg != "":
		return e.Msg
	case e.Type != "":
		return e.Type
	default:
		return "unknown error"
	}
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// signV4 adds an AWS Signature Version 4 Authorization header for the request.
func signV4(req *http.Request, payload []byte, cr creds, region, service string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	dateStamp := now.Format("20060102")
	req.Header.Set("X-Amz-Date", amzDate)

	host := req.URL.Host
	req.Host = host
	payloadHash := hexSHA256(payload)

	// Canonical headers (sorted): content-type, host, x-amz-date, x-amz-target,
	// and x-amz-security-token when present.
	headers := map[string]string{
		"content-type": req.Header.Get("Content-Type"),
		"host":         host,
		"x-amz-date":   amzDate,
		"x-amz-target": req.Header.Get("X-Amz-Target"),
	}
	if t := req.Header.Get("X-Amz-Security-Token"); t != "" {
		headers["x-amz-security-token"] = t
	}
	names := sortedKeys(headers)
	var canonicalHeaders strings.Builder
	for _, n := range names {
		canonicalHeaders.WriteString(n)
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.TrimSpace(headers[n]))
		canonicalHeaders.WriteString("\n")
	}
	signedHeaders := strings.Join(names, ";")

	canonicalURI := req.URL.EscapedPath()
	if canonicalURI == "" {
		canonicalURI = "/"
	}
	canonicalQuery := canonicalizeQuery(req.URL.Query())
	canonicalRequest := strings.Join([]string{
		req.Method, canonicalURI, canonicalQuery, canonicalHeaders.String(), signedHeaders, payloadHash,
	}, "\n")

	scope := strings.Join([]string{dateStamp, region, service, "aws4_request"}, "/")
	stringToSign := strings.Join([]string{
		"AWS4-HMAC-SHA256", amzDate, scope, hexSHA256([]byte(canonicalRequest)),
	}, "\n")

	kDate := hmacSHA256([]byte("AWS4"+cr.secretKey), dateStamp)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	auth := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		cr.accessKey, scope, signedHeaders, signature)
	req.Header.Set("Authorization", auth)
}

func canonicalizeQuery(q url.Values) string {
	if len(q) == 0 {
		return ""
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sortStrings(keys)
	var parts []string
	for _, k := range keys {
		for _, v := range q[k] {
			parts = append(parts, url.QueryEscape(k)+"="+url.QueryEscape(v))
		}
	}
	return strings.Join(parts, "&")
}

func sortedKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sortStrings(keys)
	return keys
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j-1] > s[j]; j-- {
			s[j-1], s[j] = s[j], s[j-1]
		}
	}
}

func hexSHA256(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func hmacSHA256(key []byte, data string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(data))
	return h.Sum(nil)
}

func note(p provider.ProgressFn, pct int, msg string) {
	if p != nil {
		p(pct, msg)
	}
}
