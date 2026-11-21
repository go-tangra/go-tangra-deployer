package fortigate

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"
)

// issuerChain returns a chain as lcm stores it: an issuing CA then the
// self-signed root that signed it.
func issuerChain(t *testing.T) (interPEM, rootPEM string) {
	t.Helper()
	mk := func(cn string, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) (*x509.Certificate, *ecdsa.PrivateKey, string) {
		key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
		tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
			IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
		if parent == nil {
			parent, parentKey = tmpl, key
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, parent, &key.PublicKey, parentKey)
		if err != nil {
			t.Fatal(err)
		}
		c, _ := x509.ParseCertificate(der)
		return c, key, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	}
	root, rootKey, rootPEM := mk("Test Root", nil, nil)
	_, _, interPEM = mk("Test Issuing CA", root, rootKey)
	return interPEM, rootPEM
}

func TestDeployImportsIssuingCAOnce(t *testing.T) {
	inter, root := issuerChain(t)
	f := newFakeFortiOS()
	srv := f.serve(t)
	noSettle(t)
	cd := newCertData(t, 21)
	cd.CertificateChain = inter + root
	cfg := map[string]any{"replace_strategy": "delete"}

	res, err := (Provider{}).Deploy(context.Background(), cd, cfg, fgCreds(srv), nil)
	noSecrets(t, cd, res, err)
	if err != nil || !res.Success || res.Details["issuer_cas_imported"] != 1 || res.Details["issuer_cas_present"] != 0 {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	// Only the issuing CA is imported (the root is left out), with the scope.
	imp := f.bodies(http.MethodPost, "monitor/vpn-certificate/ca/import")
	if len(imp) != 1 || imp[0]["scope"] != "global" || imp[0]["import_method"] != "file" {
		t.Fatalf("CA imports = %v", imp)
	}
	if dec, _ := base64.StdEncoding.DecodeString(imp[0]["file_content"].(string)); string(dec) != inter {
		t.Fatal("imported CA is not the issuing CA")
	}
	// The local certificate is still the leaf alone (FortiOS refuses a chain).
	local := f.bodies(http.MethodPost, "monitor/vpn-certificate/local/import")
	if dec, _ := base64.StdEncoding.DecodeString(local[0]["file_content"].(string)); strings.Count(string(dec), "BEGIN CERTIFICATE") != 1 {
		t.Fatal("local certificate import carries more than the leaf")
	}

	// A second deployment finds the CA on the device and imports nothing.
	res, err = (Provider{}).Deploy(context.Background(), cd, cfg, fgCreds(srv), nil)
	if err != nil || !res.Success || res.Details["issuer_cas_imported"] != 0 || res.Details["issuer_cas_present"] != 1 {
		t.Fatalf("redeploy = %+v, %v", res, err)
	}
	if n := len(f.bodies(http.MethodPost, "monitor/vpn-certificate/ca/import")); n != 1 {
		t.Fatalf("CA imported %d times", n)
	}
}

func TestDeployCAImportFailureIsNotFatal(t *testing.T) {
	inter, root := issuerChain(t)
	f := newFakeFortiOS()
	f.failCA = 403
	srv := f.serve(t)
	noSettle(t)
	cd := newCertData(t, 22)
	cd.CertificateChain = inter + root

	res, err := (Provider{}).Deploy(context.Background(), cd, map[string]any{"replace_strategy": "delete"}, fgCreds(srv), nil)
	noSecrets(t, cd, res, err)
	if err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	if !strings.Contains(res.Message, "issuing CA certificate could not be imported") || res.Details["issuer_cas_error"] == nil {
		t.Fatalf("no warning: %+v", res)
	}
}

func TestDeployWithoutChainTouchesNoCAs(t *testing.T) {
	f := newFakeFortiOS()
	srv := f.serve(t)
	noSettle(t)
	cd := newCertData(t, 23)
	if res, err := (Provider{}).Deploy(context.Background(), cd, map[string]any{"replace_strategy": "delete"}, fgCreds(srv), nil); err != nil || !res.Success {
		t.Fatalf("deploy = %+v, %v", res, err)
	}
	if f.count(http.MethodGet, "cmdb/certificate/ca") != 0 {
		t.Fatal("CA store read without issuing CAs")
	}
}
