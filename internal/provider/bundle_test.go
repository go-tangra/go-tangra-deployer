package provider

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"testing"
	"time"
)

type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pem  string
}

// mint signs a certificate for cn with parent (self-signed when parent is nil).
func mint(t *testing.T, cn string, isCA bool, parent *testCA, key *ecdsa.PrivateKey) *testCA {
	t.Helper()
	if key == nil {
		var err error
		if key, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader); err != nil {
			t.Fatal(err)
		}
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: cn}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: isCA, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature}
	signer, signerKey := tmpl, key
	if parent != nil {
		signer, signerKey = parent.cert, parent.key
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, signer, &key.PublicKey, signerKey)
	if err != nil {
		t.Fatal(err)
	}
	c, _ := x509.ParseCertificate(der)
	return &testCA{cert: c, key: key, pem: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))}
}

func TestBundleIsLeafPlusIssuersWithoutRoots(t *testing.T) {
	oldRoot := mint(t, "Old Root", true, nil, nil)
	root := mint(t, "Root G5", true, nil, nil)
	cross := mint(t, "Root G5", true, oldRoot, root.key) // Root G5 cross-signed by Old Root
	inter := mint(t, "Issuing CA", true, root, nil)
	leaf := mint(t, "www.example.org", false, inter, nil)

	// The shape of a DigiCert chain as stored by lcm, plus the leaf repeated
	// and the intermediate twice.
	c := CertificateData{CertificatePEM: leaf.pem, CertificateChain: leaf.pem + inter.pem + root.pem + cross.pem + inter.pem + oldRoot.pem}

	if got := c.IssuerCerts(); len(got) != 2 || got[0] != inter.pem || got[1] != cross.pem {
		t.Fatalf("IssuerCerts = %d certs, want the intermediate then the cross-signed root", len(got))
	}
	if got, want := c.Bundle(), leaf.pem+inter.pem+cross.pem; got != want {
		t.Fatalf("Bundle = %q, want leaf + intermediate + cross-signed root", got)
	}
	if c.Leaf() != leaf.pem {
		t.Fatal("Leaf is not the certificate")
	}
}

func TestBundleEdgeCases(t *testing.T) {
	root := mint(t, "Root", true, nil, nil)
	inter := mint(t, "Issuing CA", true, root, nil)
	leaf := mint(t, "a.example.org", false, inter, nil)

	// A bundle in CertificatePEM is reduced to its leaf.
	if got := (CertificateData{CertificatePEM: leaf.pem + inter.pem}).Leaf(); got != leaf.pem {
		t.Fatal("Leaf of a bundle is not its first certificate")
	}
	// No chain: the bundle is the certificate alone.
	if got := (CertificateData{CertificatePEM: leaf.pem}).Bundle(); got != leaf.pem {
		t.Fatalf("Bundle without a chain = %q", got)
	}
	// A chain of roots only adds nothing.
	if got := (CertificateData{CertificatePEM: leaf.pem, CertificateChain: root.pem}).Bundle(); got != leaf.pem {
		t.Fatal("a root was bundled")
	}
	// Text that is not PEM passes through unchanged (as before).
	c := CertificateData{CertificatePEM: "C", CertificateChain: "CH"}
	if c.Leaf() != "C" || c.IssuerChain() != "CH" || c.Bundle() != "C\nCH" || len(c.IssuerCerts()) != 0 {
		t.Fatalf("non-PEM input: leaf %q chain %q bundle %q", c.Leaf(), c.IssuerChain(), c.Bundle())
	}
}
