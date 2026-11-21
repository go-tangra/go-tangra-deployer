package provider

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"strings"
)

// Leaf returns the first certificate of CertificatePEM, PEM-encoded (lcm's
// cert_pem is the leaf alone; a bundle passed there is reduced to its leaf).
// CertificatePEM without a PEM certificate is returned as is.
func (c CertificateData) Leaf() string {
	if blocks := certBlocks(c.CertificatePEM); len(blocks) > 0 {
		return string(pem.EncodeToMemory(blocks[0]))
	}
	return c.CertificatePEM
}

// IssuerCerts returns the issuing CA certificates of the chain, PEM-encoded
// one per entry, in chain order: CertificateChain without self-signed roots
// (a client has its own roots; sending them only grows the handshake), without
// a copy of the leaf and without duplicates. Certificates that cannot be
// parsed are kept, since there is no way to tell they are roots.
func (c CertificateData) IssuerCerts() []string {
	var leafDER []byte
	if blocks := certBlocks(c.CertificatePEM); len(blocks) > 0 {
		leafDER = blocks[0].Bytes
	}
	seen := map[string]bool{}
	var out []string
	for _, b := range certBlocks(c.CertificateChain) {
		if bytes.Equal(b.Bytes, leafDER) || seen[string(b.Bytes)] {
			continue
		}
		seen[string(b.Bytes)] = true
		if crt, err := x509.ParseCertificate(b.Bytes); err == nil && selfSigned(crt) {
			continue
		}
		out = append(out, string(pem.EncodeToMemory(b)))
	}
	return out
}

// IssuerChain is IssuerCerts concatenated: the chain a server sends after its
// own certificate. It is "" when there are no issuing CA certificates; a
// CertificateChain without any PEM certificate is returned as is.
func (c CertificateData) IssuerChain() string {
	if len(certBlocks(c.CertificateChain)) == 0 {
		return c.CertificateChain
	}
	return strings.Join(c.IssuerCerts(), "")
}

// Bundle is the certificate followed by its issuing CA certificates (the
// "fullchain" a TLS server serves): Leaf then IssuerChain. Roots are left out.
func (c CertificateData) Bundle() string {
	leaf, issuers := c.Leaf(), c.IssuerChain()
	if issuers != "" && leaf != "" && !strings.HasSuffix(leaf, "\n") {
		leaf += "\n"
	}
	return leaf + issuers
}

func certBlocks(s string) []*pem.Block {
	var out []*pem.Block
	rest := []byte(s)
	for {
		var b *pem.Block
		b, rest = pem.Decode(rest)
		if b == nil {
			return out
		}
		if b.Type == "CERTIFICATE" {
			out = append(out, &pem.Block{Type: b.Type, Bytes: b.Bytes})
		}
	}
}

func selfSigned(c *x509.Certificate) bool {
	return bytes.Equal(c.RawSubject, c.RawIssuer) && c.CheckSignatureFrom(c) == nil
}
