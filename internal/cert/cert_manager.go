// Package cert is the module-side bridge to go-tangra-common's
// certificate bootstrap pipeline. NewCertManager runs cert.Ensure
// at every boot — when the local cert is valid + fresh it's a fast
// no-op (one stat + one parse); when missing/expired/inside the
// renewal window it dials LCM:9101, signs a CSR, and writes the
// new cert to disk before returning.
//
// The wire graph is unaware of this — it still calls NewCertManager
// once at startup and gets back a fully populated *CertManager.
package cert

import (
	"context"

	commonCert "github.com/go-tangra/go-tangra-common/cert"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

type CertManager = commonCert.CertManager

// NewCertManager bootstraps + loads the module's mTLS certificates.
// All knobs come from the environment — see go-tangra-common's
// cert.EnsureConfig for the full list. The required ones are:
//
//	LCM_BOOTSTRAP_ENDPOINT   lcm-service:9101
//	MODULE_BOOTSTRAP_SECRET  shared secret matching LCM's config
//	LCM_CA_FINGERPRINT       SHA-256 hex of the LCM root CA
//
// CERTS_DIR (default /app/certs) is where the {ca,server,client}
// subdirs live. The compose stack mounts a per-service named volume
// here so each module owns its own cert state.
func NewCertManager(ctx *bootstrap.Context) (*CertManager, error) {
	return commonCert.Ensure(context.Background(), commonCert.EnsureConfig{
		ModuleID: "deployer",
		Logger:   ctx.GetLogger(),
	})
}
