package cert

import (
	commonCert "github.com/go-tangra/go-tangra-common/cert"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
)

// CertManager is a type alias for the common cert manager
type CertManager = commonCert.CertManager

// NewCertManager creates a new certificate manager for the deployer service
func NewCertManager(ctx *bootstrap.Context) (*CertManager, error) {
	return commonCert.NewCertManager(ctx, "DEPLOYER")
}
