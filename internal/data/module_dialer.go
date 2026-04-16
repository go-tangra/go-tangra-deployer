package data

import (
	"os"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-common/grpcx"
	"github.com/go-tangra/go-tangra-common/registration"
)

// NewRegistrationClient creates a registration client connected to admin-service.
// This is created early (during Wire DI) so its admin connection can be shared
// with ModuleDialer for module-to-module resolution.
func NewRegistrationClient(ctx *bootstrap.Context) (*registration.Client, error) {
	adminEndpoint := os.Getenv("ADMIN_GRPC_ENDPOINT")
	if adminEndpoint == "" {
		return nil, nil
	}

	cfg := &registration.Config{
		AdminEndpoint: adminEndpoint,
		MaxRetries:    60,
	}

	return registration.NewClient(ctx.GetLogger(), cfg)
}

// NewModuleDialer creates a ModuleDialer from the registration client's admin connection.
func NewModuleDialer(ctx *bootstrap.Context, regClient *registration.Client) *grpcx.ModuleDialer {
	if regClient == nil {
		return nil
	}
	certsDir := os.Getenv("CERTS_DIR")
	return grpcx.NewModuleDialer(ctx.GetLogger(), "deployer", regClient.AdminConn(), certsDir)
}

// RegistrationClientCleanup returns a cleanup function for the registration client.
func RegistrationClientCleanup(client *registration.Client) func() {
	return func() {
		if client != nil {
			_ = client.Close()
		}
	}
}

// RegistrationBundle holds the registration client and logger for lifecycle management.
type RegistrationBundle struct {
	Client *registration.Client
	Logger log.Logger
}
