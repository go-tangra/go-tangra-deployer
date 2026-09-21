// Package dummy is a mock deployment provider for testing and the MVP demo. It
// performs no external I/O: it reports progress and returns success, or a
// deterministic failure when config["fail"] is truthy, so the full deploy/job/
// retry/verify/rollback flow can be exercised without real infrastructure.
package dummy

import (
	"context"

	"github.com/go-freya/freya/services/deployer/internal/provider"
)

func init() { provider.Register(Provider{}) }

// Provider is the dummy backend. It supports verify and rollback so those paths
// are testable, and needs no config or credentials.
type Provider struct{}

// Capabilities describes the dummy provider.
func (Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:             "dummy",
		DisplayName:      "Dummy (testing)",
		SupportsVerify:   true,
		SupportsRollback: true,
	}
}

// fails reports whether config asks for a deterministic failure.
func fails(config map[string]any) bool {
	switch v := config["fail"].(type) {
	case bool:
		return v
	case string:
		return v == "true" || v == "1"
	default:
		return false
	}
}

func (Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	if progress != nil {
		progress(10, "starting")
		progress(60, "installing")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fails(config) {
		return &provider.Result{Success: false, Message: "dummy: forced failure"}, context.Canceled
	}
	if progress != nil {
		progress(100, "done")
	}
	sn := ""
	if cert != nil {
		sn = cert.SerialNumber
	}
	return &provider.Result{Success: true, Message: "dummy: deployed", Details: map[string]any{"serial": sn}}, nil
}

func (Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if fails(config) {
		return &provider.Result{Success: false, Message: "dummy: verify mismatch"}, nil
	}
	return &provider.Result{Success: true, Message: "dummy: verified"}, nil
}

func (Provider) Rollback(ctx context.Context, cert *provider.CertificateData, config, creds map[string]any) (*provider.Result, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &provider.Result{Success: true, Message: "dummy: rolled back"}, nil
}

// ValidateCredentials always succeeds for the dummy provider (no credentials).
func (Provider) ValidateCredentials(context.Context, map[string]any, map[string]any) error {
	return nil
}
