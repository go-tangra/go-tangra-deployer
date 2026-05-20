// Package tangra_client implements the "tangra-client" deployer provider.
//
// This provider delivers an issued certificate to one or more registered
// tangra-client agents by publishing a certificate update event that the LCM
// streamer forwards over each client's existing mTLS stream. The agent then
// installs the certificate locally (e.g. into nginx).
//
// Because providers are constructed by the registry through a zero-argument
// factory, this package exposes a Pusher seam that the deployer binary wires
// at startup with a concrete implementation that has access to Redis and the
// LCM service client.
package tangra_client

import (
	"context"
	"errors"
	"sync"

	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"
)

// ErrNoPusher is returned when the package-level pusher has not been wired.
// This happens when the provider's Deploy/Verify/Rollback is called before
// SetPusher is invoked during application bootstrap.
var ErrNoPusher = errors.New("tangra-client provider: pusher not initialized — call SetPusher() during bootstrap")

// PushResult is returned per target client by Pusher.PushCertificate.
type PushResult struct {
	ClientID string
	Success  bool
	Message  string
}

// InstalledStatus describes an agent-reported install state for a single
// (client_id, cert_name) pair.
type InstalledStatus struct {
	ClientID          string
	Name              string
	SerialNumber      string
	FingerprintSHA256 string
	// Status mirrors lcmV1.InstalledCertificateStatus as a string so the
	// provider stays free of generated proto types. Possible values:
	// "INSTALLED", "FAILED", "REMOVED", "UNKNOWN".
	Status  string
	Message string
}

// Pusher is the abstraction the provider uses to reach LCM/Redis. The default
// implementation lives in the deployer's internal/data package so the provider
// remains free of infrastructure dependencies and easy to test.
type Pusher interface {
	// ResolveClients turns a target configuration into a concrete list of
	// tangra-client client_ids. If client_ids are provided, they are returned
	// verbatim. If only labels are provided, the implementation queries LCM
	// for matching registered clients. If both are provided, the explicit
	// list takes precedence.
	ResolveClients(ctx context.Context, clientIDs []string, labels map[string]string) ([]string, error)

	// PushCertificate emits one certificate.issued event per client_id so the
	// LCM streamer forwards the certificate over each client's mTLS stream.
	// Returns a per-client result list — the caller decides whether partial
	// success counts as success.
	PushCertificate(ctx context.Context, clientIDs []string, certName string, cert *registry.CertificateData) ([]PushResult, error)

	// VerifyInstalled queries LCM for the agent-reported install state of
	// `certName` on each of `clientIDs`. Clients that have not reported are
	// represented by an entry with Status="UNKNOWN". Used by Provider.Verify
	// to confirm a push actually landed.
	VerifyInstalled(ctx context.Context, clientIDs []string, certName string) ([]InstalledStatus, error)
}

var (
	pusherMu sync.RWMutex
	pusher   Pusher
)

// SetPusher installs the package-level Pusher used by every Provider instance.
// Call this once during bootstrap, after the data layer is initialized.
func SetPusher(p Pusher) {
	pusherMu.Lock()
	defer pusherMu.Unlock()
	pusher = p
}

// getPusher returns the installed Pusher or an error if none has been set.
func getPusher() (Pusher, error) {
	pusherMu.RLock()
	defer pusherMu.RUnlock()
	if pusher == nil {
		return nil, ErrNoPusher
	}
	return pusher, nil
}
