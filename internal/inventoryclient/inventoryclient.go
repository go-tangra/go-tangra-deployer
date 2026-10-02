package inventoryclient

import (
	"context"
	"time"

	"google.golang.org/grpc"

	invsdk "github.com/go-tangra/go-tangra-inventory/sdk/v4/pkg/inventoryclient"
)

// DefaultTimeout bounds one inventory call.
const DefaultTimeout = 15 * time.Second

// Client calls inventory.v1.CertificateDeliveryService over the mesh with a
// per-call deadline. gRPC status errors are returned unchanged so callers can
// tell refusals (InvalidArgument, FailedPrecondition, PermissionDenied) from
// transient failures.
type Client struct {
	sdk     *invsdk.Client
	timeout time.Duration
}

// New builds a client over a connected (SPIFFE-mTLS) connection to the
// inventory service, e.g. freya.App.Client(ctx, "inventory").
func New(conn grpc.ClientConnInterface) *Client {
	return &Client{sdk: invsdk.New(conn), timeout: DefaultTimeout}
}

func (c *Client) ctx(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, c.timeout)
}

// CreateCertificateDelivery creates (or idempotently replays) a delivery.
func (c *Client) CreateCertificateDelivery(ctx context.Context, r invsdk.DeliveryRequest) (invsdk.Delivery, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.sdk.CreateCertificateDelivery(cctx, r)
}

// GetCertificateDelivery reads a delivery of the tenant.
func (c *Client) GetCertificateDelivery(ctx context.Context, tenantID, id string) (invsdk.Delivery, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.sdk.GetCertificateDelivery(cctx, tenantID, id)
}

// PreviewCertificateTargets resolves a host selection without writes.
func (c *Client) PreviewCertificateTargets(ctx context.Context, tenantID string, ids, tags []string) (invsdk.Preview, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.sdk.PreviewCertificateTargets(cctx, tenantID, ids, tags)
}

// VerifyHostCertificates compares the hosts' last reports with a fingerprint.
func (c *Client) VerifyHostCertificates(ctx context.Context, tenantID string, ids, tags []string, name, fingerprint string) (invsdk.Verification, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.sdk.VerifyHostCertificates(cctx, tenantID, ids, tags, name, fingerprint)
}

// MarkCertificateRevoked cancels queued deliveries of a revoked certificate
// and flags hosts holding it (files stay on the hosts).
func (c *Client) MarkCertificateRevoked(ctx context.Context, tenantID, certificateID string) (int, int, error) {
	cctx, cancel := c.ctx(ctx)
	defer cancel()
	return c.sdk.MarkCertificateRevoked(cctx, tenantID, certificateID)
}
