package inventoryclient_test

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	invsdk "github.com/go-tangra/go-tangra-inventory/sdk/v4/pkg/inventoryclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/inventoryclient"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/inventoryclient/inventorytest"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/providers/inventoryagent"
)

var _ inventoryagent.Inventory = (*inventoryclient.Client)(nil)

const tenant = "11111111-1111-1111-1111-111111111111"

// The adapter carries references only and passes gRPC status codes through.
func TestAdapterOverTheWire(t *testing.T) {
	fake := inventorytest.NewFake("h1", "h2")
	c := inventoryclient.New(inventorytest.DialFake(t, fake))
	ctx := context.Background()

	d, err := c.CreateCertificateDelivery(ctx, invsdk.DeliveryRequest{TenantID: tenant, IdempotencyKey: "job-1", CertificateID: "cert-1",
		Name: "www", KeyPolicy: "require", HostIDs: []string{"h1"}, RearmFailed: true})
	if err != nil || !d.Created || len(d.Items) != 2 || d.Items[0].State != "installed" {
		t.Fatalf("create = %+v, %v", d, err)
	}
	if again, _ := c.CreateCertificateDelivery(ctx, invsdk.DeliveryRequest{TenantID: tenant, IdempotencyKey: "job-1", CertificateID: "cert-1", Name: "www"}); again.Created || again.ID != d.ID {
		t.Fatalf("replay = %+v", again)
	}
	if got, err := c.GetCertificateDelivery(ctx, tenant, d.ID); err != nil || got.ID != d.ID {
		t.Fatalf("get = %+v, %v", got, err)
	}
	if _, err := c.GetCertificateDelivery(ctx, "other", d.ID); status.Code(err) != codes.NotFound {
		t.Fatalf("foreign get: %v", err)
	}
	if pv, err := c.PreviewCertificateTargets(ctx, tenant, []string{"h1"}, nil); err != nil || len(pv.Hosts) != 2 {
		t.Fatalf("preview = %+v, %v", pv, err)
	}
	if v, err := c.VerifyHostCertificates(ctx, tenant, nil, []string{"role=web"}, "www", "ff"); err != nil || v.Matched != 2 {
		t.Fatalf("verify = %+v, %v", v, err)
	}
	if n, h, err := c.MarkCertificateRevoked(ctx, tenant, "cert-1"); err != nil || n != 1 || h != 2 {
		t.Fatalf("revoke = %d %d %v", n, h, err)
	}
	fake.Fail = status.Error(codes.FailedPrecondition, "certificate delivery is disabled")
	if _, err := c.CreateCertificateDelivery(ctx, invsdk.DeliveryRequest{TenantID: tenant, IdempotencyKey: "job-2", CertificateID: "c", Name: "www"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("status not passed through: %v", err)
	}
}
