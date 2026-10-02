// Package inventorytest is an in-process fake of the inventory's
// inventory.v1.CertificateDeliveryService for deployer tests (feature 033):
// it records every request and answers deliveries whose items end in a
// configurable state. It carries references only, like the real service.
package inventorytest

import (
	"context"
	"net"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	invv1 "github.com/go-tangra/go-tangra-inventory/sdk/v4/api/proto/inventory/v1"
)

// Fake is the in-process CertificateDeliveryService.
type Fake struct {
	invv1.UnimplementedCertificateDeliveryServiceServer

	mu       sync.Mutex
	Creates  []*invv1.CreateCertificateDeliveryRequest
	Revokes  []*invv1.MarkCertificateRevokedRequest
	Verifies []*invv1.VerifyHostCertificatesRequest
	// HostIDs answer the selection of every delivery (one item per host).
	HostIDs []string
	// State of every item (default INSTALLED).
	State invv1.DeliveryState
	// Offline marks every agent offline.
	Offline bool
	// Fail, when set, is returned by every call.
	Fail       error
	deliveries map[string]*invv1.CertificateDelivery
}

// NewFake returns a fake whose deliveries resolve to hostIDs.
func NewFake(hostIDs ...string) *Fake {
	return &Fake{HostIDs: hostIDs, State: invv1.DeliveryState_DELIVERY_STATE_INSTALLED, deliveries: map[string]*invv1.CertificateDelivery{}}
}

// CreateCertificateDelivery is idempotent per (tenant, idempotency key).
func (f *Fake) CreateCertificateDelivery(_ context.Context, r *invv1.CreateCertificateDeliveryRequest) (*invv1.CertificateDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Creates = append(f.Creates, r)
	if f.Fail != nil {
		return nil, f.Fail
	}
	if r.GetTenantId() == "" || r.GetIdempotencyKey() == "" || r.GetCertificateId() == "" || r.GetName() == "" {
		return nil, status.Error(codes.InvalidArgument, "missing field")
	}
	key := r.GetTenantId() + "/" + r.GetIdempotencyKey()
	if d, ok := f.deliveries[key]; ok {
		out := proto.Clone(d).(*invv1.CertificateDelivery)
		out.Created = false
		if r.GetRearmFailed() {
			for _, it := range out.Items {
				if it.State == invv1.DeliveryState_DELIVERY_STATE_FAILED {
					it.State, it.Attempts = f.State, it.Attempts+1
				}
			}
			f.deliveries[key] = out
		}
		return out, nil
	}
	d := &invv1.CertificateDelivery{Id: "delivery-" + r.GetIdempotencyKey(), TenantId: r.GetTenantId(), CertificateId: r.GetCertificateId(),
		Name: r.GetName(), KeyPolicy: r.GetKeyPolicy(), Created: true}
	for i, h := range f.HostIDs {
		d.Items = append(d.Items, &invv1.CertificateDeliveryItem{Id: d.Id + "-" + h, HostId: h, Hostname: "host-" + string(rune('a'+i)),
			AgentOnline: !f.Offline, State: f.State, Serial: "0A", FingerprintSha256: "ab", HookExitCode: -1, Attempts: 1})
	}
	f.deliveries[key] = d
	return d, nil
}

// GetCertificateDelivery returns a stored delivery of the tenant.
func (f *Fake) GetCertificateDelivery(_ context.Context, r *invv1.GetCertificateDeliveryRequest) (*invv1.CertificateDelivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Fail != nil {
		return nil, f.Fail
	}
	for _, d := range f.deliveries {
		if d.GetId() == r.GetId() && d.GetTenantId() == r.GetTenantId() {
			return d, nil
		}
	}
	return nil, status.Error(codes.NotFound, "not found")
}

// PreviewCertificateTargets lists the configured hosts.
func (f *Fake) PreviewCertificateTargets(_ context.Context, _ *invv1.PreviewCertificateTargetsRequest) (*invv1.PreviewCertificateTargetsResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.Fail != nil {
		return nil, f.Fail
	}
	out := &invv1.PreviewCertificateTargetsResponse{}
	for _, h := range f.HostIDs {
		out.Hosts = append(out.Hosts, &invv1.TargetHost{HostId: h, Hostname: "host", AgentOnline: true, Capability: "enabled"})
	}
	return out, nil
}

// VerifyHostCertificates reports every host as matching.
func (f *Fake) VerifyHostCertificates(_ context.Context, r *invv1.VerifyHostCertificatesRequest) (*invv1.VerifyHostCertificatesResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Verifies = append(f.Verifies, r)
	if f.Fail != nil {
		return nil, f.Fail
	}
	out := &invv1.VerifyHostCertificatesResponse{Total: int32(len(f.HostIDs)), Matched: int32(len(f.HostIDs))} // #nosec G115 -- test fake, a handful of hosts
	for _, h := range f.HostIDs {
		out.Hosts = append(out.Hosts, &invv1.HostCertificateStatus{HostId: h, Status: "match", FingerprintSha256: r.GetExpectedFingerprintSha256()})
	}
	return out, nil
}

// MarkCertificateRevoked records the request.
func (f *Fake) MarkCertificateRevoked(_ context.Context, r *invv1.MarkCertificateRevokedRequest) (*invv1.MarkCertificateRevokedResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.Revokes = append(f.Revokes, r)
	if f.Fail != nil {
		return nil, f.Fail
	}
	return &invv1.MarkCertificateRevokedResponse{CancelledItems: 1, FlaggedHosts: int32(len(f.HostIDs))}, nil // #nosec G115 -- test fake, a handful of hosts
}

// Snapshot returns copies of the recorded requests.
func (f *Fake) Snapshot() (creates []*invv1.CreateCertificateDeliveryRequest, revokes []*invv1.MarkCertificateRevokedRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append(creates, f.Creates...), append(revokes, f.Revokes...)
}

// Dial serves srv registrations on an in-process listener and returns a
// client connection to it (closed with the test).
func Dial(t testing.TB, register func(*grpc.Server)) *grpc.ClientConn {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	register(gs)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// DialFake serves f and returns a connection to it.
func DialFake(t testing.TB, f *Fake) *grpc.ClientConn {
	return Dial(t, func(gs *grpc.Server) { invv1.RegisterCertificateDeliveryServiceServer(gs, f) })
}
