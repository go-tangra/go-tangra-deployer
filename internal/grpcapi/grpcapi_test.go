package grpcapi

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	deployerv1 "github.com/go-freya/freya/services/deployer/api/proto/deployer/v1"
	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/deploy"
	"github.com/go-freya/freya/services/deployer/internal/memstore"
	"github.com/go-freya/freya/services/deployer/internal/sealed"

	_ "github.com/go-freya/freya/services/deployer/internal/providers/dummy"
)

const tenant = "11111111-1111-1111-1111-111111111111"

func kit(t *testing.T) (*ConfigurationServer, *DeploymentServer) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatal(err)
	}
	az := authz.New(m)
	return &ConfigurationServer{Svc: configs.New(m, env, az)}, &DeploymentServer{Svc: deploy.New(m, az)}
}

// withFakeCaller overrides the SPIFFE resolver for the test and restores it.
func withFakeCaller(t *testing.T, id string, ok bool) {
	t.Helper()
	prev := callerFunc
	callerFunc = func(context.Context) (string, bool) { return id, ok }
	t.Cleanup(func() { callerFunc = prev })
}

func TestConfigurationCreateGetList(t *testing.T) {
	cs, _ := kit(t)
	withFakeCaller(t, "spiffe://example.org/svc/warden", true)
	ctx := context.Background()

	created, err := cs.Create(ctx, &deployerv1.CreateConfigurationRequest{
		TenantId: tenant, Name: "dummy-ep", ProviderType: "dummy",
		ConfigJson: `{"region":"eu"}`, CredentialsJson: `{"token":"secret"}`,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if created.GetId() == "" || created.GetProviderType() != "dummy" || !created.GetHasCredentials() {
		t.Fatalf("created: %+v", created)
	}

	got, err := cs.Get(ctx, &deployerv1.GetConfigurationRequest{TenantId: tenant, Id: created.GetId()})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.GetName() != "dummy-ep" {
		t.Fatalf("get: %+v", got)
	}

	list, err := cs.List(ctx, &deployerv1.ListConfigurationsRequest{TenantId: tenant})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list.GetConfigurations()) != 1 {
		t.Fatalf("list = %d, want 1", len(list.GetConfigurations()))
	}
}

func TestDeployReturnsJobID(t *testing.T) {
	cs, ds := kit(t)
	withFakeCaller(t, "spiffe://example.org/svc/warden", true)
	ctx := context.Background()
	cfg, err := cs.Create(ctx, &deployerv1.CreateConfigurationRequest{TenantId: tenant, Name: "ep", ProviderType: "dummy"})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	res, err := ds.Deploy(ctx, &deployerv1.DeployRequest{TenantId: tenant, CertificateId: "cert-1", ConfigurationId: cfg.GetId()})
	if err != nil {
		t.Fatalf("deploy: %v", err)
	}
	if res.GetJobId() == "" || res.GetStatus() != "processing" {
		t.Fatalf("deploy result: %+v", res)
	}
}

func TestUnauthenticatedAndBadTenant(t *testing.T) {
	cs, _ := kit(t)
	// No SPIFFE peer → Unauthenticated.
	withFakeCaller(t, "", false)
	_, err := cs.List(context.Background(), &deployerv1.ListConfigurationsRequest{TenantId: tenant})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("no peer: code = %v, want Unauthenticated", status.Code(err))
	}
	// Peer present but tenant is not a uuid → InvalidArgument.
	withFakeCaller(t, "spiffe://example.org/svc/warden", true)
	_, err = cs.List(context.Background(), &deployerv1.ListConfigurationsRequest{TenantId: "not-a-uuid"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("bad tenant: code = %v, want InvalidArgument", status.Code(err))
	}
}

func TestDeployUnknownConfigNotFound(t *testing.T) {
	_, ds := kit(t)
	withFakeCaller(t, "spiffe://example.org/svc/warden", true)
	_, err := ds.Deploy(context.Background(), &deployerv1.DeployRequest{TenantId: tenant, CertificateId: "c", ConfigurationId: "22222222-2222-2222-2222-222222222222"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("code = %v, want NotFound", status.Code(err))
	}
}
