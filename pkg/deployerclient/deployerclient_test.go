package deployerclient_test

import (
	"context"
	"encoding/json"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/timestamppb"

	deployerv1 "github.com/go-freya/freya/services/deployer/api/proto/deployer/v1"
	"github.com/go-freya/freya/services/deployer/pkg/deployerclient"
)

// stubConfigServer is a canned in-process implementation of
// deployer.v1.TargetConfigurationService. It records the last request it saw
// (so tests can assert the JSON encoding the client produced) and returns fixed
// responses. When failNotFound is set, Get returns codes.NotFound so we can
// verify the client surfaces server errors unchanged.
type stubConfigServer struct {
	deployerv1.UnimplementedTargetConfigurationServiceServer

	lastCreate *deployerv1.CreateConfigurationRequest
	lastGet    *deployerv1.GetConfigurationRequest
	lastList   *deployerv1.ListConfigurationsRequest

	created      *deployerv1.Configuration
	failNotFound bool
	failCreate   bool
	failList     bool
	lastDeployAt *time.Time
}

func (s *stubConfigServer) Create(_ context.Context, req *deployerv1.CreateConfigurationRequest) (*deployerv1.Configuration, error) {
	s.lastCreate = req
	if s.failCreate {
		return nil, status.Error(codes.InvalidArgument, "validation_failed")
	}
	s.created = &deployerv1.Configuration{
		Id:             "cfg-123",
		Name:           req.GetName(),
		Description:    req.GetDescription(),
		ProviderType:   req.GetProviderType(),
		Status:         "active",
		HasCredentials: req.GetCredentialsJson() != "",
	}
	if s.lastDeployAt != nil {
		s.created.LastDeploymentAt = timestamppb.New(*s.lastDeployAt)
	}
	return s.created, nil
}

func (s *stubConfigServer) Get(_ context.Context, req *deployerv1.GetConfigurationRequest) (*deployerv1.Configuration, error) {
	s.lastGet = req
	if s.failNotFound {
		return nil, status.Error(codes.NotFound, "not_found")
	}
	if s.created != nil {
		return s.created, nil
	}
	return &deployerv1.Configuration{Id: req.GetId()}, nil
}

func (s *stubConfigServer) List(_ context.Context, req *deployerv1.ListConfigurationsRequest) (*deployerv1.ListConfigurationsResponse, error) {
	s.lastList = req
	if s.failList {
		return nil, status.Error(codes.Unavailable, "temporarily_unavailable")
	}
	cfg := s.created
	if cfg == nil {
		cfg = &deployerv1.Configuration{Id: "cfg-123", ProviderType: req.GetProviderType(), Status: req.GetStatus()}
	}
	return &deployerv1.ListConfigurationsResponse{Configurations: []*deployerv1.Configuration{cfg}}, nil
}

// stubDeployServer is a canned in-process implementation of
// deployer.v1.DeploymentService.
type stubDeployServer struct {
	deployerv1.UnimplementedDeploymentServiceServer

	lastDeploy *deployerv1.DeployRequest
	fail       bool
}

func (s *stubDeployServer) Deploy(_ context.Context, req *deployerv1.DeployRequest) (*deployerv1.DeployResponse, error) {
	s.lastDeploy = req
	if s.fail {
		return nil, status.Error(codes.FailedPrecondition, "not_deployable")
	}
	return &deployerv1.DeployResponse{JobId: "job-abc", Status: "processing"}, nil
}

// newTestClient stands up the two stub servers on an in-process bufconn
// listener, dials it, and returns a deployerclient.Client wired to it.
func newTestClient(t *testing.T, cfgSrv *stubConfigServer, depSrv *stubDeployServer) *deployerclient.Client {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	deployerv1.RegisterTargetConfigurationServiceServer(gs, cfgSrv)
	deployerv1.RegisterDeploymentServiceServer(gs, depSrv)

	go func() {
		if err := gs.Serve(lis); err != nil {
			t.Logf("grpc server exited: %v", err)
		}
	}()

	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		return lis.DialContext(ctx)
	}
	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		gs.Stop()
		_ = lis.Close()
	})

	return deployerclient.New(conn)
}

const testTenant = "11111111-1111-1111-1111-111111111111"

func TestCreateConfiguration(t *testing.T) {
	cfgSrv := &stubConfigServer{}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	in := deployerclient.CreateConfigurationInput{
		TenantID:     testTenant,
		Name:         "prod-endpoint",
		Description:  "production",
		ProviderType: "dummy",
		Config:       map[string]any{"host": "example.com", "port": float64(443)},
		Credentials:  map[string]any{"token": "s3cr3t"},
	}

	got, err := c.CreateConfiguration(ctx, in)
	if err != nil {
		t.Fatalf("CreateConfiguration: %v", err)
	}

	if got.ID != "cfg-123" {
		t.Errorf("ID = %q, want cfg-123", got.ID)
	}
	if got.ProviderType != "dummy" {
		t.Errorf("ProviderType = %q, want dummy", got.ProviderType)
	}
	if got.Status != "active" {
		t.Errorf("Status = %q, want active", got.Status)
	}
	if !got.HasCredentials {
		t.Errorf("HasCredentials = false, want true")
	}

	// The request the server received must carry the JSON-encoded config/credentials.
	if cfgSrv.lastCreate == nil {
		t.Fatal("server received no Create request")
	}
	if cfgSrv.lastCreate.GetTenantId() != testTenant {
		t.Errorf("TenantId = %q, want %q", cfgSrv.lastCreate.GetTenantId(), testTenant)
	}
	var gotCfg map[string]any
	if err := json.Unmarshal([]byte(cfgSrv.lastCreate.GetConfigJson()), &gotCfg); err != nil {
		t.Fatalf("config_json not valid JSON: %v (raw %q)", err, cfgSrv.lastCreate.GetConfigJson())
	}
	if gotCfg["host"] != "example.com" || gotCfg["port"] != float64(443) {
		t.Errorf("config_json = %v, want host/port from input", gotCfg)
	}
	var gotCred map[string]any
	if err := json.Unmarshal([]byte(cfgSrv.lastCreate.GetCredentialsJson()), &gotCred); err != nil {
		t.Fatalf("credentials_json not valid JSON: %v (raw %q)", err, cfgSrv.lastCreate.GetCredentialsJson())
	}
	if gotCred["token"] != "s3cr3t" {
		t.Errorf("credentials_json = %v, want token from input", gotCred)
	}
}

func TestCreateConfigurationNoCredentials(t *testing.T) {
	cfgSrv := &stubConfigServer{}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.CreateConfiguration(ctx, deployerclient.CreateConfigurationInput{
		TenantID:     testTenant,
		Name:         "no-creds",
		ProviderType: "dummy",
		Config:       map[string]any{"host": "h"},
	})
	if err != nil {
		t.Fatalf("CreateConfiguration: %v", err)
	}
	if got.HasCredentials {
		t.Errorf("HasCredentials = true, want false for empty credentials")
	}
	if cfgSrv.lastCreate.GetCredentialsJson() != "" {
		t.Errorf("credentials_json = %q, want empty for nil credentials", cfgSrv.lastCreate.GetCredentialsJson())
	}
}

func TestGetConfiguration(t *testing.T) {
	cfgSrv := &stubConfigServer{}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create first so Get returns the same configuration.
	if _, err := c.CreateConfiguration(ctx, deployerclient.CreateConfigurationInput{
		TenantID:     testTenant,
		Name:         "prod-endpoint",
		ProviderType: "dummy",
		Credentials:  map[string]any{"token": "x"},
	}); err != nil {
		t.Fatalf("CreateConfiguration: %v", err)
	}

	got, err := c.GetConfiguration(ctx, testTenant, "cfg-123")
	if err != nil {
		t.Fatalf("GetConfiguration: %v", err)
	}
	if got.ID != "cfg-123" {
		t.Errorf("ID = %q, want cfg-123", got.ID)
	}
	if got.ProviderType != "dummy" {
		t.Errorf("ProviderType = %q, want dummy", got.ProviderType)
	}
	if !got.HasCredentials {
		t.Errorf("HasCredentials = false, want true (created with credentials)")
	}
	if cfgSrv.lastGet.GetId() != "cfg-123" || cfgSrv.lastGet.GetTenantId() != testTenant {
		t.Errorf("Get request = %+v, want id cfg-123 / tenant %s", cfgSrv.lastGet, testTenant)
	}
}

func TestListConfigurations(t *testing.T) {
	cfgSrv := &stubConfigServer{}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.ListConfigurations(ctx, testTenant, "dummy", "active")
	if err != nil {
		t.Fatalf("ListConfigurations: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len(configs) = %d, want 1", len(got))
	}
	if got[0].ProviderType != "dummy" || got[0].Status != "active" {
		t.Errorf("config[0] = %+v, want providerType dummy / status active", got[0])
	}
	if cfgSrv.lastList.GetProviderType() != "dummy" || cfgSrv.lastList.GetStatus() != "active" {
		t.Errorf("List request = %+v, want filters dummy/active", cfgSrv.lastList)
	}
}

func TestDeploy(t *testing.T) {
	depSrv := &stubDeployServer{}
	c := newTestClient(t, &stubConfigServer{}, depSrv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := c.Deploy(ctx, testTenant, "cert-1", "cfg-123", "manual")
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if jobID == "" {
		t.Fatal("Deploy returned empty job id")
	}
	if jobID != "job-abc" {
		t.Errorf("jobID = %q, want job-abc", jobID)
	}
	if depSrv.lastDeploy.GetCertificateId() != "cert-1" ||
		depSrv.lastDeploy.GetConfigurationId() != "cfg-123" ||
		depSrv.lastDeploy.GetTrigger() != "manual" {
		t.Errorf("Deploy request = %+v, want cert-1/cfg-123/manual", depSrv.lastDeploy)
	}
}

func TestCreateConfigurationMapsLastDeploymentAt(t *testing.T) {
	when := time.Date(2026, 9, 20, 10, 30, 0, 0, time.UTC)
	cfgSrv := &stubConfigServer{lastDeployAt: &when}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.CreateConfiguration(ctx, deployerclient.CreateConfigurationInput{
		TenantID:     testTenant,
		Name:         "with-ts",
		ProviderType: "dummy",
	})
	if err != nil {
		t.Fatalf("CreateConfiguration: %v", err)
	}
	if got.LastDeploymentAt == nil {
		t.Fatal("LastDeploymentAt = nil, want a timestamp")
	}
	if !got.LastDeploymentAt.Equal(when) {
		t.Errorf("LastDeploymentAt = %v, want %v", got.LastDeploymentAt, when)
	}
}

func TestCreateConfigurationErrorSurfaces(t *testing.T) {
	cfgSrv := &stubConfigServer{failCreate: true}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.CreateConfiguration(ctx, deployerclient.CreateConfigurationInput{
		TenantID:     testTenant,
		Name:         "bad",
		ProviderType: "dummy",
	})
	if err == nil {
		t.Fatal("CreateConfiguration returned nil error, want InvalidArgument")
	}
	if st, _ := status.FromError(err); st.Code() != codes.InvalidArgument {
		t.Errorf("code = %v, want InvalidArgument", st.Code())
	}
}

func TestListConfigurationsErrorSurfaces(t *testing.T) {
	cfgSrv := &stubConfigServer{failList: true}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.ListConfigurations(ctx, testTenant, "", "")
	if err == nil {
		t.Fatal("ListConfigurations returned nil error, want Unavailable")
	}
	if got != nil {
		t.Errorf("configs = %v, want nil on error", got)
	}
}

func TestDeployErrorSurfaces(t *testing.T) {
	depSrv := &stubDeployServer{fail: true}
	c := newTestClient(t, &stubConfigServer{}, depSrv)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	jobID, err := c.Deploy(ctx, testTenant, "cert-1", "cfg-123", "manual")
	if err == nil {
		t.Fatal("Deploy returned nil error, want FailedPrecondition")
	}
	if jobID != "" {
		t.Errorf("jobID = %q, want empty on error", jobID)
	}
	if st, _ := status.FromError(err); st.Code() != codes.FailedPrecondition {
		t.Errorf("code = %v, want FailedPrecondition", st.Code())
	}
}

func TestServerErrorSurfaces(t *testing.T) {
	cfgSrv := &stubConfigServer{failNotFound: true}
	c := newTestClient(t, cfgSrv, &stubDeployServer{})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := c.GetConfiguration(ctx, testTenant, "missing")
	if err == nil {
		t.Fatal("GetConfiguration returned nil error, want NotFound")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error is not a gRPC status: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("code = %v, want NotFound", st.Code())
	}
}
