//go:build integration

// Package app end-to-end test: brings up the whole service (Build + Run) against
// a real TimescaleDB and Valkey (testcontainers), a self-issued local-dev
// identity, an all-allow policy and an injected token verifier, so the wiring in
// app.go — store/migrations, envelope, event bus, services, workers, the HTTP
// and gRPC surfaces and the register/seed loops — is exercised without needing
// the auth/lcm/gateway peers to be reachable. Run with:
//
//	go test -tags integration ./internal/app/
//
// It skips cleanly when Docker/testcontainers is unavailable.
package app_test

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-tangra/go-tangra-auth/sdk/v4/pkg/authclient"
	"github.com/go-tangra/go-tangra/v4"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/app"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/config"

	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/all"
)

const appTenant = "11111111-1111-1111-1111-111111111111"

// fakeVerifier accepts the "admin" bearer token as a platform admin.
type fakeVerifier struct{}

func (fakeVerifier) Verify(_ context.Context, token string) (authclient.Identity, error) {
	if token == "admin" {
		return authclient.Identity{UserID: "u1", TenantID: appTenant, Roles: []string{"admin"}}, nil
	}
	return authclient.Identity{}, errors.New("unauthenticated")
}

func startTimescale(t *testing.T) (adminDSN, appDSN string) {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "timescale/timescaledb:latest-pg16", ExposedPorts: []string{"5432/tcp"},
			Env:        map[string]string{"POSTGRES_PASSWORD": "test", "POSTGRES_DB": "deployer"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(2 * time.Minute),
		}, Started: true,
	})
	if err != nil {
		t.Skipf("testcontainers unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "5432/tcp")
	adminDSN = "postgres://postgres:test@" + host + ":" + port.Port() + "/deployer?sslmode=disable"
	conn, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Exec(ctx, "CREATE ROLE deployer_app LOGIN PASSWORD 'app' NOBYPASSRLS")
	_ = conn.Close(ctx)
	appDSN = "postgres://deployer_app:app@" + host + ":" + port.Port() + "/deployer?sslmode=disable"
	return
}

func startValkey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "valkey/valkey:8-alpine", ExposedPorts: []string{"6379/tcp"},
			WaitingFor: wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		}, Started: true,
	})
	if err != nil {
		t.Skipf("valkey container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379/tcp")
	return host + ":" + port.Port()
}

func testConfig(t *testing.T) config.Config {
	adminDSN, appDSN := startTimescale(t)
	valkeyAddr := startValkey(t)

	c := config.Default()
	c.ServiceName, c.TrustDomain, c.Env = "deployer", "example.org", "dev"
	c.DB.DSN, c.DB.MigrateDSN, c.DB.MaxConns = appDSN, adminDSN, 4
	c.Valkey.Addresses, c.Valkey.AllowPlaintext = []string{valkeyAddr}, true
	// Ephemeral listeners so binding never conflicts.
	c.Server.GRPCAddr, c.Server.HTTPAddr, c.Admin.Addr = "127.0.0.1:0", "127.0.0.1:0", "127.0.0.1:0"
	// The peers are never reached in this test; static entries let the pool
	// build lazy connections without a discovery backend.
	c.Discovery.Static = map[string][]string{
		"lcm": {"127.0.0.1:1"}, "auth": {"127.0.0.1:1"}, "gateway": {"127.0.0.1:1"},
	}
	c.LCM.Service, c.Gateway.Service, c.Gateway.Issuer = "lcm", "gateway", "https://localhost:8443"
	c.Events.Enabled = true
	c.Enroll.Enabled = false
	c.Enroll.TenantID = appTenant
	c.Jobs.Workers = 1
	return c
}

// options returns Build options that bypass enrollment, the auth verifier and
// the KEK file: a self-issued local-dev identity + all-allow policy + injected
// verifier + a raw KEK.
func options() app.Options {
	return app.Options{
		Migrate:  true,
		KEK:      make([]byte, 32),
		Verifier: fakeVerifier{},
		Freya:    []freya.Option{freya.WithInsecureLocalDev(), freya.WithAllowAllPolicy()},
	}
}

func TestBuildWiresTheService(t *testing.T) {
	ctx := context.Background()
	a, err := app.Build(ctx, testConfig(t), options())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer a.Close()

	if a.Freya == nil || a.Store == nil || a.Repo == nil || a.Env == nil || a.HTTP == nil || a.Jobs == nil || a.Hub == nil {
		t.Fatalf("app not fully wired: %+v", a)
	}

	// The wired HTTP surface serves the deployer API: the providers catalogue is
	// reachable with the injected admin token and rejects an anonymous caller.
	get := func(tok string) int {
		r := httptest.NewRequest("GET", "https://localhost/api/deployer/v1/providers", nil)
		if tok != "" {
			r.Header.Set("Authorization", "Bearer "+tok)
		}
		w := httptest.NewRecorder()
		a.HTTP.Handler().ServeHTTP(w, r)
		return w.Code
	}
	if code := get(""); code != 401 {
		t.Fatalf("anonymous providers: %d, want 401", code)
	}
	if code := get("admin"); code != 200 {
		t.Fatalf("admin providers: %d, want 200", code)
	}
}

func TestRunStartsAndStops(t *testing.T) {
	a, err := app.Build(context.Background(), testConfig(t), options())
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer a.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Let the servers bind and the workers/register/seed loops start.
	time.Sleep(500 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop after cancel")
	}
}
