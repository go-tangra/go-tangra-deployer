// Package app wires the deployer service: configuration -> Freya runtime ->
// store/sealing/authz -> services (configs, deploy, jobs+worker) -> the Freya
// HTTP server (reached only through the gateway) and gateway registration.
package app

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/go-tangra/go-tangra-lcm/sdk/v4/pkg/lcmidentity"
	"github.com/go-tangra/go-tangra/v4"
	"google.golang.org/grpc"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
	"github.com/go-tangra/go-tangra-auth/sdk/v4/pkg/authclient"
	"github.com/go-tangra/go-tangra-portal/sdk/v4/pkg/gatewayclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audience"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/audit"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/config"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/deploy"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/events"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/grpcapi"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/httpapi"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	lcmv1 "github.com/go-tangra/go-tangra-lcm/sdk/v4/api/proto/lcm/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/backup"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/inventoryclient"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/lcmclient"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/all" // registers all deployment providers
	"github.com/go-tangra/go-tangra-deployer/v4/internal/providers/inventoryagent"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo/repodb"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/stats"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/stream"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/stream/valkeykv"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
)

// Options override infrastructure (tests) and attach optional parts.
type Options struct {
	Logger   slog.Handler
	KEK      []byte
	Verifier httpapi.Verifier
	// Audience resolves who may receive job events (nil: the auth service).
	Audience audience.Resolver
	Freya    []freya.Option
	Migrate  bool
	Remote   fs.FS // built federated UI remote (nil serves no remote)
	// LCMConn and InventoryConn replace the mesh connections to lcm and
	// inventory (tests: in-process fakes over bufconn).
	LCMConn       grpc.ClientConnInterface
	InventoryConn grpc.ClientConnInterface
}

// App is the wired service.
type App struct {
	Cfg      config.Config
	Log      *slog.Logger
	Freya    *freya.App
	Store    *store.Store
	Repo     repo.Store
	Env      *sealed.Envelope
	Verifier httpapi.Verifier
	HTTP     *httpapi.Server
	Hub      *stream.Hub
	Jobs     *jobs.Service
	// Inventory is the mesh client of inventory.v1.CertificateDeliveryService
	// (nil when inventory.service is not configured).
	Inventory *inventoryclient.Client

	closers []func()
	workers []func(context.Context)
}

// hubPublisher forwards job events to the platform bus, addressed to the
// tenant's users holding jobs:read only — never broadcast to the tenant. When
// the readers cannot be resolved the event is dropped (fail closed); the
// jobs API still serves the state.
type hubPublisher struct {
	hub     *stream.Hub
	readers audience.Resolver
	log     *slog.Logger
}

func (p hubPublisher) Publish(ctx context.Context, tenantID, eventType string, payload any) {
	if p.hub == nil || p.readers == nil {
		return
	}
	users, err := p.readers.JobReaders(ctx, tenantID)
	if err != nil {
		p.log.Warn("deployer: job event not sent: readers unavailable", "tenant_id", tenantID, "event", eventType, "err", err)
		return
	}
	for i := 0; i < len(users); i += stream.MaxTargets {
		to := users[i:min(i+stream.MaxTargets, len(users))]
		if _, err := p.hub.PublishID(ctx, tenantID, to, false, eventType, payload, false); err != nil {
			p.log.Warn("deployer: job event not sent", "tenant_id", tenantID, "event", eventType, "err", err)
		}
	}
}

// Build wires the service.
func Build(ctx context.Context, cfg config.Config, o Options) (a *App, err error) {
	a = &App{Cfg: cfg}
	handler := o.Logger
	if handler == nil {
		handler = slog.NewJSONHandler(os.Stderr, nil)
	}
	a.Log = slog.New(handler)

	fopts := append([]freya.Option{freya.WithLogger(handler)}, o.Freya...)
	if cfg.Enroll.Enabled {
		raw, rerr := os.ReadFile(cfg.Enroll.TokenFile)
		if rerr != nil {
			return nil, fmt.Errorf("deployer: enroll token: %w", rerr)
		}
		prov, perr := lcmidentity.NewNet(ctx, lcmidentity.NetConfig{
			EnrollURL: cfg.Enroll.EnrollURL, LCMGRPCTarget: cfg.Enroll.LCMGRPCTarget,
			TenantID: cfg.Enroll.TenantID, TrustDomain: cfg.Config.TrustDomain, ServiceName: cfg.Config.ServiceName,
			EnrollmentToken: strings.TrimSpace(string(raw)), Insecure: cfg.Enroll.Insecure, StateFile: cfg.Enroll.StateFile,
		})
		if perr != nil {
			return nil, fmt.Errorf("deployer: enroll: %w", perr)
		}
		a.closers = append(a.closers, func() { _ = prov.Close() })
		fopts = append(fopts, freya.WithIdentityProvider(prov))
	}
	if a.Freya, err = freya.New(cfg.Config, fopts...); err != nil {
		return nil, err
	}
	a.closers = append(a.closers, a.Freya.Close)

	// KEK + envelope.
	kek := o.KEK
	if len(kek) == 0 {
		if kek, err = sealed.LoadKEK(cfg.KEK.Source, cfg.KEK.Path, cfg.KEK.Env); err != nil {
			return nil, fmt.Errorf("kek: %w", err)
		}
	}
	if a.Env, err = sealed.NewEnvelope(kek); err != nil {
		return nil, err
	}

	// Store (migrate then open the app pool).
	if o.Migrate {
		mdsn := cfg.DB.MigrateDSN
		if mdsn == "" {
			mdsn = cfg.DB.DSN
		}
		if err = store.Migrate(ctx, mdsn); err != nil {
			return nil, err
		}
	}
	if a.Store, err = store.Open(ctx, cfg.DB.DSN, cfg.DB.MaxConns); err != nil {
		return nil, err
	}
	a.closers = append(a.closers, a.Store.Close)
	a.Repo = repodb.New(a.Store)
	az := authz.New(a.Repo)

	// Verifier (platform token) and the job-event audience from auth.
	a.Verifier = o.Verifier
	readers := o.Audience
	if a.Verifier == nil || readers == nil {
		conn, cerr := a.Freya.Client(ctx, "auth")
		if cerr != nil {
			return nil, fmt.Errorf("auth client: %w", cerr)
		}
		if a.Verifier == nil {
			a.Verifier = authclient.New(authclient.Config{Issuer: cfg.Gateway.Issuer},
				authclient.GRPCKeys{Client: authv1.NewKeysClient(conn)},
				authclient.GRPCRevocations{Client: authv1.NewSessionsClient(conn)})
		}
		if readers == nil {
			readers = audience.New(conn)
		}
	}

	// lcm client (mTLS to lcm's browser API port).
	lcmC, lerr := a.newLCMClient(ctx, o.LCMConn)
	if lerr != nil {
		return nil, lerr
	}
	// inventory-agent provider (feature 033): registered only when the
	// inventory peer is configured, before workers and HTTP start.
	if err = a.wireInventory(ctx, o.InventoryConn); err != nil {
		return nil, err
	}
	// Audit sink for configuration validation and revocation forwarding.
	aw := audit.NewWriter(a.Repo, func(err error) { a.Log.Warn("deployer: audit write failed", "err", err) })
	a.closers = append(a.closers, aw.Close)

	// Event bus (Valkey Streams).
	sc := valkeykv.Config{Addresses: cfg.Valkey.Addresses, Username: cfg.Valkey.Username, Password: cfg.Valkey.Password, AllowPlaintext: cfg.Valkey.AllowPlaintext}
	if cfg.Valkey.CAFile != "" {
		if sc.CAPEM, err = os.ReadFile(cfg.Valkey.CAFile); err != nil {
			return nil, fmt.Errorf("valkey ca: %w", err)
		}
	}
	streamClient, serr := valkeykv.New(sc)
	if serr != nil {
		return nil, fmt.Errorf("event bus: %w", serr)
	}
	a.Hub = stream.NewHub(streamClient, stream.Config{}, a.Log)
	a.closers = append(a.closers, a.Hub.Close)

	// Services.
	cs := configs.New(a.Repo, a.Env, az)
	cs.SetLogger(a.Log)
	cs.SetAuditor(aw)
	ds := deploy.New(a.Repo, az)
	tgs := targets.New(a.Repo, az)
	a.Jobs = jobs.New(a.Repo, az, lcmC, cs, hubPublisher{hub: a.Hub, readers: readers, log: a.Log}, jobs.Config{
		Workers: cfg.Jobs.Workers, Interval: cfg.Interval(), Lease: cfg.Lease(), JobTimeout: cfg.JobTimeout(),
		MaxRetries: cfg.Jobs.MaxRetries, RetryDelay: cfg.RetryDelay(), Backoff: cfg.Jobs.BackoffMultiplier, Cleanup: cfg.CleanupWindow(),
	})
	a.Jobs.SetLogger(a.Log)
	a.workers = append(a.workers, func(c context.Context) { a.Jobs.Run(c, a.Log) })

	// Auto-deploy: consume certificate.issued/renewed from the shared platform
	// event bus and spawn deployment jobs for matching targets.
	if cfg.Events.Enabled {
		cons := events.NewConsumer(a.Repo, lcmC, a.Log)
		if a.Inventory != nil {
			// certificate.revoked → inventory (feature 033, research D14).
			cons.SetRevocationForwarder(a.Inventory, aw)
		}
		reader := streamReader{streamClient}
		tenants := []string{cfg.Enroll.TenantID}
		a.workers = append(a.workers, func(c context.Context) { cons.Run(c, reader, tenants, stream.Key) })
	}

	// HTTP.
	hopts := []httpapi.Option{httpapi.WithVerifier(a.Verifier)}
	if o.Remote != nil {
		hopts = append(hopts, httpapi.WithRemote(o.Remote))
	}
	if a.HTTP, err = httpapi.NewHandler(a.Freya, hopts...); err != nil {
		return nil, err
	}
	a.HTTP.Register(httpapi.Deps{Configs: cs, Targets: tgs, Deploy: ds, Jobs: a.Jobs, Stats: stats.New(a.Repo), Backup: backup.New(a.Repo)})
	a.Freya.HTTP().HandlePrefix("/", a.HTTP.Handler())
	// deployer.v1 service-to-service gRPC (not gateway-proxied).
	grpcapi.Register(a.Freya.GRPC(), grpcapi.Deps{Configs: cs, Deploy: ds})
	return a, nil
}

// newLCMClient builds the module gRPC client to lcm (SPIFFE mTLS, resolved and
// pooled by the Freya app). Certificate downloads go over lcm.v1.Certificates.
func (a *App) newLCMClient(ctx context.Context, conn grpc.ClientConnInterface) (*lcmclient.Client, error) {
	if conn == nil {
		c, err := a.Freya.Client(ctx, a.Cfg.LCM.Service)
		if err != nil {
			return nil, fmt.Errorf("lcm client: %w", err)
		}
		conn = c
	}
	return lcmclient.New(lcmv1.NewCertificatesClient(conn)), nil
}

// wireInventory builds the mesh client to the inventory service (SPIFFE mTLS;
// the inventory accepts CertificateDeliveryService calls only from the
// deployer's identity) and registers the inventory-agent provider. Without
// inventory.service the provider is absent from the catalogue.
func (a *App) wireInventory(ctx context.Context, conn grpc.ClientConnInterface) error {
	if a.Cfg.Inventory.Service == "" {
		return nil
	}
	if conn == nil {
		c, err := a.Freya.Client(ctx, a.Cfg.Inventory.Service)
		if err != nil {
			return fmt.Errorf("inventory client: %w", err)
		}
		conn = c
	}
	a.Inventory = inventoryclient.New(conn)
	registerInventoryAgent(a.Inventory)
	return nil
}

// registerInventoryAgent registers the provider once per process; a later
// Build in the same process (tests) rebinds its client. The registry stays
// write-once for every other provider.
func registerInventoryAgent(inv inventoryagent.Inventory) {
	if p, err := provider.Get(inventoryagent.Type); err == nil {
		if ia, ok := p.(*inventoryagent.Provider); ok {
			ia.SetInventory(inv)
		}
		return
	}
	provider.Register(inventoryagent.New(inv))
}

// Run starts the verifier, gateway registration, workers, and the Freya runtime.
func (a *App) Run(ctx context.Context) error {
	wctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if v, ok := a.Verifier.(*authclient.Verifier); ok {
		go func() {
			for wctx.Err() == nil {
				if err := v.Start(wctx, func(err error) { a.Log.Warn("verifier", "err", err) }); err == nil {
					return
				}
				select {
				case <-wctx.Done():
					return
				case <-time.After(2 * time.Second):
				}
			}
		}()
	}
	go a.register(wctx)
	for _, w := range a.workers {
		go w(wctx)
	}
	go func() {
		for wctx.Err() == nil && !a.Freya.Ready() {
			time.Sleep(100 * time.Millisecond)
		}
		a.seedLoop(wctx)
	}()
	return a.Freya.Run(ctx)
}

// Close releases resources.
func (a *App) Close() {
	for i := len(a.closers) - 1; i >= 0; i-- {
		a.closers[i]()
	}
	a.closers = nil
}

// register keeps the gateway lease for the manifest.
func (a *App) register(ctx context.Context) {
	for ctx.Err() == nil && !a.Freya.Ready() {
		select {
		case <-ctx.Done():
			return
		case <-time.After(100 * time.Millisecond):
		}
	}
	man, err := deployermanifest.Manifest()
	if err != nil {
		a.Log.Error("gateway manifest", "err", err)
		return
	}
	httpEP, err := a.Freya.HTTP().Endpoint()
	if err != nil {
		a.Log.Error("gateway registration: http endpoint", "err", err)
		return
	}
	grpcEP, err := a.Freya.GRPC().Endpoint()
	if err != nil {
		a.Log.Error("gateway registration: grpc endpoint", "err", err)
		return
	}
	var client *gatewayclient.Client
	for ctx.Err() == nil && client == nil {
		conn, cerr := a.Freya.Client(ctx, a.Cfg.Gateway.Service)
		if cerr != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}
		client, err = gatewayclient.New(conn, gatewayclient.Options{Manifest: man, HTTPURL: "https://" + httpEP.Host, GRPCTarget: grpcEP.Host, Logger: a.Log,
			OnState: func(s gatewayclient.State) {
				a.Log.Info("gateway lease", "registered", s.Registered, "lease", s.LeaseID, "err", s.Err)
			}})
		if err != nil {
			a.Log.Error("gateway client", "err", err)
			return
		}
	}
	if client != nil {
		if err := client.Run(ctx); err != nil {
			a.Log.Error("gateway registration", "err", err)
		}
	}
}

// streamReader adapts the Valkey stream client to events.Reader.
type streamReader struct{ c stream.Client }

func (r streamReader) XRead(ctx context.Context, key, afterID string, block time.Duration, count int64) ([]events.Entry, error) {
	es, err := r.c.XRead(ctx, key, afterID, block, count)
	if err != nil {
		return nil, err
	}
	out := make([]events.Entry, len(es))
	for i, e := range es {
		out[i] = events.Entry{ID: e.ID, Fields: e.Fields}
	}
	return out, nil
}
func (r streamReader) XLast(ctx context.Context, key string) (string, error) {
	return r.c.XLast(ctx, key)
}
