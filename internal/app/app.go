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

	"github.com/go-freya/freya"
	"github.com/go-freya/freya/services/lcm/pkg/lcmidentity"

	authv1 "github.com/go-freya/freya/services/auth/api/proto/auth/v1"
	"github.com/go-freya/freya/services/auth/pkg/authclient"
	"github.com/go-freya/freya/services/gateway/pkg/gatewayclient"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/config"
	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/deploy"
	"github.com/go-freya/freya/services/deployer/internal/events"
	"github.com/go-freya/freya/services/deployer/internal/grpcapi"
	"github.com/go-freya/freya/services/deployer/internal/httpapi"
	"github.com/go-freya/freya/services/deployer/internal/jobs"
	lcmv1 "github.com/go-freya/freya/services/lcm/api/proto/lcm/v1"

	"github.com/go-freya/freya/services/deployer/internal/backup"
	"github.com/go-freya/freya/services/deployer/internal/lcmclient"
	_ "github.com/go-freya/freya/services/deployer/internal/providers/all" // registers all deployment providers
	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/repo/repodb"
	"github.com/go-freya/freya/services/deployer/internal/sealed"
	"github.com/go-freya/freya/services/deployer/internal/stats"
	"github.com/go-freya/freya/services/deployer/internal/store"
	"github.com/go-freya/freya/services/deployer/internal/stream"
	"github.com/go-freya/freya/services/deployer/internal/stream/valkeykv"
	"github.com/go-freya/freya/services/deployer/internal/targets"
	"github.com/go-freya/freya/services/deployer/pkg/deployermanifest"
)

// Options override infrastructure (tests) and attach optional parts.
type Options struct {
	Logger   slog.Handler
	KEK      []byte
	Verifier httpapi.Verifier
	Freya    []freya.Option
	Migrate  bool
	Remote   fs.FS // built federated UI remote (nil serves no remote)
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

	closers []func()
	workers []func(context.Context)
}

// hubPublisher forwards realtime events to the platform bus.
type hubPublisher struct{ hub *stream.Hub }

func (p hubPublisher) Publish(ctx context.Context, tenantID, eventType string, payload any) {
	if p.hub != nil {
		_, _ = p.hub.PublishID(ctx, tenantID, nil, true, eventType, payload, false)
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

	// Verifier (platform token) from auth.
	a.Verifier = o.Verifier
	if a.Verifier == nil {
		conn, cerr := a.Freya.Client(ctx, "auth")
		if cerr != nil {
			return nil, fmt.Errorf("auth client: %w", cerr)
		}
		a.Verifier = authclient.New(authclient.Config{Issuer: cfg.Gateway.Issuer},
			authclient.GRPCKeys{Client: authv1.NewKeysClient(conn)},
			authclient.GRPCRevocations{Client: authv1.NewSessionsClient(conn)})
	}

	// lcm client (mTLS to lcm's browser API port).
	lcmC, lerr := a.newLCMClient(ctx)
	if lerr != nil {
		return nil, lerr
	}

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
	ds := deploy.New(a.Repo, az)
	tgs := targets.New(a.Repo, az)
	a.Jobs = jobs.New(a.Repo, az, lcmC, cs, hubPublisher{a.Hub}, jobs.Config{
		Workers: cfg.Jobs.Workers, Interval: cfg.Interval(), Lease: cfg.Lease(), JobTimeout: cfg.JobTimeout(),
		MaxRetries: cfg.Jobs.MaxRetries, RetryDelay: cfg.RetryDelay(), Backoff: cfg.Jobs.BackoffMultiplier, Cleanup: cfg.CleanupWindow(),
	})
	a.workers = append(a.workers, func(c context.Context) { a.Jobs.Run(c, a.Log) })

	// Auto-deploy: consume certificate.issued/renewed from the shared platform
	// event bus and spawn deployment jobs for matching targets.
	if cfg.Events.Enabled {
		cons := events.NewConsumer(a.Repo, lcmC, a.Log)
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
func (a *App) newLCMClient(ctx context.Context) (*lcmclient.Client, error) {
	conn, err := a.Freya.Client(ctx, a.Cfg.LCM.Service)
	if err != nil {
		return nil, fmt.Errorf("lcm client: %w", err)
	}
	return lcmclient.New(lcmv1.NewCertificatesClient(conn)), nil
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
