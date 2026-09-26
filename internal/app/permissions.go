package app

import (
	"context"
	"log/slog"
	"time"

	"google.golang.org/grpc"

	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
)

// Registration cadence: retry quickly until auth accepts, then refresh so
// tenants created later receive the roles and grants.
const (
	registerRetry  = 5 * time.Second
	registerPeriod = 5 * time.Minute
)

// SeedPermissions registers the module with the auth service
// (auth.v1.Authorization/RegisterPermissions, feature 019): its permissions,
// module roles and built-in role grants for every tenant (idempotent). The
// gateway also registers the permissions from the manifest; only the deployer
// knows the roles and grants.
func (a *App) SeedPermissions(ctx context.Context) error {
	conn, err := a.Freya.Client(ctx, "auth") // pooled; owned by the Freya app
	if err != nil {
		return err
	}
	return register(ctx, conn, a.Log)
}

// register sends the registration over conn; the auth SDK validates it and
// logs skipped grants and rejected roles.
func register(ctx context.Context, conn grpc.ClientConnInterface, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err := deployermanifest.Registration().Register(ctx, conn, log)
	return err
}

// seedLoop registers at start (retrying until it succeeds) and then every
// five minutes.
func (a *App) seedLoop(ctx context.Context) {
	registrationLoop(ctx, a.SeedPermissions, a.Log, registerRetry, registerPeriod)
}

func registrationLoop(ctx context.Context, seed func(context.Context) error, log *slog.Logger, retry, period time.Duration) {
	for ctx.Err() == nil {
		err := seed(ctx)
		if err == nil {
			break
		}
		log.Warn("auth registration failed; retrying", "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
	t := time.NewTicker(period)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := seed(ctx); err != nil {
				log.Warn("auth registration", "err", err)
			}
		}
	}
}
