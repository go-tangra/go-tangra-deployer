package app

import (
	"context"
	"time"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
)

// SeedPermissions registers the module's permissions with the auth service for
// every tenant it serves and grants them to the built-in roles
// (contracts/manifest.md; idempotent). The gateway registers the permissions
// from the manifest for routing; only the deployer knows the role grants, so it
// pushes them to auth here.
func (a *App) SeedPermissions(ctx context.Context) error {
	conn, err := a.Freya.Client(ctx, "auth") // pooled; owned by the Freya app
	if err != nil {
		return err
	}
	req := &authv1.RegisterPermissionsRequest{}
	for _, p := range deployermanifest.Permissions {
		req.Permissions = append(req.Permissions, &authv1.PermissionDef{Resource: p.Resource, Action: p.Action, Description: p.Description})
	}
	for _, slug := range []string{"owner", "admin", "member", "auditor", "operator"} {
		req.BuiltinGrants = append(req.BuiltinGrants, &authv1.BuiltinGrant{Role: slug, Permissions: deployermanifest.Grants[slug]})
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, err = authv1.NewAuthorizationClient(conn).RegisterPermissions(ctx, req)
	return err
}

// seedLoop seeds at start (retrying until it succeeds) and then every five
// minutes so tenants created later receive the grants.
func (a *App) seedLoop(ctx context.Context) {
	for ctx.Err() == nil {
		if err := a.SeedPermissions(ctx); err == nil {
			break
		} else {
			a.Log.Warn("permission seeding failed; retrying", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
	t := time.NewTicker(5 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := a.SeedPermissions(ctx); err != nil {
				a.Log.Warn("permission seeding", "err", err)
			}
		}
	}
}
