// Package authz is the deployer's tenant-scoped authorization. The application
// gateway already enforces each route's coarse permission (e.g. targets:manage)
// before a request reaches the module; this package enforces tenant ownership
// and the admin role, and defines the resource/action vocabulary and the
// Permissions projection the UI consumes. Fine-grained per-resource
// owner/editor/viewer grants (the Grant table) are reserved for a follow-up;
// the interface below already accommodates them.
package authz

import (
	"context"
	"errors"

	"github.com/go-freya/freya/services/deployer/internal/store"
)

// Resource types.
const (
	Target        = "deployer_target"
	Configuration = "deployer_configuration"
	Job           = "deployer_job"
)

// Actions.
const (
	Read   = "read"
	Manage = "manage"
	Deploy = "deploy"
)

// Errors.
var (
	ErrForbidden = errors.New("authz: forbidden")
	ErrNotFound  = errors.New("authz: not found")
)

// Subjects is the caller. Service callers act for a tenant with no user/roles.
type Subjects struct {
	TenantID  string
	UserID    string
	Roles     []string
	ActorKind string // user | service | system
}

// IsAdmin reports whether the caller holds the admin role.
func (s Subjects) IsAdmin() bool {
	for _, r := range s.Roles {
		if r == "admin" {
			return true
		}
	}
	return false
}

// ActorID returns the user id (or the service/system id when there is no user).
func (s Subjects) ActorID() string {
	if s.UserID != "" {
		return s.UserID
	}
	return s.ActorKind
}

// Permissions is the effective per-resource capability projection for the UI.
type Permissions struct {
	Read   bool `json:"read"`
	Manage bool `json:"manage"`
	Deploy bool `json:"deploy"`
}

// Locator verifies a resource exists in a tenant (existence-masking to
// ErrNotFound). repo.Store satisfies it via the concrete getters.
type Locator interface {
	Exists(ctx context.Context, tenantID, resourceType, id string) (bool, error)
}

// Authorizer enforces tenant-scoped access.
type Authorizer struct{ loc Locator }

// New builds an Authorizer over a resource locator.
func New(loc Locator) *Authorizer { return &Authorizer{loc: loc} }

// Check authorizes an action on a resource for the caller. In v1 every member
// of the owning tenant (the gateway already gated the route permission) may
// perform any action on that tenant's resources; cross-tenant access is denied
// via existence masking. Admins pass even when a resource no longer resolves.
func (a *Authorizer) Check(ctx context.Context, s Subjects, resourceType, resourceID, action string) error {
	if !valid(resourceType) || !validAction(action) {
		return ErrForbidden
	}
	if resourceID == "" {
		// A collection-level action (e.g. create/list): tenant membership suffices.
		return nil
	}
	ok, err := a.loc.Exists(ctx, s.TenantID, resourceType, resourceID)
	if err != nil {
		if s.IsAdmin() {
			return nil
		}
		return err
	}
	if !ok {
		return ErrNotFound // masks cross-tenant existence
	}
	return nil
}

// Effective returns the caller's permissions on a resource (all-true for a
// tenant member in v1; a hook for per-resource grants later).
func (a *Authorizer) Effective(ctx context.Context, s Subjects, resourceType, resourceID string) Permissions {
	if resourceID != "" {
		if ok, err := a.loc.Exists(ctx, s.TenantID, resourceType, resourceID); err != nil || !ok {
			if !s.IsAdmin() {
				return Permissions{}
			}
		}
	}
	return Permissions{Read: true, Manage: true, Deploy: true}
}

// GrantOwner is a no-op in v1 (per-resource grants deferred); it exists so
// callers can record ownership once grants are wired.
func (a *Authorizer) GrantOwner(ctx context.Context, tenantID, resourceType, resourceID, userID string) error {
	return nil
}

func valid(t string) bool { return t == Target || t == Configuration || t == Job }
func validAction(x string) bool {
	return x == Read || x == Manage || x == Deploy
}

// mapErr converts a store not-found into the authz not-found.
func mapErr(err error) error {
	if errors.Is(err, store.ErrNotFound) {
		return ErrNotFound
	}
	return err
}

var _ = mapErr
