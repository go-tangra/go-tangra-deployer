// Package audience answers "who may see deployment jobs" for realtime events:
// the active members of a tenant holding the deployer's jobs:read permission,
// resolved through the auth service (Profiles/ListMembers for membership,
// Authorization/Check for the permission) over the Freya SPIFFE channel.
//
// Job events carry statuses and failure causes, so they are addressed to
// these users only, never broadcast to the tenant. Results are cached per
// tenant for a short TTL (a grant or revocation takes effect within it); a
// stale list is served while auth is briefly unreachable, and with nothing
// cached the resolver fails closed (the event is not sent).
package audience

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
)

// The permission job events require (module deployer, jobs:read).
const (
	Module   = "deployer"
	Resource = "jobs"
	Action   = "read"
)

// Defaults.
const (
	DefaultTTL  = 30 * time.Second
	maxMembers  = 100_000
	checkers    = 16 // concurrent permission checks
	callTimeout = 5 * time.Second
)

// ErrUnavailable is returned when auth cannot be reached and nothing is cached.
var ErrUnavailable = errors.New("audience: directory unavailable")

// Resolver lists the users allowed to receive a tenant's job events.
type Resolver interface {
	JobReaders(ctx context.Context, tenantID string) ([]string, error)
}

// Members lists a tenant's active members (authv1.ProfilesClient satisfies it).
type Members interface {
	ListMembers(ctx context.Context, in *authv1.ListMembersRequest, opts ...grpc.CallOption) (*authv1.ListMembersResponse, error)
}

// Checker decides one permission (authv1.AuthorizationClient satisfies it).
type Checker interface {
	Check(ctx context.Context, in *authv1.CheckRequest, opts ...grpc.CallOption) (*authv1.CheckResponse, error)
}

type cached struct {
	users []string
	at    time.Time
}

// Auth is the Resolver over the auth service.
type Auth struct {
	Members Members
	Perms   Checker
	TTL     time.Duration
	Now     func() time.Time

	mu    sync.Mutex
	cache map[string]cached
	// loading serialises the resolution per tenant so a burst of events
	// triggers one directory walk.
	loading map[string]*sync.Mutex
}

// New builds the resolver on a mesh connection to "auth".
func New(conn grpc.ClientConnInterface) *Auth {
	return &Auth{Members: authv1.NewProfilesClient(conn), Perms: authv1.NewAuthorizationClient(conn), TTL: DefaultTTL}
}

func (a *Auth) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

func (a *Auth) ttl() time.Duration {
	if a.TTL > 0 {
		return a.TTL
	}
	return DefaultTTL
}

func (a *Auth) fresh(tenantID string) (cached, bool, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	c, ok := a.cache[tenantID]
	return c, ok, ok && a.now().Sub(c.at) < a.ttl()
}

func (a *Auth) lock(tenantID string) *sync.Mutex {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.loading == nil {
		a.loading = map[string]*sync.Mutex{}
	}
	l, ok := a.loading[tenantID]
	if !ok {
		l = &sync.Mutex{}
		a.loading[tenantID] = l
	}
	return l
}

// JobReaders implements Resolver.
func (a *Auth) JobReaders(ctx context.Context, tenantID string) ([]string, error) {
	if c, _, ok := a.fresh(tenantID); ok {
		return append([]string(nil), c.users...), nil
	}
	l := a.lock(tenantID)
	l.Lock()
	defer l.Unlock()
	c, had, ok := a.fresh(tenantID)
	if ok { // resolved while we waited
		return append([]string(nil), c.users...), nil
	}
	users, err := a.load(ctx, tenantID)
	if err != nil {
		if had {
			return append([]string(nil), c.users...), nil
		}
		return nil, err
	}
	a.mu.Lock()
	if a.cache == nil {
		a.cache = map[string]cached{}
	}
	a.cache[tenantID] = cached{users: users, at: a.now()}
	a.mu.Unlock()
	return append([]string(nil), users...), nil
}

// Forget drops the tenant's cached list.
func (a *Auth) Forget(tenantID string) {
	a.mu.Lock()
	delete(a.cache, tenantID)
	a.mu.Unlock()
}

func (a *Auth) load(ctx context.Context, tenantID string) ([]string, error) {
	if a.Members == nil || a.Perms == nil {
		return nil, ErrUnavailable
	}
	var ids []string
	cursor := ""
	for {
		cctx, cancel := context.WithTimeout(ctx, callTimeout)
		res, err := a.Members.ListMembers(cctx, &authv1.ListMembersRequest{TenantId: tenantID, Cursor: cursor, Limit: 1000})
		cancel()
		if err != nil {
			return nil, ErrUnavailable
		}
		ids = append(ids, res.GetUserIds()...)
		if res.GetNextCursor() == "" || len(res.GetUserIds()) == 0 || len(ids) >= maxMembers {
			break
		}
		cursor = res.GetNextCursor()
	}
	allowed := make([]bool, len(ids))
	var (
		wg     sync.WaitGroup
		failed bool
		fmu    sync.Mutex
	)
	sem := make(chan struct{}, checkers)
	for i, id := range ids {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, id string) {
			defer wg.Done()
			defer func() { <-sem }()
			cctx, cancel := context.WithTimeout(ctx, callTimeout)
			defer cancel()
			res, err := a.Perms.Check(cctx, &authv1.CheckRequest{TenantId: tenantID, UserId: id, Module: Module, Resource: Resource, Action: Action})
			if err != nil {
				fmu.Lock()
				failed = true
				fmu.Unlock()
				return
			}
			allowed[i] = res.GetAllowed()
		}(i, id)
	}
	wg.Wait()
	if failed {
		// A partial answer would silently drop readers: keep the last list.
		return nil, ErrUnavailable
	}
	out := make([]string, 0, len(ids))
	for i, id := range ids {
		if allowed[i] {
			out = append(out, id)
		}
	}
	return out, nil
}

// Static is a fixed Resolver (tests, development): tenant → user ids.
type Static map[string][]string

// JobReaders implements Resolver.
func (s Static) JobReaders(_ context.Context, tenantID string) ([]string, error) {
	return append([]string(nil), s[tenantID]...), nil
}

var (
	_ Resolver = (*Auth)(nil)
	_ Resolver = Static(nil)
)
