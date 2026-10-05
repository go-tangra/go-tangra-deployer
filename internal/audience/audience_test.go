package audience_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audience"
)

// fakeAuth is a two-page member directory plus a permission table.
type fakeAuth struct {
	mu       sync.Mutex
	members  []string
	allowed  map[string]bool
	down     bool
	lists    int
	lastReq  *authv1.CheckRequest
	checkErr string // user whose check fails
}

func (f *fakeAuth) ListMembers(_ context.Context, in *authv1.ListMembersRequest, _ ...grpc.CallOption) (*authv1.ListMembersResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.down {
		return nil, errors.New("unavailable")
	}
	if in.GetCursor() == "" {
		f.lists++
		half := len(f.members) / 2
		return &authv1.ListMembersResponse{UserIds: f.members[:half], NextCursor: f.members[half-1]}, nil
	}
	return &authv1.ListMembersResponse{UserIds: f.members[len(f.members)/2:]}, nil
}

func (f *fakeAuth) Check(_ context.Context, in *authv1.CheckRequest, _ ...grpc.CallOption) (*authv1.CheckResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastReq = in
	if f.down || in.GetUserId() == f.checkErr {
		return nil, errors.New("unavailable")
	}
	return &authv1.CheckResponse{Allowed: f.allowed[in.GetUserId()]}, nil
}

func newAuth(f *fakeAuth, now *time.Time) *audience.Auth {
	return &audience.Auth{Members: f, Perms: f, TTL: time.Minute, Now: func() time.Time { return *now }}
}

// Only members holding deployer jobs:read are readers, across member pages.
func TestJobReadersFiltersByPermission(t *testing.T) {
	f := &fakeAuth{members: []string{"u1", "u2", "u3", "u4"}, allowed: map[string]bool{"u1": true, "u4": true}}
	now := time.Now()
	got, err := newAuth(f, &now).JobReaders(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got, []string{"u1", "u4"}) {
		t.Fatalf("readers = %v", got)
	}
	r := f.lastReq
	if r.GetTenantId() != "t1" || r.GetModule() != "deployer" || r.GetResource() != "jobs" || r.GetAction() != "read" {
		t.Fatalf("check = %+v", r)
	}
}

// The list is cached for the TTL, then re-resolved (a grant takes effect).
func TestJobReadersCachesForTTL(t *testing.T) {
	f := &fakeAuth{members: []string{"u1", "u2"}, allowed: map[string]bool{"u1": true}}
	now := time.Now()
	a := newAuth(f, &now)
	ctx := context.Background()
	_, _ = a.JobReaders(ctx, "t1")
	f.mu.Lock()
	f.allowed["u2"] = true
	f.mu.Unlock()
	if got, _ := a.JobReaders(ctx, "t1"); !slices.Equal(got, []string{"u1"}) || f.lists != 1 {
		t.Fatalf("within TTL: readers = %v lists = %d", got, f.lists)
	}
	now = now.Add(2 * time.Minute)
	if got, _ := a.JobReaders(ctx, "t1"); !slices.Equal(got, []string{"u1", "u2"}) {
		t.Fatalf("after TTL: readers = %v", got)
	}
}

// Auth down: the last list is served; with nothing cached the resolver fails
// closed. A partial permission answer never replaces the list.
func TestJobReadersFailsClosed(t *testing.T) {
	ctx := context.Background()
	now := time.Now()
	f := &fakeAuth{members: []string{"u1", "u2"}, allowed: map[string]bool{"u1": true}, down: true}
	if _, err := newAuth(f, &now).JobReaders(ctx, "t1"); !errors.Is(err, audience.ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}

	f.down = false
	a := newAuth(f, &now)
	_, _ = a.JobReaders(ctx, "t1")
	now = now.Add(2 * time.Minute)
	f.checkErr = "u2"
	if got, err := a.JobReaders(ctx, "t1"); err != nil || !slices.Equal(got, []string{"u1"}) {
		t.Fatalf("stale on failure: readers = %v err = %v", got, err)
	}
}
