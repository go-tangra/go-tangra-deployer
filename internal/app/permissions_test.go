package app

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	authv1 "github.com/go-tangra/go-tangra-auth/sdk/v4/api/proto/auth/v1"
	"github.com/go-tangra/go-tangra-deployer/v4/pkg/deployermanifest"
)

// fakeAuth answers Authorization/RegisterPermissions in memory; the first
// fail calls return an error.
type fakeAuth struct {
	mu     sync.Mutex
	fail   int
	calls  int
	method string
	got    *authv1.RegisterPermissionsRequest
}

func (f *fakeAuth) Invoke(_ context.Context, method string, args, reply any, _ ...grpc.CallOption) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.method = method
	f.got = proto.Clone(args.(*authv1.RegisterPermissionsRequest)).(*authv1.RegisterPermissionsRequest)
	if f.calls <= f.fail {
		return errors.New("auth unavailable")
	}
	reply.(*authv1.RegisterPermissionsResponse).Registered = uint32(len(f.got.GetPermissions()))
	return nil
}

func (f *fakeAuth) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, errors.New("no streams")
}

func (f *fakeAuth) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// TestRegister checks the request the deployer sends to auth (feature 019): module,
// display name, the declared role set and the built-in grants.
func TestRegister(t *testing.T) {
	auth := &fakeAuth{}
	if err := register(context.Background(), auth, slog.New(slog.DiscardHandler)); err != nil {
		t.Fatal(err)
	}
	req := auth.got
	if auth.method != authv1.Authorization_RegisterPermissions_FullMethodName || req.GetModule() != "deployer" || req.GetModuleDisplayName() != "Deployer" || !req.GetDeclaresRoles() {
		t.Fatalf("%s %v", auth.method, req)
	}
	if len(req.GetPermissions()) != len(deployermanifest.Permissions) {
		t.Fatalf("%d permissions", len(req.GetPermissions()))
	}
	roles := map[string][]string{}
	for _, r := range req.GetRoles() {
		roles[r.GetSlug()] = r.GetPermissions()
	}
	if len(roles) != 3 || len(roles["administrator"]) != len(deployermanifest.Permissions) || len(roles["operator"]) != 5 || len(roles["viewer"]) != 4 {
		t.Fatalf("%v", roles)
	}
	grants := map[string]int{}
	for _, g := range req.GetBuiltinGrants() {
		grants[g.GetRole()] = len(g.GetPermissions())
	}
	for slug, refs := range deployermanifest.Grants {
		if grants[slug] != len(refs) {
			t.Errorf("grant %s: %d, want %d", slug, grants[slug], len(refs))
		}
	}
	// Transport errors reach the caller.
	if err := register(context.Background(), &fakeAuth{fail: 1}, nil); err == nil {
		t.Fatal("error swallowed")
	}
}

// TestRegistrationLoop checks the cadence: retry until auth accepts, then
// re-register every period until the context ends.
func TestRegistrationLoop(t *testing.T) {
	auth := &fakeAuth{fail: 3}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		registrationLoop(ctx, func(ctx context.Context) error { return register(ctx, auth, nil) }, slog.New(slog.DiscardHandler), time.Millisecond, 5*time.Millisecond)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for auth.count() < 6 { // 3 failures, the first success, two periodic runs
		if time.Now().After(deadline) {
			t.Fatalf("%d calls", auth.count())
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("loop ignores cancellation")
	}
	// A cancelled context stops the retry phase too.
	registrationLoop(ctx, func(context.Context) error { return errors.New("down") }, slog.New(slog.DiscardHandler), time.Hour, time.Hour)
}
