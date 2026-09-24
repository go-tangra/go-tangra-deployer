package authz

import (
	"context"
	"errors"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// fakeLocator is a test double for the Locator interface. It returns the
// configured (exists, err) for every call and records the last arguments so a
// test can assert the authorizer forwards the tenant/resource correctly.
type fakeLocator struct {
	exists bool
	err    error

	gotTenantID     string
	gotResourceType string
	gotID           string
	calls           int
}

func (f *fakeLocator) Exists(ctx context.Context, tenantID, resourceType, id string) (bool, error) {
	f.calls++
	f.gotTenantID = tenantID
	f.gotResourceType = resourceType
	f.gotID = id
	return f.exists, f.err
}

func TestSubjectsIsAdmin(t *testing.T) {
	if !(Subjects{Roles: []string{"viewer", "admin"}}).IsAdmin() {
		t.Fatal("subject with admin role must be admin")
	}
	if (Subjects{Roles: []string{"viewer", "editor"}}).IsAdmin() {
		t.Fatal("subject without admin role must not be admin")
	}
	if (Subjects{}).IsAdmin() {
		t.Fatal("subject with no roles must not be admin")
	}
}

func TestSubjectsActorID(t *testing.T) {
	// A user caller reports its user id.
	if got := (Subjects{UserID: "user-1", ActorKind: "user"}).ActorID(); got != "user-1" {
		t.Fatalf("ActorID with UserID = %q, want %q", got, "user-1")
	}
	// A service/system caller (no user) falls back to its actor kind.
	if got := (Subjects{ActorKind: "service"}).ActorID(); got != "service" {
		t.Fatalf("ActorID without UserID = %q, want %q", got, "service")
	}
	if got := (Subjects{ActorKind: "system"}).ActorID(); got != "system" {
		t.Fatalf("ActorID system = %q, want %q", got, "system")
	}
}

func TestCheckInvalidResourceType(t *testing.T) {
	a := New(&fakeLocator{exists: true})
	if err := a.Check(context.Background(), Subjects{TenantID: "t1"}, "bogus_type", "id1", Read); !errors.Is(err, ErrForbidden) {
		t.Fatalf("invalid resource type = %v, want ErrForbidden", err)
	}
}

func TestCheckInvalidAction(t *testing.T) {
	a := New(&fakeLocator{exists: true})
	if err := a.Check(context.Background(), Subjects{TenantID: "t1"}, Target, "id1", "delete"); !errors.Is(err, ErrForbidden) {
		t.Fatalf("invalid action = %v, want ErrForbidden", err)
	}
}

func TestCheckCollectionLevel(t *testing.T) {
	loc := &fakeLocator{}
	a := New(loc)
	// Empty resourceID is a collection-level action: tenant membership suffices
	// and the locator is never consulted.
	if err := a.Check(context.Background(), Subjects{TenantID: "t1"}, Configuration, "", Manage); err != nil {
		t.Fatalf("collection-level check = %v, want nil", err)
	}
	if loc.calls != 0 {
		t.Fatalf("locator called %d times for a collection-level action, want 0", loc.calls)
	}
}

func TestCheckResourceExists(t *testing.T) {
	loc := &fakeLocator{exists: true}
	a := New(loc)
	if err := a.Check(context.Background(), Subjects{TenantID: "t1"}, Target, "id1", Deploy); err != nil {
		t.Fatalf("existing resource check = %v, want nil", err)
	}
	if loc.gotTenantID != "t1" || loc.gotResourceType != Target || loc.gotID != "id1" {
		t.Fatalf("locator got (%q,%q,%q), want (t1,%q,id1)", loc.gotTenantID, loc.gotResourceType, loc.gotID, Target)
	}
}

func TestCheckResourceNotFound(t *testing.T) {
	a := New(&fakeLocator{exists: false})
	if err := a.Check(context.Background(), Subjects{TenantID: "t1"}, Job, "id1", Read); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing resource check = %v, want ErrNotFound", err)
	}
}

func TestCheckLocatorErrorNonAdmin(t *testing.T) {
	boom := errors.New("db down")
	a := New(&fakeLocator{err: boom})
	if err := a.Check(context.Background(), Subjects{TenantID: "t1"}, Target, "id1", Read); !errors.Is(err, boom) {
		t.Fatalf("locator error (non-admin) = %v, want the underlying error", err)
	}
}

func TestCheckLocatorErrorAdmin(t *testing.T) {
	a := New(&fakeLocator{err: errors.New("db down")})
	// Admins pass even when the resource cannot be resolved.
	if err := a.Check(context.Background(), Subjects{TenantID: "t1", Roles: []string{"admin"}}, Target, "id1", Read); err != nil {
		t.Fatalf("locator error (admin) = %v, want nil", err)
	}
}

func allTrue(p Permissions) bool { return p.Read && p.Manage && p.Deploy }

func TestEffectiveCollectionLevel(t *testing.T) {
	loc := &fakeLocator{}
	a := New(loc)
	// Empty resourceID skips the locator and yields all-true permissions.
	if p := a.Effective(context.Background(), Subjects{TenantID: "t1"}, Target, ""); !allTrue(p) {
		t.Fatalf("collection-level Effective = %+v, want all-true", p)
	}
	if loc.calls != 0 {
		t.Fatalf("locator called %d times, want 0", loc.calls)
	}
}

func TestEffectiveResourcePresent(t *testing.T) {
	a := New(&fakeLocator{exists: true})
	if p := a.Effective(context.Background(), Subjects{TenantID: "t1"}, Configuration, "id1"); !allTrue(p) {
		t.Fatalf("present-resource Effective = %+v, want all-true", p)
	}
}

func TestEffectiveAbsentNonAdmin(t *testing.T) {
	a := New(&fakeLocator{exists: false})
	if p := a.Effective(context.Background(), Subjects{TenantID: "t1"}, Job, "id1"); p != (Permissions{}) {
		t.Fatalf("absent-resource non-admin Effective = %+v, want empty", p)
	}
}

func TestEffectiveErrorNonAdmin(t *testing.T) {
	a := New(&fakeLocator{err: errors.New("db down")})
	if p := a.Effective(context.Background(), Subjects{TenantID: "t1"}, Job, "id1"); p != (Permissions{}) {
		t.Fatalf("locator-error non-admin Effective = %+v, want empty", p)
	}
}

func TestEffectiveAbsentAdmin(t *testing.T) {
	a := New(&fakeLocator{exists: false})
	// Absent resource but admin caller → still all-true.
	if p := a.Effective(context.Background(), Subjects{TenantID: "t1", Roles: []string{"admin"}}, Target, "id1"); !allTrue(p) {
		t.Fatalf("absent-resource admin Effective = %+v, want all-true", p)
	}
}

func TestGrantOwner(t *testing.T) {
	a := New(&fakeLocator{})
	if err := a.GrantOwner(context.Background(), "t1", Target, "id1", "user-1"); err != nil {
		t.Fatalf("GrantOwner = %v, want nil (no-op in v1)", err)
	}
}

func TestValidHelpers(t *testing.T) {
	for _, rt := range []string{Target, Configuration, Job} {
		if !valid(rt) {
			t.Fatalf("valid(%q) = false, want true", rt)
		}
	}
	if valid("nope") {
		t.Fatal("valid(nope) = true, want false")
	}
	for _, act := range []string{Read, Manage, Deploy} {
		if !validAction(act) {
			t.Fatalf("validAction(%q) = false, want true", act)
		}
	}
	if validAction("nope") {
		t.Fatal("validAction(nope) = true, want false")
	}
}

func TestResourceAndActionConsts(t *testing.T) {
	// Pin the vocabulary the gateway and store agree on.
	if Target != "deployer_target" || Configuration != "deployer_configuration" || Job != "deployer_job" {
		t.Fatalf("resource consts drifted: %q %q %q", Target, Configuration, Job)
	}
	if Read != "read" || Manage != "manage" || Deploy != "deploy" {
		t.Fatalf("action consts drifted: %q %q %q", Read, Manage, Deploy)
	}
}

func TestMapErr(t *testing.T) {
	// A store not-found is masked to the authz not-found.
	if err := mapErr(store.ErrNotFound); !errors.Is(err, ErrNotFound) {
		t.Fatalf("mapErr(store.ErrNotFound) = %v, want ErrNotFound", err)
	}
	// Any other error passes through unchanged.
	other := errors.New("other")
	if err := mapErr(other); !errors.Is(err, other) {
		t.Fatalf("mapErr(other) = %v, want the original error", err)
	}
}
