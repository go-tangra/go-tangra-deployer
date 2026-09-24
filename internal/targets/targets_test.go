package targets_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"

	// Register the dummy provider so real configurations can be created.
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/dummy"
)

func adminSubject() authz.Subjects {
	return authz.Subjects{
		TenantID:  "11111111-1111-1111-1111-111111111111",
		UserID:    "u1",
		Roles:     []string{"admin"},
		ActorKind: "user",
	}
}

func newServices(t *testing.T) (*targets.Service, *configs.Service, *memstore.Mem) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatalf("sealed.NewEnvelope: %v", err)
	}
	az := authz.New(m)
	return targets.New(m, az), configs.New(m, env, az), m
}

// makeConfig creates a real active configuration and returns its id.
func makeConfig(t *testing.T, cs *configs.Service, subj authz.Subjects, name string) string {
	t.Helper()
	v, err := cs.Create(context.Background(), subj, configs.Input{
		Name:         name,
		ProviderType: "dummy",
		Config:       map[string]any{"region": "eu"},
		Credentials:  map[string]any{"token": "secret-abc"},
	})
	if err != nil {
		t.Fatalf("configs.Create(%s): %v", name, err)
	}
	return v.ID
}

// TestCRUDRoundTrip exercises Create/Get/List/Update/Delete.
func TestCRUDRoundTrip(t *testing.T) {
	ctx := context.Background()
	ts, _, _ := newServices(t)
	subj := adminSubject()

	created, err := ts.Create(ctx, subj, targets.Input{Name: "edge-fleet", Description: "edge"})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" || created.Name != "edge-fleet" {
		t.Errorf("Create: got %+v, want non-empty id and name edge-fleet", created)
	}
	// Fresh target has an empty (non-nil) ConfigurationIDs slice.
	if created.ConfigurationIDs == nil {
		t.Error("Create: ConfigurationIDs should be a non-nil empty slice")
	}

	got, err := ts.Get(ctx, subj, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID {
		t.Errorf("Get: id = %q, want %q", got.ID, created.ID)
	}

	list, err := ts.List(ctx, subj)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	found := false
	for _, v := range list {
		if v.ID == created.ID {
			found = true
		}
	}
	if !found {
		t.Errorf("List: created target %s not present in %d results", created.ID, len(list))
	}

	updated, err := ts.Update(ctx, subj, created.ID, targets.Input{Name: "edge-fleet", Description: "changed", AutoDeploy: true})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Description != "changed" || !updated.AutoDeploy {
		t.Errorf("Update: got %+v, want description=changed auto_deploy=true", updated)
	}

	if err := ts.Delete(ctx, subj, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := ts.Get(ctx, subj, created.ID); !errors.Is(err, targets.ErrNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
	}
}

// TestCreateNameValidation enforces the 1-100 character name rule and the
// unique-by-name-per-tenant constraint.
func TestCreateNameValidation(t *testing.T) {
	ctx := context.Background()
	ts, _, _ := newServices(t)
	subj := adminSubject()

	if _, err := ts.Create(ctx, subj, targets.Input{Name: ""}); err == nil {
		t.Error("Create(empty name): expected a validation error, got nil")
	}

	if _, err := ts.Create(ctx, subj, targets.Input{Name: "dup"}); err != nil {
		t.Fatalf("Create(dup): %v", err)
	}
	if _, err := ts.Create(ctx, subj, targets.Input{Name: "dup"}); err == nil {
		t.Error("Create(duplicate name): expected a conflict/validation error, got nil")
	}
}

// TestAttachDetach covers attaching configurations and that View reflects them,
// then detaching them again.
func TestAttachDetach(t *testing.T) {
	ctx := context.Background()
	ts, cs, _ := newServices(t)
	subj := adminSubject()

	tgt, err := ts.Create(ctx, subj, targets.Input{Name: "attach-target"})
	if err != nil {
		t.Fatalf("Create target: %v", err)
	}
	c1 := makeConfig(t, cs, subj, "cfg-1")
	c2 := makeConfig(t, cs, subj, "cfg-2")

	if err := ts.Attach(ctx, subj, tgt.ID, []string{c1, c2}, nil); err != nil {
		t.Fatalf("Attach: %v", err)
	}

	got, err := ts.Get(ctx, subj, tgt.ID)
	if err != nil {
		t.Fatalf("Get after Attach: %v", err)
	}
	if len(got.ConfigurationIDs) != 2 {
		t.Errorf("View.ConfigurationIDs = %v, want 2 ids", got.ConfigurationIDs)
	}

	if err := ts.Detach(ctx, subj, tgt.ID, []string{c1}); err != nil {
		t.Fatalf("Detach: %v", err)
	}
	got, err = ts.Get(ctx, subj, tgt.ID)
	if err != nil {
		t.Fatalf("Get after Detach: %v", err)
	}
	if len(got.ConfigurationIDs) != 1 || got.ConfigurationIDs[0] != c2 {
		t.Errorf("View.ConfigurationIDs = %v, want just %q", got.ConfigurationIDs, c2)
	}
}

// TestAttachRejectsCredentialOverrides is the FR-009 contract: overrides may
// carry provider CONFIG only; a credential-shaped key is rejected.
func TestAttachRejectsCredentialOverrides(t *testing.T) {
	ctx := context.Background()
	ts, cs, _ := newServices(t)
	subj := adminSubject()

	tgt, err := ts.Create(ctx, subj, targets.Input{Name: "override-target"})
	if err != nil {
		t.Fatalf("Create target: %v", err)
	}
	c1 := makeConfig(t, cs, subj, "cfg-ov")

	// A config-only override is accepted.
	okOverride := map[string]map[string]any{c1: {"region": "us"}}
	if err := ts.Attach(ctx, subj, tgt.ID, []string{c1}, okOverride); err != nil {
		t.Fatalf("Attach(config override): unexpected error %v", err)
	}

	// Credential-shaped keys are rejected with a ValidationError.
	for _, badKey := range []string{"password", "api_token", "secret"} {
		bad := map[string]map[string]any{c1: {badKey: "leak"}}
		err := ts.Attach(ctx, subj, tgt.ID, []string{c1}, bad)
		if err == nil {
			t.Errorf("Attach(override key %q): expected a validation error, got nil", badKey)
			continue
		}
		var ve *targets.ValidationError
		if !errors.As(err, &ve) {
			t.Errorf("Attach(override key %q): err = %v, want *targets.ValidationError", badKey, err)
		}
	}
}

// TestUnknownTargetIsNotFound confirms existence-masking to ErrNotFound.
func TestUnknownTargetIsNotFound(t *testing.T) {
	ctx := context.Background()
	ts, _, _ := newServices(t)
	subj := adminSubject()

	if _, err := ts.Get(ctx, subj, "00000000-0000-0000-0000-000000000000"); !errors.Is(err, targets.ErrNotFound) {
		t.Errorf("Get(unknown): err = %v, want ErrNotFound", err)
	}
}
