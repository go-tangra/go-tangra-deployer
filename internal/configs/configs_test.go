package configs_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/memstore"
	"github.com/go-freya/freya/services/deployer/internal/sealed"

	// Register the dummy provider so validation and the catalogue have an entry.
	_ "github.com/go-freya/freya/services/deployer/internal/providers/dummy"
)

// plaintextSecret is the credential value that must NEVER surface through a read
// projection (View / Get / List). It is retrievable only via OpenCredentials.
const plaintextSecret = "secret-abc"

func newService(t *testing.T) (*configs.Service, *memstore.Mem) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatalf("sealed.NewEnvelope: %v", err)
	}
	az := authz.New(m)
	return configs.New(m, env, az), m
}

func adminSubject() authz.Subjects {
	return authz.Subjects{
		TenantID:  "11111111-1111-1111-1111-111111111111",
		UserID:    "u1",
		Roles:     []string{"admin"},
		ActorKind: "user",
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}
	return string(b)
}

// TestCRUDAndShapes exercises the full lifecycle and asserts the View shape.
func TestCRUDAndShapes(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()

	created, err := cs.Create(ctx, subj, configs.Input{
		Name:         "prod-endpoint",
		Description:  "production",
		ProviderType: "dummy",
		Config:       map[string]any{"region": "eu"},
		Credentials:  map[string]any{"token": plaintextSecret},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if created.ID == "" {
		t.Error("Create: expected a non-empty id")
	}
	if !created.HasCredentials {
		t.Error("Create: expected HasCredentials == true")
	}
	if created.ProviderType != "dummy" {
		t.Errorf("Create: ProviderType = %q, want %q", created.ProviderType, "dummy")
	}

	// Get round-trips.
	got, err := cs.Get(ctx, subj, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != created.ID || got.Name != "prod-endpoint" {
		t.Errorf("Get: got %+v, want id=%s name=prod-endpoint", got, created.ID)
	}
	if !got.HasCredentials {
		t.Error("Get: expected HasCredentials == true")
	}

	// List returns it.
	list, err := cs.List(ctx, subj, "", "")
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
		t.Errorf("List: created config %s not present in %d results", created.ID, len(list))
	}

	// Update changes the description and round-trips.
	updated, err := cs.Update(ctx, subj, created.ID, configs.Input{
		Name:        "prod-endpoint",
		Description: "changed-desc",
	})
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if updated.Description != "changed-desc" {
		t.Errorf("Update: Description = %q, want %q", updated.Description, "changed-desc")
	}
	// Credentials were not resent; they must remain present.
	if !updated.HasCredentials {
		t.Error("Update: expected credentials to be preserved (HasCredentials == true)")
	}

	// Delete removes it (Get -> ErrNotFound).
	if err := cs.Delete(ctx, subj, created.ID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := cs.Get(ctx, subj, created.ID); !errors.Is(err, configs.ErrNotFound) {
		t.Errorf("Get after Delete: err = %v, want ErrNotFound", err)
	}
}

// TestCredentialRedaction is the SR-002 security contract (T025): the plaintext
// credential must not appear in ANY read projection, and must be retrievable
// only through the explicit unseal path (OpenCredentials).
func TestCredentialRedaction(t *testing.T) {
	ctx := context.Background()
	cs, m := newService(t)
	subj := adminSubject()

	created, err := cs.Create(ctx, subj, configs.Input{
		Name:         "sealed-endpoint",
		ProviderType: "dummy",
		Credentials:  map[string]any{"token": plaintextSecret},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	// 1) The Create View carries no field with the plaintext.
	if s := mustJSON(t, created); strings.Contains(s, plaintextSecret) {
		t.Errorf("Create View leaks plaintext credential: %s", s)
	}
	if created.Config != nil {
		if _, ok := created.Config["token"]; ok {
			t.Error("Create View: credential leaked into Config map")
		}
	}

	// 2) The Get View carries no field with the plaintext.
	got, err := cs.Get(ctx, subj, created.ID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if s := mustJSON(t, got); strings.Contains(s, plaintextSecret) {
		t.Errorf("Get View leaks plaintext credential: %s", s)
	}

	// 3) The List View carries no field with the plaintext.
	list, err := cs.List(ctx, subj, "", "")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if s := mustJSON(t, list); strings.Contains(s, plaintextSecret) {
		t.Errorf("List View leaks plaintext credential: %s", s)
	}

	// 4) The worker path DOES recover the plaintext (sealing round-trips), so the
	// secret is retrievable only through the explicit unseal path.
	row, err := m.GetConfiguration(ctx, subj.TenantID, created.ID)
	if err != nil {
		t.Fatalf("memstore.GetConfiguration: %v", err)
	}
	// The stored blob is sealed ciphertext, not the plaintext.
	if strings.Contains(string(row.CredentialsSealed), plaintextSecret) {
		t.Error("stored CredentialsSealed contains the plaintext credential")
	}
	opened, err := cs.OpenCredentials(row)
	if err != nil {
		t.Fatalf("OpenCredentials: %v", err)
	}
	if opened["token"] != plaintextSecret {
		t.Errorf("OpenCredentials: token = %v, want %q", opened["token"], plaintextSecret)
	}
}

// TestValidate covers the provider credential-validation path.
func TestValidate(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()

	t.Run("known provider succeeds", func(t *testing.T) {
		err := cs.Validate(ctx, subj, "dummy",
			map[string]any{"token": plaintextSecret},
			map[string]any{"region": "eu"})
		if err != nil {
			t.Errorf("Validate(dummy): unexpected error %v", err)
		}
	})

	t.Run("unknown provider returns error (no panic)", func(t *testing.T) {
		err := cs.Validate(ctx, subj, "does-not-exist", nil, nil)
		if err == nil {
			t.Error("Validate(unknown): expected an error, got nil")
		}
	})
}

// TestListProviders asserts the catalogue is non-empty and includes dummy.
func TestListProviders(t *testing.T) {
	cs, _ := newService(t)
	catalogue := cs.ListProviders()
	if len(catalogue) == 0 {
		t.Fatal("ListProviders: empty catalogue")
	}
	var dummy *configs.ProviderInfo
	for i := range catalogue {
		if catalogue[i].Type == "dummy" {
			dummy = &catalogue[i]
		}
	}
	if dummy == nil {
		t.Fatal("ListProviders: dummy provider not in catalogue")
	}
	if dummy.DisplayName == "" {
		t.Error("ListProviders: dummy provider has no display name")
	}
}

// TestCreateUnknownProvider ensures an unknown provider type is a returned
// error, not a panic.
func TestCreateUnknownProvider(t *testing.T) {
	ctx := context.Background()
	cs, _ := newService(t)
	subj := adminSubject()

	_, err := cs.Create(ctx, subj, configs.Input{
		Name:         "bad-endpoint",
		ProviderType: "not-a-real-provider",
	})
	if err == nil {
		t.Fatal("Create(unknown provider): expected an error, got nil")
	}
}
