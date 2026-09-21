package backup_test

import (
	"context"
	"testing"

	"github.com/go-freya/freya/services/deployer/internal/authz"
	"github.com/go-freya/freya/services/deployer/internal/backup"
	"github.com/go-freya/freya/services/deployer/internal/memstore"
	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/store"
)

const tenantA = "11111111-1111-1111-1111-111111111111"
const tenantB = "22222222-2222-2222-2222-222222222222"

func subjA() authz.Subjects { return authz.Subjects{TenantID: tenantA, UserID: "u1"} }
func subjB() authz.Subjects { return authz.Subjects{TenantID: tenantB, UserID: "u2"} }

func seed(t *testing.T, m *memstore.Mem) (cfgID, tgtID string) {
	t.Helper()
	ctx := context.Background()
	cfgID = store.NewID()
	_ = m.InsertConfiguration(ctx, store.TargetConfiguration{
		ID: cfgID, TenantID: tenantA, Name: "acm", ProviderType: "aws_acm",
		Config: map[string]any{"region": "us-east-1"}, CredentialsSealed: []byte("SEALED-BLOB"), Status: store.ConfigActive,
	})
	tgtID = store.NewID()
	_ = m.InsertTarget(ctx, store.DeploymentTarget{ID: tgtID, TenantID: tenantA, Name: "prod", AutoDeploy: true})
	_ = m.AttachConfigurations(ctx, tenantA, tgtID, []string{cfgID})
	return
}

func TestExportExcludesCredentialsByDefault(t *testing.T) {
	m := memstore.New()
	seed(t, m)
	svc := backup.New(m)
	b, err := svc.Export(context.Background(), subjA(), false)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(b.Configurations) != 1 || len(b.Configurations[0].CredentialsSealed) != 0 {
		t.Fatalf("credentials leaked without request: %+v", b.Configurations)
	}
	if b.SchemaVersion != backup.SchemaVersion {
		t.Fatalf("schema version = %d", b.SchemaVersion)
	}
}

func TestExportIncludesCredentialsOnRequest(t *testing.T) {
	m := memstore.New()
	seed(t, m)
	svc := backup.New(m)
	b, err := svc.Export(context.Background(), subjA(), true)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if string(b.Configurations[0].CredentialsSealed) != "SEALED-BLOB" {
		t.Fatalf("sealed creds not exported: %+v", b.Configurations[0])
	}
}

func TestRoundTripIntoEmptyTenant(t *testing.T) {
	m := memstore.New()
	cfgID, tgtID := seed(t, m)
	svc := backup.New(m)
	b, _ := svc.Export(context.Background(), subjA(), true)

	res, err := svc.Import(context.Background(), subjB(), b, backup.ModeSkip)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	if res.ConfigurationsImported != 1 || res.TargetsImported != 1 {
		t.Fatalf("import result: %+v", res)
	}
	// Config recreated under tenant B, same id, sealed blob preserved.
	c, err := m.GetConfiguration(context.Background(), tenantB, cfgID)
	if err != nil {
		t.Fatalf("get imported config: %v", err)
	}
	if c.Name != "acm" || string(c.CredentialsSealed) != "SEALED-BLOB" {
		t.Fatalf("imported config wrong: %+v", c)
	}
	// Target link preserved.
	ids, _ := m.ListTargetConfigurationIDs(context.Background(), tenantB, tgtID)
	if len(ids) != 1 || ids[0] != cfgID {
		t.Fatalf("links not restored: %+v", ids)
	}
}

func TestImportSkipVsOverwrite(t *testing.T) {
	ctx := context.Background()
	m := memstore.New()
	seed(t, m)
	svc := backup.New(m)
	b, _ := svc.Export(ctx, subjA(), true)

	// Tenant B already has a config named "acm" (different id).
	_ = m.InsertConfiguration(ctx, store.TargetConfiguration{ID: store.NewID(), TenantID: tenantB, Name: "acm", ProviderType: "cloudflare", Status: store.ConfigActive})

	// Skip: existing kept.
	res, err := svc.Import(ctx, subjB(), b, backup.ModeSkip)
	if err != nil {
		t.Fatalf("skip import: %v", err)
	}
	if res.ConfigurationsSkipped != 1 || res.ConfigurationsImported != 0 {
		t.Fatalf("skip result: %+v", res)
	}
	got, _ := m.ListConfigurations(ctx, tenantB, repo.ConfigFilter{})
	if len(got) != 1 || got[0].ProviderType != "cloudflare" {
		t.Fatalf("skip should keep existing: %+v", got)
	}

	// Overwrite: replaced by the imported aws_acm config.
	res, err = svc.Import(ctx, subjB(), b, backup.ModeOverwrite)
	if err != nil {
		t.Fatalf("overwrite import: %v", err)
	}
	if res.ConfigurationsImported != 1 {
		t.Fatalf("overwrite result: %+v", res)
	}
	got, _ = m.ListConfigurations(ctx, tenantB, repo.ConfigFilter{})
	if len(got) != 1 || got[0].ProviderType != "aws_acm" {
		t.Fatalf("overwrite should replace: %+v", got)
	}
}

func TestImportRejectsBadSchema(t *testing.T) {
	m := memstore.New()
	svc := backup.New(m)
	_, err := svc.Import(context.Background(), subjB(), backup.Backup{SchemaVersion: 999}, backup.ModeSkip)
	if err == nil {
		t.Fatal("expected bad-schema error")
	}
}
