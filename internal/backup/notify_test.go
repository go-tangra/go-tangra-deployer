package backup_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/backup"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// A target's status e-mail settings survive export and import.
func TestNotifySettingsRoundTrip(t *testing.T) {
	ctx := context.Background()
	src := memstore.New()
	tid := store.NewID()
	_ = src.InsertTarget(ctx, store.DeploymentTarget{ID: tid, TenantID: tenantA, Name: "edge", NotifyEnabled: true, NotifyOn: store.NotifyFailure})
	b, err := backup.New(src).Export(ctx, subjA(), false)
	if err != nil {
		t.Fatal(err)
	}
	if b.Targets[0].NotifyEnabled != true || b.Targets[0].NotifyOn != store.NotifyFailure {
		t.Fatalf("export: %+v", b.Targets[0])
	}
	dst := memstore.New()
	if _, err := backup.New(dst).Import(ctx, subjA(), b, backup.ModeSkip); err != nil {
		t.Fatal(err)
	}
	if got, _ := dst.GetTarget(ctx, tenantA, tid); !got.NotifyEnabled || got.NotifyOn != store.NotifyFailure {
		t.Fatalf("import: %+v", got)
	}
}

// A backup taken before status e-mails existed imports with them off.
func TestOldBackupImportsWithNotificationsOff(t *testing.T) {
	ctx := context.Background()
	tid := store.NewID()
	raw := `{"schema_version":1,"exported_at":"2026-01-01T00:00:00Z","include_credentials":false,"configurations":[],
		"targets":[{"id":"` + tid + `","name":"legacy","auto_deploy":true,"certificate_filters":[],"configuration_ids":[]}],"jobs":[]}`
	var b backup.Backup
	if err := json.Unmarshal([]byte(raw), &b); err != nil {
		t.Fatal(err)
	}
	m := memstore.New()
	if _, err := backup.New(m).Import(ctx, subjA(), b, backup.ModeSkip); err != nil {
		t.Fatal(err)
	}
	got, err := m.GetTarget(ctx, tenantA, tid)
	if err != nil || got.NotifyEnabled || got.NotifyOn != store.NotifyAlways || !got.AutoDeploy {
		t.Fatalf("legacy target: %+v %v", got, err)
	}
}

func TestImportRefusesUnknownNotifyOn(t *testing.T) {
	b := backup.Backup{SchemaVersion: backup.SchemaVersion, Targets: []backup.TargetExport{{ID: store.NewID(), Name: "x", NotifyOn: "sometimes"}}}
	_, err := backup.New(memstore.New()).Import(context.Background(), subjA(), b, backup.ModeSkip)
	var ve *backup.ValidationError
	if !errors.As(err, &ve) || ve.Field != "targets[0].notify_on" {
		t.Fatalf("err = %v", err)
	}
}
