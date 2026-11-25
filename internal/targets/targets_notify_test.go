package targets_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
)

func ptr[T any](v T) *T { return &v }

// Status e-mail settings: defaults on create, kept by an update that omits
// them (unlike auto_deploy, which an update always sets), stored and read
// back, and an unknown notify_on refused on the notify_on field.
func TestNotifySettings(t *testing.T) {
	ctx := context.Background()
	ts, _, m := newServices(t)
	subj := adminSubject()

	v, err := ts.Create(ctx, subj, targets.Input{Name: "plain"})
	if err != nil || v.NotifyEnabled || v.NotifyOn != store.NotifyAlways {
		t.Fatalf("defaults: %+v %v", v, err)
	}
	v, err = ts.Create(ctx, subj, targets.Input{Name: "edge", NotifyEnabled: ptr(true), NotifyOn: ptr(store.NotifyFailure)})
	if err != nil || !v.NotifyEnabled || v.NotifyOn != store.NotifyFailure {
		t.Fatalf("create: %+v %v", v, err)
	}
	if row, _ := m.GetTarget(ctx, subj.TenantID, v.ID); !row.NotifyEnabled || row.NotifyOn != store.NotifyFailure {
		t.Fatalf("stored: %+v", row)
	}
	u, err := ts.Update(ctx, subj, v.ID, targets.Input{Name: "edge", Description: "d"})
	if err != nil || !u.NotifyEnabled || u.NotifyOn != store.NotifyFailure {
		t.Fatalf("update without the fields: %+v %v", u, err)
	}
	u, err = ts.Update(ctx, subj, v.ID, targets.Input{Name: "edge", NotifyEnabled: ptr(false)})
	if err != nil || u.NotifyEnabled || u.NotifyOn != store.NotifyFailure {
		t.Fatalf("switch off: %+v %v", u, err)
	}
	if g, _ := ts.Get(ctx, subj, v.ID); g.NotifyEnabled || g.NotifyOn != store.NotifyFailure {
		t.Fatalf("get: %+v", g)
	}

	for _, bad := range []string{"", "never", "ALWAYS"} {
		_, err := ts.Create(ctx, subj, targets.Input{Name: "bad", NotifyOn: ptr(bad)})
		var ve *targets.ValidationError
		if !errors.As(err, &ve) || ve.Field != "notify_on" {
			t.Fatalf("create notify_on=%q: %v", bad, err)
		}
		_, err = ts.Update(ctx, subj, v.ID, targets.Input{Name: "edge", NotifyOn: ptr(bad)})
		if !errors.As(err, &ve) || ve.Field != "notify_on" {
			t.Fatalf("update notify_on=%q: %v", bad, err)
		}
	}
	if g, _ := ts.Get(ctx, subj, v.ID); g.NotifyOn != store.NotifyFailure {
		t.Fatalf("a refused update changed the target: %+v", g)
	}
}
