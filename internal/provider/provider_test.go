package provider

import (
	"context"
	"testing"
)

type fake struct{ caps Capabilities }

func (f fake) Deploy(context.Context, *CertificateData, map[string]any, map[string]any, ProgressFn) (*Result, error) {
	return &Result{Success: true}, nil
}
func (f fake) Verify(context.Context, *CertificateData, map[string]any, map[string]any) (*Result, error) {
	return &Result{Success: true}, nil
}
func (f fake) Rollback(context.Context, *CertificateData, map[string]any, map[string]any) (*Result, error) {
	return &Result{Success: true}, nil
}
func (f fake) ValidateCredentials(context.Context, map[string]any, map[string]any) error { return nil }
func (f fake) Capabilities() Capabilities                                                { return f.caps }

func TestRegisterGetListInfo(t *testing.T) {
	reset()
	t.Cleanup(reset)
	Register(fake{caps: Capabilities{Type: "beta", DisplayName: "Beta", SupportsVerify: true}})
	Register(fake{caps: Capabilities{Type: "alpha", DisplayName: "Alpha", SupportsRollback: true}})

	if !Exists("alpha") || Exists("nope") {
		t.Fatal("Exists wrong")
	}
	if _, err := Get("nope"); err != ErrUnknownProvider {
		t.Fatalf("Get(nope) = %v, want ErrUnknownProvider", err)
	}
	if _, err := Get("alpha"); err != nil {
		t.Fatalf("Get(alpha): %v", err)
	}
	list := List()
	if len(list) != 2 || list[0].Type != "alpha" || list[1].Type != "beta" {
		t.Fatalf("List not sorted deterministically: %+v", list)
	}
	c, ok := Info("beta")
	if !ok || !c.SupportsVerify {
		t.Fatalf("Info(beta) = %+v, %v", c, ok)
	}
	if _, ok := Info("nope"); ok {
		t.Fatal("Info(nope) should be false")
	}
}

func TestRegisterEmptyTypePanics(t *testing.T) {
	reset()
	t.Cleanup(reset)
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on empty type")
		}
	}()
	Register(fake{caps: Capabilities{Type: ""}})
}

func TestRegisterDuplicatePanics(t *testing.T) {
	reset()
	t.Cleanup(reset)
	Register(fake{caps: Capabilities{Type: "x"}})
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate type")
		}
	}()
	Register(fake{caps: Capabilities{Type: "x"}})
}
