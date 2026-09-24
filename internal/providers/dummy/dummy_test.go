package dummy

import (
	"context"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

func TestDummyRegistersAndDeploys(t *testing.T) {
	p, err := provider.Get("dummy")
	if err != nil {
		t.Fatalf("dummy not registered: %v", err)
	}
	c := p.Capabilities()
	if !c.SupportsVerify || !c.SupportsRollback || c.DisplayName == "" {
		t.Fatalf("capabilities: %+v", c)
	}
	var last int
	r, err := p.Deploy(context.Background(), &provider.CertificateData{SerialNumber: "01"}, nil, nil, func(pct int, _ string) { last = pct })
	if err != nil || !r.Success || last != 100 {
		t.Fatalf("deploy: r=%+v err=%v last=%d", r, err, last)
	}
	if r.Details["serial"] != "01" {
		t.Fatalf("details: %+v", r.Details)
	}
}

func TestDummyForcedFailure(t *testing.T) {
	p, _ := provider.Get("dummy")
	r, err := p.Deploy(context.Background(), nil, map[string]any{"fail": true}, nil, nil)
	if err == nil || (r != nil && r.Success) {
		t.Fatalf("expected failure, got r=%+v err=%v", r, err)
	}
	vr, _ := p.Verify(context.Background(), nil, map[string]any{"fail": true}, nil)
	if vr.Success {
		t.Fatal("expected verify mismatch")
	}
}

func TestDummyRespectsContext(t *testing.T) {
	p, _ := provider.Get("dummy")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := p.Deploy(ctx, nil, nil, nil, nil); err == nil {
		t.Fatal("expected ctx cancellation")
	}
}
