package app

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/audience"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/stream"
)

type failingReaders struct{}

func (failingReaders) JobReaders(context.Context, string) ([]string, error) {
	return nil, audience.ErrUnavailable
}

func next(t *testing.T, s *stream.Subscription) (stream.Event, bool) {
	t.Helper()
	select {
	case ev := <-s.Events():
		return ev, true
	case <-time.After(300 * time.Millisecond):
		return stream.Event{}, false
	}
}

// Job events reach only the users holding jobs:read; nothing is sent when
// the readers cannot be resolved.
func TestHubPublisherTargetsJobReaders(t *testing.T) {
	ctx := context.Background()
	hub := stream.NewHub(stream.NewMemory(), stream.Config{ReadBlock: 20 * time.Millisecond}, nil)
	defer hub.Close()
	reader, err := hub.Subscribe(ctx, "t1", "reader", "")
	if err != nil {
		t.Fatal(err)
	}
	other, _ := hub.Subscribe(ctx, "t1", "other", "")

	p := hubPublisher{hub: hub, readers: audience.Static{"t1": {"reader"}}, log: slog.Default()}
	p.Publish(ctx, "t1", "deployment.failed", map[string]any{"job_id": "j1", "error": "boom"})
	if ev, ok := next(t, reader); !ok || ev.Type != "deployment.failed" {
		t.Fatalf("reader got %+v (ok=%v)", ev, ok)
	}
	if ev, ok := next(t, other); ok {
		t.Fatalf("user without jobs:read got %+v", ev)
	}

	p.readers = failingReaders{}
	p.Publish(ctx, "t1", "deployment.failed", map[string]any{"job_id": "j2"})
	if ev, ok := next(t, reader); ok {
		t.Fatalf("event sent without a resolved audience: %+v", ev)
	}
}
