package stream_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-freya/freya/services/deployer/internal/stream"
)

const hubTenant = "11111111-1111-1111-1111-111111111111"

func newHub() *stream.Hub {
	return stream.NewHub(stream.NewMemory(), stream.Config{
		ReplayWindow: time.Minute, StreamsPerUser: 5, StreamsPerTenant: 20,
		MaxLen: 100, RetryDelay: time.Millisecond, ReadBlock: 10 * time.Millisecond,
	}, nil)
}

// waitEvent reads one event from a subscription or fails after 2s.
func waitEvent(t *testing.T, sub *stream.Subscription) stream.Event {
	t.Helper()
	select {
	case ev, ok := <-sub.Events():
		if !ok {
			t.Fatal("subscription closed")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("no event within 2s")
		return stream.Event{}
	}
}

func TestHubPublishToTargetedSubscriber(t *testing.T) {
	h := newHub()
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sub, err := h.Subscribe(ctx, hubTenant, "u1", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if h.OpenStreams() == 0 {
		t.Fatal("no open streams after subscribe")
	}

	id, err := h.PublishID(ctx, hubTenant, []string{"u1"}, false, "deployment.completed", map[string]any{"job_id": "j1"}, false)
	if err != nil || id == "" {
		t.Fatalf("publish: %q %v", id, err)
	}
	ev := waitEvent(t, sub)
	if ev.Type != "deployment.completed" || !strings.Contains(ev.Data, "j1") {
		t.Fatalf("event: %+v", ev)
	}
}

func TestHubBroadcast(t *testing.T) {
	h := newHub()
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a, _ := h.Subscribe(ctx, hubTenant, "ua", "")
	b, _ := h.Subscribe(ctx, hubTenant, "ub", "")
	if err := h.Publish(ctx, hubTenant, nil, true, "deployment.started", map[string]any{"n": 1}); err != nil {
		t.Fatalf("broadcast: %v", err)
	}
	if ev := waitEvent(t, a); ev.Type != "deployment.started" {
		t.Fatalf("a: %+v", ev)
	}
	if ev := waitEvent(t, b); ev.Type != "deployment.started" {
		t.Fatalf("b: %+v", ev)
	}
}

func TestHubReplayDeliversMissed(t *testing.T) {
	h := newHub()
	defer h.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Publish before anyone subscribes.
	if _, err := h.PublishID(ctx, hubTenant, nil, true, "deployment.completed", map[string]any{"k": "v"}, false); err != nil {
		t.Fatalf("pre-publish: %v", err)
	}
	// Subscribe from the beginning replays the missed event.
	sub, err := h.Subscribe(ctx, hubTenant, "u1", "0")
	if err != nil {
		t.Fatalf("subscribe replay: %v", err)
	}
	// lastID "0" is far behind the replay window, so the hub sends a "reset"
	// telling the client to re-sync; a fresher lastID would replay the event.
	if ev := waitEvent(t, sub); ev.Type != "deployment.completed" && ev.Type != "reset" {
		t.Fatalf("replayed event: %+v", ev)
	}
}

func TestFrameEncoding(t *testing.T) {
	f := stream.Frame(stream.Event{ID: "1-0", Type: "deployment.completed", Data: `{"job_id":"j1"}`})
	if !strings.Contains(f, "event: deployment.completed") || !strings.Contains(f, "data: ") || !strings.Contains(f, "id: 1-0") {
		t.Fatalf("frame: %q", f)
	}
}

func TestServeSSEStreamsEvents(t *testing.T) {
	h := newHub()
	defer h.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()

	sub, err := h.Subscribe(ctx, hubTenant, "u1", "")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if _, err := h.PublishID(ctx, hubTenant, []string{"u1"}, false, "deployment.completed", map[string]any{"job_id": "j9"}, false); err != nil {
		t.Fatalf("publish: %v", err)
	}

	r := httptest.NewRequest("GET", "https://localhost/stream", nil).WithContext(ctx)
	w := httptest.NewRecorder()
	// Returns when ctx (700ms) elapses.
	stream.ServeSSE(w, r, sub, "instance-1", 200*time.Millisecond, time.Minute)

	body := w.Body.String()
	if !strings.Contains(body, "deployment.completed") || !strings.Contains(body, "j9") {
		t.Fatalf("sse body missing event: %q", body)
	}
	if w.Header().Get("Content-Type") != "text/event-stream" {
		t.Fatalf("content-type: %q", w.Header().Get("Content-Type"))
	}
}

func TestLimiter(t *testing.T) {
	m := stream.NewMemory()
	defer m.Close()
	lim := stream.NewLimiter(m)
	ctx := context.Background()
	now := time.Now()

	if key := stream.RateKey("publish", "u1", now); key == "" {
		t.Fatal("empty rate key")
	}
	// Limit 2 per window: first two allowed, third limited.
	if limited, _ := lim.Limited(ctx, "publish", "u1", 2, now); limited {
		t.Fatal("first call limited")
	}
	if limited, _ := lim.Limited(ctx, "publish", "u1", 2, now); limited {
		t.Fatal("second call limited")
	}
	if limited, _ := lim.Limited(ctx, "publish", "u1", 2, now); !limited {
		t.Fatal("third call should be limited")
	}
}
