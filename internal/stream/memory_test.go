package stream_test

import (
	"context"
	"testing"
	"time"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/stream"
)

// TestMemoryStreamSemantics exercises the in-process Client used in place of
// Valkey: append, read-after, range, last-id, trim, and the counter/limiter.
func TestMemoryStreamSemantics(t *testing.T) {
	ctx := context.Background()
	m := stream.NewMemory()
	defer m.Close()

	if err := m.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	key := "platform:events:11111111-1111-1111-1111-111111111111"
	id1, err := m.XAdd(ctx, key, map[string]string{"type": "certificate.issued", "n": "1"}, 100)
	if err != nil || id1 == "" {
		t.Fatalf("xadd 1: %q %v", id1, err)
	}
	id2, err := m.XAdd(ctx, key, map[string]string{"type": "certificate.renewed", "n": "2"}, 100)
	if err != nil {
		t.Fatalf("xadd 2: %v", err)
	}
	if m.Len(key) != 2 {
		t.Fatalf("len = %d, want 2", m.Len(key))
	}

	// Read everything from the start.
	all, err := m.XRange(ctx, key, "0", 10)
	if err != nil || len(all) != 2 {
		t.Fatalf("xrange: %d %v", len(all), err)
	}
	if all[0].Fields["type"] != "certificate.issued" {
		t.Fatalf("first entry: %+v", all[0])
	}

	// Read after the first id returns only the second.
	after, err := m.XRead(ctx, key, id1, 10*time.Millisecond, 10)
	if err != nil || len(after) != 1 || after[0].ID != id2 {
		t.Fatalf("xread after: %d %v", len(after), err)
	}

	// Last id is the second entry.
	last, err := m.XLast(ctx, key)
	if err != nil || last != id2 {
		t.Fatalf("xlast: %q %v", last, err)
	}

	// Trim below the second id drops the first.
	if err := m.XTrimMinID(ctx, key, id2); err != nil {
		t.Fatalf("xtrim: %v", err)
	}
	if m.Len(key) != 1 {
		t.Fatalf("len after trim = %d, want 1", m.Len(key))
	}

	// Counter / limiter.
	n, err := m.Incr(ctx, "rate:tenant", time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("incr: %d %v", n, err)
	}
	if n2, _ := m.Incr(ctx, "rate:tenant", time.Minute); n2 != 2 {
		t.Fatalf("incr 2: %d", n2)
	}
	if lim := stream.NewLimiter(m); lim == nil {
		t.Fatal("nil limiter")
	}
}

// TestHubConstruction confirms the hub builds over the in-process client and
// closes cleanly (the SSE fan-out plumbing).
func TestHubConstruction(t *testing.T) {
	m := stream.NewMemory()
	h := stream.NewHub(m, stream.Config{
		ReplayWindow: time.Minute, StreamsPerUser: 5, StreamsPerTenant: 20,
		RetryDelay: time.Millisecond, ReadBlock: 10 * time.Millisecond,
	}, nil)
	if h == nil {
		t.Fatal("nil hub")
	}
	h.Close()
}
