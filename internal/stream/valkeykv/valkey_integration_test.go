//go:build integration

// Package valkeykv integration test: exercises the Valkey-backed stream client
// against a real Valkey (testcontainers) — append, read-after, range, last-id,
// trim and the counter — covering the rueidis command mapping. Run with:
//
//	go test -tags integration ./internal/stream/valkeykv/
//
// It skips cleanly when Docker/testcontainers is unavailable.
package valkeykv_test

import (
	"context"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/stream/valkeykv"
)

func startValkey(t *testing.T) string {
	t.Helper()
	ctx := context.Background()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "valkey/valkey:8-alpine", ExposedPorts: []string{"6379/tcp"},
			WaitingFor: wait.ForListeningPort("6379/tcp").WithStartupTimeout(time.Minute),
		}, Started: true,
	})
	if err != nil {
		t.Skipf("valkey container unavailable: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })
	host, _ := c.Host(ctx)
	port, _ := c.MappedPort(ctx, "6379/tcp")
	return host + ":" + port.Port()
}

func TestValkeyStreamRoundTrip(t *testing.T) {
	addr := startValkey(t)
	client, err := valkeykv.New(valkeykv.Config{Addresses: []string{addr}, AllowPlaintext: true})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	if err := client.Ping(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}

	key := "platform:events:11111111-1111-1111-1111-111111111111"
	id1, err := client.XAdd(ctx, key, map[string]string{"type": "certificate.issued"}, 100)
	if err != nil || id1 == "" {
		t.Fatalf("xadd 1: %q %v", id1, err)
	}
	id2, err := client.XAdd(ctx, key, map[string]string{"type": "certificate.renewed"}, 100)
	if err != nil {
		t.Fatalf("xadd 2: %v", err)
	}

	all, err := client.XRange(ctx, key, "0", 10)
	if err != nil || len(all) != 2 {
		t.Fatalf("xrange: %d %v", len(all), err)
	}
	if all[0].Fields["type"] != "certificate.issued" {
		t.Fatalf("range order/fields: %+v", all[0])
	}

	after, err := client.XRead(ctx, key, id1, 50*time.Millisecond, 10)
	if err != nil || len(after) != 1 || after[0].ID != id2 {
		t.Fatalf("xread after: %d %v", len(after), err)
	}

	if last, err := client.XLast(ctx, key); err != nil || last != id2 {
		t.Fatalf("xlast: %q %v", last, err)
	}

	if err := client.XTrimMinID(ctx, key, id2); err != nil {
		t.Fatalf("xtrim: %v", err)
	}
	if remaining, err := client.XRange(ctx, key, "0", 10); err != nil || len(remaining) != 1 {
		t.Fatalf("after trim: %d %v", len(remaining), err)
	}

	n, err := client.Incr(ctx, "rate:11111111", time.Minute)
	if err != nil || n != 1 {
		t.Fatalf("incr: %d %v", n, err)
	}
	if n2, _ := client.Incr(ctx, "rate:11111111", time.Minute); n2 != 2 {
		t.Fatalf("incr 2: %d", n2)
	}
}
