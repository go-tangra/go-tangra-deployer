package jobs_test

import (
	"context"
	"strings"
	"testing"
	"time"

	invv1 "github.com/go-tangra/go-tangra-inventory/sdk/v4/api/proto/inventory/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/events"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/inventoryclient"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/inventoryclient/inventorytest"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/providers/inventoryagent"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
)

var invAgent = inventoryagent.New(nil)

func init() { provider.Register(invAgent) }

const autoHost = "0192a7c0-0000-7000-8000-00000000000a"

// autoKit wires an auto-deploy target with an inventory-agent configuration
// to the in-process inventory fake through the real adapter.
func autoKit(t *testing.T, fake *inventorytest.Fake, cfg map[string]any) (*events.Consumer, *jobs.Service, string) {
	t.Helper()
	invAgent.SetInventory(inventoryclient.New(inventorytest.DialFake(t, fake)))
	m, cs, js := kitWith(t, fakeCerts{}, jobs.Config{Workers: 2, Lease: time.Minute, MaxRetries: 3, RetryDelay: time.Millisecond})
	ctx := context.Background()
	subj := adminSubj()
	conf, err := cs.Create(ctx, subj, configs.Input{Name: "hosts", ProviderType: inventoryagent.Type, Config: cfg})
	if err != nil {
		t.Fatal(err)
	}
	ts := targets.New(m, authz.New(m))
	tgt, err := ts.Create(ctx, subj, targets.Input{Name: "edge", AutoDeploy: true, Filters: []store.CertificateFilter{{CommonNamePattern: "*.example.com"}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := ts.Attach(ctx, subj, tgt.ID, []string{conf.ID}, map[string]map[string]any{conf.ID: {"host_ids": []any{autoHost}}}); err != nil {
		t.Fatal(err)
	}
	return events.NewConsumer(m, fakeCerts{}, nil), js, tgt.ID
}

func childJobs(t *testing.T, js *jobs.Service) []jobs.View {
	t.Helper()
	all, err := js.List(context.Background(), adminSubj(), repo.JobFilter{JobType: store.JobTypeChild})
	if err != nil {
		t.Fatal(err)
	}
	return all
}

// T061: a renewal matching the filter creates a job whose provider call
// carries the new certificate id and the auto_deploy trigger; hosts whose
// agents are offline leave the job completed with "queued for N".
func TestAutoDeployRenewalToInventoryAgent(t *testing.T) {
	fake := inventorytest.NewFake(autoHost)
	fake.State, fake.Offline = invv1.DeliveryState_DELIVERY_STATE_PENDING, true
	// The configuration leaves the hosts to the target (D25) and does not wait.
	cons, js, _ := autoKit(t, fake, map[string]any{"wait_seconds": 0})
	ctx := context.Background()
	if n, err := cons.Handle(ctx, adminSubj().TenantID, "cert-renewed-2", true); err != nil || n != 1 {
		t.Fatalf("handle = %d, %v", n, err)
	}
	for i := 0; i < 3; i++ {
		js.Once(ctx, nil)
	}
	kids := childJobs(t, js)
	if len(kids) != 1 || kids[0].Status != store.JobCompleted || !strings.Contains(kids[0].StatusMessage, "queued for 1") {
		t.Fatalf("child = %+v", kids)
	}
	creates, _ := fake.Snapshot()
	if len(creates) != 1 {
		t.Fatalf("creates = %d", len(creates))
	}
	c := creates[0]
	if c.GetCertificateId() != "cert-renewed-2" || c.GetTrigger() != provider.TriggerAutoDeploy || c.GetIdempotencyKey() != kids[0].ID ||
		c.GetSelector().GetHostIds()[0] != autoHost || c.GetTargetId() == "" {
		t.Fatalf("request = %v", c)
	}
}

// T061: a failed delivery retries the job with the same idempotency key, so
// the inventory re-arms only the failed hosts.
func TestInventoryAgentRetryReusesIdempotencyKey(t *testing.T) {
	fake := inventorytest.NewFake(autoHost)
	fake.State = invv1.DeliveryState_DELIVERY_STATE_FAILED
	cons, js, _ := autoKit(t, fake, map[string]any{"wait_seconds": 0})
	ctx := context.Background()
	if _, err := cons.Handle(ctx, adminSubj().TenantID, "cert-3", false); err != nil {
		t.Fatal(err)
	}
	clk := time.Now()
	js.SetClock(func() time.Time { return clk })
	js.Once(ctx, nil)
	kids := childJobs(t, js)
	if kids[0].Status != store.JobRetrying {
		t.Fatalf("first attempt = %+v", kids[0])
	}
	fake.State = invv1.DeliveryState_DELIVERY_STATE_INSTALLED // the re-armed host installs
	clk = clk.Add(time.Hour)
	js.Once(ctx, nil)
	kids = childJobs(t, js)
	creates, _ := fake.Snapshot()
	if len(creates) != 2 || creates[1].GetIdempotencyKey() != creates[0].GetIdempotencyKey() || !creates[1].GetRearmFailed() ||
		creates[1].GetTrigger() != provider.TriggerRetry {
		t.Fatalf("retry requests = %v", creates)
	}
	if kids[0].Status != store.JobCompleted {
		t.Fatalf("after retry = %+v", kids[0])
	}
}
