//go:build integration

package app_test

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"

	lcmv1 "github.com/go-tangra/go-tangra-lcm/sdk/v4/api/proto/lcm/v1"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/app"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/inventoryclient/inventorytest"
)

// fakeLCM serves lcm.v1.Certificates/Download and records include_key.
type fakeLCM struct {
	lcmv1.UnimplementedCertificatesServer
	mu      sync.Mutex
	include []bool
	certPEM string
}

func (f *fakeLCM) Download(_ context.Context, r *lcmv1.DownloadRequest) (*lcmv1.CertificateBundle, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.include = append(f.include, r.GetIncludeKey())
	b := &lcmv1.CertificateBundle{Certificate: &lcmv1.Certificate{Serial: "0A", Subject: "www.example.com"}, CertPem: f.certPEM}
	if r.GetIncludeKey() {
		b.KeyPem = "-----BEGIN PRIVATE KEY-----\nMUST-NOT-BE-FETCHED\n-----END PRIVATE KEY-----\n"
	}
	return b, nil
}

func (f *fakeLCM) includes() []bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]bool(nil), f.include...)
}

func apiCall(t *testing.T, a *app.App, method, path, body string) (int, map[string]any) {
	t.Helper()
	r := httptest.NewRequest(method, "https://localhost"+path, strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer admin")
	if body != "" {
		r.Header.Set("Content-Type", "application/json")
	}
	if method != "GET" {
		r.Header.Set("X-CSRF-Token", "t")
	}
	w := httptest.NewRecorder()
	a.HTTP.Handler().ServeHTTP(w, r)
	var m map[string]any
	_ = json.Unmarshal(w.Body.Bytes(), &m)
	return w.Code, m
}

// T053: a job with the inventory-agent provider, run by the real worker
// against the real store, delivers by reference through an in-process
// inventory CertificateDeliveryService: the job completes, the result and the
// history carry the delivery id and counts, and the private key is never
// fetched from lcm (include_key=false for Deploy and Verify).
func TestInventoryAgentJobEndToEnd(t *testing.T) {
	ctx := context.Background()
	const host = "0192a7c0-0000-7000-8000-00000000000a"
	inv := inventorytest.NewFake(host)
	certPEM, err := os.ReadFile("../providers/inventoryagent/testdata/rsa.crt")
	if err != nil {
		t.Fatal(err)
	}
	lcm := &fakeLCM{certPEM: string(certPEM)}

	cfg := testConfig(t)
	cfg.Inventory.Service = "inventory"
	o := options()
	o.InventoryConn = inventorytest.DialFake(t, inv)
	o.LCMConn = inventorytest.Dial(t, func(gs *grpc.Server) { lcmv1.RegisterCertificatesServer(gs, lcm) })
	a, err := app.Build(ctx, cfg, o)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	defer a.Close()
	if a.Inventory == nil {
		t.Fatal("inventory client not wired")
	}

	code, m := apiCall(t, a, "GET", "/api/deployer/v1/providers", "")
	if code != 200 || !strings.Contains(mustJSON(m), `"inventory-agent"`) {
		t.Fatalf("catalogue: %d", code)
	}
	code, m = apiCall(t, a, "POST", "/api/deployer/v1/configurations",
		`{"name":"web-hosts","provider_type":"inventory-agent","config":{"host_ids":["`+host+`"],"cert_name":"www","wait_seconds":0}}`)
	if code != 201 {
		t.Fatalf("create configuration: %d %v", code, m)
	}
	cfgID := m["id"].(string)
	// Preview hosts through the validate action.
	code, m = apiCall(t, a, "POST", "/api/deployer/v1/configurations/validate",
		`{"provider_type":"inventory-agent","config":{"host_ids":["`+host+`"]}}`)
	if code != 200 || m["details"] == nil {
		t.Fatalf("preview: %d %v", code, m)
	}

	code, m = apiCall(t, a, "POST", "/api/deployer/v1/deploy", `{"certificate_id":"cert-77","configuration_id":"`+cfgID+`"}`)
	if code != 202 {
		t.Fatalf("deploy: %d %v", code, m)
	}
	jobID := m["job_id"].(string)
	deadline := time.Now().Add(30 * time.Second)
	for {
		a.Jobs.Once(ctx, nil)
		_, m = apiCall(t, a, "GET", "/api/deployer/v1/jobs/"+jobID+"/result", "")
		if m["status"] == "completed" || m["status"] == "failed" || time.Now().After(deadline) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if m["status"] != "completed" {
		t.Fatalf("job = %v", m)
	}
	res, _ := m["result"].(map[string]any)
	details, _ := res["details"].(map[string]any)
	counts, _ := details["counts"].(map[string]any)
	if details["delivery_id"] != "delivery-"+jobID || counts["installed"] != float64(1) {
		t.Fatalf("job result = %v", res)
	}
	hist, _ := m["history"].([]any)
	if len(hist) != 1 {
		t.Fatalf("history = %v", hist)
	}
	rows, err := a.Repo.ListHistory(ctx, appTenant, jobID)
	if err != nil || len(rows) != 1 || !strings.Contains(string(rows[0].Details), "delivery-"+jobID) {
		t.Fatalf("history details = %+v, %v", rows, err)
	}

	creates, _ := inv.Snapshot()
	if len(creates) != 1 {
		t.Fatalf("deliveries requested = %d", len(creates))
	}
	c := creates[0]
	if c.GetTenantId() != appTenant || c.GetIdempotencyKey() != jobID || c.GetConfigurationId() != cfgID || c.GetCertificateId() != "cert-77" ||
		c.GetName() != "www" || c.GetTrigger() != "manual" || !c.GetRearmFailed() || c.GetSelector().GetHostIds()[0] != host {
		t.Fatalf("delivery request = %v", c)
	}

	code, m = apiCall(t, a, "POST", "/api/deployer/v1/deploy/"+jobID+"/verify", "")
	if code != 200 || m["success"] != true {
		t.Fatalf("verify: %d %v", code, m)
	}
	inc := lcm.includes()
	if len(inc) < 2 {
		t.Fatalf("lcm downloads = %v", inc)
	}
	for _, k := range inc {
		if k {
			t.Fatal("the deployer fetched the private key for a by-reference provider")
		}
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
