package notify

import (
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func TestVariables(t *testing.T) {
	v := Variables(outcome(), "https://portal.example.org")
	want := map[string]string{
		"target": "edge", "status": "partial", "triggered_by": "event", "certificate": "api.example.com",
		"total": "2", "succeeded": "1", "failed": "1", "finished_at": "2026-11-25T10:01:12Z", "duration": "1m12s",
		"results": "alpha — completed\nbeta — failed: refused", "link": "https://portal.example.org/deployer/jobs?job=job-1", "job_id": "job-1",
	}
	for k, w := range want {
		if v[k] != w {
			t.Errorf("%s = %q, want %q", k, v[k], w)
		}
	}
	if len(v) != len(want) {
		t.Errorf("variables = %v", v)
	}
	// No portal URL, no certificate name: no link, no certificate.
	o := outcome()
	o.Certificate = ""
	v = Variables(o, "")
	if _, ok := v["link"]; ok {
		t.Errorf("link without a portal URL: %q", v["link"])
	}
	if _, ok := v["certificate"]; ok {
		t.Errorf("empty certificate sent")
	}
	// Finish times are UTC.
	o.FinishedAt = time.Date(2026, 11, 25, 12, 0, 0, 0, time.FixedZone("EET", 2*3600))
	if got := Variables(o, "")["finished_at"]; got != "2026-11-25T10:00:00Z" {
		t.Errorf("finished_at = %q", got)
	}
}

func TestResultsTruncation(t *testing.T) {
	var rs []Result
	for i := range 60 {
		rs = append(rs, Result{Configuration: fmt.Sprintf("cfg-%02d", i), Status: "failed", Message: strings.Repeat("é", 400) + "\nsecond line"})
	}
	rs[0].Status, rs[0].Message = "completed", "ignored for a success"
	out := Results(rs)
	lines := strings.Split(out, "\n")
	if len(lines) != MaxResultLines+1 || lines[MaxResultLines] != "… and 10 more" {
		t.Fatalf("lines = %d, last = %q", len(lines), lines[len(lines)-1])
	}
	if lines[0] != "cfg-00 — completed" {
		t.Fatalf("first = %q", lines[0])
	}
	for _, l := range lines {
		if len(l) > MaxLineBytes || !utf8.ValidString(l) {
			t.Fatalf("line of %d bytes (valid %v): %q", len(l), utf8.ValidString(l), l)
		}
	}
	if !strings.HasPrefix(lines[1], "cfg-01 — failed: éé") || !strings.HasSuffix(lines[1], "…") {
		t.Fatalf("failed line = %q", lines[1])
	}
	// Everything stays well under the notification module's 64 KiB.
	total := 0
	o := outcome()
	o.Results = rs
	for k, v := range Variables(o, "https://portal.example.org") {
		total += len(k) + len(v)
	}
	if total > 64<<10 {
		t.Fatalf("variables are %d bytes", total)
	}
	if Results(nil) != "" {
		t.Fatal("results of no children")
	}
}

func TestVariablesCounts(t *testing.T) {
	o := outcome()
	o.Total, o.Succeeded, o.Failed, o.Status = 12, 0, 12, "failed"
	v := Variables(o, "")
	if v["total"] != "12" || v["succeeded"] != "0" || v["failed"] != "12" || v["status"] != "failed" {
		t.Fatalf("counts = %v", v)
	}
	o.TargetName = "" // deleted target: its id names it
	if Variables(o, "")["target"] != "tgt-1" {
		t.Fatal("target without a name")
	}
}
