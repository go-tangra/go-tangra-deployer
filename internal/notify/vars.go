package notify

import (
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// Bounds of the template variables (the notification module accepts at most
// 50 variables and 64 KiB in total).
const (
	MaxResultLines = 50
	MaxLineBytes   = 300
	maxValueBytes  = 1024
)

// Variables are the template variables of deployer.deployment_finished.
// Required by the template: target, status, triggered_by, total, succeeded.
func Variables(o Outcome, portalURL string) map[string]string {
	v := map[string]string{
		"target":       clip(oneLine(o.TargetName), maxValueBytes),
		"status":       o.Status,
		"triggered_by": o.TriggeredBy,
		"total":        strconv.Itoa(o.Total),
		"succeeded":    strconv.Itoa(o.Succeeded),
		"failed":       strconv.Itoa(o.Failed),
		"job_id":       o.JobID,
	}
	if v["target"] == "" {
		v["target"] = o.TargetID
	}
	if o.Certificate != "" {
		v["certificate"] = clip(oneLine(o.Certificate), maxValueBytes)
	}
	if !o.FinishedAt.IsZero() {
		v["finished_at"] = o.FinishedAt.UTC().Format(time.RFC3339)
		if !o.StartedAt.IsZero() && !o.FinishedAt.Before(o.StartedAt) {
			v["duration"] = o.FinishedAt.Sub(o.StartedAt).Round(time.Second).String()
		}
	}
	if r := Results(o.Results); r != "" {
		v["results"] = r
	}
	if portalURL != "" && o.JobID != "" {
		v["link"] = strings.TrimRight(portalURL, "/") + "/deployer/jobs?job=" + url.QueryEscape(o.JobID)
	}
	return v
}

// Results is one line per child job, "<configuration> — <status>" plus
// ": <message>" for a failed one; each line at most MaxLineBytes, at most
// MaxResultLines lines, then "… and N more".
func Results(rs []Result) string {
	lines := make([]string, 0, min(len(rs), MaxResultLines)+1)
	for i, r := range rs {
		if i == MaxResultLines {
			lines = append(lines, "… and "+strconv.Itoa(len(rs)-MaxResultLines)+" more")
			break
		}
		l := oneLine(r.Configuration) + " — " + r.Status
		if msg := oneLine(r.Message); msg != "" && r.Status == "failed" {
			l += ": " + msg
		}
		lines = append(lines, clip(l, MaxLineBytes))
	}
	return strings.Join(lines, "\n")
}

// oneLine folds whitespace runs (newlines included) into single spaces.
func oneLine(s string) string { return strings.Join(strings.Fields(s), " ") }

// clip shortens s to at most n bytes of valid UTF-8, marking the cut with "…".
func clip(s string, n int) string {
	if len(s) <= n {
		return s
	}
	const ell = "…"
	cut := n - len(ell)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + ell
}
