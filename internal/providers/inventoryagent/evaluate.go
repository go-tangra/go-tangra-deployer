package inventoryagent

import (
	"fmt"
	"sort"
	"strings"

	invsdk "github.com/go-tangra/go-tangra-inventory/sdk/v4/pkg/inventoryclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// maxDetailHosts bounds the per-host list in job details.
const maxDetailHosts = 200

// Delivery item states (inventory data-model §1.1).
const (
	statePending     = "pending"
	stateDelivered   = "delivered"
	stateFetched     = "fetched"
	stateInstalled   = "installed"
	stateUnchanged   = "unchanged"
	stateFailed      = "failed"
	stateHookFailed  = "hook_failed"
	stateUnsupported = "unsupported"
	stateSuperseded  = "superseded"
	stateExpired     = "expired"
	stateCancelled   = "cancelled"
)

// Outcome classes of research D9.
const (
	classDone        = "done"
	classQueued      = "queued"
	classFailed      = "failed"
	classUnsupported = "unsupported"
	classIgnored     = "ignored"
)

func classify(state string) string {
	switch state {
	case stateInstalled, stateUnchanged:
		return classDone
	case stateFailed, stateHookFailed, stateExpired, stateCancelled:
		return classFailed
	case stateUnsupported:
		return classUnsupported
	case stateSuperseded:
		return classIgnored
	default: // pending, delivered, fetched and states unknown to this version
		return classQueued
	}
}

// terminal reports whether an item will not change without a new request.
func terminal(state string) bool {
	switch classify(state) {
	case classDone, classFailed, classUnsupported, classIgnored:
		return true
	}
	return false
}

// settled reports whether the provider need not wait for the item any longer:
// it is terminal, or (after the grace period) it waits for an offline agent.
func settled(it invsdk.DeliveryItem, graceOver bool) bool {
	if terminal(it.State) {
		return true
	}
	return graceOver && !it.AgentOnline && (it.State == statePending || it.State == stateDelivered)
}

// Counts are the per-state totals reported in the job details.
type Counts struct {
	Installed   int `json:"installed"`
	Unchanged   int `json:"unchanged"`
	Queued      int `json:"queued"`
	Failed      int `json:"failed"`
	Unsupported int `json:"unsupported"`
	Superseded  int `json:"superseded"`
	Total       int `json:"total"`
}

// evaluate turns the delivery at the end of the wait into the job result
// (research D9, FR-004): with require_all_success a job succeeds iff no host
// failed or is unsupported (queued hosts are not failures, Q2); otherwise iff
// at least one host is done or queued, or nothing failed (every host
// superseded). No hosts at all is a permanent failure.
func evaluate(d invsdk.Delivery, name string, requireAll bool) *provider.Result {
	var c Counts
	for _, it := range d.Items {
		c.Total++
		switch classify(it.State) {
		case classDone:
			if it.State == stateInstalled {
				c.Installed++
			} else {
				c.Unchanged++
			}
		case classQueued:
			c.Queued++
		case classFailed:
			c.Failed++
		case classUnsupported:
			c.Unsupported++
		case classIgnored:
			c.Superseded++
		}
	}
	details := map[string]any{"delivery_id": d.ID, "name": name, "counts": c}
	if len(d.UnknownHostIDs) > 0 {
		details["unknown_host_ids"] = clip(d.UnknownHostIDs, maxDetailHosts)
	}
	hosts, truncated := hostDetails(d.Items)
	details["hosts"] = hosts
	if truncated {
		details["hosts_truncated"] = true
	}
	if c.Total == 0 {
		return &provider.Result{Success: false, Permanent: true, Message: "no hosts matched", Details: details}
	}
	bad := c.Failed + c.Unsupported
	var ok bool
	if requireAll {
		ok = bad == 0
	} else {
		ok = c.Installed+c.Unchanged+c.Queued >= 1 || bad == 0
	}
	return &provider.Result{Success: ok, Message: message(c), Details: details}
}

func message(c Counts) string {
	msg := fmt.Sprintf("Installed on %d, unchanged on %d, queued for %d, failed on %d, unsupported on %d",
		c.Installed, c.Unchanged, c.Queued, c.Failed, c.Unsupported)
	if c.Superseded > 0 {
		msg += fmt.Sprintf(", superseded on %d", c.Superseded)
	}
	return msg
}

// hostDetails lists up to maxDetailHosts hosts, those not done first.
func hostDetails(items []invsdk.DeliveryItem) ([]map[string]any, bool) {
	sorted := append([]invsdk.DeliveryItem(nil), items...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return classify(sorted[i].State) != classDone && classify(sorted[j].State) == classDone
	})
	truncated := len(sorted) > maxDetailHosts
	if truncated {
		sorted = sorted[:maxDetailHosts]
	}
	out := make([]map[string]any, 0, len(sorted))
	for _, it := range sorted {
		h := map[string]any{
			"host_id": it.HostID, "hostname": it.Hostname, "state": it.State, "agent_online": it.AgentOnline,
			"attempts": it.Attempts, "hook_exit_code": it.HookExitCode,
		}
		if it.Reason != "" {
			h["reason"] = it.Reason
		}
		if it.Serial != "" {
			h["serial"] = it.Serial
		}
		if it.Fingerprint != "" {
			h["fingerprint"] = it.Fingerprint
		}
		out = append(out, h)
	}
	return out, truncated
}

// verifyResult evaluates a VerifyHostCertificates answer (FR-005).
func verifyResult(v invsdk.Verification, requireAll bool) *provider.Result {
	counts := map[string]int{}
	var hosts []map[string]any
	for _, h := range v.Hosts {
		counts[h.Status]++
		if h.Status != "match" && len(hosts) < maxDetailHosts {
			e := map[string]any{"host_id": h.HostID, "hostname": h.Hostname, "status": h.Status}
			if h.Fingerprint != "" {
				e["fingerprint"] = h.Fingerprint
			}
			if h.Reason != "" {
				e["reason"] = h.Reason
			}
			hosts = append(hosts, e)
		}
	}
	details := map[string]any{"matched": v.Matched, "total": v.Total, "statuses": counts, "hosts": hosts}
	if v.Total == 0 {
		return &provider.Result{Success: false, Message: "no hosts matched", Details: details}
	}
	var ok bool
	if requireAll {
		ok = v.Matched == v.Total
	} else {
		ok = v.Matched >= 1 && counts["mismatch"] == 0 && counts["revoked"] == 0
	}
	msg := fmt.Sprintf("Certificate present on %d of %d hosts", v.Matched, v.Total)
	var other []string
	for _, st := range []string{"mismatch", "revoked", "pending", "failed", "missing"} {
		if counts[st] > 0 {
			other = append(other, fmt.Sprintf("%s %d", st, counts[st]))
		}
	}
	if len(other) > 0 {
		msg += " (" + strings.Join(other, ", ") + ")"
	}
	return &provider.Result{Success: ok, Message: msg, Details: details}
}

func clip(xs []string, n int) []string {
	if len(xs) > n {
		return xs[:n]
	}
	return xs
}
