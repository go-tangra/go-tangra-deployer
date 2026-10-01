package store

import "github.com/go-tangra/go-tangra/v4/listquery"

// jobTypeExpr derives DeploymentJob.JobType() in SQL (child before parent).
const jobTypeExpr = "(CASE WHEN parent_job_id IS NOT NULL THEN 'child' WHEN deployment_target_id IS NOT NULL THEN 'parent' ELSE 'direct' END)"

// List definitions of the deployer tables (specs/032-server-side-tables in
// go-tangra, contracts/sortable-fields.md "deployer"). Sort fields map to
// constant SQL expressions only; the memstore sorts the same public names in Go.
var (
	// ConfigList pages deployer_configs: name order by default.
	ConfigList = listquery.Spec{
		Fields: map[string]listquery.Field{
			"name":          {Expr: "name", Text: true},
			"provider_type": {Expr: "provider_type", Text: true},
			"status":        {Expr: "status"},
			"created_at":    {Expr: "created_at", DefaultDir: listquery.Desc},
		},
		Default: "name", TieBreak: "id",
	}
	// TargetList pages deployer_targets: name order by default.
	TargetList = listquery.Spec{
		Fields: map[string]listquery.Field{
			"name":       {Expr: "name", Text: true},
			"created_at": {Expr: "created_at", DefaultDir: listquery.Desc},
		},
		Default: "name", TieBreak: "id",
	}
	// JobList pages deployer_jobs (jobs table, dashboard): newest first.
	JobList = listquery.Spec{
		Fields: map[string]listquery.Field{
			"created_at":   {Expr: "created_at", DefaultDir: listquery.Desc},
			"status":       {Expr: "status"},
			"job_type":     {Expr: jobTypeExpr},
			"completed_at": {Expr: "completed_at", DefaultDir: listquery.Desc},
		},
		Default: "created_at", TieBreak: "id",
	}
	// ChildJobList pages a parent job's children: newest first.
	ChildJobList = listquery.Spec{
		Fields:  map[string]listquery.Field{"created_at": {Expr: "created_at", DefaultDir: listquery.Desc}},
		Default: "created_at", TieBreak: "id",
	}
	// HistoryList pages a job's deployment history: newest first.
	HistoryList = listquery.Spec{
		Fields:  map[string]listquery.Field{"created_at": {Expr: "created_at", DefaultDir: listquery.Desc}},
		Default: "created_at", TieBreak: "id",
	}
)

// ListRequest completes r with the Spec's defaults (a zero Request from an
// internal caller pages with the defaults); an invalid hand-built Request
// falls back to the defaults entirely.
func ListRequest(r listquery.Request, s listquery.Spec) listquery.Request {
	out, err := listquery.New(r.Page, r.PageSize, r.Sort, r.Order, s)
	if err != nil {
		out, _ = listquery.New(0, 0, "", "", s)
	}
	return out
}
