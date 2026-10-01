package store

import (
	"strings"
	"testing"

	"github.com/go-tangra/go-tangra/v4/listquery"
)

func TestListSpecs(t *testing.T) {
	for name, s := range map[string]listquery.Spec{"configs": ConfigList, "targets": TargetList, "jobs": JobList, "children": ChildJobList, "history": HistoryList} {
		if err := s.Validate(); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if r := ListRequest(listquery.Request{}, ConfigList); r != (listquery.Request{Page: 1, PageSize: 25, Sort: "name", Order: listquery.Asc}) {
		t.Fatalf("zero request = %+v", r)
	}
	if r := ListRequest(listquery.Request{Page: 2, PageSize: 10, Sort: "completed_at"}, JobList); r != (listquery.Request{Page: 2, PageSize: 10, Sort: "completed_at", Order: listquery.Desc}) {
		t.Fatalf("partial request = %+v", r)
	}
	if r := ListRequest(listquery.Request{Page: 3, PageSize: 500, Sort: "nope"}, JobList); r != (listquery.Request{Page: 1, PageSize: 25, Sort: "created_at", Order: listquery.Desc}) {
		t.Fatalf("invalid request = %+v", r)
	}
	if got := (listquery.Request{Sort: "job_type", Order: listquery.Asc}).OrderBy(JobList); got != jobTypeExpr+" ASC NULLS LAST, id ASC" {
		t.Fatalf("job_type order = %s", got)
	}
	if got := (listquery.Request{Sort: "name", Order: listquery.Desc}).OrderBy(TargetList); got != "lower(name) DESC NULLS LAST, id DESC" {
		t.Fatalf("name order = %s", got)
	}
}

func TestJobCondsWhere(t *testing.T) {
	for _, c := range []struct {
		in   JobConds
		want string
		args int
	}{
		{JobConds{}, "tenant_id=$1", 1},
		{JobConds{Status: "failed", TriggeredBy: "event", CertificateID: "c", ParentJobID: "p", TargetID: "t"},
			"tenant_id=$1 AND status=$2 AND triggered_by=$3 AND certificate_id=$4 AND parent_job_id=$5::uuid AND deployment_target_id=$6::uuid", 6},
		{JobConds{JobType: JobTypeChild}, "tenant_id=$1 AND parent_job_id IS NOT NULL", 1},
		{JobConds{JobType: JobTypeParent}, "tenant_id=$1 AND parent_job_id IS NULL AND deployment_target_id IS NOT NULL", 1},
		{JobConds{JobType: JobTypeDirect}, "tenant_id=$1 AND parent_job_id IS NULL AND deployment_target_id IS NULL", 1},
		{JobConds{JobType: "x'; DROP"}, "tenant_id=$1 AND false", 1},
	} {
		where, args := c.in.where("tid")
		if where != c.want || len(args) != c.args {
			t.Fatalf("%+v: %q %d", c.in, where, len(args))
		}
		if strings.Contains(where, "DROP") {
			t.Fatal("filter value reached SQL text")
		}
	}
}
