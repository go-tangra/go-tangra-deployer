package deploy_test

import (
	"context"
	"errors"
	"testing"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/deploy"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/memstore"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/sealed"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"

	// Register the dummy provider so real configurations can be created.
	_ "github.com/go-tangra/go-tangra-deployer/v4/internal/providers/dummy"
)

const certID = "cert-123"

func adminSubject() authz.Subjects {
	return authz.Subjects{
		TenantID:  "11111111-1111-1111-1111-111111111111",
		UserID:    "u1",
		Roles:     []string{"admin"},
		ActorKind: "user",
	}
}

func newServices(t *testing.T) (*deploy.Service, *configs.Service, *targets.Service, *memstore.Mem) {
	t.Helper()
	m := memstore.New()
	env, err := sealed.NewEnvelope(make([]byte, 32))
	if err != nil {
		t.Fatalf("sealed.NewEnvelope: %v", err)
	}
	az := authz.New(m)
	return deploy.New(m, az), configs.New(m, env, az), targets.New(m, az), m
}

func makeConfig(t *testing.T, cs *configs.Service, subj authz.Subjects, name string) string {
	t.Helper()
	v, err := cs.Create(context.Background(), subj, configs.Input{
		Name:         name,
		ProviderType: "dummy",
		Config:       map[string]any{"region": "eu"},
		Credentials:  map[string]any{"token": "secret-abc"},
	})
	if err != nil {
		t.Fatalf("configs.Create(%s): %v", name, err)
	}
	return v.ID
}

func listJobs(t *testing.T, m *memstore.Mem, subj authz.Subjects, f repo.JobFilter) []store.DeploymentJob {
	t.Helper()
	jobs, err := m.ListJobs(context.Background(), subj.TenantID, f)
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	return jobs
}

// TestDeployDirect covers the DIRECT job happy path plus the inactive and
// unknown-configuration error cases.
func TestDeployDirect(t *testing.T) {
	ctx := context.Background()
	ds, cs, _, m := newServices(t)
	subj := adminSubject()

	t.Run("success creates a direct job", func(t *testing.T) {
		cid := makeConfig(t, cs, subj, "direct-cfg")
		id, err := ds.Deploy(ctx, subj, certID, cid, "manual")
		if err != nil {
			t.Fatalf("Deploy: %v", err)
		}
		if id == "" {
			t.Fatal("Deploy: expected a non-empty job id")
		}
		job, err := m.GetJob(ctx, subj.TenantID, id)
		if err != nil {
			t.Fatalf("GetJob: %v", err)
		}
		if job.JobType() != store.JobTypeDirect {
			t.Errorf("JobType = %q, want %q", job.JobType(), store.JobTypeDirect)
		}
		if job.CertificateID != certID {
			t.Errorf("CertificateID = %q, want %q", job.CertificateID, certID)
		}
		if job.TargetConfigurationID == nil || *job.TargetConfigurationID != cid {
			t.Errorf("TargetConfigurationID = %v, want %q", job.TargetConfigurationID, cid)
		}
	})

	t.Run("inactive configuration is rejected", func(t *testing.T) {
		cid := makeConfig(t, cs, subj, "inactive-cfg")
		if err := m.SetConfigurationStatus(ctx, subj.TenantID, cid, store.ConfigInactive, "", nil); err != nil {
			t.Fatalf("SetConfigurationStatus: %v", err)
		}
		if _, err := ds.Deploy(ctx, subj, certID, cid, "manual"); !errors.Is(err, deploy.ErrInactive) {
			t.Errorf("Deploy(inactive): err = %v, want ErrInactive", err)
		}
	})

	t.Run("unknown configuration is not found", func(t *testing.T) {
		_, err := ds.Deploy(ctx, subj, certID, "00000000-0000-0000-0000-000000000000", "manual")
		if !errors.Is(err, deploy.ErrNotFound) {
			t.Errorf("Deploy(unknown): err = %v, want ErrNotFound", err)
		}
	})
}

// TestDeployToTarget covers the PARENT + one CHILD per attached configuration,
// plus the no-configs and unknown-target error cases.
func TestDeployToTarget(t *testing.T) {
	ctx := context.Background()
	ds, cs, ts, m := newServices(t)
	subj := adminSubject()

	t.Run("success creates a parent and one child per config", func(t *testing.T) {
		tgt, err := ts.Create(ctx, subj, targets.Input{Name: "fleet"})
		if err != nil {
			t.Fatalf("targets.Create: %v", err)
		}
		c1 := makeConfig(t, cs, subj, "fleet-cfg-1")
		c2 := makeConfig(t, cs, subj, "fleet-cfg-2")
		if err := ts.Attach(ctx, subj, tgt.ID, []string{c1, c2}, nil); err != nil {
			t.Fatalf("Attach: %v", err)
		}

		parentID, err := ds.DeployToTarget(ctx, subj, certID, tgt.ID, "manual")
		if err != nil {
			t.Fatalf("DeployToTarget: %v", err)
		}

		parent, err := m.GetJob(ctx, subj.TenantID, parentID)
		if err != nil {
			t.Fatalf("GetJob(parent): %v", err)
		}
		if parent.JobType() != store.JobTypeParent {
			t.Errorf("parent JobType = %q, want %q", parent.JobType(), store.JobTypeParent)
		}
		if parent.DeploymentTargetID == nil || *parent.DeploymentTargetID != tgt.ID {
			t.Errorf("parent DeploymentTargetID = %v, want %q", parent.DeploymentTargetID, tgt.ID)
		}

		children := listJobs(t, m, subj, repo.JobFilter{ParentJobID: parentID})
		if len(children) != 2 {
			t.Fatalf("children = %d, want 2", len(children))
		}
		for _, ch := range children {
			if ch.JobType() != store.JobTypeChild {
				t.Errorf("child JobType = %q, want %q", ch.JobType(), store.JobTypeChild)
			}
			if ch.ParentJobID == nil || *ch.ParentJobID != parentID {
				t.Errorf("child ParentJobID = %v, want %q", ch.ParentJobID, parentID)
			}
		}
	})

	t.Run("target with no configurations is rejected", func(t *testing.T) {
		tgt, err := ts.Create(ctx, subj, targets.Input{Name: "empty-fleet"})
		if err != nil {
			t.Fatalf("targets.Create: %v", err)
		}
		if _, err := ds.DeployToTarget(ctx, subj, certID, tgt.ID, "manual"); !errors.Is(err, deploy.ErrNoConfigs) {
			t.Errorf("DeployToTarget(no configs): err = %v, want ErrNoConfigs", err)
		}
	})

	t.Run("unknown target is not found", func(t *testing.T) {
		_, err := ds.DeployToTarget(ctx, subj, certID, "00000000-0000-0000-0000-000000000000", "manual")
		if !errors.Is(err, deploy.ErrNotFound) {
			t.Errorf("DeployToTarget(unknown): err = %v, want ErrNotFound", err)
		}
	})
}

// TestDeployToConfigurations covers the multi-config fan-out and the
// stop-on-first-error behavior.
func TestDeployToConfigurations(t *testing.T) {
	ctx := context.Background()
	ds, cs, _, m := newServices(t)
	subj := adminSubject()

	t.Run("many configs yield one direct job each", func(t *testing.T) {
		c1 := makeConfig(t, cs, subj, "multi-cfg-1")
		c2 := makeConfig(t, cs, subj, "multi-cfg-2")
		c3 := makeConfig(t, cs, subj, "multi-cfg-3")

		ids, err := ds.DeployToConfigurations(ctx, subj, certID, []string{c1, c2, c3}, "manual")
		if err != nil {
			t.Fatalf("DeployToConfigurations: %v", err)
		}
		if len(ids) != 3 {
			t.Fatalf("ids = %d, want 3", len(ids))
		}
		for _, id := range ids {
			job, err := m.GetJob(ctx, subj.TenantID, id)
			if err != nil {
				t.Fatalf("GetJob(%s): %v", id, err)
			}
			if job.JobType() != store.JobTypeDirect {
				t.Errorf("JobType = %q, want %q", job.JobType(), store.JobTypeDirect)
			}
		}
	})

	t.Run("stops and returns partial ids on first error", func(t *testing.T) {
		good := makeConfig(t, cs, subj, "partial-cfg")
		bad := "00000000-0000-0000-0000-000000000000"

		ids, err := ds.DeployToConfigurations(ctx, subj, certID, []string{good, bad, good}, "manual")
		if err == nil {
			t.Fatal("DeployToConfigurations(with unknown): expected an error, got nil")
		}
		if len(ids) != 1 {
			t.Errorf("ids = %v, want exactly the one succeeding job before the error", ids)
		}
	})
}
