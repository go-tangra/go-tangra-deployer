// Package deploy is the manual-deployment service: it validates the request and
// creates deployment jobs (a DIRECT job for one configuration, or a PARENT job
// plus one CHILD per configuration for a target group). The worker pool executes
// them asynchronously; callers get a job id (202) and learn the outcome over SSE.
package deploy

import (
	"context"
	"errors"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/repo"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/store"
)

// Errors.
var (
	ErrNotFound  = errors.New("deploy: not found")
	ErrNoConfigs = errors.New("deploy: target has no configurations")
	ErrInactive  = errors.New("deploy: configuration is not active")
)

// Service creates deployment jobs.
type Service struct {
	st repo.Store
	az *authz.Authorizer
}

// New builds the service.
func New(st repo.Store, az *authz.Authorizer) *Service { return &Service{st: st, az: az} }

// Deploy creates a DIRECT job deploying certID to one configuration.
func (s *Service) Deploy(ctx context.Context, subj authz.Subjects, certID, configID, trigger string) (string, error) {
	if err := s.az.Check(ctx, subj, authz.Configuration, configID, authz.Deploy); err != nil {
		return "", mapNF(err)
	}
	conf, err := s.st.GetConfiguration(ctx, subj.TenantID, configID)
	if err != nil {
		return "", mapNF(err)
	}
	if conf.Status != store.ConfigActive {
		return "", ErrInactive
	}
	id := store.NewID()
	j := store.DeploymentJob{
		ID: id, TenantID: subj.TenantID, TargetConfigurationID: &conf.ID, CertificateID: certID,
		Status: store.JobPending, MaxRetries: 3, TriggeredBy: def(trigger, store.TriggerManual),
	}
	if err := s.st.InsertJob(ctx, j); err != nil {
		return "", err
	}
	return id, nil
}

// DeployToTarget creates a PARENT job for the target plus one CHILD per attached
// configuration. Returns the parent job id.
func (s *Service) DeployToTarget(ctx context.Context, subj authz.Subjects, certID, targetID, trigger string) (string, error) {
	if err := s.az.Check(ctx, subj, authz.Target, targetID, authz.Deploy); err != nil {
		return "", mapNF(err)
	}
	if _, err := s.st.GetTarget(ctx, subj.TenantID, targetID); err != nil {
		return "", mapNF(err)
	}
	cfgIDs, err := s.st.ListTargetConfigurationIDs(ctx, subj.TenantID, targetID)
	if err != nil {
		return "", err
	}
	if len(cfgIDs) == 0 {
		return "", ErrNoConfigs
	}
	parentID := store.NewID()
	tgt := targetID
	err = s.st.Atomic(ctx, subj.TenantID, func(tx repo.Store) error {
		if e := tx.InsertJob(ctx, store.DeploymentJob{
			ID: parentID, TenantID: subj.TenantID, DeploymentTargetID: &tgt, CertificateID: certID,
			Status: store.JobPending, MaxRetries: 3, TriggeredBy: def(trigger, store.TriggerManual),
		}); e != nil {
			return e
		}
		for _, cid := range cfgIDs {
			c := cid
			pid := parentID
			if e := tx.InsertJob(ctx, store.DeploymentJob{
				ID: store.NewID(), TenantID: subj.TenantID, TargetConfigurationID: &c, ParentJobID: &pid,
				CertificateID: certID, Status: store.JobPending, MaxRetries: 3, TriggeredBy: def(trigger, store.TriggerManual),
			}); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return parentID, nil
}

// DeployToConfigurations creates a DIRECT job per configuration; returns the ids.
func (s *Service) DeployToConfigurations(ctx context.Context, subj authz.Subjects, certID string, configIDs []string, trigger string) ([]string, error) {
	out := make([]string, 0, len(configIDs))
	for _, cid := range configIDs {
		id, err := s.Deploy(ctx, subj, certID, cid, trigger)
		if err != nil {
			return out, err
		}
		out = append(out, id)
	}
	return out, nil
}

func def(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func mapNF(err error) error {
	if errors.Is(err, store.ErrNotFound) || errors.Is(err, authz.ErrNotFound) {
		return ErrNotFound
	}
	return err
}
