package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/timestamppb"

	entCrud "github.com/tx7do/go-crud/entgo"

	"github.com/go-tangra/go-tangra-common/grpcx"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenthistory"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenttarget"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/targetconfiguration"
)

const (
	backupModule  = "deployer"
	backupVersion = "1.0"
)

type BackupService struct {
	deployerV1.UnimplementedBackupServiceServer

	log       *log.Helper
	entClient *entCrud.EntClient[*ent.Client]
}

func NewBackupService(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *BackupService {
	return &BackupService{
		log:       ctx.NewLoggerHelper("deployer/service/backup"),
		entClient: entClient,
	}
}

type backupData struct {
	Module     string         `json:"module"`
	Version    string         `json:"version"`
	ExportedAt time.Time     `json:"exportedAt"`
	TenantID   uint32        `json:"tenantId"`
	FullBackup bool          `json:"fullBackup"`
	Data       backupEntities `json:"data"`
}

type backupEntities struct {
	DeploymentTargets    []json.RawMessage `json:"deploymentTargets,omitempty"`
	TargetConfigurations []json.RawMessage `json:"targetConfigurations,omitempty"`
	DeploymentJobs       []json.RawMessage `json:"deploymentJobs,omitempty"`
	DeploymentHistory    []json.RawMessage `json:"deploymentHistory,omitempty"`
}

func marshalEntities[T any](entities []*T) ([]json.RawMessage, error) {
	result := make([]json.RawMessage, 0, len(entities))
	for _, e := range entities {
		b, err := json.Marshal(e)
		if err != nil {
			return nil, err
		}
		result = append(result, b)
	}
	return result, nil
}

func (s *BackupService) ExportBackup(ctx context.Context, req *deployerV1.ExportBackupRequest) (*deployerV1.ExportBackupResponse, error) {
	tenantID := grpcx.GetTenantIDFromContext(ctx)
	full := false

	if grpcx.IsPlatformAdmin(ctx) && req.TenantId != nil && *req.TenantId == 0 {
		full = true
		tenantID = 0
	} else if req.TenantId != nil && *req.TenantId != 0 {
		if grpcx.IsPlatformAdmin(ctx) {
			tenantID = *req.TenantId
		}
	}

	client := s.entClient.Client()
	now := time.Now()

	deploymentTargets, err := s.exportDeploymentTargets(ctx, client, tenantID, full)
	if err != nil {
		return nil, fmt.Errorf("export deployment targets: %w", err)
	}
	targetConfigurations, err := s.exportTargetConfigurations(ctx, client, tenantID, full)
	if err != nil {
		return nil, fmt.Errorf("export target configurations: %w", err)
	}
	deploymentJobs, err := s.exportDeploymentJobs(ctx, client, tenantID, full)
	if err != nil {
		return nil, fmt.Errorf("export deployment jobs: %w", err)
	}
	deploymentHistory, err := s.exportDeploymentHistory(ctx, client, tenantID, full)
	if err != nil {
		return nil, fmt.Errorf("export deployment history: %w", err)
	}

	backup := backupData{
		Module:     backupModule,
		Version:    backupVersion,
		ExportedAt: now,
		TenantID:   tenantID,
		FullBackup: full,
		Data: backupEntities{
			DeploymentTargets:    deploymentTargets,
			TargetConfigurations: targetConfigurations,
			DeploymentJobs:       deploymentJobs,
			DeploymentHistory:    deploymentHistory,
		},
	}

	data, err := json.Marshal(backup)
	if err != nil {
		return nil, fmt.Errorf("marshal backup: %w", err)
	}

	entityCounts := map[string]int64{
		"deploymentTargets":    int64(len(deploymentTargets)),
		"targetConfigurations": int64(len(targetConfigurations)),
		"deploymentJobs":       int64(len(deploymentJobs)),
		"deploymentHistory":    int64(len(deploymentHistory)),
	}

	s.log.Infof("exported backup: module=%s tenant=%d full=%v entities=%v", backupModule, tenantID, full, entityCounts)

	return &deployerV1.ExportBackupResponse{
		Data:         data,
		Module:       backupModule,
		Version:      backupVersion,
		ExportedAt:   timestamppb.New(now),
		TenantId:     tenantID,
		EntityCounts: entityCounts,
	}, nil
}

func (s *BackupService) ImportBackup(ctx context.Context, req *deployerV1.ImportBackupRequest) (*deployerV1.ImportBackupResponse, error) {
	tenantID := grpcx.GetTenantIDFromContext(ctx)
	isPlatformAdmin := grpcx.IsPlatformAdmin(ctx)
	mode := req.GetMode()

	var backup backupData
	if err := json.Unmarshal(req.GetData(), &backup); err != nil {
		return nil, fmt.Errorf("invalid backup data: %w", err)
	}

	if backup.Module != backupModule {
		return nil, fmt.Errorf("backup module mismatch: expected %s, got %s", backupModule, backup.Module)
	}
	if backup.Version != backupVersion {
		return nil, fmt.Errorf("backup version mismatch: expected %s, got %s", backupVersion, backup.Version)
	}

	// For full backups, only platform admins can restore
	if backup.FullBackup && !isPlatformAdmin {
		return nil, fmt.Errorf("only platform admins can restore full backups")
	}

	// Non-platform admins always restore to their own tenant
	if !isPlatformAdmin || !backup.FullBackup {
		tenantID = grpcx.GetTenantIDFromContext(ctx)
	} else {
		tenantID = 0 // Signal for full backup restore — each entity carries its own tenant_id
	}

	client := s.entClient.Client()
	var results []*deployerV1.EntityImportResult
	var warnings []string

	// Import in FK dependency order
	importFuncs := []struct {
		name string
		fn   func(ctx context.Context, client *ent.Client, items []json.RawMessage, tenantID uint32, full bool, mode deployerV1.RestoreMode) (*deployerV1.EntityImportResult, []string)
	}{
		{"deploymentTargets", s.importDeploymentTargets},
		{"targetConfigurations", s.importTargetConfigurations},
		{"deploymentJobs", s.importDeploymentJobs},
		{"deploymentHistory", s.importDeploymentHistory},
	}

	dataMap := map[string][]json.RawMessage{
		"deploymentTargets":    backup.Data.DeploymentTargets,
		"targetConfigurations": backup.Data.TargetConfigurations,
		"deploymentJobs":       backup.Data.DeploymentJobs,
		"deploymentHistory":    backup.Data.DeploymentHistory,
	}

	for _, imp := range importFuncs {
		items := dataMap[imp.name]
		if len(items) == 0 {
			continue
		}
		result, w := imp.fn(ctx, client, items, tenantID, backup.FullBackup, mode)
		if result != nil {
			results = append(results, result)
		}
		warnings = append(warnings, w...)
	}

	s.log.Infof("imported backup: module=%s tenant=%d mode=%v results=%d warnings=%d", backupModule, tenantID, mode, len(results), len(warnings))

	return &deployerV1.ImportBackupResponse{
		Success:  true,
		Results:  results,
		Warnings: warnings,
	}, nil
}

// --- Export helpers ---

func (s *BackupService) exportDeploymentTargets(ctx context.Context, client *ent.Client, tenantID uint32, full bool) ([]json.RawMessage, error) {
	query := client.DeploymentTarget.Query()
	if !full {
		query = query.Where(deploymenttarget.TenantID(tenantID))
	}
	entities, err := query.All(ctx)
	if err != nil {
		return nil, err
	}
	return marshalEntities(entities)
}

func (s *BackupService) exportTargetConfigurations(ctx context.Context, client *ent.Client, tenantID uint32, full bool) ([]json.RawMessage, error) {
	query := client.TargetConfiguration.Query()
	if !full {
		query = query.Where(targetconfiguration.TenantID(tenantID))
	}
	entities, err := query.All(ctx)
	if err != nil {
		return nil, err
	}
	return marshalEntities(entities)
}

func (s *BackupService) exportDeploymentJobs(ctx context.Context, client *ent.Client, tenantID uint32, full bool) ([]json.RawMessage, error) {
	query := client.DeploymentJob.Query()
	if !full {
		query = query.Where(deploymentjob.TenantID(tenantID))
	}
	entities, err := query.All(ctx)
	if err != nil {
		return nil, err
	}
	return marshalEntities(entities)
}

func (s *BackupService) exportDeploymentHistory(ctx context.Context, client *ent.Client, tenantID uint32, full bool) ([]json.RawMessage, error) {
	query := client.DeploymentHistory.Query()
	if !full {
		// DeploymentHistory has no TenantID — filter via parent DeploymentJob's TenantID
		query = query.Where(deploymenthistory.HasJobWith(deploymentjob.TenantID(tenantID)))
	}
	entities, err := query.All(ctx)
	if err != nil {
		return nil, err
	}
	return marshalEntities(entities)
}

// --- Import helpers ---

func (s *BackupService) importDeploymentTargets(ctx context.Context, client *ent.Client, items []json.RawMessage, tenantID uint32, full bool, mode deployerV1.RestoreMode) (*deployerV1.EntityImportResult, []string) {
	result := &deployerV1.EntityImportResult{EntityType: "deploymentTargets", Total: int64(len(items))}
	var warnings []string

	for _, raw := range items {
		var e ent.DeploymentTarget
		if err := json.Unmarshal(raw, &e); err != nil {
			warnings = append(warnings, fmt.Sprintf("deploymentTargets: unmarshal error: %v", err))
			result.Failed++
			continue
		}

		tid := tenantID
		if full && e.TenantID != nil {
			tid = *e.TenantID
		}

		existing, _ := client.DeploymentTarget.Get(ctx, e.ID)
		if existing != nil {
			if mode == deployerV1.RestoreMode_RESTORE_MODE_SKIP {
				result.Skipped++
				continue
			}
			_, err := client.DeploymentTarget.UpdateOneID(e.ID).
				SetName(e.Name).
				SetDescription(e.Description).
				SetAutoDeployOnRenewal(e.AutoDeployOnRenewal).
				SetCertificateFilters(e.CertificateFilters).
				SetNillableCreateBy(e.CreateBy).
				SetNillableUpdateBy(e.UpdateBy).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("deploymentTargets: update %s: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Updated++
		} else {
			_, err := client.DeploymentTarget.Create().
				SetID(e.ID).
				SetNillableTenantID(&tid).
				SetName(e.Name).
				SetDescription(e.Description).
				SetAutoDeployOnRenewal(e.AutoDeployOnRenewal).
				SetCertificateFilters(e.CertificateFilters).
				SetNillableCreateBy(e.CreateBy).
				SetNillableUpdateBy(e.UpdateBy).
				SetNillableCreateTime(e.CreateTime).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("deploymentTargets: create %s: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Created++
		}
	}

	return result, warnings
}

func (s *BackupService) importTargetConfigurations(ctx context.Context, client *ent.Client, items []json.RawMessage, tenantID uint32, full bool, mode deployerV1.RestoreMode) (*deployerV1.EntityImportResult, []string) {
	result := &deployerV1.EntityImportResult{EntityType: "targetConfigurations", Total: int64(len(items))}
	var warnings []string

	for _, raw := range items {
		var e ent.TargetConfiguration
		if err := json.Unmarshal(raw, &e); err != nil {
			warnings = append(warnings, fmt.Sprintf("targetConfigurations: unmarshal error: %v", err))
			result.Failed++
			continue
		}

		tid := tenantID
		if full && e.TenantID != nil {
			tid = *e.TenantID
		}

		existing, _ := client.TargetConfiguration.Get(ctx, e.ID)
		if existing != nil {
			if mode == deployerV1.RestoreMode_RESTORE_MODE_SKIP {
				result.Skipped++
				continue
			}
			_, err := client.TargetConfiguration.UpdateOneID(e.ID).
				SetName(e.Name).
				SetDescription(e.Description).
				SetProviderType(e.ProviderType).
				SetCredentialsEncrypted(e.CredentialsEncrypted).
				SetConfig(e.Config).
				SetStatus(e.Status).
				SetStatusMessage(e.StatusMessage).
				SetNillableLastDeploymentAt(e.LastDeploymentAt).
				SetNillableCreateBy(e.CreateBy).
				SetNillableUpdateBy(e.UpdateBy).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("targetConfigurations: update %s: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Updated++
		} else {
			_, err := client.TargetConfiguration.Create().
				SetID(e.ID).
				SetNillableTenantID(&tid).
				SetName(e.Name).
				SetDescription(e.Description).
				SetProviderType(e.ProviderType).
				SetCredentialsEncrypted(e.CredentialsEncrypted).
				SetConfig(e.Config).
				SetStatus(e.Status).
				SetStatusMessage(e.StatusMessage).
				SetNillableLastDeploymentAt(e.LastDeploymentAt).
				SetNillableCreateBy(e.CreateBy).
				SetNillableUpdateBy(e.UpdateBy).
				SetNillableCreateTime(e.CreateTime).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("targetConfigurations: create %s: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Created++
		}
	}

	return result, warnings
}

func (s *BackupService) importDeploymentJobs(ctx context.Context, client *ent.Client, items []json.RawMessage, tenantID uint32, full bool, mode deployerV1.RestoreMode) (*deployerV1.EntityImportResult, []string) {
	result := &deployerV1.EntityImportResult{EntityType: "deploymentJobs", Total: int64(len(items))}
	var warnings []string

	for _, raw := range items {
		var e ent.DeploymentJob
		if err := json.Unmarshal(raw, &e); err != nil {
			warnings = append(warnings, fmt.Sprintf("deploymentJobs: unmarshal error: %v", err))
			result.Failed++
			continue
		}

		tid := tenantID
		if full && e.TenantID != nil {
			tid = *e.TenantID
		}

		existing, _ := client.DeploymentJob.Get(ctx, e.ID)
		if existing != nil {
			if mode == deployerV1.RestoreMode_RESTORE_MODE_SKIP {
				result.Skipped++
				continue
			}
			_, err := client.DeploymentJob.UpdateOneID(e.ID).
				SetNillableDeploymentTargetID(e.DeploymentTargetID).
				SetNillableTargetConfigurationID(e.TargetConfigurationID).
				SetNillableParentJobID(e.ParentJobID).
				SetCertificateID(e.CertificateID).
				SetCertificateSerial(e.CertificateSerial).
				SetStatus(e.Status).
				SetStatusMessage(e.StatusMessage).
				SetProgress(e.Progress).
				SetRetryCount(e.RetryCount).
				SetMaxRetries(e.MaxRetries).
				SetTriggeredBy(e.TriggeredBy).
				SetResult(e.Result).
				SetNillableStartedAt(e.StartedAt).
				SetNillableCreateBy(e.CreateBy).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("deploymentJobs: update %s: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Updated++
		} else {
			_, err := client.DeploymentJob.Create().
				SetID(e.ID).
				SetNillableTenantID(&tid).
				SetNillableDeploymentTargetID(e.DeploymentTargetID).
				SetNillableTargetConfigurationID(e.TargetConfigurationID).
				SetNillableParentJobID(e.ParentJobID).
				SetCertificateID(e.CertificateID).
				SetCertificateSerial(e.CertificateSerial).
				SetStatus(e.Status).
				SetStatusMessage(e.StatusMessage).
				SetProgress(e.Progress).
				SetRetryCount(e.RetryCount).
				SetMaxRetries(e.MaxRetries).
				SetTriggeredBy(e.TriggeredBy).
				SetResult(e.Result).
				SetNillableStartedAt(e.StartedAt).
				SetNillableCreateBy(e.CreateBy).
				SetNillableCreateTime(e.CreateTime).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("deploymentJobs: create %s: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Created++
		}
	}

	return result, warnings
}

func (s *BackupService) importDeploymentHistory(ctx context.Context, client *ent.Client, items []json.RawMessage, tenantID uint32, full bool, mode deployerV1.RestoreMode) (*deployerV1.EntityImportResult, []string) {
	result := &deployerV1.EntityImportResult{EntityType: "deploymentHistory", Total: int64(len(items))}
	var warnings []string

	for _, raw := range items {
		var e ent.DeploymentHistory
		if err := json.Unmarshal(raw, &e); err != nil {
			warnings = append(warnings, fmt.Sprintf("deploymentHistory: unmarshal error: %v", err))
			result.Failed++
			continue
		}

		existing, _ := client.DeploymentHistory.Get(ctx, e.ID)
		if existing != nil {
			if mode == deployerV1.RestoreMode_RESTORE_MODE_SKIP {
				result.Skipped++
				continue
			}
			_, err := client.DeploymentHistory.UpdateOneID(e.ID).
				SetJobID(e.JobID).
				SetAction(e.Action).
				SetResult(e.Result).
				SetMessage(e.Message).
				SetDurationMs(e.DurationMs).
				SetDetails(e.Details).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("deploymentHistory: update %d: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Updated++
		} else {
			_, err := client.DeploymentHistory.Create().
				SetID(e.ID).
				SetJobID(e.JobID).
				SetAction(e.Action).
				SetResult(e.Result).
				SetMessage(e.Message).
				SetDurationMs(e.DurationMs).
				SetDetails(e.Details).
				SetNillableCreateTime(e.CreateTime).
				Save(ctx)
			if err != nil {
				warnings = append(warnings, fmt.Sprintf("deploymentHistory: create %d: %v", e.ID, err))
				result.Failed++
				continue
			}
			result.Created++
		}
	}

	return result, warnings
}
