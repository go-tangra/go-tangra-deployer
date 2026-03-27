package service

import (
	"context"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"
	"google.golang.org/protobuf/types/known/timestamppb"

	entCrud "github.com/tx7do/go-crud/entgo"

	"github.com/go-tangra/go-tangra-common/backup"
	"github.com/go-tangra/go-tangra-common/grpcx"

	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenthistory"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymenttarget"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/targetconfiguration"
)

const (
	backupModule        = "deployer"
	backupSchemaVersion = 1
)

// Migrations registry — add entries here when schema changes.
var migrations = backup.NewMigrationRegistry(backupModule)

// Register migrations in init. Example for future use:
//
//	func init() {
//	    migrations.Register(1, func(entities map[string]json.RawMessage) error {
//	        return backup.MigrateAddField(entities, "deploymentTargets", "newField", "")
//	    })
//	}

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

// ExportBackup exports all deployer entities as a gzipped archive.
func (s *BackupService) ExportBackup(ctx context.Context, req *deployerV1.ExportBackupRequest) (*deployerV1.ExportBackupResponse, error) {
	tenantID := grpcx.GetTenantIDFromContext(ctx)
	full := false

	if grpcx.IsPlatformAdmin(ctx) && req.TenantId != nil && *req.TenantId == 0 {
		full = true
		tenantID = 0
	} else if req.TenantId != nil && *req.TenantId != 0 && grpcx.IsPlatformAdmin(ctx) {
		tenantID = *req.TenantId
	}

	client := s.entClient.Client()
	a := backup.NewArchive(backupModule, backupSchemaVersion, tenantID, full)

	// Export deployment targets
	dtQuery := client.DeploymentTarget.Query()
	if !full {
		dtQuery = dtQuery.Where(deploymenttarget.TenantID(tenantID))
	}
	deploymentTargets, err := dtQuery.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("export deployment targets: %w", err)
	}
	if err := backup.SetEntities(a, "deploymentTargets", deploymentTargets); err != nil {
		return nil, fmt.Errorf("set deployment targets: %w", err)
	}

	// Export target configurations
	tcQuery := client.TargetConfiguration.Query()
	if !full {
		tcQuery = tcQuery.Where(targetconfiguration.TenantID(tenantID))
	}
	targetConfigurations, err := tcQuery.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("export target configurations: %w", err)
	}
	if err := backup.SetEntities(a, "targetConfigurations", targetConfigurations); err != nil {
		return nil, fmt.Errorf("set target configurations: %w", err)
	}

	// Export deployment jobs
	djQuery := client.DeploymentJob.Query()
	if !full {
		djQuery = djQuery.Where(deploymentjob.TenantID(tenantID))
	}
	deploymentJobs, err := djQuery.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("export deployment jobs: %w", err)
	}
	if err := backup.SetEntities(a, "deploymentJobs", deploymentJobs); err != nil {
		return nil, fmt.Errorf("set deployment jobs: %w", err)
	}

	// Export deployment history (no TenantID — filter via parent DeploymentJob)
	dhQuery := client.DeploymentHistory.Query()
	if !full {
		dhQuery = dhQuery.Where(deploymenthistory.HasJobWith(deploymentjob.TenantID(tenantID)))
	}
	deploymentHistory, err := dhQuery.All(ctx)
	if err != nil {
		return nil, fmt.Errorf("export deployment history: %w", err)
	}
	if err := backup.SetEntities(a, "deploymentHistory", deploymentHistory); err != nil {
		return nil, fmt.Errorf("set deployment history: %w", err)
	}

	// Pack (JSON + gzip)
	data, err := backup.Pack(a)
	if err != nil {
		return nil, fmt.Errorf("pack backup: %w", err)
	}

	s.log.Infof("exported backup: module=%s tenant=%d full=%v entities=%v", backupModule, tenantID, full, a.Manifest.EntityCounts)

	return &deployerV1.ExportBackupResponse{
		Data:          data,
		Module:        backupModule,
		Version:       fmt.Sprintf("%d", backupSchemaVersion),
		ExportedAt:    timestamppb.New(a.Manifest.ExportedAt),
		TenantId:      tenantID,
		EntityCounts:  a.Manifest.EntityCounts,
		SchemaVersion: int32(backupSchemaVersion),
	}, nil
}

// ImportBackup restores deployer entities from a gzipped archive.
func (s *BackupService) ImportBackup(ctx context.Context, req *deployerV1.ImportBackupRequest) (*deployerV1.ImportBackupResponse, error) {
	tenantID := grpcx.GetTenantIDFromContext(ctx)
	isPlatformAdmin := grpcx.IsPlatformAdmin(ctx)
	mode := mapDeployerRestoreMode(req.GetMode())

	// Unpack
	a, err := backup.Unpack(req.GetData())
	if err != nil {
		return nil, fmt.Errorf("unpack backup: %w", err)
	}

	// Validate
	if err := backup.Validate(a, backupModule, backupSchemaVersion); err != nil {
		return nil, err
	}

	// Full backups require platform admin
	if a.Manifest.FullBackup && !isPlatformAdmin {
		return nil, fmt.Errorf("only platform admins can restore full backups")
	}

	// Run migrations if needed
	sourceVersion := a.Manifest.SchemaVersion
	applied, err := migrations.RunMigrations(a, backupSchemaVersion)
	if err != nil {
		return nil, fmt.Errorf("migration failed: %w", err)
	}

	// Determine restore tenant
	if !isPlatformAdmin || !a.Manifest.FullBackup {
		tenantID = grpcx.GetTenantIDFromContext(ctx)
	} else {
		tenantID = 0
	}

	client := s.entClient.Client()
	result := backup.NewRestoreResult(sourceVersion, backupSchemaVersion, applied)

	// Import in FK dependency order
	s.importDeploymentTargets(ctx, client, a, tenantID, a.Manifest.FullBackup, mode, result)
	s.importTargetConfigurations(ctx, client, a, tenantID, a.Manifest.FullBackup, mode, result)
	s.importDeploymentJobs(ctx, client, a, tenantID, a.Manifest.FullBackup, mode, result)
	s.importDeploymentHistory(ctx, client, a, a.Manifest.FullBackup, mode, result)

	s.log.Infof("imported backup: module=%s tenant=%d mode=%v migrations=%d results=%d",
		backupModule, tenantID, mode, applied, len(result.Results))

	// Convert to proto response
	protoResults := make([]*deployerV1.EntityImportResult, len(result.Results))
	for i, r := range result.Results {
		protoResults[i] = &deployerV1.EntityImportResult{
			EntityType: r.EntityType,
			Total:      r.Total,
			Created:    r.Created,
			Updated:    r.Updated,
			Skipped:    r.Skipped,
			Failed:     r.Failed,
		}
	}

	return &deployerV1.ImportBackupResponse{
		Success:           result.Success,
		Results:           protoResults,
		Warnings:          result.Warnings,
		SourceVersion:     int32(result.SourceVersion),
		TargetVersion:     int32(result.TargetVersion),
		MigrationsApplied: int32(result.MigrationsApplied),
	}, nil
}

func mapDeployerRestoreMode(m deployerV1.RestoreMode) backup.RestoreMode {
	if m == deployerV1.RestoreMode_RESTORE_MODE_OVERWRITE {
		return backup.RestoreModeOverwrite
	}
	return backup.RestoreModeSkip
}

// --- Import helpers ---

func (s *BackupService) importDeploymentTargets(ctx context.Context, client *ent.Client, a *backup.Archive, tenantID uint32, full bool, mode backup.RestoreMode, result *backup.RestoreResult) {
	targets, err := backup.GetEntities[ent.DeploymentTarget](a, "deploymentTargets")
	if err != nil {
		result.AddWarning(fmt.Sprintf("deploymentTargets: unmarshal error: %v", err))
		return
	}
	if len(targets) == 0 {
		return
	}

	er := backup.EntityResult{EntityType: "deploymentTargets", Total: int64(len(targets))}

	for _, e := range targets {
		tid := tenantID
		if full && e.TenantID != nil {
			tid = *e.TenantID
		}

		existing, getErr := client.DeploymentTarget.Get(ctx, e.ID)
		if getErr != nil && !ent.IsNotFound(getErr) {
			result.AddWarning(fmt.Sprintf("deploymentTargets: lookup %s: %v", e.ID, getErr))
			er.Failed++
			continue
		}
		if existing != nil {
			if mode == backup.RestoreModeSkip {
				er.Skipped++
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
				result.AddWarning(fmt.Sprintf("deploymentTargets: update %s: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Updated++
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
				result.AddWarning(fmt.Sprintf("deploymentTargets: create %s: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Created++
		}
	}

	result.AddResult(er)
}

func (s *BackupService) importTargetConfigurations(ctx context.Context, client *ent.Client, a *backup.Archive, tenantID uint32, full bool, mode backup.RestoreMode, result *backup.RestoreResult) {
	configs, err := backup.GetEntities[ent.TargetConfiguration](a, "targetConfigurations")
	if err != nil {
		result.AddWarning(fmt.Sprintf("targetConfigurations: unmarshal error: %v", err))
		return
	}
	if len(configs) == 0 {
		return
	}

	er := backup.EntityResult{EntityType: "targetConfigurations", Total: int64(len(configs))}

	for _, e := range configs {
		tid := tenantID
		if full && e.TenantID != nil {
			tid = *e.TenantID
		}

		existing, getErr := client.TargetConfiguration.Get(ctx, e.ID)
		if getErr != nil && !ent.IsNotFound(getErr) {
			result.AddWarning(fmt.Sprintf("targetConfigurations: lookup %s: %v", e.ID, getErr))
			er.Failed++
			continue
		}
		if existing != nil {
			if mode == backup.RestoreModeSkip {
				er.Skipped++
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
				result.AddWarning(fmt.Sprintf("targetConfigurations: update %s: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Updated++
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
				result.AddWarning(fmt.Sprintf("targetConfigurations: create %s: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Created++
		}
	}

	result.AddResult(er)
}

func (s *BackupService) importDeploymentJobs(ctx context.Context, client *ent.Client, a *backup.Archive, tenantID uint32, full bool, mode backup.RestoreMode, result *backup.RestoreResult) {
	jobs, err := backup.GetEntities[ent.DeploymentJob](a, "deploymentJobs")
	if err != nil {
		result.AddWarning(fmt.Sprintf("deploymentJobs: unmarshal error: %v", err))
		return
	}
	if len(jobs) == 0 {
		return
	}

	er := backup.EntityResult{EntityType: "deploymentJobs", Total: int64(len(jobs))}

	for _, e := range jobs {
		tid := tenantID
		if full && e.TenantID != nil {
			tid = *e.TenantID
		}

		existing, getErr := client.DeploymentJob.Get(ctx, e.ID)
		if getErr != nil && !ent.IsNotFound(getErr) {
			result.AddWarning(fmt.Sprintf("deploymentJobs: lookup %s: %v", e.ID, getErr))
			er.Failed++
			continue
		}
		if existing != nil {
			if mode == backup.RestoreModeSkip {
				er.Skipped++
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
				result.AddWarning(fmt.Sprintf("deploymentJobs: update %s: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Updated++
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
				result.AddWarning(fmt.Sprintf("deploymentJobs: create %s: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Created++
		}
	}

	result.AddResult(er)
}

func (s *BackupService) importDeploymentHistory(ctx context.Context, client *ent.Client, a *backup.Archive, full bool, mode backup.RestoreMode, result *backup.RestoreResult) {
	history, err := backup.GetEntities[ent.DeploymentHistory](a, "deploymentHistory")
	if err != nil {
		result.AddWarning(fmt.Sprintf("deploymentHistory: unmarshal error: %v", err))
		return
	}
	if len(history) == 0 {
		return
	}

	er := backup.EntityResult{EntityType: "deploymentHistory", Total: int64(len(history))}

	for _, e := range history {
		existing, getErr := client.DeploymentHistory.Get(ctx, e.ID)
		if getErr != nil && !ent.IsNotFound(getErr) {
			result.AddWarning(fmt.Sprintf("deploymentHistory: lookup %d: %v", e.ID, getErr))
			er.Failed++
			continue
		}
		if existing != nil {
			if mode == backup.RestoreModeSkip {
				er.Skipped++
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
				result.AddWarning(fmt.Sprintf("deploymentHistory: update %d: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Updated++
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
				result.AddWarning(fmt.Sprintf("deploymentHistory: create %d: %v", e.ID, err))
				er.Failed++
				continue
			}
			er.Created++
		}
	}

	result.AddResult(er)
}
