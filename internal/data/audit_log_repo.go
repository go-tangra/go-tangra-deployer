package data

import (
	"context"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	entCrud "github.com/tx7do/go-crud/entgo"

	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/auditlog"

	"github.com/go-tangra/go-tangra-common/middleware/audit"
	deployerV1 "github.com/go-tangra/go-tangra-deployer/gen/go/deployer/service/v1"
)

// AuditLogRepo implements audit.AuditLogRepository for deployer
type AuditLogRepo struct {
	entClient *entCrud.EntClient[*ent.Client]
	log       *log.Helper
}

// NewAuditLogRepo creates a new AuditLogRepo
func NewAuditLogRepo(ctx *bootstrap.Context, entClient *entCrud.EntClient[*ent.Client]) *AuditLogRepo {
	return &AuditLogRepo{
		log:       ctx.NewLoggerHelper("deployer/audit_log_repo"),
		entClient: entClient,
	}
}

// CreateFromEntry implements audit.AuditLogRepository
func (r *AuditLogRepo) CreateFromEntry(ctx context.Context, entry *audit.AuditLogEntry) error {
	builder := r.entClient.Client().AuditLog.Create().
		SetAuditID(entry.AuditID).
		SetOperation(entry.Operation).
		SetServiceName(entry.ServiceName).
		SetSuccess(entry.Success).
		SetIsAuthenticated(entry.IsAuthenticated).
		SetLatencyMs(entry.LatencyMs).
		SetCreateTime(entry.Timestamp)

	if entry.TenantID > 0 {
		builder.SetTenantID(entry.TenantID)
	}
	if entry.RequestID != "" {
		builder.SetRequestID(entry.RequestID)
	}
	if entry.ClientID != "" {
		builder.SetClientID(entry.ClientID)
	}
	if entry.ClientCommonName != "" {
		builder.SetClientCommonName(entry.ClientCommonName)
	}
	if entry.ClientOrganization != "" {
		builder.SetClientOrganization(entry.ClientOrganization)
	}
	if entry.ClientSerialNumber != "" {
		builder.SetClientSerialNumber(entry.ClientSerialNumber)
	}
	if entry.ErrorCode != 0 {
		builder.SetErrorCode(entry.ErrorCode)
	}
	if entry.ErrorMessage != "" {
		builder.SetErrorMessage(entry.ErrorMessage)
	}
	if entry.PeerAddress != "" {
		builder.SetPeerAddress(entry.PeerAddress)
	}
	if entry.GeoLocation != nil {
		builder.SetGeoLocation(entry.GeoLocation)
	}
	if entry.LogHash != "" {
		builder.SetLogHash(entry.LogHash)
	}
	if entry.Signature != nil {
		builder.SetSignature(entry.Signature)
	}
	if entry.Metadata != nil {
		builder.SetMetadata(entry.Metadata)
	}

	_, err := builder.Save(ctx)
	if err != nil {
		r.log.Errorf("create audit log failed: %s", err.Error())
		return err
	}

	return nil
}

// GetByAuditID retrieves an audit log by its audit ID
func (r *AuditLogRepo) GetByAuditID(ctx context.Context, auditID string) (*ent.AuditLog, error) {
	entity, err := r.entClient.Client().AuditLog.Query().
		Where(auditlog.AuditIDEQ(auditID)).
		Only(ctx)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get audit log failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get audit log failed")
	}
	return entity, nil
}

// GetByID retrieves an audit log by ID
func (r *AuditLogRepo) GetByID(ctx context.Context, id uint32) (*ent.AuditLog, error) {
	entity, err := r.entClient.Client().AuditLog.Get(ctx, id)
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		r.log.Errorf("get audit log failed: %s", err.Error())
		return nil, deployerV1.ErrorInternalServerError("get audit log failed")
	}
	return entity, nil
}

// AuditLogListOptions contains options for listing audit logs
type AuditLogListOptions struct {
	TenantID    *uint32
	ClientID    *string
	Operation   *string
	Success     *bool
	PeerAddress *string
	StartTime   *time.Time
	EndTime     *time.Time
	Limit       int
	Offset      int
}

// List retrieves audit logs with filtering options
func (r *AuditLogRepo) List(ctx context.Context, opts *AuditLogListOptions) ([]*ent.AuditLog, int, error) {
	query := r.entClient.Client().AuditLog.Query()

	// Apply filters
	if opts != nil {
		if opts.TenantID != nil {
			query = query.Where(auditlog.TenantIDEQ(*opts.TenantID))
		}
		if opts.ClientID != nil {
			query = query.Where(auditlog.ClientIDEQ(*opts.ClientID))
		}
		if opts.Operation != nil {
			query = query.Where(auditlog.OperationContains(*opts.Operation))
		}
		if opts.Success != nil {
			query = query.Where(auditlog.SuccessEQ(*opts.Success))
		}
		if opts.PeerAddress != nil {
			query = query.Where(auditlog.PeerAddressEQ(*opts.PeerAddress))
		}
		if opts.StartTime != nil {
			query = query.Where(auditlog.CreateTimeGTE(*opts.StartTime))
		}
		if opts.EndTime != nil {
			query = query.Where(auditlog.CreateTimeLTE(*opts.EndTime))
		}
	}

	// Get total count
	total, err := query.Clone().Count(ctx)
	if err != nil {
		r.log.Errorf("count audit logs failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("count audit logs failed")
	}

	// Apply pagination
	query = query.Order(ent.Desc(auditlog.FieldCreateTime))
	if opts != nil {
		if opts.Limit > 0 {
			query = query.Limit(opts.Limit)
		}
		if opts.Offset > 0 {
			query = query.Offset(opts.Offset)
		}
	}

	entities, err := query.All(ctx)
	if err != nil {
		r.log.Errorf("list audit logs failed: %s", err.Error())
		return nil, 0, deployerV1.ErrorInternalServerError("list audit logs failed")
	}

	return entities, total, nil
}

// DeleteOlderThan deletes audit logs older than the specified time
func (r *AuditLogRepo) DeleteOlderThan(ctx context.Context, before time.Time) (int, error) {
	deleted, err := r.entClient.Client().AuditLog.Delete().
		Where(auditlog.CreateTimeLT(before)).
		Exec(ctx)
	if err != nil {
		r.log.Errorf("delete old audit logs failed: %s", err.Error())
		return 0, deployerV1.ErrorInternalServerError("delete old audit logs failed")
	}
	return deleted, nil
}
