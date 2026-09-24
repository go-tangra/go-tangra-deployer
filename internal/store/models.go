// Package store holds the deployer's persistence models and TimescaleDB repos.
// This file defines the entity structs (pure Go types, no DB dependency) used
// across the service; repos.go binds them to SQL under per-tenant RLS.
package store

import "time"

// Status / enum constants.
const (
	ConfigActive   = "active"
	ConfigInactive = "inactive"
	ConfigError    = "error"

	JobPending    = "pending"
	JobProcessing = "processing"
	JobCompleted  = "completed"
	JobFailed     = "failed"
	JobCancelled  = "cancelled"
	JobRetrying   = "retrying"
	JobPartial    = "partial"

	TriggerManual      = "manual"
	TriggerEvent       = "event"
	TriggerAutoRenewal = "auto_renewal"

	ActionDeploy   = "deploy"
	ActionVerify   = "verify"
	ActionRollback = "rollback"

	ResultSuccess = "success"
	ResultFailure = "failure"
	ResultPartial = "partial"
)

// CertificateFilter is an AND-matched auto-deploy rule. An empty filter (all
// fields blank) matches every certificate.
type CertificateFilter struct {
	IssuerName          string `json:"issuer_name,omitempty"`
	CommonNamePattern   string `json:"common_name_pattern,omitempty"`
	SANPattern          string `json:"san_pattern,omitempty"`
	SubjectOrganization string `json:"subject_organization,omitempty"`
	SubjectOrgUnit      string `json:"subject_org_unit,omitempty"`
	SubjectCountry      string `json:"subject_country,omitempty"`
}

// Empty reports whether the filter has no criteria (matches all).
func (f CertificateFilter) Empty() bool {
	return f.IssuerName == "" && f.CommonNamePattern == "" && f.SANPattern == "" &&
		f.SubjectOrganization == "" && f.SubjectOrgUnit == "" && f.SubjectCountry == ""
}

// DeploymentTarget is a tenant-scoped group of endpoints + auto-deploy rules.
type DeploymentTarget struct {
	ID                 string
	TenantID           string
	Name               string
	Description        string
	AutoDeploy         bool
	CertificateFilters []CertificateFilter
	ConfigOverrides    map[string]map[string]any // configuration_id -> config overlay (never credentials)
	CreatedBy          string
	UpdatedBy          string
	CreatedAt          time.Time
	UpdatedAt          time.Time
}

// TargetConfiguration is a single deployment endpoint.
type TargetConfiguration struct {
	ID                string
	TenantID          string
	Name              string
	Description       string
	ProviderType      string
	Config            map[string]any
	CredentialsSealed []byte // envelope-sealed; never returned
	Status            string
	StatusMessage     string
	LastDeploymentAt  *time.Time
	CreatedBy         string
	UpdatedBy         string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// DeploymentJob is a unit of deployment work (parent | child | direct — computed).
type DeploymentJob struct {
	ID                    string
	TenantID              string
	DeploymentTargetID    *string // set for PARENT
	TargetConfigurationID *string // set for CHILD/DIRECT
	ParentJobID           *string // set for CHILD
	CertificateID         string
	CertificateSerial     string
	Status                string
	StatusMessage         string
	Progress              int
	RetryCount            int
	MaxRetries            int
	TriggeredBy           string
	Result                []byte // JSON
	LeaseUntil            *time.Time
	StartedAt             *time.Time
	CompletedAt           *time.Time
	NextRetryAt           *time.Time
	CreatedAt             time.Time
	UpdatedAt             time.Time
}

// Job types (computed, never stored).
const (
	JobTypeParent = "parent"
	JobTypeChild  = "child"
	JobTypeDirect = "direct"
)

// JobType returns the computed job type.
func (j DeploymentJob) JobType() string {
	switch {
	case j.ParentJobID != nil:
		return JobTypeChild
	case j.DeploymentTargetID != nil:
		return JobTypeParent
	default:
		return JobTypeDirect
	}
}

// DeploymentHistory records one deploy/verify/rollback action for a job.
type DeploymentHistory struct {
	ID         string
	TenantID   string
	JobID      string
	Action     string
	Result     string
	Message    string
	DurationMS int
	Details    []byte // JSON, non-secret
	CreatedAt  time.Time
}

// AuditRow is one append-only audit entry (schema shared across modules).
type AuditRow struct {
	TS            time.Time
	TenantID      string
	EventType     string
	ActorKind     string // user | service | system
	ActorID       string
	SubjectKind   string // target | configuration | job | backup | grant | system
	SubjectID     string
	SubjectName   string
	Outcome       string // ok | refused | failed
	Reason        string
	CorrelationID string
	Details       []byte // guarded: no key material or secret
}

// Grant is a Zanzibar tuple (fine-grained per-resource access). Wired minimally
// in v1 (tenant-scoped + admin); per-user owner/editor/viewer grants are a
// follow-up.
type Grant struct {
	ID, TenantID             string
	ResourceType, ResourceID string
	SubjectType, SubjectID   string // user | role | tenant ('' for tenant)
	Relation                 string // owner | editor | viewer
	GrantedBy                *string
	GrantedAt                time.Time
	ExpiresAt                *time.Time
}
