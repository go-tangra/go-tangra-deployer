package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
	"github.com/tx7do/go-crud/entgo/mixin"
)

// CertificateFilter represents filter criteria for auto-deployment
// All specified fields must match (AND logic). Empty fields are ignored.
type CertificateFilter struct {
	// IssuerName matches the certificate issuer (exact match)
	IssuerName string `json:"issuer_name,omitempty"`

	// CommonNamePattern matches the certificate Common Name (regex)
	CommonNamePattern string `json:"common_name_pattern,omitempty"`

	// SANPattern matches any Subject Alternative Name - DNS names (regex)
	SANPattern string `json:"san_pattern,omitempty"`

	// SubjectOrganization matches the certificate Subject Organization (exact match)
	SubjectOrganization string `json:"subject_organization,omitempty"`

	// SubjectOrgUnit matches the certificate Subject Organizational Unit (exact match)
	SubjectOrgUnit string `json:"subject_org_unit,omitempty"`

	// SubjectCountry matches the certificate Subject Country (exact match)
	SubjectCountry string `json:"subject_country,omitempty"`

	// Labels for future use (e.g., certificate tags/labels)
	Labels []string `json:"labels,omitempty"`

	// Deprecated: Use CommonNamePattern and SANPattern instead
	DomainPattern string `json:"domain_pattern,omitempty"`
}

// DeploymentTarget holds the schema definition for the DeploymentTarget entity.
// This represents a deployment target GROUP that contains multiple target configurations.
// It defines which certificates should be auto-deployed via filters.
type DeploymentTarget struct {
	ent.Schema
}

// Annotations of the DeploymentTarget.
func (DeploymentTarget) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "deployer_targets"},
		entsql.WithComments(true),
	}
}

// Fields of the DeploymentTarget.
func (DeploymentTarget) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			NotEmpty().
			Unique().
			Comment("UUID primary key"),

		field.String("name").
			NotEmpty().
			Comment("Target group name"),

		field.String("description").
			Optional().
			Comment("Target group description"),

		field.Bool("auto_deploy_on_renewal").
			Default(false).
			Comment("Auto-deploy certificates on renewal/issuance"),

		field.JSON("certificate_filters", []CertificateFilter{}).
			Optional().
			Comment("Filters for auto-deployment"),
	}
}

// Edges of the DeploymentTarget.
func (DeploymentTarget) Edges() []ent.Edge {
	return []ent.Edge{
		// Configurations in this group (M:N relationship)
		edge.To("configurations", TargetConfiguration.Type).
			Comment("Target configurations in this group"),

		// Parent jobs for this deployment target group
		edge.To("jobs", DeploymentJob.Type).
			Comment("Parent deployment jobs for this target group"),
	}
}

// Mixin of the DeploymentTarget.
func (DeploymentTarget) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.CreateBy{},
		mixin.UpdateBy{},
		mixin.Time{},
		mixin.TenantID[uint32]{},
	}
}

// Indexes of the DeploymentTarget.
func (DeploymentTarget) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("tenant_id", "name").Unique(),
		index.Fields("tenant_id"),
		index.Fields("auto_deploy_on_renewal"),
	}
}
