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

// TargetConfiguration holds the schema definition for the TargetConfiguration entity.
// This represents a single deployment endpoint configuration (e.g., Cloudflare credentials, AWS ACM settings).
type TargetConfiguration struct {
	ent.Schema
}

// Annotations of the TargetConfiguration.
func (TargetConfiguration) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "deployer_target_configs"},
		entsql.WithComments(true),
	}
}

// Fields of the TargetConfiguration.
func (TargetConfiguration) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			NotEmpty().
			Unique().
			Comment("UUID primary key"),

		field.String("name").
			NotEmpty().
			Comment("Configuration name"),

		field.String("description").
			Optional().
			Comment("Configuration description"),

		field.String("provider_type").
			NotEmpty().
			Comment("Provider type (e.g., cloudflare, aws_acm)"),

		field.Bytes("credentials_encrypted").
			Sensitive().
			Comment("Encrypted provider credentials (JSON)"),

		field.JSON("config", map[string]interface{}{}).
			Optional().
			Comment("Provider-specific configuration"),

		field.Enum("status").
			Values("CONFIG_STATUS_UNSPECIFIED", "CONFIG_STATUS_ACTIVE", "CONFIG_STATUS_INACTIVE", "CONFIG_STATUS_ERROR").
			Default("CONFIG_STATUS_ACTIVE").
			Comment("Configuration status"),

		field.String("status_message").
			Optional().
			Comment("Status message (e.g., error details)"),

		field.Time("last_deployment_at").
			Optional().
			Nillable().
			Comment("Last deployment timestamp"),
	}
}

// Edges of the TargetConfiguration.
func (TargetConfiguration) Edges() []ent.Edge {
	return []ent.Edge{
		// Jobs that use this configuration (child jobs)
		edge.To("jobs", DeploymentJob.Type).
			Comment("Deployment jobs for this configuration"),

		// Deployment targets (groups) that include this configuration
		edge.From("deployment_targets", DeploymentTarget.Type).
			Ref("configurations").
			Comment("Deployment target groups that include this configuration"),
	}
}

// Mixin of the TargetConfiguration.
func (TargetConfiguration) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.CreateBy{},
		mixin.UpdateBy{},
		mixin.Time{},
		mixin.TenantID[uint32]{},
	}
}

// Indexes of the TargetConfiguration.
func (TargetConfiguration) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("tenant_id", "name").Unique(),
		index.Fields("tenant_id"),
		index.Fields("provider_type"),
		index.Fields("status"),
	}
}
