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

// DeploymentJob holds the schema definition for the DeploymentJob entity.
// This represents a deployment job that deploys a certificate to a target.
//
// Job Types:
// - Parent Job: deployment_target_id is set, target_configuration_id is null
//   Created when deploying to a target group, spawns child jobs.
// - Child Job: parent_job_id is set, target_configuration_id is set
//   Created by parent job, one per target configuration.
// - Direct Job: target_configuration_id is set, deployment_target_id and parent_job_id are null
//   Created for direct deployment to a single configuration (legacy/manual).
type DeploymentJob struct {
	ent.Schema
}

// Annotations of the DeploymentJob.
func (DeploymentJob) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "deployer_jobs"},
		entsql.WithComments(true),
	}
}

// Fields of the DeploymentJob.
func (DeploymentJob) Fields() []ent.Field {
	return []ent.Field{
		field.String("id").
			NotEmpty().
			Unique().
			Comment("UUID primary key"),

		// For parent jobs: FK to deployment target group
		field.String("deployment_target_id").
			Optional().
			Nillable().
			Comment("FK to deployment target group (for parent jobs)"),

		// For child jobs and direct jobs: FK to target configuration
		field.String("target_configuration_id").
			Optional().
			Nillable().
			Comment("FK to target configuration (for child/direct jobs)"),

		// For child jobs: FK to parent job
		field.String("parent_job_id").
			Optional().
			Nillable().
			Comment("FK to parent job (for child jobs)"),

		field.String("certificate_id").
			NotEmpty().
			Comment("LCM certificate ID"),

		field.String("certificate_serial").
			Optional().
			Comment("Certificate serial number"),

		field.Enum("status").
			Values("JOB_STATUS_UNSPECIFIED", "JOB_STATUS_PENDING", "JOB_STATUS_PROCESSING", "JOB_STATUS_COMPLETED", "JOB_STATUS_FAILED", "JOB_STATUS_CANCELLED", "JOB_STATUS_RETRYING", "JOB_STATUS_PARTIAL").
			Default("JOB_STATUS_PENDING").
			Comment("Job status"),

		field.String("status_message").
			Optional().
			Comment("Status message"),

		field.Int32("progress").
			Default(0).
			Comment("Progress percentage (0-100)"),

		field.Int32("retry_count").
			Default(0).
			Comment("Number of retry attempts"),

		field.Int32("max_retries").
			Default(3).
			Comment("Maximum retry attempts"),

		field.Enum("triggered_by").
			Values("TRIGGER_TYPE_UNSPECIFIED", "TRIGGER_TYPE_MANUAL", "TRIGGER_TYPE_EVENT", "TRIGGER_TYPE_AUTO_RENEWAL").
			Default("TRIGGER_TYPE_MANUAL").
			Comment("How the job was triggered"),

		field.JSON("result", map[string]interface{}{}).
			Optional().
			Comment("Deployment result details"),

		field.Time("started_at").
			Optional().
			Nillable().
			Comment("Job start time"),

		field.Time("completed_at").
			Optional().
			Nillable().
			Comment("Job completion time"),

		field.Time("next_retry_at").
			Optional().
			Nillable().
			Comment("Next retry time"),
	}
}

// Edges of the DeploymentJob.
func (DeploymentJob) Edges() []ent.Edge {
	return []ent.Edge{
		// For parent jobs: link to deployment target group
		edge.From("deployment_target", DeploymentTarget.Type).
			Ref("jobs").
			Field("deployment_target_id").
			Unique().
			Comment("Deployment target group (for parent jobs)"),

		// For child/direct jobs: link to target configuration
		edge.From("target_configuration", TargetConfiguration.Type).
			Ref("jobs").
			Field("target_configuration_id").
			Unique().
			Comment("Target configuration (for child/direct jobs)"),

		// Parent-child relationship
		edge.To("child_jobs", DeploymentJob.Type).
			From("parent_job").
			Field("parent_job_id").
			Unique().
			Comment("Child jobs (for parent jobs) / Parent job (for child jobs)"),

		// History entries
		edge.To("history", DeploymentHistory.Type).
			Comment("Job history entries"),
	}
}

// Mixin of the DeploymentJob.
func (DeploymentJob) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.CreateBy{},
		mixin.Time{},
		mixin.TenantID[uint32]{},
	}
}

// Indexes of the DeploymentJob.
func (DeploymentJob) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("tenant_id"),
		index.Fields("deployment_target_id"),
		index.Fields("target_configuration_id"),
		index.Fields("parent_job_id"),
		index.Fields("certificate_id"),
		index.Fields("status"),
		index.Fields("triggered_by"),
		index.Fields("create_time"),
	}
}
