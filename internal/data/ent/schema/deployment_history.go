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

// DeploymentHistory holds the schema definition for the DeploymentHistory entity.
// This represents a history entry for a deployment job.
type DeploymentHistory struct {
	ent.Schema
}

// Annotations of the DeploymentHistory.
func (DeploymentHistory) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "deployer_history"},
		entsql.WithComments(true),
	}
}

// Fields of the DeploymentHistory.
func (DeploymentHistory) Fields() []ent.Field {
	return []ent.Field{
		field.String("job_id").
			NotEmpty().
			Comment("FK to deployment job"),

		field.Enum("action").
			Values("ACTION_DEPLOY", "ACTION_VERIFY", "ACTION_ROLLBACK").
			Comment("Action type"),

		field.Enum("result").
			Values("RESULT_SUCCESS", "RESULT_FAILURE", "RESULT_PARTIAL").
			Comment("Action result"),

		field.String("message").
			Optional().
			Comment("Result message"),

		field.Int64("duration_ms").
			Default(0).
			Comment("Action duration in milliseconds"),

		field.JSON("details", map[string]interface{}{}).
			Optional().
			Comment("Additional details"),
	}
}

// Edges of the DeploymentHistory.
func (DeploymentHistory) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("job", DeploymentJob.Type).
			Ref("history").
			Field("job_id").
			Required().
			Unique().
			Comment("Parent deployment job"),
	}
}

// Mixin of the DeploymentHistory.
func (DeploymentHistory) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixin.AutoIncrementId{},
		mixin.Time{},
	}
}

// Indexes of the DeploymentHistory.
func (DeploymentHistory) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("job_id"),
		index.Fields("action"),
		index.Fields("result"),
		index.Fields("create_time"),
	}
}
