package httpapi

import (
	"errors"
	"net/http"

	"github.com/go-tangra/go-tangra/v4/listquery"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/authz"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/backup"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/configs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/deploy"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/jobs"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/stats"
	"github.com/go-tangra/go-tangra-deployer/v4/internal/targets"
)

// Deps wire the deployer HTTP handlers.
type Deps struct {
	Configs *configs.Service
	Targets *targets.Service
	Deploy  *deploy.Service
	Jobs    *jobs.Service
	Stats   *stats.Service
	Backup  *backup.Service
}

// subjects derives the authz subject from the verified platform identity.
func subjects(r *http.Request) (authz.Subjects, error) {
	id, err := Caller(r)
	if err != nil {
		return authz.Subjects{}, err
	}
	return authz.Subjects{TenantID: id.TenantID, UserID: id.UserID, Roles: id.Roles, ActorKind: "user"}, nil
}

// failSvc maps a service error to an HTTP response.
func failSvc(w http.ResponseWriter, err error) {
	var ve *configs.ValidationError
	var tve *targets.ValidationError
	var bve *backup.ValidationError
	switch {
	case errors.As(err, &ve):
		// Paths and codes are client-safe; values never are. "fields" is the
		// contract (contracts/deployer-config-ui.md §3); field/message repeat
		// the first error for older clients.
		d := map[string]any{"field": ve.Field, "message": ve.Msg, "fields": fieldsOf(ve.Fields, ve.Field, ve.Msg)}
		if len(ve.Targets) > 0 {
			d["targets"] = ve.Targets
		}
		WriteDetail(w, ErrValidation, d)
	case errors.As(err, &tve):
		d := map[string]any{"field": tve.Field, "message": tve.Msg, "fields": fieldsOf(tve.Fields, tve.Field, tve.Msg)}
		if tve.ConfigurationID != "" {
			d["configuration_id"] = tve.ConfigurationID
		}
		WriteDetail(w, ErrValidation, d)
	case errors.Is(err, configs.ErrCredentialsRejected):
		WriteError(w, http.StatusUnprocessableEntity, "credentials_rejected")
	case errors.As(err, &bve):
		WriteDetail(w, ErrValidation, map[string]any{"field": bve.Field, "message": bve.Msg})
	case errors.Is(err, authz.ErrForbidden):
		WriteError(w, http.StatusForbidden, "forbidden")
	case errors.Is(err, configs.ErrNotFound), errors.Is(err, jobs.ErrNotFound), errors.Is(err, deploy.ErrNotFound), errors.Is(err, targets.ErrNotFound):
		WriteError(w, http.StatusNotFound, "not_found")
	case errors.Is(err, deploy.ErrInactive), errors.Is(err, deploy.ErrNoConfigs):
		WriteError(w, http.StatusUnprocessableEntity, "validation_failed")
	case errors.Is(err, jobs.ErrUnsupported):
		WriteError(w, http.StatusConflict, "unsupported")
	case errors.Is(err, backup.ErrBadSchema):
		WriteError(w, http.StatusUnprocessableEntity, "validation_failed")
	default:
		WriteError(w, http.StatusInternalServerError, "internal")
	}
}

func fieldsOf(fields map[string]string, field, msg string) map[string]string {
	if len(fields) > 0 {
		return fields
	}
	return map[string]string{field: msg}
}

// parseList reads the list contract parameters (page, page_size, sort, order;
// go-tangra specs/032-server-side-tables) against spec. An invalid value is
// answered with validation_failed naming the parameter (never its value).
func parseList(w http.ResponseWriter, r *http.Request, spec listquery.Spec) (listquery.Request, bool) {
	req, err := listquery.Parse(r.URL.Query(), spec)
	var le *listquery.Error
	if errors.As(err, &le) {
		WriteDetail(w, ErrValidation, map[string]any{"param": le.Param})
		return req, false
	}
	if err != nil {
		WriteError(w, ErrValidation.Status, ErrValidation.Reason)
		return req, false
	}
	return req, true
}
