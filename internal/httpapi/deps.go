package httpapi

import (
	"errors"
	"net/http"

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
	switch {
	case errors.As(err, &ve), errors.As(err, &tve):
		WriteError(w, http.StatusUnprocessableEntity, "validation_failed")
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
