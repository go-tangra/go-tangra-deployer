package httpapi

import (
	"net/http"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/backup"
)

// registerStatsBackup mounts the US5 statistics and backup routes. The gateway
// enforces the API permissions (stats:read, backup:manage) from the OpenAPI;
// the services add tenant scope (and admin for the system-wide view).
func (s *Server) registerStatsBackup(d Deps) {
	p := "/api/deployer/v1"

	// System-wide statistics (admin only; non-admins get 403 from the service).
	s.MustHandle("GET", p+"/statistics", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Stats.SystemWide(r.Context(), subj)
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})

	// Caller's tenant statistics.
	s.MustHandle("GET", p+"/statistics/tenant", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Stats.Tenant(r.Context(), subj)
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})

	// Export the tenant's data. Credentials are included only on request.
	s.MustHandle("POST", p+"/backup/export", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			IncludeCredentials bool `json:"include_credentials"`
		}
		if r.ContentLength != 0 {
			if err := DecodeJSON(r, &in, 0); err != nil {
				Fail(w, r, nil, err)
				return
			}
		}
		b, err := d.Backup.Export(r.Context(), subj, in.IncludeCredentials)
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, b)
	})

	// Import a backup into the tenant (mode: skip|overwrite).
	s.MustHandle("POST", p+"/backup/import", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			Mode   string        `json:"mode"`
			Backup backup.Backup `json:"backup"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		res, err := d.Backup.Import(r.Context(), subj, in.Backup, in.Mode)
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, res)
	})
}
