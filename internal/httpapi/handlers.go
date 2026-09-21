package httpapi

import (
	"net/http"

	"github.com/go-freya/freya/services/deployer/internal/configs"
	"github.com/go-freya/freya/services/deployer/internal/repo"
	"github.com/go-freya/freya/services/deployer/internal/store"
	"github.com/go-freya/freya/services/deployer/internal/targets"
)

// Register mounts the deployer HTTP routes (declared in the OpenAPI document).
func (s *Server) Register(d Deps) {
	p := "/api/deployer/v1"

	// Providers.
	s.MustHandle("GET", p+"/providers", func(w http.ResponseWriter, r *http.Request) {
		if _, err := subjects(r); err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"items": d.Configs.ListProviders()})
	})

	// Configurations.
	s.MustHandle("GET", p+"/configurations", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		items, err := d.Configs.List(r.Context(), subj, r.URL.Query().Get("provider_type"), r.URL.Query().Get("status"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	s.MustHandle("POST", p+"/configurations", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			Name         string         `json:"name"`
			Description  string         `json:"description"`
			ProviderType string         `json:"provider_type"`
			Config       map[string]any `json:"config"`
			Credentials  map[string]any `json:"credentials"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		v, err := d.Configs.Create(r.Context(), subj, configs.Input{Name: in.Name, Description: in.Description, ProviderType: in.ProviderType, Config: in.Config, Credentials: in.Credentials})
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusCreated, v)
	})
	s.MustHandle("POST", p+"/configurations/validate", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			ProviderType string         `json:"provider_type"`
			Credentials  map[string]any `json:"credentials"`
			Config       map[string]any `json:"config"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		if err := d.Configs.Validate(r.Context(), subj, in.ProviderType, in.Credentials, in.Config); err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"valid": true})
	})
	s.MustHandle("GET", p+"/configurations/{id}", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Configs.Get(r.Context(), subj, r.PathValue("id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("PUT", p+"/configurations/{id}", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			Name        string         `json:"name"`
			Description string         `json:"description"`
			Config      map[string]any `json:"config"`
			Credentials map[string]any `json:"credentials"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		v, err := d.Configs.Update(r.Context(), subj, r.PathValue("id"), configs.Input{Name: in.Name, Description: in.Description, Config: in.Config, Credentials: in.Credentials})
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("POST", p+"/configurations/{id}/remove", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		if err := d.Configs.Delete(r.Context(), subj, r.PathValue("id")); err != nil {
			failSvc(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})

	// Deploy.
	s.MustHandle("POST", p+"/deploy", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			CertificateID   string `json:"certificate_id"`
			ConfigurationID string `json:"configuration_id"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		jobID, err := d.Deploy.Deploy(r.Context(), subj, in.CertificateID, in.ConfigurationID, "")
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusAccepted, map[string]any{"status": "processing", "job_id": jobID})
	})
	s.MustHandle("POST", p+"/deploy/target", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			CertificateID string `json:"certificate_id"`
			TargetID      string `json:"target_id"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		jobID, err := d.Deploy.DeployToTarget(r.Context(), subj, in.CertificateID, in.TargetID, "")
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusAccepted, map[string]any{"status": "processing", "job_id": jobID})
	})
	s.MustHandle("POST", p+"/deploy/configurations", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			CertificateID    string   `json:"certificate_id"`
			ConfigurationIDs []string `json:"configuration_ids"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		ids, err := d.Deploy.DeployToConfigurations(r.Context(), subj, in.CertificateID, in.ConfigurationIDs, "")
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusAccepted, map[string]any{"status": "processing", "job_ids": ids})
	})

	// Targets.
	s.MustHandle("GET", p+"/targets", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		items, err := d.Targets.List(r.Context(), subj)
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	s.MustHandle("POST", p+"/targets", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			Name        string                    `json:"name"`
			Description string                    `json:"description"`
			AutoDeploy  bool                      `json:"auto_deploy"`
			Filters     []store.CertificateFilter `json:"certificate_filters"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		v, err := d.Targets.Create(r.Context(), subj, targets.Input{Name: in.Name, Description: in.Description, AutoDeploy: in.AutoDeploy, Filters: in.Filters})
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusCreated, v)
	})
	s.MustHandle("GET", p+"/targets/{id}", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Targets.Get(r.Context(), subj, r.PathValue("id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("PUT", p+"/targets/{id}", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			Name        string                    `json:"name"`
			Description string                    `json:"description"`
			AutoDeploy  bool                      `json:"auto_deploy"`
			Filters     []store.CertificateFilter `json:"certificate_filters"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		v, err := d.Targets.Update(r.Context(), subj, r.PathValue("id"), targets.Input{Name: in.Name, Description: in.Description, AutoDeploy: in.AutoDeploy, Filters: in.Filters})
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("POST", p+"/targets/{id}/remove", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		if err := d.Targets.Delete(r.Context(), subj, r.PathValue("id")); err != nil {
			failSvc(w, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	s.MustHandle("GET", p+"/targets/{id}/configurations", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Targets.Get(r.Context(), subj, r.PathValue("id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"items": v.ConfigurationIDs})
	})
	s.MustHandle("POST", p+"/targets/{id}/configurations", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			ConfigurationIDs []string                  `json:"configuration_ids"`
			Overrides        map[string]map[string]any `json:"config_overrides"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		if err := d.Targets.Attach(r.Context(), subj, r.PathValue("id"), in.ConfigurationIDs, in.Overrides); err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"attached": true})
	})
	s.MustHandle("POST", p+"/targets/{id}/configurations/remove", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		var in struct {
			ConfigurationIDs []string `json:"configuration_ids"`
		}
		if err := DecodeJSON(r, &in, 0); err != nil {
			Fail(w, r, nil, err)
			return
		}
		if err := d.Targets.Detach(r.Context(), subj, r.PathValue("id"), in.ConfigurationIDs); err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"detached": true})
	})

	// Jobs.
	s.MustHandle("GET", p+"/jobs", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		q := r.URL.Query()
		items, err := d.Jobs.List(r.Context(), subj, repo.JobFilter{
			Status: q.Get("status"), TriggeredBy: q.Get("triggered_by"), CertificateID: q.Get("certificate_id"),
			JobType: q.Get("job_type"), ParentJobID: q.Get("parent_job_id"),
		})
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, map[string]any{"items": items})
	})
	s.MustHandle("GET", p+"/jobs/{id}", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Jobs.Get(r.Context(), subj, r.PathValue("id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("POST", p+"/jobs/{id}/cancel", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Jobs.Cancel(r.Context(), subj, r.PathValue("id"), true)
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("POST", p+"/jobs/{id}/retry", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Jobs.Retry(r.Context(), subj, r.PathValue("id"), r.URL.Query().Get("force") == "true")
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("GET", p+"/jobs/{id}/result", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		v, err := d.Jobs.GetResult(r.Context(), subj, r.PathValue("id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, v)
	})
	s.MustHandle("POST", p+"/deploy/{job_id}/verify", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		res, err := d.Jobs.Verify(r.Context(), subj, r.PathValue("job_id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, res)
	})
	s.MustHandle("POST", p+"/deploy/{job_id}/rollback", func(w http.ResponseWriter, r *http.Request) {
		subj, err := subjects(r)
		if err != nil {
			failSvc(w, err)
			return
		}
		res, err := d.Jobs.Rollback(r.Context(), subj, r.PathValue("job_id"))
		if err != nil {
			failSvc(w, err)
			return
		}
		WriteJSON(w, http.StatusOK, res)
	})

	s.registerStatsBackup(d)
}
