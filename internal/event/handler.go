package event

import (
	"context"
	"regexp"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/data"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/deploymentjob"
	"github.com/go-tangra/go-tangra-deployer/internal/data/ent/schema"
)

// Handler handles certificate events and creates deployment jobs
type Handler struct {
	log        *log.Helper
	targetRepo *data.DeploymentTargetRepo
	jobRepo    *data.DeploymentJobRepo
}

// NewHandler creates a new event handler
func NewHandler(ctx *bootstrap.Context, targetRepo *data.DeploymentTargetRepo, jobRepo *data.DeploymentJobRepo) *Handler {
	return &Handler{
		log:        ctx.NewLoggerHelper("deployer/event/handler"),
		targetRepo: targetRepo,
		jobRepo:    jobRepo,
	}
}

// HandleCertificateEvent handles a certificate event from LCM
func (h *Handler) HandleCertificateEvent(ctx context.Context, event *CertificateEvent) error {
	h.log.Infof("Handling certificate event: type=%s, tenant=%d, cert=%s",
		event.EventType, event.TenantID, event.CertificateID)

	switch event.EventType {
	case "certificate.issued":
		return h.handleCertificateIssued(ctx, event)
	case "renewal.completed":
		return h.handleRenewalCompleted(ctx, event)
	default:
		h.log.Infof("Ignoring unknown event type: %s", event.EventType)
		return nil
	}
}

// handleCertificateIssued handles a newly issued certificate
func (h *Handler) handleCertificateIssued(ctx context.Context, event *CertificateEvent) error {
	// Find deployment target GROUPS with auto-deploy enabled that match this certificate
	targets, err := h.findMatchingTargets(ctx, event)
	if err != nil {
		return err
	}

	if len(targets) == 0 {
		h.log.Infof("No matching auto-deploy target groups for certificate %s", event.CertificateID)
		return nil
	}

	// Create parent deployment jobs for each matching target group
	triggerType := deploymentjob.TriggeredByTRIGGER_TYPE_EVENT
	if event.IsRenewal {
		triggerType = deploymentjob.TriggeredByTRIGGER_TYPE_AUTO_RENEWAL
	}

	for _, target := range targets {
		// Get configurations linked to this target
		configs := target.Edges.Configurations
		if len(configs) == 0 {
			h.log.Infof("Target group %s has no configurations, skipping", target.ID)
			continue
		}

		// Create parent job for the target group
		parentJob, err := h.jobRepo.CreateParentJob(ctx, event.TenantID, target.ID, event.CertificateID,
			event.SerialNumber, triggerType, 3)
		if err != nil {
			h.log.Errorf("Failed to create parent job for target group %s: %v", target.ID, err)
			continue
		}
		h.log.Infof("Created parent deployment job %s for target group %s", parentJob.ID, target.ID)

		// Create child jobs for each configuration
		for _, config := range configs {
			childJob, err := h.jobRepo.CreateChildJob(ctx, event.TenantID, parentJob.ID, config.ID,
				event.CertificateID, event.SerialNumber, triggerType, 3)
			if err != nil {
				h.log.Errorf("Failed to create child job for configuration %s: %v", config.ID, err)
				continue
			}
			h.log.Infof("Created child deployment job %s for configuration %s (parent: %s)", childJob.ID, config.ID, parentJob.ID)
		}
	}

	return nil
}

// handleRenewalCompleted handles a completed certificate renewal
func (h *Handler) handleRenewalCompleted(ctx context.Context, event *CertificateEvent) error {
	// Same logic as handleCertificateIssued but with auto-renewal trigger type
	event.IsRenewal = true
	return h.handleCertificateIssued(ctx, event)
}

// findMatchingTargets finds deployment target groups that should receive this certificate
func (h *Handler) findMatchingTargets(ctx context.Context, event *CertificateEvent) ([]*ent.DeploymentTarget, error) {
	// Get all target groups with auto-deploy enabled (includes configurations)
	targets, err := h.targetRepo.ListByAutoDeployEnabled(ctx)
	if err != nil {
		return nil, err
	}

	// Filter by tenant and certificate filters
	var matched []*ent.DeploymentTarget
	for _, target := range targets {
		// Filter by tenant if specified
		if event.TenantID != 0 && target.TenantID != nil && *target.TenantID != event.TenantID {
			continue
		}

		// Check certificate filters
		if h.matchesCertificateFilters(target.CertificateFilters, event) {
			// Only include targets that have at least one configuration
			if target.Edges.Configurations != nil && len(target.Edges.Configurations) > 0 {
				matched = append(matched, target)
			}
		}
	}

	return matched, nil
}

// matchesCertificateFilters checks if a certificate matches the target's filters
func (h *Handler) matchesCertificateFilters(filters []schema.CertificateFilter, event *CertificateEvent) bool {
	// If no filters, match all certificates
	if len(filters) == 0 {
		return true
	}

	// Check if any filter matches
	for _, filter := range filters {
		if h.matchesFilter(filter, event) {
			return true
		}
	}

	return false
}

// matchesFilter checks if a single filter matches the event
// All specified fields must match (AND logic). Empty fields are ignored.
func (h *Handler) matchesFilter(filter schema.CertificateFilter, event *CertificateEvent) bool {
	// Check issuer name (exact match)
	if filter.IssuerName != "" && filter.IssuerName != event.IssuerName {
		return false
	}

	// Check Common Name pattern (regex)
	if filter.CommonNamePattern != "" {
		matched, err := h.matchesPattern(filter.CommonNamePattern, event.CommonName)
		if err != nil {
			h.log.Warnf("Invalid common name pattern %s: %v", filter.CommonNamePattern, err)
			return false
		}
		if !matched {
			return false
		}
	}

	// Check SAN pattern (regex) - matches if ANY SAN matches
	if filter.SANPattern != "" {
		matched, err := h.matchesAnyPattern(filter.SANPattern, event.SANs)
		if err != nil {
			h.log.Warnf("Invalid SAN pattern %s: %v", filter.SANPattern, err)
			return false
		}
		if !matched {
			return false
		}
	}

	// Check Subject Organization (exact match)
	if filter.SubjectOrganization != "" && filter.SubjectOrganization != event.SubjectOrganization {
		return false
	}

	// Check Subject Organizational Unit (exact match)
	if filter.SubjectOrgUnit != "" && filter.SubjectOrgUnit != event.SubjectOrgUnit {
		return false
	}

	// Check Subject Country (exact match)
	if filter.SubjectCountry != "" && filter.SubjectCountry != event.SubjectCountry {
		return false
	}

	// Legacy: Check domain pattern (deprecated, for backwards compatibility)
	// Matches against both CommonName and SANs
	if filter.DomainPattern != "" {
		matched, err := h.matchesDomainPattern(filter.DomainPattern, event.CommonName, event.SANs)
		if err != nil {
			h.log.Warnf("Invalid domain pattern %s: %v", filter.DomainPattern, err)
			return false
		}
		if !matched {
			return false
		}
	}

	// Check labels (not implemented in events yet, but reserved for future use)
	// Labels would need to be added to the certificate event

	return true
}

// matchesPattern checks if a value matches a regex pattern
func (h *Handler) matchesPattern(pattern, value string) (bool, error) {
	if value == "" {
		return false, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}
	return re.MatchString(value), nil
}

// matchesAnyPattern checks if any value in the slice matches a regex pattern
func (h *Handler) matchesAnyPattern(pattern string, values []string) (bool, error) {
	if len(values) == 0 {
		return false, nil
	}
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}
	for _, v := range values {
		if re.MatchString(v) {
			return true, nil
		}
	}
	return false, nil
}

// matchesDomainPattern checks if any domain matches the pattern (legacy, deprecated)
func (h *Handler) matchesDomainPattern(pattern, commonName string, sans []string) (bool, error) {
	re, err := regexp.Compile(pattern)
	if err != nil {
		return false, err
	}

	// Check common name
	if commonName != "" && re.MatchString(commonName) {
		return true, nil
	}

	// Check SANs
	for _, san := range sans {
		if re.MatchString(san) {
			return true, nil
		}
	}

	return false, nil
}
