package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/conf"
	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/providers/tangra_client"
	"github.com/go-tangra/go-tangra-deployer/pkg/deploy/registry"

	lcmV1 "github.com/go-tangra/go-tangra-lcm/gen/go/lcm/service/v1"
)

// TangraClientPusher is the default tangra_client.Pusher implementation for
// the deployer. It uses the Redis client to publish certificate update events
// onto the LCM streamer's pub/sub channels, which LCM forwards over each
// tangra-client's mTLS stream. For label-based target selection it uses the
// LCM ListLcmClients admin RPC.
type TangraClientPusher struct {
	log         *log.Helper
	redisClient *redis.Client
	lcm         *LcmClient
	topicPrefix string
}

// NewTangraClientPusher constructs the default pusher. Returned by Wire and
// installed into the tangra_client package via SetPusher() at bootstrap.
func NewTangraClientPusher(ctx *bootstrap.Context, redisClient *redis.Client, lcm *LcmClient) *TangraClientPusher {
	prefix := "lcm"
	if cfg, ok := ctx.GetCustomConfig("deployer"); ok && cfg != nil {
		if dep, ok := cfg.(*conf.Deployer); ok && dep.GetEvents() != nil {
			if tp := dep.GetEvents().GetTopicPrefix(); tp != "" {
				prefix = tp
			}
		}
	}

	p := &TangraClientPusher{
		log:         ctx.NewLoggerHelper("deployer/tangra-client-pusher"),
		redisClient: redisClient,
		lcm:         lcm,
		topicPrefix: prefix,
	}
	tangra_client.SetPusher(p)
	p.log.Infof("Tangra-client pusher initialized (topic prefix: %s)", prefix)
	return p
}

// ResolveClients turns the provider's TargetConfiguration into the list of
// client_ids to publish to. Explicit IDs are returned verbatim; label
// selectors are resolved by calling LCM ListLcmClients with the labels as a
// metadata filter (AND-matched against each client's registered metadata).
func (p *TangraClientPusher) ResolveClients(ctx context.Context, clientIDs []string, labels map[string]string) ([]string, error) {
	if len(clientIDs) > 0 {
		return dedupe(clientIDs), nil
	}
	if len(labels) == 0 {
		return nil, fmt.Errorf("no client_ids or labels provided")
	}
	if p.lcm == nil {
		return nil, fmt.Errorf("lcm client not available for label resolution")
	}
	if err := p.lcm.resolve(); err != nil {
		return nil, fmt.Errorf("connect to lcm: %w", err)
	}
	if p.lcm.ClientService == nil {
		return nil, fmt.Errorf("lcm client service unavailable")
	}

	resp, err := p.lcm.ClientService.ListLcmClients(ctx, &lcmV1.ListLcmClientsRequest{
		MetadataFilter: labels,
	})
	if err != nil {
		return nil, fmt.Errorf("list lcm clients by labels: %w", err)
	}

	out := make([]string, 0, len(resp.GetItems()))
	for _, c := range resp.GetItems() {
		if id := c.GetClientId(); id != "" {
			out = append(out, id)
		}
	}
	if len(out) == 0 {
		p.log.Warnf("label selector matched 0 lcm clients (labels=%v)", labels)
	}
	return dedupe(out), nil
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

// PushCertificate publishes one certificate.issued event per client_id. The
// event payload matches the schema the LCM streamer already understands; the
// only extension is that the certificate PEM/key/CA fields travel inside the
// payload so the tangra-client can install without a follow-up pull.
func (p *TangraClientPusher) PushCertificate(ctx context.Context, clientIDs []string, certName string, cert *registry.CertificateData) ([]tangra_client.PushResult, error) {
	if p.redisClient == nil {
		return nil, fmt.Errorf("redis client not available")
	}
	if cert == nil {
		return nil, fmt.Errorf("certificate data is nil")
	}

	topic := p.topicPrefix + ".certificate.issued"
	results := make([]tangra_client.PushResult, 0, len(clientIDs))

	for _, clientID := range clientIDs {
		payload := buildClientPayload(clientID, certName, cert)
		evt := pushEvent{
			ID:        uuid.New().String(),
			Type:      "certificate.issued",
			Source:    "deployer-service",
			Timestamp: time.Now().UTC(),
			Data:      payload,
		}

		body, err := json.Marshal(evt)
		if err != nil {
			results = append(results, tangra_client.PushResult{
				ClientID: clientID,
				Success:  false,
				Message:  fmt.Sprintf("marshal event: %v", err),
			})
			continue
		}

		if err := p.redisClient.Publish(ctx, topic, body).Err(); err != nil {
			p.log.Errorf("Failed to publish certificate event for client %s: %v", clientID, err)
			results = append(results, tangra_client.PushResult{
				ClientID: clientID,
				Success:  false,
				Message:  fmt.Sprintf("publish to %s: %v", topic, err),
			})
			continue
		}

		p.log.Infof("Published certificate event for client %s on %s (cert=%s)", clientID, topic, certName)
		results = append(results, tangra_client.PushResult{
			ClientID: clientID,
			Success:  true,
			Message:  "event published",
		})
	}

	return results, nil
}

// VerifyInstalled queries LCM for agent-reported install state of certName
// across the supplied clientIDs. Clients that have not reported (no row in
// the installed_certificates table) are represented by Status="UNKNOWN" so
// the caller can distinguish "missing report" from "explicit failure".
func (p *TangraClientPusher) VerifyInstalled(ctx context.Context, clientIDs []string, certName string) ([]tangra_client.InstalledStatus, error) {
	if len(clientIDs) == 0 {
		return nil, nil
	}
	if p.lcm == nil {
		return nil, fmt.Errorf("lcm client not available for verification")
	}
	if err := p.lcm.resolve(); err != nil {
		return nil, fmt.Errorf("connect to lcm: %w", err)
	}
	if p.lcm.ClientService == nil {
		return nil, fmt.Errorf("lcm client service unavailable")
	}

	req := &lcmV1.ListClientInstalledCertificatesRequest{
		ClientIds: clientIDs,
	}
	if certName != "" {
		req.Name = &certName
	}

	resp, err := p.lcm.ClientService.ListClientInstalledCertificates(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("list installed certificates: %w", err)
	}

	// Index reported rows by client_id so we can fill in UNKNOWN for any
	// requested client that LCM has no row for.
	reported := make(map[string]tangra_client.InstalledStatus, len(resp.GetItems()))
	for _, row := range resp.GetItems() {
		clientID := row.GetClientId()
		if clientID == "" {
			continue
		}
		reported[clientID] = tangra_client.InstalledStatus{
			ClientID:          clientID,
			Name:              row.GetName(),
			SerialNumber:      row.GetSerialNumber(),
			FingerprintSHA256: row.GetFingerprintSha256(),
			Status:            installedStatusToString(row.GetStatus()),
			Message:           row.GetMessage(),
		}
	}

	out := make([]tangra_client.InstalledStatus, 0, len(clientIDs))
	for _, cid := range clientIDs {
		if st, ok := reported[cid]; ok {
			out = append(out, st)
			continue
		}
		out = append(out, tangra_client.InstalledStatus{
			ClientID: cid,
			Name:     certName,
			Status:   "UNKNOWN",
		})
	}
	return out, nil
}

func installedStatusToString(s lcmV1.InstalledCertificateStatus) string {
	switch s {
	case lcmV1.InstalledCertificateStatus_INSTALLED_CERT_STATUS_INSTALLED:
		return "INSTALLED"
	case lcmV1.InstalledCertificateStatus_INSTALLED_CERT_STATUS_FAILED:
		return "FAILED"
	case lcmV1.InstalledCertificateStatus_INSTALLED_CERT_STATUS_REMOVED:
		return "REMOVED"
	default:
		return "UNKNOWN"
	}
}

// pushEvent mirrors the LCMEvent envelope expected by LCM's stream consumer.
type pushEvent struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Source    string    `json:"source"`
	Timestamp time.Time `json:"timestamp"`
	TenantID  uint32    `json:"tenant_id,omitempty"`
	Data      any       `json:"data"`
}

// clientCertPayload is the per-client data field of the published event. The
// LCM streamer extracts client_id to route to the matching mTLS stream and
// then forwards the remaining fields into the CertificateUpdateEvent message.
type clientCertPayload struct {
	ClientID         string   `json:"client_id"`
	Name             string   `json:"name,omitempty"`
	CommonName       string   `json:"common_name,omitempty"`
	SerialNumber     string   `json:"serial_number,omitempty"`
	IssuerName       string   `json:"issuer_name,omitempty"`
	DNSNames         []string `json:"dns_names,omitempty"`
	IPAddresses      []string `json:"ip_addresses,omitempty"`
	IssuedAt         int64    `json:"issued_at,omitempty"`
	ExpiresAt        int64    `json:"expires_at,omitempty"`
	CertificatePEM   string   `json:"certificate_pem,omitempty"`
	CACertificatePEM string   `json:"ca_certificate_pem,omitempty"`
	PrivateKeyPEM    string   `json:"private_key_pem,omitempty"`
	Fingerprint      string   `json:"fingerprint_sha256,omitempty"`
}

func buildClientPayload(clientID, certName string, cert *registry.CertificateData) *clientCertPayload {
	name := certName
	if name == "" {
		name = cert.CommonName
	}
	return &clientCertPayload{
		ClientID:         clientID,
		Name:             name,
		CommonName:       cert.CommonName,
		SerialNumber:     cert.SerialNumber,
		DNSNames:         cert.SANs,
		ExpiresAt:        cert.ExpiresAt,
		CertificatePEM:   cert.CertificatePEM,
		CACertificatePEM: cert.CertificateChain,
		PrivateKeyPEM:    cert.PrivateKeyPEM,
	}
}
