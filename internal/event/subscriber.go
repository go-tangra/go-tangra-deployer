package event

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"github.com/tx7do/kratos-bootstrap/bootstrap"

	"github.com/go-tangra/go-tangra-deployer/internal/conf"

	appViewer "github.com/go-tangra/go-tangra-common/viewer"
)

// LCMEvent is the wrapper structure that LCM service publishes
type LCMEvent struct {
	ID        string          `json:"id"`
	Type      string          `json:"type"`
	Source    string          `json:"source"`
	Timestamp time.Time       `json:"timestamp"`
	TenantID  uint32          `json:"tenant_id"`
	Data      json.RawMessage `json:"data"`
}

// CertificateIssuedData represents the data field for certificate.issued events
type CertificateIssuedData struct {
	JobID        string    `json:"job_id"`
	ClientID     string    `json:"client_id"`
	TenantID     uint32    `json:"tenant_id"`
	IssuerName   string    `json:"issuer_name"`
	IssuerType   string    `json:"issuer_type"`
	SerialNumber string    `json:"serial_number"`
	CommonName   string    `json:"common_name"`
	DNSNames     []string  `json:"dns_names,omitempty"`
	IssuedAt     time.Time `json:"issued_at"`
	ExpiresAt    time.Time `json:"expires_at"`

	// Subject fields for certificate matching
	SubjectOrganization string `json:"subject_organization,omitempty"`
	SubjectOrgUnit      string `json:"subject_org_unit,omitempty"`
	SubjectCountry      string `json:"subject_country,omitempty"`
}

// RenewalCompletedData represents the data field for renewal.completed events
type RenewalCompletedData struct {
	RenewalID       int       `json:"renewal_id"`
	CertificateID   string    `json:"certificate_id"`
	ClientID        string    `json:"client_id"`
	TenantID        uint32    `json:"tenant_id"`
	NewSerialNumber string    `json:"new_serial_number"`
	NewExpiresAt    time.Time `json:"new_expires_at"`
	AttemptNumber   int32     `json:"attempt_number"`
}

// CertificateEvent represents the normalized certificate event for handler
type CertificateEvent struct {
	EventType      string   `json:"event_type"`
	TenantID       uint32   `json:"tenant_id"`
	CertificateID  string   `json:"certificate_id"`
	SerialNumber   string   `json:"serial_number,omitempty"`
	CommonName     string   `json:"common_name,omitempty"`
	SANs           []string `json:"sans,omitempty"`
	IssuerName     string   `json:"issuer_name,omitempty"`
	NotBefore      int64    `json:"not_before,omitempty"`
	NotAfter       int64    `json:"not_after,omitempty"`
	IsRenewal      bool     `json:"is_renewal,omitempty"`
	PreviousCertID string   `json:"previous_certificate_id,omitempty"`

	// Subject fields
	SubjectOrganization string `json:"subject_organization,omitempty"`
	SubjectOrgUnit      string `json:"subject_org_unit,omitempty"`
	SubjectCountry      string `json:"subject_country,omitempty"`
}

// Subscriber handles Redis pub/sub event subscriptions
type Subscriber struct {
	log     *log.Helper
	rdb     *redis.Client
	handler *Handler
	config  *conf.EventConfig
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup
	running bool
	mu      sync.Mutex
}

// NewSubscriber creates a new event subscriber
func NewSubscriber(ctx *bootstrap.Context, rdb *redis.Client, handler *Handler) *Subscriber {
	// Get deployer config
	var eventCfg *conf.EventConfig
	if cfg, ok := ctx.GetCustomConfig("deployer"); ok && cfg != nil {
		if deployerCfg, ok := cfg.(*conf.Deployer); ok && deployerCfg.Events != nil {
			eventCfg = deployerCfg.Events
		}
	}

	// Default config if not set
	if eventCfg == nil {
		eventCfg = &conf.EventConfig{
			Enabled:     true,
			TopicPrefix: "lcm",
			SubscribeEvents: []string{
				"certificate.issued",
				"renewal.completed",
			},
		}
	}

	return &Subscriber{
		log:     ctx.NewLoggerHelper("deployer/event/subscriber"),
		rdb:     rdb,
		handler: handler,
		config:  eventCfg,
	}
}

// Start starts the event subscriber
func (s *Subscriber) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.running {
		return nil
	}

	if !s.config.Enabled {
		s.log.Info("Event subscriber is disabled")
		return nil
	}

	if s.rdb == nil {
		s.log.Warn("Redis client not available, event subscriber disabled")
		return nil
	}

	// Use system viewer context for background operations (bypasses tenant privacy checks)
	baseCtx := appViewer.NewSystemViewerContext(context.Background())
	s.ctx, s.cancel = context.WithCancel(baseCtx)
	s.running = true

	// Build channel patterns
	prefix := s.config.TopicPrefix
	if prefix == "" {
		prefix = "lcm"
	}

	channels := make([]string, len(s.config.SubscribeEvents))
	for i, event := range s.config.SubscribeEvents {
		channels[i] = fmt.Sprintf("%s.%s", prefix, event)
	}

	s.log.Infof("Starting event subscriber for channels: %v", channels)

	// Subscribe to channels
	pubsub := s.rdb.PSubscribe(s.ctx, channels...)

	s.wg.Add(1)
	go s.listen(pubsub)

	return nil
}

// Stop stops the event subscriber
func (s *Subscriber) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return nil
	}

	s.log.Info("Stopping event subscriber")
	s.cancel()
	s.wg.Wait()
	s.running = false

	return nil
}

// listen listens for events on the pub/sub channels
func (s *Subscriber) listen(pubsub *redis.PubSub) {
	defer s.wg.Done()
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		select {
		case <-s.ctx.Done():
			s.log.Info("Event subscriber stopped")
			return
		case msg, ok := <-ch:
			if !ok {
				s.log.Warn("Pub/sub channel closed")
				return
			}
			s.handleMessage(msg)
		}
	}
}

// handleMessage processes a pub/sub message
func (s *Subscriber) handleMessage(msg *redis.Message) {
	s.log.Debugf("Received event on channel %s: %s", msg.Channel, msg.Payload)

	// Parse the LCM event wrapper
	var lcmEvent LCMEvent
	if err := json.Unmarshal([]byte(msg.Payload), &lcmEvent); err != nil {
		s.log.Errorf("Failed to unmarshal LCM event: %v", err)
		return
	}

	// Extract event type from channel name
	prefix := s.config.TopicPrefix
	if prefix == "" {
		prefix = "lcm"
	}
	var eventType string
	if len(msg.Channel) > len(prefix)+1 {
		eventType = msg.Channel[len(prefix)+1:]
	}

	// Convert LCM event to CertificateEvent based on event type
	certEvent, err := s.convertToCertificateEvent(eventType, &lcmEvent)
	if err != nil {
		s.log.Errorf("Failed to convert event: %v", err)
		return
	}

	// Handle the event
	if err := s.handler.HandleCertificateEvent(s.ctx, certEvent); err != nil {
		s.log.Errorf("Failed to handle event: %v", err)
	}
}

// convertToCertificateEvent converts an LCMEvent to a CertificateEvent
func (s *Subscriber) convertToCertificateEvent(eventType string, lcmEvent *LCMEvent) (*CertificateEvent, error) {
	switch eventType {
	case "certificate.issued":
		var data CertificateIssuedData
		if err := json.Unmarshal(lcmEvent.Data, &data); err != nil {
			return nil, fmt.Errorf("failed to parse certificate.issued data: %w", err)
		}
		return &CertificateEvent{
			EventType:           eventType,
			TenantID:            data.TenantID,
			CertificateID:       data.JobID, // Use job ID as certificate ID (for fetching from LCM)
			SerialNumber:        data.SerialNumber,
			CommonName:          data.CommonName,
			SANs:                data.DNSNames,
			IssuerName:          data.IssuerName,
			NotBefore:           data.IssuedAt.Unix(),
			NotAfter:            data.ExpiresAt.Unix(),
			IsRenewal:           false,
			SubjectOrganization: data.SubjectOrganization,
			SubjectOrgUnit:      data.SubjectOrgUnit,
			SubjectCountry:      data.SubjectCountry,
		}, nil

	case "renewal.completed":
		var data RenewalCompletedData
		if err := json.Unmarshal(lcmEvent.Data, &data); err != nil {
			return nil, fmt.Errorf("failed to parse renewal.completed data: %w", err)
		}
		return &CertificateEvent{
			EventType:      eventType,
			TenantID:       data.TenantID,
			CertificateID:  data.NewSerialNumber,
			SerialNumber:   data.NewSerialNumber,
			NotAfter:       data.NewExpiresAt.Unix(),
			IsRenewal:      true,
			PreviousCertID: data.CertificateID,
		}, nil

	default:
		return nil, fmt.Errorf("unknown event type: %s", eventType)
	}
}
