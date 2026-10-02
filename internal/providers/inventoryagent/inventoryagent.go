package inventoryagent

import (
	"context"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	invsdk "github.com/go-tangra/go-tangra-inventory/sdk/v4/pkg/inventoryclient"

	"github.com/go-tangra/go-tangra-deployer/v4/internal/provider"
)

// Type is the provider type string.
const Type = "inventory-agent"

// Inventory is the subset of the inventory SDK client the provider uses
// (internal/inventoryclient adapts the SDK over the mesh connection).
type Inventory interface {
	CreateCertificateDelivery(ctx context.Context, r invsdk.DeliveryRequest) (invsdk.Delivery, error)
	GetCertificateDelivery(ctx context.Context, tenantID, id string) (invsdk.Delivery, error)
	PreviewCertificateTargets(ctx context.Context, tenantID string, ids, tags []string) (invsdk.Preview, error)
	VerifyHostCertificates(ctx context.Context, tenantID string, ids, tags []string, name, fingerprint string) (invsdk.Verification, error)
}

// Timing of the wait for host results (contracts/deployer-provider.md §3).
const (
	pollInterval   = 2 * time.Second
	offlineGrace   = 5 * time.Second
	deadlineMargin = 10 * time.Second
)

// Provider delivers certificates to inventory hosts by reference.
type Provider struct {
	mu  sync.RWMutex
	inv Inventory

	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// New builds the provider over an inventory client.
func New(inv Inventory) *Provider {
	return &Provider{inv: inv, now: time.Now, sleep: sleepCtx}
}

// SetInventory replaces the inventory client (app re-wiring in one process).
func (p *Provider) SetInventory(inv Inventory) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.inv = inv
}

func (p *Provider) client() (Inventory, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.inv == nil {
		return nil, errors.New("inventory-agent: inventory client not configured")
	}
	return p.inv, nil
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

// Capabilities declares the provider (contracts/deployer-provider.md §1).
// Every field is overridable (research D25, Q8): a shared configuration may
// leave the host selection to the targets.
func (p *Provider) Capabilities() provider.Capabilities {
	return provider.Capabilities{
		Type:                Type,
		DisplayName:         "Inventory agent",
		Description:         "Delivers the certificate to inventory hosts through their agents (certbot layout).",
		SupportsVerify:      true,
		SupportsRollback:    false,
		DeliversByReference: true,
		SchemaVersion:       1,
		ConfigFields: []provider.Field{
			{Key: keyHostIDs, Label: "Hosts", Type: provider.TypeHostSelector, Overridable: true, Group: provider.GroupConnection,
				MaxItems: MaxHostIDs, MaxLength: 36, Help: "Inventory hosts that receive the certificate."},
			{Key: keyHostTags, Label: "Host tags", Type: provider.TypeStringList, Overridable: true, Group: provider.GroupConnection,
				MaxItems: MaxHostTags, MaxLength: 319, Pattern: tagPattern, Placeholder: "role=web",
				Help: "key or key=value; a host must match all tags."},
			{Key: keyCertName, Label: "Certificate name", Type: provider.TypeString, Overridable: true, Group: provider.GroupOptions,
				MaxLength: MaxNameLen, Pattern: namePattern, Placeholder: "www",
				Help: "Directory name under live/ on the host; default from the common name."},
			{Key: keyKeyPolicy, Label: "Private key", Type: provider.TypeEnum, Overridable: true, Group: provider.GroupOptions,
				Default: KeyPolicyRequire, Options: []provider.Option{
					{Value: KeyPolicyRequire, Label: "Required"},
					{Value: KeyPolicyCertificateOnly, Label: "Certificate only (keep the host's key)"}},
				Help: "Deliver the private key with the certificate, or only the certificate for hosts that generated their own key."},
			{Key: keyRequireAll, Label: "Require all hosts", Type: provider.TypeBool, Overridable: true, Group: provider.GroupOptions,
				Default: false, Help: "Fail the deployment when any selected host fails or cannot receive certificates."},
			{Key: keyWait, Label: "Wait for hosts (s)", Type: provider.TypeInt, Overridable: true, Group: provider.GroupOptions,
				Default: DefaultWaitSeconds, Min: provider.IntPtr(0), Max: provider.IntPtr(MaxWaitSeconds),
				Help: "How long a deployment waits for host results; hosts still offline are reported as queued."},
		},
		CredentialFields: []provider.Field{},
		OneOfRequired:    [][]string{{keyHostIDs, keyHostTags}},
	}
}

// ValidateConfig is the save-time check of the rules the descriptors cannot
// express (UUIDs, no "..", unique items).
func (p *Provider) ValidateConfig(config map[string]any) error {
	_, err := ParseConfig(config)
	return err
}

// errNoJob is returned when a provider call lacks the job context.
var errNoJob = errors.New("inventory-agent: job context missing")

func jobMeta(ctx context.Context) (provider.JobMeta, error) {
	m, ok := provider.JobFrom(ctx)
	if !ok || m.TenantID == "" {
		return provider.JobMeta{}, errNoJob
	}
	return m, nil
}

func permanent(msg string) *provider.Result {
	return &provider.Result{Success: false, Permanent: true, Message: msg}
}

// Deploy asks the inventory to deliver the certificate to the selected hosts
// and waits (bounded) for their results. The deployer never holds the key:
// only the certificate id and the selection are sent (FR-006).
func (p *Provider) Deploy(ctx context.Context, cert *provider.CertificateData, config, _ map[string]any, progress provider.ProgressFn) (*provider.Result, error) {
	meta, err := jobMeta(ctx)
	if err != nil || meta.JobID == "" {
		return permanent("job context missing"), nil
	}
	inv, err := p.client()
	if err != nil {
		return nil, err
	}
	cfg, err := ParseConfig(config)
	if err != nil {
		return permanent("configuration invalid: " + err.Error()), nil
	}
	if !cfg.HasSelector() {
		return permanent("configuration incomplete: Hosts or Host tags must be provided by the target"), nil
	}
	name, err := certName(cfg, cert)
	if err != nil {
		return permanent(err.Error()), nil
	}
	d, err := inv.CreateCertificateDelivery(ctx, invsdk.DeliveryRequest{
		TenantID: meta.TenantID, IdempotencyKey: meta.JobID, ConfigurationID: meta.ConfigurationID, TargetID: meta.TargetID,
		Trigger: trigger(meta.Trigger), CertificateID: cert.ID, Name: name, KeyPolicy: cfg.KeyPolicy,
		HostIDs: cfg.HostIDs, HostTags: cfg.HostTags, RearmFailed: true,
	})
	if err != nil {
		if res := refusal(err); res != nil {
			return res, nil
		}
		return nil, fmt.Errorf("inventory-agent: create delivery: %w", err)
	}
	report(progress, 10, "delivery created")
	if len(d.Items) == 0 {
		return evaluate(d, name, cfg.RequireAll), nil
	}
	d = p.wait(ctx, inv, meta.TenantID, d, cfg.WaitSeconds, progress)
	return evaluate(d, name, cfg.RequireAll), nil
}

// wait polls the delivery until every item is settled or the wait (bounded by
// the job deadline minus a margin) ends. Read errors end the wait with the
// last known state.
func (p *Provider) wait(ctx context.Context, inv Inventory, tenantID string, d invsdk.Delivery, waitSeconds int, progress provider.ProgressFn) invsdk.Delivery {
	start := p.now()
	limit := time.Duration(waitSeconds) * time.Second
	if dl, ok := ctx.Deadline(); ok {
		if left := dl.Sub(start) - deadlineMargin; left < limit {
			limit = left
		}
	}
	best := 10
	for {
		elapsed := p.now().Sub(start)
		n := 0
		for _, it := range d.Items {
			if settled(it, elapsed >= offlineGrace) {
				n++
			}
		}
		if pct := 10 + 90*n/len(d.Items); pct > best {
			best = pct
			report(progress, best, fmt.Sprintf("%d of %d hosts settled", n, len(d.Items)))
		}
		if n == len(d.Items) || elapsed >= limit {
			return d
		}
		step := pollInterval
		if rest := limit - elapsed; rest < step {
			step = rest
		}
		if p.sleep(ctx, step) != nil {
			return d
		}
		next, err := inv.GetCertificateDelivery(ctx, tenantID, d.ID)
		if err != nil {
			return d
		}
		d = next
	}
}

func report(progress provider.ProgressFn, pct int, note string) {
	if progress != nil {
		progress(pct, note)
	}
}

// trigger maps the job trigger to the inventory's vocabulary.
func trigger(t string) string {
	switch t {
	case provider.TriggerAutoDeploy, provider.TriggerRetry:
		return t
	default:
		return provider.TriggerManual
	}
}

// refusal maps an inventory refusal that a retry cannot fix to a permanent
// failure; transient errors (unavailable, deadline) return nil.
func refusal(err error) *provider.Result {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.FailedPrecondition, codes.PermissionDenied, codes.NotFound, codes.Unauthenticated:
		msg := st.Message()
		if len(msg) > 200 {
			msg = msg[:200]
		}
		return permanent("inventory refused the delivery: " + msg)
	}
	return nil
}

func certName(cfg Config, cert *provider.CertificateData) (string, error) {
	if cfg.CertName != "" {
		return cfg.CertName, nil
	}
	if n, ok := DefaultName(cert.CommonName); ok {
		return n, nil
	}
	return "", errors.New("certificate name cannot be derived from the common name; set Certificate name")
}

// Fingerprint returns the lowercase hex SHA-256 of the first certificate in
// a PEM bundle (the leaf).
func Fingerprint(certPEM string) (string, error) {
	rest := []byte(certPEM)
	for {
		var blk *pem.Block
		blk, rest = pem.Decode(rest)
		if blk == nil {
			return "", errors.New("inventory-agent: no certificate in PEM")
		}
		if blk.Type != "CERTIFICATE" {
			continue
		}
		if _, err := x509.ParseCertificate(blk.Bytes); err != nil {
			return "", fmt.Errorf("inventory-agent: parse certificate: %w", err)
		}
		sum := sha256.Sum256(blk.Bytes)
		return hex.EncodeToString(sum[:]), nil
	}
}

// Verify compares what the selected hosts last reported for the name with
// the certificate's fingerprint (FR-005).
func (p *Provider) Verify(ctx context.Context, cert *provider.CertificateData, config, _ map[string]any) (*provider.Result, error) {
	meta, err := jobMeta(ctx)
	if err != nil {
		return nil, err
	}
	inv, err := p.client()
	if err != nil {
		return nil, err
	}
	cfg, err := ParseConfig(config)
	if err != nil {
		return &provider.Result{Success: false, Message: "configuration invalid: " + err.Error()}, nil
	}
	if !cfg.HasSelector() {
		return &provider.Result{Success: false, Message: "configuration incomplete: Hosts or Host tags must be provided by the target"}, nil
	}
	name, err := certName(cfg, cert)
	if err != nil {
		return &provider.Result{Success: false, Message: err.Error()}, nil
	}
	fp, err := Fingerprint(cert.CertificatePEM)
	if err != nil {
		return &provider.Result{Success: false, Message: "certificate unavailable for verification"}, nil
	}
	v, err := inv.VerifyHostCertificates(ctx, meta.TenantID, cfg.HostIDs, cfg.HostTags, name, fp)
	if err != nil {
		return nil, fmt.Errorf("inventory-agent: verify: %w", err)
	}
	return verifyResult(v, cfg.RequireAll), nil
}

// Rollback is not supported (as in v3): files are never removed from hosts.
func (p *Provider) Rollback(context.Context, *provider.CertificateData, map[string]any, map[string]any) (*provider.Result, error) {
	return nil, provider.ErrUnsupported
}

// ValidateCredentials validates the configuration and resolves the selection
// (no credentials exist for this provider).
func (p *Provider) ValidateCredentials(ctx context.Context, _ map[string]any, config map[string]any) error {
	_, err := p.Preview(ctx, config)
	return err
}

// Preview resolves the selection as a delivery would ("Preview hosts"); no
// host matching is reported on the selection fields.
func (p *Provider) Preview(ctx context.Context, config map[string]any) (map[string]any, error) {
	meta, err := jobMeta(ctx)
	if err != nil {
		return nil, err
	}
	inv, err := p.client()
	if err != nil {
		return nil, err
	}
	cfg, err := ParseConfig(config)
	if err != nil {
		return nil, err
	}
	if !cfg.HasSelector() {
		return nil, &provider.FieldError{Field: provider.PathConfig + keyHostIDs, Msg: provider.CodeOneOfRequired + ":" + keyHostIDs + "," + keyHostTags}
	}
	pv, err := inv.PreviewCertificateTargets(ctx, meta.TenantID, cfg.HostIDs, cfg.HostTags)
	if err != nil {
		return nil, fmt.Errorf("inventory-agent: preview: %w", err)
	}
	if len(pv.Hosts) == 0 {
		return nil, &provider.FieldError{Field: provider.PathConfig + keyHostIDs, Msg: "no_hosts_matched"}
	}
	hosts := make([]map[string]any, 0, len(pv.Hosts))
	for _, h := range pv.Hosts {
		hosts = append(hosts, map[string]any{
			"host_id": h.HostID, "hostname": h.Hostname, "os_name": h.OSName, "tags": h.Tags,
			"agent_online": h.AgentOnline, "capability": h.Capability,
		})
	}
	unknown := pv.UnknownHostIDs
	if unknown == nil {
		unknown = []string{}
	}
	return map[string]any{"matched_hosts": hosts, "unknown_host_ids": unknown, "truncated": pv.Truncated}, nil
}

var (
	_ provider.Provider        = (*Provider)(nil)
	_ provider.ConfigValidator = (*Provider)(nil)
	_ provider.Previewer       = (*Provider)(nil)
)
