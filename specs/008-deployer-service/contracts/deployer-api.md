# Phase 1 Contracts: Deployer Service

Interface contracts for the deployer module. HTTP is the gateway-proxied browser surface
(declared in `api/openapi/deployer.yaml`, prefix `/api/deployer/v1`, each route carrying
`x-freya-permission`); gRPC is module-to-module (not gateway-proxied). Shapes below are
the contract; field-level schemas are enforced by OpenAPI/proto (Constitution III).

## 1. HTTP API (browser, via gateway) — `/api/deployer/v1`

Permissions: `configurations:read|manage`, `targets:read|manage`, `jobs:read|manage`,
`deploy:execute`, `stats:read`, `backup:manage`. All mutating routes take the CSRF
parameter; all responses redact credentials.

### Target configurations
| Method | Path | Permission | Purpose |
|--------|------|-----------|---------|
| GET | `/configurations` | configurations:read | list (filter provider_type, status; paginate) |
| POST | `/configurations` | configurations:manage | create (provider_type, config, credentials) |
| GET | `/configurations/{id}` | configurations:read | get (credentials redacted) |
| PUT | `/configurations/{id}` | configurations:manage | update (credentials optional; set-marker keeps) |
| POST | `/configurations/{id}/remove` | configurations:manage | delete |
| POST | `/configurations/validate` | configurations:manage | ValidateCredentials (provider_type + creds + optional config) |
| GET | `/providers` | configurations:read | ListProviders (display_name, supports_verify/rollback, required config+cred fields) |

### Deployment targets
| Method | Path | Permission | Purpose |
|--------|------|-----------|---------|
| GET | `/targets` | targets:read | list |
| POST | `/targets` | targets:manage | create (name, auto_deploy, filters) |
| GET | `/targets/{id}` | targets:read | get (optionally with configurations) |
| PUT | `/targets/{id}` | targets:manage | update |
| POST | `/targets/{id}/remove` | targets:manage | delete |
| POST | `/targets/{id}/configurations` | targets:manage | attach configurations (+per-config overrides) |
| POST | `/targets/{id}/configurations/remove` | targets:manage | detach configurations |
| GET | `/targets/{id}/configurations` | targets:read | list a target's configurations |

### Deployment
| Method | Path | Permission | Purpose |
|--------|------|-----------|---------|
| POST | `/deploy` | deploy:execute | deploy a cert to one configuration → DIRECT job (202) |
| POST | `/deploy/target` | deploy:execute | deploy to a target group → PARENT + CHILD jobs (202) |
| POST | `/deploy/configurations` | deploy:execute | deploy to many configs → per-config results (202) |
| POST | `/deploy/{jobId}/verify` | deploy:execute | verify a completed deployment (optional target context) |
| POST | `/deploy/{jobId}/rollback` | deploy:execute | rollback (provider-dependent) |

Deploy responses are **202 Accepted** (`{status:"processing", job_id}`) — issuance/
deployment is async; the browser learns the outcome over SSE.

### Jobs
| Method | Path | Permission | Purpose |
|--------|------|-----------|---------|
| GET | `/jobs` | jobs:read | list (filters: target/config/cert/status/trigger/job_type/parent/time range/paginate) |
| GET | `/jobs/{id}` | jobs:read | status (optional include_child_jobs) |
| GET | `/jobs/{id}/result` | jobs:read | result + per-action history |
| POST | `/jobs/{id}/cancel` | jobs:manage | cancel (optional cascade to children) |
| POST | `/jobs/{id}/retry` | jobs:manage | retry (optional failed-children-only, force) |

### Statistics & backup
| Method | Path | Permission | Purpose |
|--------|------|-----------|---------|
| GET | `/statistics` | stats:read | system-wide + (admin) per-tenant metrics |
| GET | `/statistics/tenant` | stats:read | current tenant's metrics |
| POST | `/backup/export` | backup:manage | export (include_credentials flag) |
| POST | `/backup/import` | backup:manage | import (mode: skip|overwrite) |

## 2. gRPC (module-to-module) — `deployer.v1`

Peer-verified by SPIFFE mTLS. Mirrors the HTTP capabilities for other modules/agents:
- `DeploymentService`: Deploy, DeployToTarget, DeployToConfigurations, Verify, Rollback.
- `DeploymentJobService`: CreateJob, GetJobStatus, GetJobResult, ListJobs, CancelJob,
  RetryJob.
- `DeploymentTargetService`: Create/Get/List/Update/Delete, Add/RemoveConfigurations,
  ListTargetConfigurations.
- `TargetConfigurationService`: Create/Get/List/Update/Delete, ValidateCredentials,
  ListProviders.
- `DeployerStatisticsService`: GetStatistics, GetTenantStatistics.

## 3. Provider interface (internal contract, `internal/provider`)

```
type Provider interface {
    Deploy(ctx, *CertificateData, config map[string]any, creds map[string]any, progress func(int, string)) (*Result, error)
    Verify(ctx, *CertificateData, config, creds) (*Result, error)
    Rollback(ctx, *CertificateData, config, creds) (*Result, error)
    ValidateCredentials(ctx, creds, config) error
    Capabilities() Capabilities   // SupportsVerify, SupportsRollback, RequiredConfigFields, RequiredCredFields, DisplayName
}
```
- `CertificateData`: ID, SerialNumber, CommonName, SANs, CertificatePEM, PrivateKeyPEM,
  CertificateChain, ExpiresAt.
- Registry: `Register(type, factory)`, `Get(type)`, `List()`, `Info(type)`; thread-safe,
  providers self-register in `init()`.
- Providers + fields: aws_acm (config: region; creds: access_key_id, secret_access_key;
  verify✓ rollback✗), cloudflare (config: zone_id; creds: api_token; verify✓ rollback✗),
  bigip (config: partition; creds: host, username, password; verify✓ rollback✓),
  fortigate (config: vdom; creds: host, api_token; verify✓ rollback✓), webhook (config:
  url; verify✓ rollback✓), dummy (none; verify✓ rollback✓).
- Contract rules: never log/return creds; honour ctx timeout; report progress; return a
  client-safe error; `ValidateCredentials` performs a real, side-effect-free check.

## 4. Event contract

### Consumed (auto-deploy) — from the shared `platform:events:<tenant>` Valkey stream
- Types: `certificate.issued`, `certificate.renewed`.
- Envelope (as produced by lcm): `{ id, type, module, to, at, data }`; `data` today =
  `{ certificate_id, spiffe_id, not_after }`. The deployer treats `data` as untrusted,
  uses `certificate_id` to fetch the cert from lcm, and reads issuer/CN/SAN/org from the
  fetched cert for filter matching (fetch-to-match). Events with `module == "deployer"`
  are ignored (loop guard).

### Published (realtime UI) — to `platform:events:<tenant>`
- `deployment.started`, `deployment.completed`, `deployment.failed`, `job.updated` with
  `{ job_id, target_id?, configuration_id?, status, progress, error? }`. No secrets.

## 5. lcm client contract (`internal/lcmclient`)

- `FetchCertificate(ctx, certOrJobID, includePrivateKey) (CertificateBundle, error)` over
  SPIFFE mTLS to lcm. Preferred: lcm module gRPC (issued-certificate get / job result).
  Fallback: lcm HTTP `GET /api/lcm/v1/certificates/{id}` + `/certificates/{id}/key` over
  mTLS. Returns cert PEM + chain + (optional) key PEM + subject/SANs/serial/expiry.
- The deployer authenticates as its own SVID; lcm authorises via its Zanzibar `use`/`read`
  on the certificate (or a module-trust grant). Never caches key material.

## Contract tests (Constitution IV)

- OpenAPI: document parses; every declared route is mounted; no unsafe verbs; credential
  fields never appear in any response body.
- gRPC: every proto RPC is registered and peer-auth-gated.
- Provider: each registered provider's `Info()` lists its declared fields; `Capabilities`
  match the table; `ValidateCredentials` and `Deploy` never emit creds (asserted via a
  log/response scrubber).
- Events: malformed/oversized/foreign-tenant/self-originated events are rejected/ignored
  (negative tests); the filter matcher has fuzz + ReDoS tests.
