# Deployer service — operations

The **deployer** is a tenant-scoped certificate *deployment* module. It pulls
certificates issued by **lcm** and installs them onto infrastructure targets via
pluggable providers, and auto-deploys them when lcm emits certificate lifecycle
events. It registers with the application gateway (browser API under
`/api/deployer`) and exposes a service-to-service gRPC API (`deployer.v1`, not
gateway-proxied).

## Running

```
deployersvc -config deploy/container.yaml     # run (applies migrations)
deployersvc bootstrap -config <cfg>           # apply migrations and exit
```

In the containerized platform stack it comes up with one command; see
`deploy/stack/README.md`. The service:

- enrolls for its SVID (`spiffe://<td>/svc/deployer`) over lcm's enrollment gRPC,
- migrates its TimescaleDB schema (per-tenant row-level security),
- serves the browser API (via the gateway) and `deployer.v1` gRPC on `:9945`,
  with a health/readiness admin endpoint on `:9690`,
- registers its routes, permissions, CASL abilities and nav with the gateway,
- seeds its API permissions into auth and grants them to the built-in roles.

## Configuration

`container.yaml` sections: `db`, `valkey`, `kek` (envelope key), `jobs` (worker
pool: workers, interval, lease, job timeout, retry policy, backoff, cleanup),
`events` (auto-deploy consumer), `gateway`, `lcm` (`service: lcm` — the mesh
peer certificates are fetched from), and `enroll` (SVID enrollment).

## Providers

A provider installs a certificate onto one endpoint. Each declares its required
config and credential fields and whether it supports verify/rollback. Shipped
providers (self-registered via `internal/providers/all`):

| Type        | Config       | Credentials                          | Verify | Rollback |
|-------------|--------------|--------------------------------------|:------:|:--------:|
| `aws_acm`   | `region`     | `access_key_id`, `secret_access_key` | ✓      | —        |
| `cloudflare`| `zone_id`    | `api_token`                          | ✓      | —        |
| `bigip`     | `partition`  | `host`, `username`, `password`       | ✓      | ✓        |
| `fortigate` | `vdom`       | `host`, `api_token`                  | ✓      | ✓        |
| `webhook`   | `url`        | `token`/`secret` (optional)          | ✓      | ✓        |
| `dummy`     | —            | —                                    | ✓      | ✓        |

`aws_acm` calls ACM `ImportCertificate`/`DescribeCertificate` over hand-rolled
AWS SigV4 (no AWS SDK dependency). The rest use stdlib `net/http`. Providers make
real outbound calls to third-party infrastructure at deploy time. `GET
/api/deployer/v1/providers` returns the live catalogue with required fields.

## Certificate acquisition

The deployer **never stores certificate bytes** (SR-002). At deploy time it
fetches the certificate + chain + retained private key from lcm over SPIFFE mTLS
via `lcm.v1.Certificates/Download` (keyed by the lcm certificate id), hands the
bundle to the provider, and drops it. lcm's `deploy/policy.yaml` must allow the
deployer's SPIFFE id to call that RPC.

## Auto-deploy

The event consumer subscribes to the platform event bus
(`platform:events:<tenant>` Valkey stream) for `certificate.issued` and
`certificate.renewed`. On an event it fetches the certificate, AND-matches it
against each auto-deploy target's filters (issuer/org/OU/country exact;
common-name/SAN by RE2 regex), and for every match with ≥1 attached
configuration spawns a parent job + one child per configuration (trigger `event`
or `auto_renewal`). Empty filters match all; self-published events are ignored.

## Deployment jobs

Deploying to a target group creates a **parent** job that spawns one **child**
per attached configuration; deploying to a single configuration creates a
**direct** job. A distributed worker pool claims due jobs (single-winner lease),
runs child/direct jobs (parents aggregate: completed/failed/partial), and
retries with exponential backoff up to `max_retries`. Live progress is published
as `deployment.started` / `deployment.completed` / `deployment.failed` for the
gateway SSE hub, so the UI updates without polling. A cleanup worker deletes jobs
older than the configured retention.

## Security notes

- **Credentials** are sealed with envelope encryption (the `sealed` package +
  KEK) and never returned in any read, list, or export unless explicitly
  requested; a set credential is reported only as a `__set__` marker /
  `has_credentials` flag. The audit trail drops any detail key matching
  key/secret/token/password/private/csr.
- **Tenant isolation** is enforced by per-tenant PostgreSQL row-level security;
  trusted worker paths run with a system scope.
- **Config overrides** on a target attachment carry provider config only, never
  credentials (credential-shaped override keys are rejected).
- **Audit** is append-only and tamper-evident, recording actor (SVID or platform
  user), tenant, outcome and reason for every operation.

## Backup

`POST /api/deployer/v1/backup/export` exports the tenant's targets,
configurations (credentials only when `include_credentials` is set — as the
already-sealed blob, never plaintext) and job metadata, versioned by schema.
`POST /api/deployer/v1/backup/import` recreates them (mode `skip` or
`overwrite`), preserving entity ids so sealed credentials unseal unchanged.
