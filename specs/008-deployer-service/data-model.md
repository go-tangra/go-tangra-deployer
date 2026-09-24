# Phase 1 Data Model: Deployer Service

Storage: TimescaleDB (Postgres). Every table carries `tenant_id` and a per-tenant RLS
policy (as `services/lcm/internal/store/migrations/0003_rls.sql`). IDs are UUIDv7-style
strings (`store.NewID()`), timestamps are `timestamptz`. Sealed columns are `bytea`
(envelope-encrypted); public/redacted projections carry a set-marker, never the secret.

## Entities

### DeploymentTarget (table `deployer_targets`)
A tenant-scoped group of endpoints + auto-deploy filter rules.

| Field | Type | Notes |
|-------|------|-------|
| id | uuid PK | |
| tenant_id | uuid | RLS scope |
| name | text | unique per `(tenant_id, name)` |
| description | text | |
| auto_deploy | bool | default false |
| certificate_filters | jsonb | `[]CertificateFilter` (see embedded type) |
| config_overrides | jsonb | `map[configuration_id]map[string]any` — provider CONFIG only, never credentials |
| created_by / updated_by | text | actor id |
| created_at / updated_at | timestamptz | |

Relationships: M:N `DeploymentTarget` ↔ `TargetConfiguration` via
`deployer_target_configs_link (target_id, configuration_id)`; 1:N to parent
`DeploymentJob`.

### CertificateFilter (embedded in `certificate_filters`)
AND-matched; an empty filter set matches all.

| Field | Type | Match |
|-------|------|-------|
| issuer_name | text | exact |
| common_name_pattern | text | regex/glob |
| san_pattern | text | regex/glob |
| subject_organization | text | exact |
| subject_org_unit | text | exact |
| subject_country | text | exact |

### TargetConfiguration (table `deployer_configs`)
A single deployment endpoint.

| Field | Type | Notes |
|-------|------|-------|
| id | uuid PK | |
| tenant_id | uuid | RLS scope; unique `(tenant_id, name)` |
| name | text | |
| description | text | |
| provider_type | text | one of the registered providers; validated |
| config | jsonb | provider-specific, non-secret |
| credentials_sealed | bytea | envelope-sealed; never returned |
| status | text | `active` \| `inactive` \| `error` (default active) |
| status_message | text | |
| last_deployment_at | timestamptz | nullable |
| created_by / updated_by / created_at / updated_at | | |

### DeploymentJob (table `deployer_jobs`)
Central parent/child/direct hierarchy.

| Field | Type | Notes |
|-------|------|-------|
| id | uuid PK | |
| tenant_id | uuid | RLS scope |
| deployment_target_id | uuid null | set for PARENT jobs |
| target_configuration_id | uuid null | set for CHILD/DIRECT jobs |
| parent_job_id | uuid null | set for CHILD jobs (self-ref) |
| certificate_id | text | lcm cert id or job id |
| certificate_serial | text | |
| status | text | `pending`\|`processing`\|`completed`\|`failed`\|`cancelled`\|`retrying`\|`partial` (default pending) |
| status_message | text | |
| progress | int | 0–100 |
| retry_count | int | default 0 |
| max_retries | int | default 3 |
| triggered_by | text | `manual`\|`event`\|`auto_renewal` |
| result | jsonb | provider/aggregation result |
| lease_until | timestamptz null | worker claim lease |
| started_at / completed_at / next_retry_at | timestamptz null | |
| created_at / updated_at | timestamptz | |

Job **type** is computed: PARENT if `deployment_target_id` set and `parent_job_id` null;
CHILD if `parent_job_id` set; DIRECT otherwise. Indexes: `(tenant_id,status,next_retry_at)`,
a partial index on due/pending jobs, `(parent_job_id)`.

State machine: `pending → processing → {completed | failed | partial | retrying}`;
`retrying → processing`; any non-terminal → `cancelled`. Parent status is derived from
children (all completed → completed; all failed → failed; mixed → partial; else
processing with progress %). Only child/direct jobs run a provider.

### DeploymentHistory (table `deployer_history`)
Per-job action record.

| Field | Type | Notes |
|-------|------|-------|
| id | uuid PK | |
| tenant_id | uuid | RLS scope |
| job_id | uuid FK | → deployer_jobs |
| action | text | `deploy` \| `verify` \| `rollback` |
| result | text | `success` \| `failure` \| `partial` |
| message | text | client-safe |
| duration_ms | int | |
| details | jsonb | non-secret |
| created_at | timestamptz | |

### Grant (Zanzibar) — reuse the platform authz store pattern
Resource types: `deployer_target`, `deployer_configuration`, `deployer_job`.
Relations: `owner`, `editor`, `viewer`; actions map read/manage/deploy. Tenant-wide
grants supported (subject = tenant). (Same shape as lcm's `grants`.)

### Audit — reuse the framework append-only audit stream
Event types: target/configuration created/updated/deleted; deployment started/completed/
failed; job created/cancelled/retried; verify/rollback; credential validated; backup
export/import. Fields: actor kind/id, tenant, subject kind/id, outcome, reason, details
(redacted). No credential or key material.

## Transient (not stored)

### CertificateBundle
Fetched from lcm per deploy; held only for the deploy call. Fields: id, serial, common
name, SANs, certificate PEM, private key PEM, chain PEM, expires_at. Never written to
`deployer_*` tables.

## Validation rules (from spec)

- `name` 1–100 chars, unique per tenant (targets, configurations).
- `provider_type` ∈ registered set; unknown rejected (FR-007).
- `config_overrides` values are provider config only; a credentials-shaped key is
  rejected (FR-009, SR-002).
- `certificate_filters` patterns bounded in length; regex compiled with a complexity/time
  bound (SR-004).
- `progress` 0–100; `max_retries` ≥ 0 and bounded.
- credentials never present in any read/list/export-without-flag/log/error (FR-002,
  SR-002).

## Migrations (goose, mirroring lcm)

1. `0001_schema.sql` — the six tables + grants + link table + indexes.
2. `0002_hypertables.sql` — history/audit as time-series hypertables (optional, like lcm).
3. `0003_rls.sql` — enable RLS + per-tenant policies on every `deployer_*` table + grants
   to the app role.
