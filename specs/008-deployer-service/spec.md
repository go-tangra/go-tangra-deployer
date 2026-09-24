# Feature Specification: Deployer Service

**Feature Branch**: `008-deployer-service`

**Created**: 2026-09-19

**Status**: Draft

**Input**: User description: "Create a new service under services/ named `deployer`, replicating the functionality of go-tangra-deployer as a Freya platform module: a tenant-scoped certificate deployment service that distributes TLS certificates (issued by the lcm service) to infrastructure targets via pluggable providers, and auto-deploys certificates when lcm emits certificate lifecycle events."

## Overview

The **deployer** is a tenant-scoped certificate **deployment** service. Where the lcm
service *issues* certificates, the deployer *distributes* them: it pushes an issued TLS
certificate (with its chain and private key) to one or more infrastructure endpoints —
a cloud certificate store, a load balancer, a firewall, a DNS/WAF edge, or a generic
webhook — through pluggable **providers**. It can do this on demand (an operator clicks
"deploy") or automatically the moment lcm issues or renews a matching certificate.

It is a platform module: it registers with the application gateway, authenticates
callers with the platform's service identities and end-user tokens, isolates every
tenant's data, seals provider credentials at rest, and ships a browser UI as a
composed remote. It never stores certificate bytes; it fetches them from lcm at
deployment time.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Register a deployment endpoint and deploy a certificate to it (Priority: P1)

An operator adds a **target configuration** — an endpoint description consisting of a
provider type (e.g. Cloudflare), the provider's configuration (e.g. a zone), and the
credentials to reach it — validates the credentials, then manually deploys a specific
issued certificate to that endpoint and watches the deployment succeed.

**Why this priority**: This is the core value of the service — getting a real
certificate onto a real endpoint. Without it nothing else matters; it is the MVP.

**Independent Test**: Create a configuration for the built-in test ("dummy") provider,
validate its credentials, deploy a known issued certificate to it, and confirm a
completed deployment job with a success history entry — all without any target group,
auto-deploy, or additional provider.

**Acceptance Scenarios**:

1. **Given** a signed-in operator with deploy permission, **When** they create a target
   configuration with a supported provider type and valid credentials, **Then** the
   configuration is stored with status "active" and its credentials are never returned.
2. **Given** an existing active configuration, **When** the operator deploys a specific
   issued certificate to it, **Then** a deployment job is created, the certificate is
   fetched from lcm, handed to the provider, and the job reaches "completed" with a
   recorded deploy-action history entry and a duration.
3. **Given** a configuration whose credentials are wrong, **When** the operator runs
   credential validation, **Then** the service reports the failure with a client-safe
   message and does not persist the endpoint as active.

---

### User Story 2 - Auto-deploy on certificate issuance/renewal (Priority: P1)

An operator groups endpoints into a **deployment target** and attaches
**certificate-filter rules** (which issuer, common name, SANs, organization a
certificate must match). From then on, whenever lcm issues or renews a certificate
that matches, the deployer deploys it to every endpoint in the group with no human
action.

**Why this priority**: Automated, event-driven distribution is the defining feature of
the service and the main reason it exists as a separate module rather than a manual
button in lcm.

**Independent Test**: Create a target with one filter and one attached configuration
and auto-deploy enabled; emit (or cause) a matching certificate issuance; confirm a
parent deployment job with a child job per configuration is created automatically with
trigger "event" and completes.

**Acceptance Scenarios**:

1. **Given** a target with auto-deploy enabled and a filter matching an issuer/common
   name, **When** lcm publishes a matching certificate-issued event for the tenant,
   **Then** the deployer creates a parent job plus one child job per attached
   configuration, each marked triggered-by "event", and deploys.
2. **Given** the same target, **When** a certificate-renewed event arrives, **Then**
   jobs are created marked triggered-by "auto-renewal".
3. **Given** a certificate that matches no target's filters, **When** its event
   arrives, **Then** no deployment job is created.
4. **Given** a target with an empty filter list, **When** any certificate event for the
   tenant arrives, **Then** the target matches (empty filters match all) provided it
   has at least one attached configuration.

---

### User Story 3 - Track, retry, verify and roll back deployment jobs (Priority: P2)

An operator monitors deployment jobs, drills into a failed one to read its per-action
history, retries the failed parts, verifies a live deployment against the endpoint,
and — where the provider supports it — rolls back to the previous certificate.

**Why this priority**: Deployments to third-party infrastructure fail transiently;
operators need visibility and recovery without which the automation is untrustworthy.

**Independent Test**: Force a child job to fail (dummy provider set to error), observe
the parent job become "partial"/"failed", retry the failed children, and confirm the
job completes and records the retry in history.

**Acceptance Scenarios**:

1. **Given** a parent job with several child jobs where some failed, **When** the
   operator views the job, **Then** the parent shows "partial", each child shows its
   own status and progress, and the history lists each attempt.
2. **Given** a failed job with retries remaining, **When** the operator retries it
   (optionally failed-children-only), **Then** the affected children are re-queued and
   re-executed.
3. **Given** a completed deployment on a provider that supports verification, **When**
   the operator runs verify, **Then** the service confirms the deployed certificate is
   present at the endpoint and records a verify-action history entry.
4. **Given** a completed deployment on a provider that supports rollback, **When** the
   operator rolls back, **Then** the endpoint is restored to the previous certificate
   and a rollback-action history entry is recorded.

---

### User Story 4 - Multiple endpoints and shared credentials with per-endpoint overrides (Priority: P2)

An operator deploys one certificate to several endpoints that share a single credential
but differ in configuration (e.g. the same Cloudflare token across several zones, or
one BIG-IP credential across partitions), using per-attachment configuration overrides
so the credential is stored once.

**Why this priority**: Real deployments fan out to many endpoints; forcing a separate
credential per endpoint is error-prone and leaks secrets across more rows than needed.

**Independent Test**: Attach one configuration to a target with an override (or attach
several configurations) and deploy; confirm each child job applies its effective
(overridden) configuration and the shared credential is opened once per child.

**Acceptance Scenarios**:

1. **Given** a target with a configuration attached and a per-attachment config
   override, **When** a deployment runs, **Then** the child job deploys with the
   override layered over the configuration's base config.
2. **Given** a config override, **When** it is stored, **Then** it contains only
   provider configuration, never credentials.

---

### User Story 5 - Discover providers, view statistics, export/import (Priority: P3)

An operator sees which providers are available and what each needs, views deployment
statistics (success rates, jobs by status, configurations by provider), and
exports/imports the tenant's targets and configurations for backup or migration.

**Why this priority**: Operability and portability features; valuable but not required
to deploy a certificate.

**Independent Test**: Call the provider catalogue and confirm each provider lists its
required config and credential fields and its verification/rollback support; view the
statistics dashboard; export a tenant's data and re-import it into an empty tenant.

**Acceptance Scenarios**:

1. **Given** any operator, **When** they list providers, **Then** each entry gives a
   display name, whether it supports verification and rollback, and its required
   configuration and credential fields.
2. **Given** deployment activity, **When** the operator views statistics, **Then** they
   see jobs by status and trigger type, last-24h and last-7d success rates, target and
   configuration counts, and recent errors.
3. **Given** an export of a tenant's targets and configurations, **When** it is
   imported into another tenant, **Then** targets and configurations are recreated,
   duplicates are skipped or overwritten per the chosen mode, and credentials are
   included only when explicitly requested.

---

### Edge Cases

- **No configurations attached**: a target that matches a certificate but has no
  attached configuration produces no child jobs and no deployment (an auto-deploy
  match requires at least one configuration).
- **Concurrent workers**: two service instances must never run the same job twice; a job
  is claimed by exactly one worker.
- **Certificate unavailable at deploy time**: if lcm cannot return the certificate for
  the referenced id (not yet completed, revoked, deleted), the job fails with a
  client-safe reason and is retried within the retry budget.
- **Provider does not support rollback/verify**: verify/rollback requests against such a
  provider are rejected with a clear "unsupported" outcome, not a silent success.
- **Self-triggered loops**: a certificate event that the deployer itself caused (e.g. a
  provider that re-publishes) must not trigger another round of deployments.
- **Regex filter safety**: a malformed or catastrophically slow filter pattern must not
  hang matching or crash the service.
- **Credential exposure**: credentials must never appear in API responses, logs, audit
  records, backups (unless explicitly requested), or error messages.
- **Retry exhaustion**: a job that exhausts its retries is marked failed and its parent
  aggregates to failed/partial; it is not retried forever.
- **Cross-tenant isolation**: an operator of one tenant can never read, deploy, or
  target another tenant's certificates, endpoints, or jobs.

## Requirements *(mandatory)*

### Functional Requirements

**Target configurations (endpoints)**

- **FR-001**: Operators MUST be able to create, read, update, and delete target
  configurations, each identifying a provider type, a provider configuration, and
  credentials; names MUST be unique within a tenant.
- **FR-002**: The system MUST seal configuration credentials at rest and MUST NOT return
  them in any read, list, export (unless explicitly requested), log, or error; a
  redaction marker MUST indicate a credential is set.
- **FR-003**: Operators MUST be able to validate a configuration's credentials against
  the provider (optionally with configuration context such as a zone), receiving a
  clear pass/fail without exposing the credential.
- **FR-004**: A configuration MUST carry a status (active, inactive, error) with a
  human-readable status message and a last-deployment timestamp.

**Providers**

- **FR-005**: The system MUST expose a catalogue of available providers; each entry MUST
  declare a display name, whether it supports verification and rollback, and its
  required configuration and credential fields.
- **FR-006**: The system MUST support the providers: AWS Certificate Manager, Cloudflare,
  F5 BIG-IP, FortiGate, a generic Webhook, and a test/dummy provider; each MUST accept
  its documented configuration and credential fields.
- **FR-007**: The system MUST select the provider for a deployment from the target
  configuration's provider type and MUST reject an unknown provider type.

**Deployment targets (groups)**

- **FR-008**: Operators MUST be able to create, read, update, and delete deployment
  targets, each with a name unique per tenant, an auto-deploy flag, and a set of
  certificate-filter rules.
- **FR-009**: Operators MUST be able to attach and detach configurations to/from a
  target, list a target's configurations, and set per-attachment configuration
  overrides; overrides MUST contain only provider configuration, never credentials.
- **FR-010**: Certificate-filter rules MUST support AND-matching on issuer name (exact),
  common name (pattern), SANs (pattern), and subject organization, organizational unit,
  and country (exact); an empty filter set MUST match every certificate.

**Manual deployment**

- **FR-011**: Operators MUST be able to deploy a specified issued certificate to a
  single configuration (a direct job), to a whole target group (a parent job with one
  child per configuration), or to several configurations at once (returning a per-config
  result).
- **FR-012**: The system MUST fetch the certificate, chain, and private key from the lcm
  service at deployment time using the certificate or job identifier; it MUST NOT persist
  the certificate bytes.

**Auto-deployment**

- **FR-013**: The system MUST subscribe to the platform's certificate lifecycle events
  (issued and renewed) per tenant and, for each event, match it against the tenant's
  auto-deploy-enabled targets' filters.
- **FR-014**: For each matching target the system MUST create a parent job and one child
  job per attached configuration, marked triggered-by "event" for issuance or
  "auto-renewal" for renewal, with no human action.
- **FR-015**: The system MUST ignore certificate events it itself originated so a
  deployment cannot cause an unbounded loop.

**Job lifecycle & execution**

- **FR-016**: The system MUST execute deployment jobs asynchronously through a pool of
  workers; a job MUST be claimed and executed by exactly one worker even across multiple
  service instances.
- **FR-017**: A job MUST track a status (pending, processing, completed, failed,
  cancelled, retrying, partial), a status message, a progress percentage, a retry count
  against a maximum, a trigger type, timestamps, and a result.
- **FR-018**: Parent jobs MUST NOT execute a provider; they MUST aggregate their child
  jobs' outcomes into processing (with a progress percentage), completed (all children
  succeeded), failed (all failed), or partial (mixed).
- **FR-019**: On a child/direct job failure the system MUST retry with an exponential
  backoff up to the configured maximum, then mark the job failed and update its parent.
- **FR-020**: Operators MUST be able to list jobs with filters (target, configuration,
  certificate, status, trigger, job type, parent, time range, pagination), get a job's
  status (optionally with its children), get a job's result with per-action history, and
  cancel a job (optionally cascading to children).
- **FR-021**: Operators MUST be able to retry a job (optionally failed-children-only),
  with an operator-only "force" option to override normal retry limits.
- **FR-022**: The system MUST delete deployment jobs older than a configurable retention
  window.

**Verify & rollback**

- **FR-023**: Operators MUST be able to verify a completed deployment against the
  endpoint on providers that support it, and MUST receive an "unsupported" outcome on
  providers that do not.
- **FR-024**: Operators MUST be able to roll back a deployment to a previous certificate
  on providers that support it, and MUST receive an "unsupported" outcome on providers
  that do not.

**History, audit, statistics, backup**

- **FR-025**: The system MUST record a per-job history of each deploy, verify, and
  rollback action with its result (success, failure, partial), message, and duration.
- **FR-026**: The system MUST write every state-changing operation to an append-only,
  tamper-evident audit trail carrying the actor identity, tenant, outcome, and reason.
- **FR-027**: The system MUST provide per-tenant and system-wide statistics: jobs by
  status and by trigger type, last-24h and last-7d breakdowns with success rates, target
  counts, configuration counts by status and by provider type, recent errors, and a
  per-tenant breakdown for administrators.
- **FR-028**: The system MUST support per-tenant export and import of targets,
  configurations (credentials only when explicitly requested), job metadata, and
  permissions, with duplicate handling (skip or overwrite).

**Access, tenancy, platform**

- **FR-029**: Access MUST be governed by fine-grained, tenant-scoped permissions: a
  subject may read, manage, or deploy targets, configurations, and jobs; tenant-wide
  grants MUST be supported; a distinct "deploy" permission gates both manual and
  automatic deployment.
- **FR-030**: All data MUST be isolated per tenant such that no operation can read or
  affect another tenant's data.
- **FR-031**: The system MUST register itself with the application gateway (its routes,
  API permissions, and UI abilities) and MUST expose a browser UI, composed by the
  platform shell, offering a dashboard, a deployment-targets manager, a
  target-configurations manager, and a deployment-jobs viewer.
- **FR-032**: The browser UI MUST reflect deployment outcomes in real time (a
  deployment appearing, progressing, and completing or failing) without a manual
  refresh.

### Security Requirements *(mandatory — Constitution: Development Workflow)*

- **Trust boundaries crossed**:
  - Browser → gateway → deployer (end-user requests carrying a platform token).
  - deployer ↔ lcm and deployer ↔ auth (service-to-service, mutually authenticated
    service identities).
  - deployer → external provider endpoints (outbound to third-party infrastructure over
    the network, using tenant-supplied credentials).
  - deployer ← platform event bus (inbound certificate lifecycle events).
- **Data classification**: provider credentials (secret — cloud keys, API tokens,
  device passwords); private keys of deployed certificates (secret, transient — fetched
  and handed to a provider, never stored); target/configuration/job metadata
  (internal, tenant-scoped); audit records (internal, integrity-protected).
- **Authentication/Authorization**: service-to-service calls MUST use verified service
  identities; browser calls MUST carry a valid platform token verified by the platform;
  every operation MUST pass a tenant-scoped, fine-grained permission check enforced
  before the handler runs.
- **Threat scenarios**: credential exfiltration via API/log/backup/error leakage; a
  tenant reaching another tenant's endpoints or certificates; forged or replayed
  certificate events triggering unwanted deployments; a malicious filter pattern causing
  denial of service; an oversized or malformed provider response; a private key lingering
  in memory, logs, or storage; an operator escalating via the "force" retry path.
- **SR-001**: The system MUST reject any request that lacks a valid service identity or
  platform token, and MUST enforce a tenant-scoped permission check on every operation
  before acting.
- **SR-002**: The system MUST seal provider credentials with envelope encryption at rest
  and MUST never emit credentials or fetched private keys in responses, logs, audit
  records, error messages, or backups (unless a credential export is explicitly
  requested).
- **SR-003**: The system MUST scope every read and write to the caller's tenant so no
  operation can observe or affect another tenant's data.
- **SR-004**: The system MUST validate all inputs — provider types, filter patterns,
  configuration/credential shapes, event payloads, and pagination — against an explicit
  schema and bound their size; filter-pattern evaluation MUST be time- or
  complexity-bounded so a hostile pattern cannot hang matching.
- **SR-005**: The system MUST only act on certificate events for the event's own tenant,
  MUST ignore events it originated, and MUST treat event payloads as untrusted input.
- **SR-006**: The system MUST bound each deployment attempt with a timeout and each job
  with a maximum retry count, and MUST cap outbound provider interactions so a slow or
  hostile endpoint cannot exhaust workers.

### Key Entities *(include if feature involves data)*

- **Deployment Target**: a named, tenant-scoped group of endpoint configurations plus
  certificate-filter rules and an auto-deploy flag; holds per-attachment configuration
  overrides. Relates many-to-many to Target Configurations and one-to-many to parent
  Deployment Jobs.
- **Certificate Filter**: embedded match criteria (issuer, common-name pattern, SAN
  pattern, organization, organizational unit, country) evaluated with AND logic to
  decide which certificates auto-deploy to a target.
- **Target Configuration**: a single deployment endpoint — provider type, provider
  configuration, sealed credentials, status and status message, last-deployment time;
  unique per tenant by name.
- **Provider**: a pluggable deployment backend (AWS ACM, Cloudflare, BIG-IP, FortiGate,
  Webhook, Dummy) that deploys, and optionally verifies and rolls back, a certificate;
  declares its required configuration and credential fields and its capabilities.
- **Deployment Job**: a unit of deployment work — parent (a group, aggregates children),
  child (one per configuration, executes a provider), or direct (a single configuration).
  Carries the referenced certificate id/serial, status, progress, retry count, trigger
  type, result, and timestamps; relates to a target, a configuration, a parent, and
  history.
- **Deployment History**: a per-job record of a deploy, verify, or rollback action with
  its result, message, duration, and details.
- **Certificate Bundle**: the transient certificate, chain, and private key fetched from
  lcm at deploy time and handed to a provider; never persisted.
- **Audit Record**: an append-only, tamper-evident entry for every state-changing
  operation, with actor, tenant, operation, outcome, and reason.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An operator can register an endpoint, validate it, and manually deploy an
  issued certificate to it in under 3 minutes without ever seeing or handling the stored
  credential.
- **SC-002**: After a matching certificate is issued or renewed, the corresponding
  deployment begins automatically with no human action in at least 99% of matching
  events, and a deployment job is visible within seconds.
- **SC-003**: A single deployment request to a group of endpoints fans out to every
  endpoint, and the operator can see per-endpoint success or failure for 100% of the
  endpoints in the group.
- **SC-004**: When a deployment to an endpoint fails transiently, the system retries and
  ultimately succeeds without operator intervention in the majority of cases, and every
  attempt is visible in the job's history.
- **SC-005**: No provider credential or fetched private key ever appears in any API
  response, log line, audit record, error message, or backup that did not explicitly
  request credentials — verified by inspection and automated tests.
- **SC-006**: No operator can read, deploy, or affect another tenant's targets,
  configurations, certificates, or jobs — verified by cross-tenant tests.
- **SC-007**: The same job is never executed twice when multiple service instances run
  concurrently — verified under a concurrency test.
- **SC-008**: An operator can identify why a deployment failed from the job's status and
  history for 100% of failed jobs, without access to service internals.
- **SC-009**: The browser UI reflects a deployment's appearance and completion/failure in
  real time without a manual refresh.

## Assumptions

- The lcm service is the source of issued certificates and exposes, over an
  authenticated service channel, a way to fetch a certificate's chain and private key by
  id or job id (lcm retains generated keys for retrieval), and publishes certificate
  issued/renewed lifecycle events onto the shared platform event bus that this service
  can consume.
- The application gateway, the auth service (identity, tenants, roles, display names),
  the platform event bus, the sealing/KEK facility, and the shared datastore already
  exist and are reused; this service does not re-implement them.
- Provider backends reach third-party infrastructure over the network; the demonstrable
  end-to-end path in a development environment uses the dummy provider and/or a webhook
  endpoint, since real cloud/appliance targets are not present in dev.
- The initial provider set is AWS ACM, Cloudflare, F5 BIG-IP, FortiGate, Webhook, and
  Dummy; the tangra "push-to-agent" provider (deploying to enrolled workload agents) is
  out of scope for the first version and may be added later.
- Tenancy, permission model, audit, and UI composition follow the same conventions as
  the existing lcm, warden, and notification modules.
- A worker/instance count, retry policy, backoff multiplier, per-job timeout, and job
  retention window are operator-configurable with secure defaults.
