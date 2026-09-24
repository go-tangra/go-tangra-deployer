# Phase 0 Research: Deployer Service

All decisions below re-apply patterns already proven in `services/lcm`,
`services/warden`, and `services/notification`. The reference implementation is
`/home/jadmin/projects/go-tangra/go-tangra-deployer`; each tangra mechanism is mapped to
its Freya equivalent.

## Decision 1 — Module shape mirrors `services/lcm`

- **Decision**: Create `services/deployer` with the same package layout, build/wire/run
  flow, OpenAPI-driven HTTP surface, gRPC module surface, RLS store, sealed secrets, and
  Module-Federation UI as lcm.
- **Rationale**: lcm is the newest, most complete module and already solves identity,
  gateway registration, authz, sealing, RLS, and the event bus. Copying its shape keeps
  the deployer inside the established platform contract and minimises novel surface.
- **Alternatives rejected**: (a) fold deployment into lcm — rejected, mixes issuance with
  distribution and bloats the CA service (the user chose a separate service); (b) invent a
  new module shape — rejected, no benefit over the proven one.

## Decision 2 — Auth: SVID mTLS + platform token (replaces tangra mTLS-CN + shared secret)

- **Decision**: service-to-service over SPIFFE mTLS verified against the mesh bundle;
  browser API via the platform token forwarded by the gateway and verified with
  `authclient`; no shared bootstrap secret. The deployer enrolls/obtains its identity the
  same way other workloads do.
- **Rationale**: Constitution II (zero trust); consistency with every other module; the
  gateway already forwards the platform token and the framework already enforces authz
  middleware before handlers.
- **Alternatives rejected**: tangra's CN allow-list + HMAC gateway-claims + per-module
  shared bootstrap secret — rejected as non-Freya and a standing secret-management burden.

## Decision 3 — Certificate acquisition from lcm at deploy time

- **Decision**: an `lcmclient` fetches cert + chain + private key from lcm over SPIFFE
  mTLS, keyed by the certificate id (renewals) or certificate-job id (issuance) carried in
  the event/request; nothing is persisted. Uses lcm's issued-certificate download / key
  endpoint (lcm now retains generated keys for retrieval) with a gRPC fallback.
- **Rationale**: single source of truth; the deployer must never be a second store of
  private keys (SR-002); lcm already supports retained-key retrieval.
- **Open item for plan**: confirm the exact lcm surface (gRPC method vs. `/certificates/{id}`
  + `/certificates/{id}/key` HTTP over mTLS). Both exist; the module gRPC is preferred for
  module-to-module. Recorded in contracts/lcm-client.md.

## Decision 4 — Auto-deploy via the platform Valkey event bus (replaces Redis pub/sub)

- **Decision**: consume the shared `platform:events:<tenant>` Valkey stream (the bus built
  for the gateway SSE hub) using the same `stream` package pattern lcm/notification/gateway
  use; subscribe to `certificate.issued` and `certificate.renewed`; a per-tenant reader
  fans events to the matcher. Self-originated events are ignored via the event's module
  field.
- **Rationale**: the platform already standardised on Valkey Streams for cross-module
  realtime; reusing it avoids a second bus and gives durable replay + multi-instance
  fan-out for free.
- **Alternatives rejected**: (a) a dedicated Redis pub/sub like tangra — rejected,
  duplicates infrastructure; (b) lcm calling the deployer directly on issuance — rejected,
  couples issuance to deployment and breaks if the deployer is down (the bus buffers).
- **Note**: the lcm cert event payload today carries `{certificate_id, spiffe_id,
  not_after}`. For generic (ACME) certs the deployer needs issuer/CN/SAN/org to match
  filters; plan.md/contracts note that either (a) the deployer fetches the cert from lcm to
  read its subject for matching, or (b) lcm's event payload is enriched with
  issuer/CN/SAN. Chosen default: **fetch-to-match** (no lcm change required); enrichment is
  a possible optimisation.

## Decision 5 — Provider registry + interface

- **Decision**: a thread-safe global registry; providers self-register via `init()` and are
  selected by `provider_type`. Interface: `Deploy(ctx, CertificateData, config, creds,
  progress) (Result, error)`, `Verify`, `Rollback`, `ValidateCredentials`,
  `Capabilities()`. Ship AWS ACM, Cloudflare, F5 BIG-IP, FortiGate, Webhook, Dummy.
- **Rationale**: matches tangra's proven abstraction and lets providers be added without
  touching the core; capabilities drive UI (verify/rollback buttons) and validation.
- **Simplicity note (Constitution VII)**: a package-level registry is global mutable state;
  it is bounded (write-once at init, read-only after), documented, and the single accepted
  exception — mirroring tangra and lego-style provider registries.
- **Dependency justification (Constitution VI)**:
  - AWS ACM — AWS SDK for Go v2 (ACM client only) OR a thin SigV4 `net/http` client;
    prefer the minimal SigV4 client to avoid the large SDK; decide in tasks.
  - Cloudflare, FortiGate, Webhook — `net/http` only (REST + bearer/token).
  - F5 BIG-IP — `net/http` (iControl REST) only.
  - Dummy — stdlib only.
  No new crypto dependencies; TLS from stdlib.

## Decision 6 — Parent/child jobs + distributed worker pool (SQL lease)

- **Decision**: model PARENT (group), CHILD (per configuration), DIRECT (single) jobs;
  a worker pool polls pending/retryable jobs and atomically claims each with a
  `FOR UPDATE SKIP LOCKED` lease (exactly the pattern in lcm's `renew`/`enroll`
  schedulers) so one instance runs each job; parents aggregate children; failures retry
  with exponential backoff up to `max_retries`; a cleanup worker prunes old jobs.
- **Rationale**: reuses a battle-tested Freya concurrency primitive; no external queue.
- **Alternatives rejected**: an in-memory queue (loses jobs on restart, no multi-instance
  safety); a Valkey work queue (a second mechanism when the SQL lease already exists).

## Decision 7 — Credential sealing (replaces single static AES key)

- **Decision**: seal provider credentials with the platform `sealed` envelope-encryption
  package + KEK (as lcm seals issuer/ACME/DNS credentials); redact on read with a set
  marker; `config_overrides` never carry credentials.
- **Rationale**: Constitution I/VI; consistent with warden/notification/lcm; supports key
  rotation, unlike tangra's single static AES key.

## Decision 8 — Storage: TimescaleDB + per-tenant RLS

- **Decision**: TimescaleDB with RLS policies per table (as lcm); goose migrations; a
  scoped system subject (carrying the event's tenant) for the worker/event paths rather
  than a global privacy bypass.
- **Rationale**: Constitution (tenant isolation enforced at the data layer, not just the
  app); matches lcm's `0003_rls.sql` approach; tangra's Ent + system-viewer bypass is
  explicitly replaced.

## Decision 9 — Realtime UI updates reuse the gateway SSE bus

- **Decision**: the deployer publishes its own lifecycle events (`deployment.started`,
  `deployment.completed`, `deployment.failed`, `job.updated`) to the shared
  `platform:events:<tenant>` stream; the UI subscribes via the shell's shared live client
  (`ctx.live`) so jobs update in real time without polling.
- **Rationale**: SC-009; the SSE bus and shared shell client already exist.

## STRIDE Threat Model (Constitution — feature touches auth, transport, parsing, secrets)

| Threat | Vector | Mitigation |
|--------|--------|------------|
| **Spoofing** | forged caller / forged cert event | SPIFFE mTLS peer verify; platform-token verify via authclient; events consumed only for their own tenant and treated as untrusted; self-originated events ignored (SR-001, SR-005). |
| **Tampering** | altered job/config rows; altered audit | RLS + tenant scoping; append-only tamper-evident audit; server-side status transitions only (SR-003, FR-026). |
| **Repudiation** | operator denies a deploy/rollback | every state change audited with actor/tenant/outcome/reason; per-job action history (FR-025/026). |
| **Information disclosure** | credential/private-key leak via API/log/backup/error | envelope-sealed creds, never returned; redaction of credential + key fields; backups exclude credentials unless explicitly requested; scrubbed error messages (SR-002, FR-002). |
| **Denial of service** | ReDoS filter pattern; slow/hostile provider; oversized event/response | complexity-bounded matcher; per-deploy timeout; bounded retries; capped provider interaction; framework edge body/rate limits (SR-004, SR-006). |
| **Elevation of privilege** | cross-tenant access; `force` retry abuse; direct mTLS forging user claims | tenant-scoped Zanzibar checks before every handler; `force` gated to operator/admin and audited; no user-claim path bypasses authz (SR-001, SR-003; replaces tangra CRIT-3.2 HMAC-claim forgery risk). |

## Resolved unknowns

- Provider set for v1: AWS ACM, Cloudflare, BIG-IP, FortiGate, Webhook, Dummy;
  tangra-client (push-to-agent) deferred.
- Event matching without richer payloads: fetch-to-match (default), enrichment optional.
- Concurrency: SQL `FOR UPDATE SKIP LOCKED` lease (no external queue).
- Credential encryption: platform `sealed` + KEK (no static AES key).

No remaining NEEDS CLARIFICATION.
