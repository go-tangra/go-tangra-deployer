# Implementation Plan: Deployer Service

**Branch**: `008-deployer-service` | **Date**: 2026-09-19 | **Spec**: [spec.md](./spec.md)

**Input**: Feature specification from `/specs/008-deployer-service/spec.md`

## Summary

Build a new Freya platform module, **`services/deployer`**, that distributes lcm-issued
TLS certificates to infrastructure endpoints through pluggable providers, both on demand
and automatically on lcm certificate lifecycle events. It reuses the exact platform
conventions already proven in `services/lcm`, `services/warden`, and
`services/notification`: the Freya framework (SPIFFE mTLS, identity, audit,
observability), gateway registration + Module-Federation UI, an OpenAPI-declared HTTP
surface proxied by the gateway, gRPC for module-to-module calls, Zanzibar-style authz,
envelope-sealed secrets, and TimescaleDB with per-tenant RLS. The distinctive new
machinery is: a **provider registry** (AWS ACM, Cloudflare, F5 BIG-IP, FortiGate,
Webhook, Dummy), a **parent/child deployment-job** model, a **distributed worker pool**
(SQL lease claim like lcm's renew/enroll schedulers) with exponential-backoff retry, an
**auto-deploy event consumer** on the shared `platform:events:<tenant>` Valkey stream
(the bus built for lcm), and an **lcm client** that fetches cert+chain+key at deploy time
over SPIFFE mTLS.

## Technical Context

**Language/Version**: Go 1.26 (matches every other Freya service).

**Primary Dependencies**: the Freya framework (`github.com/go-freya/freya`) for
transport/mTLS/identity/audit/config/observability; `services/auth/pkg/authclient`
(platform-token verification); `services/gateway/pkg/gatewayclient` (route/permission
registration + renew lease); `services/lcm` module for its gRPC/HTTP client surface and
the shared `stream` package pattern (Valkey Streams); `github.com/valkey-io/valkey-go`
(event bus + rate/lease); the Go standard library + `golang.org/x/crypto` only for
crypto; provider SDKs kept minimal (AWS ACM via the AWS SDK or a thin signed-HTTP client;
Cloudflare/FortiGate/BIG-IP/Webhook via `net/http`). Each new dependency is justified in
research.md (Constitution VI). UI: Vue 3 + Vite + Vuetify (Materio) Module-Federation
remote, mirroring `services/lcm/ui`.

**Storage**: TimescaleDB (Postgres) with per-tenant row-level security; the same
`internal/store` + `internal/repo` + goose-migration pattern as lcm; a `memstore` fake
for tests. Sealed credentials via the `sealed` envelope-encryption package + KEK.

**Testing**: Go `testing` with a `testrt` test runtime + `memstore` fake; contract tests
against the OpenAPI document (every declared route mounted) and the gRPC protos;
negative-security and fuzz tests for the event-payload parser, the certificate-filter
matcher (ReDoS/glob), and the credential-sealing/redaction path; ≥80% coverage overall,
100% on the sealing/authz/matcher packages.

**Target Platform**: Linux server container in the `deploy/stack` compose, behind the
gateway; UI composed by the gateway shell.

**Project Type**: Web service (Go backend + gRPC + OpenAPI HTTP) with a Module-Federation
frontend — the established Freya module shape.

**Performance Goals**: auto-deploy job created within ~1s of a certificate event; a
manual deploy to the dummy provider completes in <2s; the worker pool sustains the
configured concurrency without double-executing a job across instances; matcher
evaluates a tenant's targets against an event in well under 100ms.

**Constraints**: no certificate bytes persisted (fetched per deploy, held transiently);
credentials sealed at rest and never emitted; every request authenticated + tenant-authz
before the handler; per-deploy timeout and bounded retries; filter-pattern evaluation
complexity-bounded; RLS enforces tenant isolation (no system-viewer bypass — a scoped
system subject is used only for the worker/event paths and still carries a tenant).

**Scale/Scope**: dozens of targets/configurations and thousands of jobs per tenant;
6 providers in v1; ~4 UI screens; a single new service module plus one gateway
allow-list entry and one compose service.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Evaluated against `.specify/memory/constitution.md`.

- [x] **I. Secure by Default**: credentials sealed by default and never returned; the
      dummy provider and any insecure webhook (plaintext URL) require an explicit,
      warned opt-out; the service refuses to start without a KEK/DB/identity. PASS.
- [x] **II. Zero Trust**: every HTTP route carries an `x-freya-permission` (or explicit
      `x-freya-public`) enforced by the gateway + module authz middleware before the
      handler; gRPC is SPIFFE-mTLS peer-verified; the worker/event paths act as a
      scoped system subject, never an unauthenticated bypass. PASS.
- [x] **III. Boundary Validation**: OpenAPI + proto schemas validate every inbound field;
      event payloads and filter patterns are validated and size/complexity-bounded;
      provider responses are treated as untrusted; framework edge enforces body/rate
      limits. PASS.
- [x] **IV. Test-First (NON-NEGOTIABLE)**: tasks.md orders contract/unit/security tests
      before implementation; negative + fuzz tests planned for the event parser, the
      filter matcher (ReDoS), and credential redaction; coverage targets achievable. PASS.
- [x] **V. Observability**: every state change → append-only audit with actor/tenant/
      outcome/reason; redaction covers credential + private-key fields; correlation IDs
      propagate; health/readiness on the framework's separate admin port. PASS.
- [x] **VI. Supply Chain**: stdlib preferred; crypto only from stdlib/`x/crypto` (sealing
      reuses the platform `sealed` package); each provider SDK justified in research.md;
      `go.sum` pinned, `govulncheck` in CI. PASS.
- [x] **VII. Simplicity**: typed, validated config at startup (worker count, retry policy,
      timeouts, retention); no reflection/global mutable state beyond the thread-safe
      provider registry (a documented, bounded exception). PASS.
- [x] **Threat Model**: the feature touches auth, transport, parsing (events/filters), and
      secrets (credentials, private keys), so a STRIDE threat model is included in
      research.md. PASS.

No violations — Complexity Tracking is empty.

## Project Structure

### Documentation (this feature)

```text
specs/008-deployer-service/
├── plan.md              # This file
├── research.md          # Phase 0 output
├── data-model.md        # Phase 1 output
├── quickstart.md        # Phase 1 output
├── contracts/           # Phase 1 output (OpenAPI + proto + provider iface + events)
└── tasks.md             # Phase 2 output (/speckit-tasks)
```

### Source Code (repository root)

New module `services/deployer/`, modelled on `services/lcm/`:

```text
services/deployer/
├── cmd/deployersvc/            # main + bootstrap subcommand (like lcmsvc)
├── api/
│   ├── openapi/deployer.yaml   # HTTP surface (x-freya-permission per route); gateway proxies /api/deployer
│   └── proto/deployer/v1/      # gRPC (module-to-module, not gateway-proxied)
├── internal/
│   ├── app/                    # Build/Wire/Run (freya.New, gateway register, worker + event start)
│   ├── config/                 # typed, validated config (jobs, events, providers, valkey, db, kek)
│   ├── store/                  # TimescaleDB repos + goose migrations (RLS)
│   ├── repo/                   # store interface (memstore fake in tests)
│   ├── memstore/               # in-memory store for tests
│   ├── sealed/                 # envelope-sealing helpers (reuse platform pattern)
│   ├── authz/                  # Zanzibar tuples: target/configuration/job × read|manage|deploy
│   ├── targets/                # deployment-target service (CRUD, attach/detach, overrides, filters)
│   ├── configs/                # target-configuration service (CRUD, ValidateCredentials, ListProviders)
│   ├── deploy/                 # deploy service (Deploy/DeployToTarget/DeployToConfigurations/Verify/Rollback)
│   ├── jobs/                   # job service (create/list/get/result/cancel/retry) + worker pool scheduler
│   ├── events/                 # auto-deploy: platform-event consumer + certificate-filter matcher
│   ├── lcmclient/              # fetch cert+chain+key from lcm over SPIFFE mTLS
│   ├── provider/               # provider registry + Provider interface + CertificateData
│   ├── providers/              # aws_acm, cloudflare, bigip, fortigate, webhook, dummy (self-register)
│   ├── stats/                  # statistics aggregation
│   ├── backup/                 # export/import
│   ├── audit/                  # audit writer adapter
│   ├── httpapi/                # HTTP handlers + OpenAPI-validated mux + SSE (if any)
│   └── grpcapi/                # gRPC servers (module-to-module) + registration
├── pkg/
│   ├── deployermanifest/       # routes/permissions from OpenAPI for gateway registration
│   └── deployerclient/         # optional Go client for module-to-module use
├── ui/                         # Vue MF remote (dashboard, targets, configurations, jobs)
├── deploy/                     # policy.yaml, container config
├── Dockerfile
├── go.mod                      # module github.com/go-freya/freya/services/deployer (+ replaces)
└── Makefile
```

Platform wiring (outside the module):
- `deploy/stack/compose.yaml` — a `deployer` service + init/bootstrap, DB + valkey users.
- `deploy/stack/configs/deployer.yaml` — its config.
- gateway allow-list: `spiffe://example.org/svc/deployer=/api/deployer;deployer`.
- shell: the deployer remote is registered like the lcm remote (Module Federation).

**Structure Decision**: mirror `services/lcm` exactly (proven Freya module shape) so the
deployer inherits identity/enrollment, gateway registration, sealed secrets, RLS store,
the Valkey `stream` event bus, and MF-UI composition with no new platform patterns. The
only genuinely new internal packages are `provider`/`providers` (the deployment backends)
and `jobs` (the parent/child worker pool) — everything else is a re-application of
existing modules' code.

## Complexity Tracking

No constitution violations — no entries.
