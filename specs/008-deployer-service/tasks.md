---
description: "Task list for the Deployer Service (feature 008)"
---

# Tasks: Deployer Service

**Input**: Design documents from `/specs/008-deployer-service/`

**Prerequisites**: plan.md, spec.md, research.md, data-model.md, contracts/deployer-api.md, quickstart.md

**Tests**: MANDATORY (Constitution IV). Each user story lists its test tasks before its
implementation tasks; tests are written and confirmed failing first. Auth/transport/
parsing/secrets paths add negative-security and fuzz tests.

**Organization**: grouped by user story (US1–US5 from spec.md) so each is an independent,
testable increment. Module path: `services/deployer/` (mirrors `services/lcm/`).

## Format: `[ID] [P?] [Story] Description`

- **[P]**: parallelisable (different files, no incomplete dependency).
- **[Story]**: US1–US5; Setup/Foundational/Polish carry no story label.

## Path Conventions

All paths are under `services/deployer/` unless noted. The module mirrors `services/lcm/`.

---

## Phase 1: Setup (Shared Infrastructure)

- [X] T001 Create the module skeleton `services/deployer/` with the package tree from plan.md (cmd/deployersvc, api/openapi, api/proto/deployer/v1, internal/{app,config,store,repo,memstore,sealed,authz,targets,configs,deploy,jobs,events,lcmclient,provider,providers,stats,backup,audit,httpapi,grpcapi}, pkg/{deployermanifest,deployerclient}, ui, deploy).
- [X] T002 Add `services/deployer/go.mod` (module `github.com/go-freya/freya/services/deployer`, Go 1.26) with replaces for `../..`, `../auth`, `../gateway`, `../lcm`; add to the workspace/build like other services.
- [X] T003 [P] Add `services/deployer/buf.yaml` + `buf.gen.yaml` and `api/proto/deployer/v1/*.proto` stubs; wire proto codegen into the Makefile (mirror `services/lcm`).
- [X] T004 [P] Add `services/deployer/Dockerfile` (build UI remote, embed with `-tags ui`, build `deployersvc`) and `services/deployer/Makefile` mirroring lcm.
- [X] T005 [P] Scaffold `services/deployer/ui/` (Vue 3 + Vite + Vuetify Module-Federation remote named `deployer`) from `services/lcm/ui` (package.json, vite.config, main.ts, api/client, remote/{routes,nav,header}, stores, views).

---

## Phase 2: Foundational (Blocking Prerequisites)

**Purpose**: platform wiring every story depends on. No user story starts until this is done.

- [X] T006 Typed, validated config in `internal/config/config.go` (server addrs, admin addr, db, valkey user, kek, jobs{worker_count,max_retries,retry_delay,backoff_multiplier,job_timeout,cleanup_days}, events{enabled,subscribe}, lcm endpoint) + `config_test.go` asserting secure defaults and startup rejection of missing kek/db/identity.
- [X] T007 Store migrations in `internal/store/migrations/` — `0001_schema.sql` (deployer_targets, deployer_configs, deployer_jobs, deployer_history, deployer_target_configs_link, grants; indexes per data-model.md), `0002_hypertables.sql`, `0003_rls.sql` (per-tenant RLS on every deployer_* table + app-role grants). Each with `-- +goose Up/Down`.
- [X] T008 Store models + repos in `internal/store/{models.go,repos.go}` and the store interface in `internal/repo/repo.go` (targets, configs, jobs incl. ClaimDueJobs `FOR UPDATE SKIP LOCKED`, history, link table, grants, stats aggregations).
- [X] T009 [P] In-memory `internal/memstore/memstore.go` implementing `repo.Store` (with error injection) for tests.
- [X] T010 [P] `internal/sealed/` envelope-seal/open + redaction helpers (reuse the platform `sealed` pattern) + `sealed_test.go` (100% — round-trip, AD binding, redaction never leaks).
- [X] T011 [P] `internal/authz/` Zanzibar tuples for `deployer_target|deployer_configuration|deployer_job` × `read|manage|deploy`, tenant-wide grants, `Check/Effective/GrantOwner` + `authz_test.go` (100%, incl. cross-tenant denial).
- [X] T012 [P] `internal/audit/` writer adapter to the framework append-only audit (event types per data-model.md) + redaction of credential/key fields.
- [X] T013 Provider registry + interface in `internal/provider/` (`Provider`, `CertificateData`, `Capabilities`, thread-safe `Register/Get/List/Info`) + `provider_test.go` (registration, unknown-type rejection, capability reporting).
- [X] T014 [P] `internal/providers/dummy/` self-registering dummy provider (configurable success/fail, progress, verify/rollback) + tests — the MVP/test backend.
- [X] T015 `internal/lcmclient/` `FetchCertificate(ctx, certOrJobID, includePrivateKey)` over SPIFFE mTLS to lcm (gRPC preferred, HTTP fallback per contracts §5) + `lcmclient_test.go` against a fake lcm; never caches key material.
- [X] T016 Event-bus plumbing in `internal/events/` — a Valkey-Streams reader for `platform:events:<tenant>` (reuse the `stream` package pattern) and a publisher for `deployment.*`/`job.updated`; wire the Valkey client in config. (Consumer logic lands in US2.)
- [X] T017 App build/wire/run in `internal/app/{app.go,wire.go}` — `freya.New`, identity, store/KEK, authz, provider registry init, lcm client, gateway registration via `pkg/deployermanifest`, HTTP mux (OpenAPI-validated) + gRPC servers, worker pool + event consumer start/stop, admin health/readiness. Refuses to start insecure (Constitution I).
- [X] T018 `cmd/deployersvc/main.go` + `bootstrap` subcommand (config load, migrate, run) mirroring `lcmsvc`.
- [X] T019 [P] `pkg/deployermanifest/manifest.go` — derive gateway routes/permissions from `api/openapi/deployer.yaml` (Routes with Public flag), like `pkg/lcmmanifest`.
- [X] T020 `api/openapi/deployer.yaml` skeleton (info, components, CSRF/id/cursor params, error shapes) so the OpenAPI-validated mux and manifest have a document to load; contract test `tests/contract/openapi_test.go` (parses, every mounted route declared).

**Checkpoint**: framework, store+RLS, sealing, authz, registry, lcm client, event plumbing, and app wiring exist; the service starts, registers with the gateway, and serves an empty API.

---

## Phase 3: User Story 1 — Register an endpoint and deploy a certificate (Priority: P1) 🎯 MVP

**Goal**: create/validate a target configuration and manually deploy an issued certificate
to it (dummy provider), producing a completed direct job with history.

**Independent test**: quickstart Scenario 1.

### Tests (write first, must fail)
- [X] T021 [P] [US1] Contract test `tests/contract/configurations_test.go` — configurations CRUD + `/configurations/validate` + `/providers` shapes; credential fields never appear in any response.
- [X] T022 [P] [US1] Unit test `internal/configs/configs_test.go` — create seals credentials, read/list redact, ValidateCredentials pass/fail, unknown provider rejected, unique-per-tenant name.
- [X] T023 [P] [US1] Unit test `internal/deploy/deploy_test.go` — `Deploy` (single config) creates a direct job, fetches the cert via a fake lcm client, calls the provider, records deploy history, sets last_deployment_at.
- [X] T024 [P] [US1] Unit test `internal/jobs/worker_test.go` — a worker claims a pending direct job exactly once (concurrency), runs it, marks completed with progress 100; a failing provider marks failed.
- [X] T025 [P] [US1] Security test — credential/private-key redaction across responses, logs, and audit for the create/deploy path (SR-002).

### Implementation
- [X] T026 [US1] `internal/configs/configs.go` — TargetConfiguration service: Create/Get/List/Update/Delete, ValidateCredentials (delegates to provider), ListProviders (from registry), sealing + redaction, status/status_message.
- [X] T027 [US1] `internal/deploy/deploy.go` — `Deploy(subj, certID, configID)`: precheck (authz deploy + config active) then create a DIRECT job (async, 202).
- [X] T028 [US1] `internal/jobs/jobs.go` + `internal/jobs/scheduler.go` — job store ops + the worker pool (poll pending/retryable, `ClaimDueJobs` single-winner lease, per-job timeout, progress callback, deploy-history record, last_deployment_at update) + cleanup worker.
- [X] T029 [US1] HTTP handlers `internal/httpapi/configurations.go` + `internal/httpapi/deploy.go` (POST /deploy → 202) + register routes; add the routes to `api/openapi/deployer.yaml` with `x-freya-permission`.
- [X] T030 [P] [US1] gRPC `internal/grpcapi/` TargetConfigurationService + DeploymentService.Deploy (module-to-module) + register.
- [X] T031 [P] [US1] UI: `ui/src/views/configuration/` list + editor drawer (provider select from /providers, credential fields, Validate) + `stores/configuration.ts` + api; deploy action from a certificate; real-time job toast via `ctx.live`.
- [X] T032 [US1] Publish `deployment.started/completed/failed` to the event bus from the worker so the UI updates live (SC-009).

**Checkpoint**: US1 independently demoable — MVP.

---

## Phase 4: User Story 2 — Auto-deploy on issuance/renewal (Priority: P1)

**Goal**: targets with filters + attached configs auto-deploy on `certificate.issued`/
`certificate.renewed`, creating parent + child jobs.

**Independent test**: quickstart Scenario 2.

### Tests (write first, must fail)
- [X] T033 [P] [US2] Contract test `tests/contract/targets_test.go` — targets CRUD, attach/detach, list configurations, per-config overrides (override rejects credential-shaped keys).
- [X] T034 [P] [US2] Unit test `internal/events/matcher_test.go` — AND-matching on issuer/CN/SAN/org/OU/country; empty filters match all; match requires ≥1 config.
- [X] T035 [P] [US2] Fuzz + ReDoS test `internal/events/matcher_fuzz_test.go` — pathological CN/SAN patterns stay bounded (SR-004).
- [X] T036 [P] [US2] Unit test `internal/events/consumer_test.go` — issued→trigger event, renewed→trigger auto_renewal; foreign-tenant and self-originated (module==deployer) events ignored (SR-005); non-matching event creates no job.
- [X] T037 [P] [US2] Unit test `internal/deploy/deploy_target_test.go` — DeployToTarget creates one parent + one child per config; no config → no child jobs.

### Implementation
- [X] T038 [US2] `internal/targets/targets.go` — DeploymentTarget service: CRUD, attach/detach configurations (+overrides, credential-key rejected), list a target's configurations, certificate filters.
- [X] T039 [US2] `internal/events/matcher.go` — certificate-filter matcher (exact + complexity-bounded regex/glob) reading subject/SANs from the fetched cert (fetch-to-match per research).
- [X] T040 [US2] `internal/events/consumer.go` — subscribe to `certificate.issued|renewed` per tenant, ignore self/foreign, load auto-deploy targets, match, and create parent+child jobs (trigger event|auto_renewal); wire start/stop in app.
- [X] T041 [US2] `internal/deploy/deploy.go` — DeployToTarget (parent + child jobs) + DeployToConfigurations (many, per-config results).
- [X] T042 [US2] HTTP handlers `internal/httpapi/targets.go` + deploy/target + deploy/configurations routes + OpenAPI entries; gRPC DeploymentTargetService + DeploymentService.DeployToTarget/DeployToConfigurations.
- [X] T043 [P] [US2] UI: `ui/src/views/target/` list + editor drawer (filters editor, configuration attachment + overrides, auto-deploy toggle) + `stores/target.ts`.

**Checkpoint**: US1 + US2 — manual and automatic deployment both work.

---

## Phase 5: User Story 3 — Track, retry, verify, rollback (Priority: P2)

**Goal**: job visibility + recovery + verify/rollback with parent aggregation and backoff.

**Independent test**: quickstart Scenario 3 (retry/verify/rollback).

### Tests (write first, must fail)
- [X] T044 [P] [US3] Contract test `tests/contract/jobs_test.go` — list (all filters), status (+children), result (+history), cancel (+cascade), retry (+failed-children-only, force).
- [X] T045 [P] [US3] Unit test `internal/jobs/aggregate_test.go` — parent aggregation completed/failed/partial/processing-with-progress from child states.
- [X] T046 [P] [US3] Unit test `internal/jobs/retry_test.go` — exponential backoff, max_retries, next_retry_at, force override; retry-failed-children-only.
- [X] T047 [P] [US3] Unit test `internal/deploy/verify_rollback_test.go` — verify/rollback call the provider; unsupported providers return an explicit unsupported outcome (not silent success).

### Implementation
- [X] T048 [US3] `internal/jobs/jobs.go` — list/get(+children)/result(+history)/cancel(+cascade)/retry(+failed-children-only, force); parent aggregation (`updateParentJobStatus`).
- [X] T049 [US3] `internal/jobs/scheduler.go` — retry with exponential backoff (delay × multiplier^retry_count), MarkForRetry, fail-and-update-parent on exhaustion.
- [X] T050 [US3] `internal/deploy/deploy.go` — Verify and Rollback (apply target config overrides; capability-gated) + deployment-history entries.
- [X] T051 [US3] HTTP handlers `internal/httpapi/jobs.go` (list/status/result/cancel/retry) + deploy/{jobId}/verify + /rollback + OpenAPI entries; gRPC DeploymentJobService + DeploymentService.Verify/Rollback.
- [X] T052 [P] [US3] UI: `ui/src/views/job/` list (filters) + detail drawer (status, progress, per-action history, retry, cancel, verify, rollback) + `stores/job.ts`; live updates via `ctx.live` (`job.updated`).

**Checkpoint**: US1–US3 — full deploy + observe + recover lifecycle.

---

## Phase 6: User Story 4 — Multiple endpoints, shared credentials, overrides (Priority: P2)

**Goal**: fan-out to many endpoints with per-attachment config overrides over a shared credential.

**Independent test**: quickstart Scenario 3 step 5 + multi-config deploy.

### Tests (write first, must fail)
- [X] T053 [P] [US4] Unit test `internal/jobs/effective_config_test.go` — child job layers target.config_overrides over config.config; override never contains credentials; shared credential opened once per child.
- [X] T054 [P] [US4] Unit test `internal/deploy/fanout_test.go` — one certificate → children across several configurations; per-endpoint success/failure reported.

### Implementation
- [X] T055 [US4] `internal/jobs/scheduler.go` — `resolveEffectiveConfig(target, config)` merge used by child execution; open sealed credentials per child.
- [X] T056 [US4] `internal/targets/targets.go` — enforce overrides-are-config-only on attach/update (reject credential-shaped keys) with a clear validation error.
- [X] T057 [P] [US4] UI: overrides editor in the target drawer (per-attached-config config overlay), shared-credential indicator.

**Checkpoint**: US1–US4.

---

## Phase 7: User Story 5 — Providers catalogue, statistics, backup (Priority: P3)

**Goal**: provider discovery, dashboards, export/import.

**Independent test**: quickstart Scenario 4.

### Tests (write first, must fail)
- [X] T058 [P] [US5] Contract test `tests/contract/stats_backup_test.go` — /statistics, /statistics/tenant shapes; /backup export excludes credentials unless requested; import skip/overwrite.
- [X] T059 [P] [US5] Unit test `internal/stats/stats_test.go` — jobs by status/trigger, last-24h/7d success rates, config-by-provider counts, recent errors, per-tenant breakdown for admin.
- [X] T060 [P] [US5] Unit test `internal/backup/backup_test.go` — export/import round-trip; credentials only on request; duplicate skip vs overwrite; schema version.

### Implementation
- [X] T061 [US5] `internal/stats/stats.go` + HTTP `internal/httpapi/statistics.go` + gRPC DeployerStatisticsService + OpenAPI entries.
- [X] T062 [US5] `internal/backup/backup.go` + HTTP `internal/httpapi/backup.go` (export include_credentials flag, import mode) + OpenAPI entries.
- [X] T063 [P] [US5] UI: `ui/src/views/dashboard/` widgets (targets total, job trends, success gauge, jobs-by-status, configs-by-provider, auto-deploy stat); ListProviders surfaced in the configuration drawer.

**Checkpoint**: all user stories complete.

---

## Phase 8: Concrete providers, platform integration & polish

### Concrete providers (each self-registers; independent files → [P])
- [X] T064 [P] `internal/providers/webhook/` generic HTTP webhook provider (config url; verify+rollback) + tests (incl. insecure-URL opt-out warning).
- [X] T065 [P] `internal/providers/cloudflare/` (config zone_id; creds api_token; verify) + tests (net/http, fake server).
- [X] T066 [P] `internal/providers/fortigate/` (config vdom; creds host/api_token; verify+rollback) + tests.
- [X] T067 [P] `internal/providers/bigip/` (config partition; creds host/username/password; verify+rollback) + tests.
- [X] T068 [P] `internal/providers/aws_acm/` (config region; creds access_key_id/secret_access_key; verify) + tests — minimal SigV4 net/http client (justify SDK vs. hand-rolled in a code comment per research).
- [X] T069 Import all providers for side-effect registration in `internal/providers/init.go`; ListProviders returns all six with correct capabilities/fields.

### Platform integration
- [X] T070 `deploy/stack/compose.yaml` — add a `deployer` service (build, healthcheck, depends_on lcm/valkey/timescaledb), a `deployer` init/bootstrap step, and DB + valkey `deployer` users; `deploy/stack/configs/deployer.yaml`.
- [X] T071 Gateway allow-list entry `spiffe://example.org/svc/deployer=/api/deployer;deployer` in `deploy/stack/compose.yaml` (gateway-bootstrap) and the deployer permission policy in `services/deployer/deploy/policy.yaml`.
- [X] T072 Register the deployer remote in the shell (Module Federation) like the lcm remote; confirm a **Deployer** menu appears.
- [X] T073 [P] `pkg/deployerclient/` optional Go client for module-to-module use + doc.

### Polish
- [X] T074 [P] Fuzz tests for the event-payload parser and negative tests for oversized/malformed inputs across the edge (Constitution IV).
- [X] T075 [P] `services/deployer/deploy/README.md` / docs: setup, providers, auto-deploy, security notes; update `deploy/stack/README.md`.
- [X] T076 Coverage gate: `go -C services/deployer test ./...` ≥80% overall, 100% on sealed/authz/matcher; `govulncheck` clean.
- [ ] T077 Stack smoke test: bring up the stack, confirm the deployer registers (`registered:true`), then run quickstart Scenario 1 end-to-end in the UI.

---

## Dependencies & Execution Order

- **Setup (Phase 1)** → **Foundational (Phase 2)** block everything.
- **US1 (Phase 3)** is the MVP and lands first among stories.
- **US2 (Phase 4)** depends on US1's jobs/deploy/worker + Foundational events.
- **US3 (Phase 5)** depends on the job model (US1/US2).
- **US4 (Phase 6)** depends on child-job execution (US2).
- **US5 (Phase 7)** depends on the job/target/config stores (US1–US2) for data.
- **Phase 8** providers are independent [P]; platform integration depends on US1 (a runnable service).

## Parallel Opportunities

- Setup: T003, T004, T005 in parallel.
- Foundational: T009–T012, T014, T019 in parallel after T006–T008.
- Within each story, the `[P]` test tasks run together; UI and gRPC tasks parallel the HTTP work (different files).
- Phase 8 providers T064–T068 fully parallel.

## Implementation Strategy

- **MVP** = Setup + Foundational + **US1** (dummy provider, manual direct deploy) — a
  demoable slice.
- Then **US2** (auto-deploy) — the defining capability — followed by US3, US4, US5.
- Concrete external providers (Phase 8) are added incrementally after the MVP; each is
  independent and self-registering.

## Summary

- **Total tasks**: 77 across 8 phases.
- **Per story**: US1 = 12 (T021–T032), US2 = 11 (T033–T043), US3 = 9 (T044–T052),
  US4 = 5 (T053–T057), US5 = 6 (T058–T063). Setup = 5, Foundational = 15, Polish/providers/
  integration = 14.
- **MVP scope**: Phases 1–3 (Setup + Foundational + US1).
- **Format check**: every task has an id, phase, `[P]`/`[Story]` labels where applicable,
  and an explicit file path.
