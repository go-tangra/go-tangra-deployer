# Quickstart / Validation Guide: Deployer Service

Runnable scenarios that prove the deployer works end-to-end in the dev stack. Details
of shapes are in [contracts/deployer-api.md](./contracts/deployer-api.md) and
[data-model.md](./data-model.md).

## Prerequisites

- The `deploy/stack` compose is up (gateway, auth, lcm, valkey, timescaledb) and an
  operator can sign in (as for lcm).
- The `deployer` service is added to the stack: image built, a `deployer` DB + valkey
  user, config at `deploy/stack/configs/deployer.yaml`, and the gateway allow-list entry
  `spiffe://example.org/svc/deployer=/api/deployer;deployer`.
- The deployer remote is registered in the shell (Module Federation), so a **Deployer**
  menu appears after sign-in.

## Scenario 1 — Manual deploy to the dummy provider (US1, MVP)

1. Sign in; open **Deployer → Target configurations → New**.
2. Choose provider **Dummy**, name it, save. Run **Validate** → passes; status `active`.
3. Open **Certificates** (lcm), pick or issue a certificate; note its id.
4. In the deployer, **Deploy** that certificate id to the dummy configuration.
5. **Expected**: a **direct** deployment job is created (202), reaches **completed**, and
   its result shows a deploy-action history entry with a duration. The job list updates in
   real time (SSE) without a refresh.

Verifies: FR-001/002/003/004, FR-011/012, FR-016/017, FR-025, SC-001, SC-009.

## Scenario 2 — Auto-deploy on issuance (US2)

1. Create a **Deployment target**, enable **auto-deploy**, add a filter (e.g. common-name
   pattern matching a domain you will issue), and attach the dummy configuration.
2. In lcm, **issue** a certificate whose subject matches the filter (e.g. an ACME/generic
   cert for that domain, or an SVID whose SPIFFE/subject matches).
3. **Expected**: with no human action, a **parent** job + one **child** job per attached
   configuration appear, triggered-by **event**, and complete. A non-matching issuance
   creates no job. A renewal produces jobs triggered-by **auto-renewal**.

Verifies: FR-008/009/010, FR-013/014/015, US2 scenarios, SC-002.

## Scenario 3 — Fan-out, retry, verify, rollback (US3, US4)

1. Attach several configurations (or one dummy set to fail) to a target and deploy to the
   group.
2. **Expected**: the parent aggregates child outcomes → **partial** when mixed; each child
   shows its own status/progress; the history lists attempts.
3. **Retry** the failed children (failed-children-only) → they re-run and the parent
   becomes **completed**.
4. **Verify** on a verify-capable provider → records a verify history entry; **Rollback**
   on a rollback-capable provider → restores the previous cert. On a provider that lacks
   the capability, the action returns an **unsupported** outcome (not a silent success).
5. Attach one configuration with a **config override** → the child deploys with the merged
   (overridden) config; the shared credential is stored once.

Verifies: FR-018/019/020/021, FR-023/024, US3+US4 scenarios, SC-003, SC-004, SC-008.

## Scenario 4 — Providers, statistics, backup (US5)

1. **Providers**: list them → each shows display name, verify/rollback support, required
   config and credential fields.
2. **Statistics**: view the dashboard → jobs by status and trigger, last-24h/7d success
   rates, target/configuration counts by provider, recent errors.
3. **Backup**: export the tenant's targets + configurations (without credentials by
   default), import into a fresh tenant with mode **skip**/**overwrite**.

Verifies: FR-005/006/007, FR-027, FR-028, US5 scenarios.

## Security checks (must pass)

- **No credential leak**: create a configuration with credentials; GET/list/export
  (without the flag) and inspect logs and audit — the credential never appears; a set
  marker shows it is present (SR-002, SC-005).
- **Tenant isolation**: as tenant A, attempt to read/deploy/target tenant B's
  configurations, certificates, and jobs — all denied (SR-003, SC-006).
- **No double execution**: run two deployer instances; deploy to a group; confirm each
  child job runs exactly once (FR-016, SC-007).
- **Hostile input**: submit a pathological filter pattern and an oversized/foreign-tenant/
  self-originated event — matching stays bounded and the events are ignored (SR-004/005).

## Automated verification

- `go -C services/deployer test ./...` — unit + contract + negative/fuzz tests green;
  coverage ≥ 80% (100% on sealing/authz/matcher).
- Contract test: the OpenAPI document parses and every route is mounted; no response body
  ever contains a credential field.
- Stack smoke: `deploy/stack` up with the deployer; the service registers with the gateway
  (`registered:true`), the **Deployer** menu loads, and Scenario 1 completes in the UI.
