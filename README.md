# go-tangra-deployer

Certificate deployment service for the
[go-tangra v4 platform](https://github.com/go-tangra/go-tangra).

It takes certificates issued by [go-tangra-lcm](https://github.com/go-tangra/go-tangra-lcm)
and installs them on infrastructure targets through pluggable providers (AWS ACM,
Cloudflare, F5 BIG-IP, FortiGate, a generic webhook, Linux hosts through the
go-tangra-inventory agent, and a dummy provider for tests). Deployments run as jobs on a distributed worker pool with leases,
retries and exponential backoff, and are triggered by hand or automatically
when lcm publishes `certificate.issued` / `certificate.renewed` events that
match a target's filters. Provider credentials are sealed with envelope
encryption (KEK -> DEK) and never returned in full; certificate bytes are never
stored: they are fetched from lcm over SPIFFE mTLS at deploy time and dropped
afterwards. Every operation is audited per tenant.

Operations: [`deploy/README.md`](deploy/README.md).
Design history: `specs/008-deployer-service`.

## Place in the platform

```
go-tangra/go-tangra          platform module + @go-tangra/ui kit
        |
go-tangra-auth  <---->  go-tangra-portal (gateway)  <---->  go-tangra-lcm
                                  |                              |
                          go-tangra-deployer  ---- lcm.v1.Certificates/Download (mTLS)
```

- Built on `github.com/go-tangra/go-tangra/v4` (mTLS transports, identity,
  service policy, audit, observability).
- Verifies platform tokens and checks permissions through the auth SDK
  (`github.com/go-tangra/go-tangra-auth/sdk/v4`), and registers its
  permissions, module roles and built-in role grants with auth (see
  [Permissions and module roles](#permissions-and-module-roles)).
- Registers with the gateway through the portal SDK
  (`github.com/go-tangra/go-tangra-portal/sdk/v4`), which fronts the browser API
  (`/api/deployer`) and the federated UI remote.
- Enrolls for its SVID with lcm and downloads certificate bundles from lcm
  through the lcm SDK (`github.com/go-tangra/go-tangra-lcm/sdk/v4`). lcm's
  service policy must allow `spiffe://<td>/svc/deployer` to call
  `/lcm.v1.Certificates/Download`.
- Asks go-tangra-inventory to deliver certificates to its agents for the
  `inventory-agent` provider (`inventory.v1.CertificateDeliveryService` over
  mTLS, inventory SDK `github.com/go-tangra/go-tangra-inventory/sdk/v4`).
  Inventory's policy must allow `spiffe://<td>/svc/deployer` to call those
  methods, its `cert_delivery.sources` must list `deployer`, and the
  deployer needs `discovery.static.inventory` and `inventory: { service:
  inventory }` in its configuration.

The repository holds one Go module, `github.com/go-tangra/go-tangra-deployer/v4`.
Other services call it through `pkg/deployerclient` and the `deployer.v1` protos.

## Permissions and module roles

The deployer registers with auth as module `deployer` (auth SDK
`authclient.Registration`, feature 019) at start, retrying every 5 s until
auth accepts, then every five minutes: its permissions, the module roles
(`pkg/deployermanifest.Roles`) and the built-in role grants
(`pkg/deployermanifest.Grants`). Module roles are locked in auth;
administrators assign them or clone them into custom roles:

| Role | Display name | Permissions |
|---|---|---|
| `administrator` | Deployer administrator | all nine deployer permissions |
| `operator` | Deployer operator | configurations:read, targets:read, jobs:read, jobs:manage, deploy:execute |
| `viewer` | Deployer viewer | configurations:read, targets:read, jobs:read, stats:read |

Skipped built-in grants (warn) and rejected roles (error) are logged as
`auth registration: ...`.

## Provider settings

The configuration drawer is generated from the provider descriptors served by
`GET /api/deployer/v1/providers` (feature 033): no provider field list lives in
the UI, and the same descriptors drive save-time validation on the server
(HTTP 422 naming `config.<key>` / `credentials.<key>`). Secrets are write-only:
never returned, blank on edit keeps the stored value. A required field that a
target may override can be left empty on a shared configuration; every
deployment target attaching it must then supply it in its override. The API response is
authoritative; this table is a summary of the shipped providers.

### AWS Certificate Manager (`aws_acm`) — validate action: Check settings

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `region` | config | string | yes |  | yes |  |
| `certificate_arn` | config | string |  |  | yes |  |
| `access_key_id` | credentials | string | yes |  |  |  |
| `secret_access_key` | credentials | string | yes | yes |  |  |
| `session_token` | credentials | text |  | yes |  |  |

### F5 BIG-IP (`bigip`) — validate action: Test connection

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `partition` | config | string | yes |  | yes | `Common` |
| `ssl_profile` | config | string |  |  | yes |  |
| `host` | credentials | string | yes |  |  |  |
| `username` | credentials | string | yes |  |  |  |
| `password` | credentials | string | yes | yes |  |  |

### Cloudflare (`cloudflare`) — validate action: Check settings

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `zone_id` | config | string | yes |  | yes |  |
| `api_token` | credentials | string | yes | yes |  |  |

### Dummy (testing) (`dummy`) — validate action: Check settings

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `fail` | config | bool |  |  | yes | `false` |

### FortiGate (`fortigate`) — validate action: Test connection

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `vdom` | config | string | yes |  | yes | `root` |
| `import_scope` | config | enum |  |  | yes | `global` |
| `replace_strategy` | config | enum (`ssl_profile`, `rebind`, `delete`) |  |  |  | `ssl_profile` |
| `profile_suffix` | config | string |  |  | yes | `_ssl_profile` |
| `default_ssl_profile` | config | string |  |  | yes |  |
| `rebind_references` | config | bool |  |  |  | `true` |
| `prune_old` | config | bool |  |  |  | `true` |
| `host` | credentials | string | yes |  |  |  |
| `api_token` | credentials | string | yes | yes |  |  |

### Inventory agent (`inventory-agent`) — validate action: Preview hosts

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `host_ids` | config | host_selector | one of `host_ids`, `host_tags` |  | yes |  |
| `host_tags` | config | string_list | one of `host_ids`, `host_tags` |  | yes |  |
| `cert_name` | config | string |  |  | yes |  |
| `key_policy` | config | enum |  |  | yes | `require` |
| `require_all_success` | config | bool |  |  | yes | `false` |
| `wait_seconds` | config | int |  |  | yes | `60` |

### Webhook (generic HTTP) (`webhook`) — validate action: Test connection

| Key | Stored in | Type | Required | Secret | Target may override | Default |
|---|---|---|---|---|---|---|
| `url` | config | url | yes |  |  |  |
| `verify_url` | config | url |  |  |  |  |
| `rollback_url` | config | url |  |  |  |  |
| `timeout_seconds` | config | int |  |  | yes | `60` |
| `skip_tls_verify` | config | bool |  |  |  | `false` |
| `headers` | config | key_value |  |  |  |  |
| `metadata` | config | key_value |  |  | yes |  |
| `token` | credentials | string |  | yes |  |  |
| `authorization` | credentials | string |  | yes |  |  |
| `api_key` | credentials | string |  | yes |  |  |
| `secret` | credentials | string |  | yes |  |  |

### Inventory agent provider

`inventory-agent` installs a certificate on Linux hosts that run the
go-tangra-inventory agent (4.7.0 or later, `cert.v1` capability). It
delivers **by reference**: the deployer never fetches the private key for
this provider; it sends the certificate id, a name and the host selection
(`host_ids` and/or `host_tags`) to inventory, which pushes an item id to each
agent. The agent pulls the bundle (and, with `key_policy: require`, the key)
over its own authenticated connection and installs it under
`<agent certificate directory>/live/<cert_name>/` in certbot layout. Leave
`cert_name` empty to derive it from the common name (`*.example.com` →
`wildcard.example.com`). What happens on the host after the files are
written (a reload hook, owner, modes) is configured only in the agent's
local `agent.yaml`, never by the deployer.

The job waits up to `wait_seconds` for the hosts to report. Offline hosts
stay queued in inventory and get the certificate when they reconnect; they
never fail a job, not even with `require_all_success`. A failed job retried
by the deployer re-arms the failed hosts under the same delivery. Verify
compares the SHA-256 fingerprint each host reports with the deployed
certificate. There is no rollback. "Preview hosts" in the drawer lists the
hosts a selection matches and whether their agents can receive
certificates. A host claimed by more than one non-revoked agent is shown as
"Several agents claim this host" and receives nothing until the stale or
foreign agent is revoked in inventory.

### Existing appliance profiles (BIG-IP `ssl_profile`, FortiGate `default_ssl_profile`)

Both options are optional. Left empty, BIG-IP behaves as before; FortiGate
still binds the certificate into its own profile (see below).

- **BIG-IP `ssl_profile`** names an existing client-SSL profile (a name in
  the configuration's partition or `/Partition/name`). The deployment
  checks that the profile exists before it uploads anything (a missing
  profile is a permanent failure: "client-SSL profile … not found"),
  installs the certificate, key and chain objects and then updates only
  that profile's certificate, key and chain. It never creates the provider's
  own `<name>_clientssl` profile in this mode, and it never deletes or
  recreates the operator's profile. Verify checks that the profile points
  to the deployed objects. Rollback removes the objects only once the
  profile no longer references them; the profile itself is never touched.
- **FortiGate** behaves exactly as the v3 provider (3.5.1), selected by
  `replace_strategy`:
  - `ssl_profile` (default): the leaf certificate (never the chain) is
    imported under a dated name (`<base>_<yyyymmdd>`, `_01`…`_99` for a
    second certificate on the same day, ≤ 35 characters; `<base>` comes from
    the certificate subject), or the identical certificate already on the
    device is reused. If the certificate family is referenced anywhere other
    than the provider-owned profile `<base><profile_suffix>` and
    `default_ssl_profile` (VIP, SSL-VPN, admin GUI, another profile), the job
    stops with "MANUAL REVIEW REQUIRED" (the certificate stays imported,
    nothing else changes). Otherwise the owned profile is created by cloning
    the first existing replace-mode profile that has a server certificate
    (no such profile → manual review) or its server-certificate list is
    updated (family entry replaced in place, other domains kept), and the
    family entry of `default_ssl_profile` — which must exist in server
    certificate mode `replace` — is replaced in place (appended when
    absent; no write when already current). Nothing is ever deleted.
  - `rebind`: dated import as above, then every SSL/SSH profile, the SSL-VPN
    and admin GUI certificate and every VIP pointing at an older family
    member is repointed (`rebind_references`), and superseded family members
    are deleted (`prune_old`; certificates still in use are kept and
    reported). A failed rebind fails the job.
  - `delete`: the legacy delete-then-import under the bare name; it fails
    while the certificate is in use.

  Verify reports the newest family member on the device. Rollback deletes
  every family member that nothing references any more.

**Migrating from v3.** Recreate a v3 BIG-IP configuration that used
`ssl_profile` with the profile
name in `ssl_profile`; v4 refuses a profile that does not exist instead of
creating it, so create the profile on the BIG-IP first. A v3 FortiGate
configuration carries over unchanged: every v3 option has the same key and
default (`rebind_references` and `prune_old` stored as v3 strings such as
`"yes"` are still read). Differences from v3: an existing device certificate
is reused only when it is identical (v3 compared the serial only), and
profile names with a slash, backslash, quote, control character or (for
`default_ssl_profile`) a leading dot are refused. The BIG-IP `ssl_profile`
and the FortiGate `default_ssl_profile`, `profile_suffix`, `import_scope`
and `vdom` may be overridden per deployment target; the FortiGate
`replace_strategy`, `rebind_references` and `prune_old` (which decide what
the provider deletes) may not.

### Destination and credential safety

- Fields that decide where credentials go (`url`, `verify_url`,
  `rollback_url`, the appliance `host`) and the TLS and header settings are
  never overridable by a deployment target; stored overrides are filtered
  again at job start.
- Changing a configuration's destination (a URL or the credential host),
  on save or in "Test connection" of a stored configuration, requires every
  stored secret to be re-entered (or cleared) in the same request.
- Credential-bearing HTTP clients never follow redirects; a redirect is a
  failed deployment.
- Custom webhook headers may not carry authentication (`Authorization`,
  `Cookie`, names containing token, secret, key, auth, password,
  credential, jwt, session, bearer or signature); use the sealed credential
  fields.

## Layout

| Path | What |
|------|------|
| `api/openapi/deployer.yaml` | browser API contract (served under `/api/deployer/v1`) |
| `api/proto/deployer/v1/` | `deployer.v1` gRPC for services (not gateway-proxied) |
| `internal/config` | configuration + validation (secure defaults, named opt-outs) |
| `internal/store`, `internal/repo` | TimescaleDB schema (per-tenant RLS), repositories |
| `internal/sealed` | envelope encryption of provider credentials (`"__set__"` redaction) |
| `internal/audit` | append-only, tamper-evident audit trail with a credential guard |
| `internal/authz` | permission checks for the browser and gRPC APIs |
| `internal/provider`, `internal/providers/*` | provider contract and the shipped providers |
| `internal/targets`, `internal/configs` | target groups, target configurations and filters |
| `internal/deploy`, `internal/jobs` | deployment orchestration and the worker pool |
| `internal/events` | auto-deploy consumer of the platform event bus |
| `internal/lcmclient` | mTLS certificate download from lcm |
| `internal/inventoryclient` | mTLS client of inventory's `CertificateDeliveryService` (inventory-agent provider) |
| `internal/stream` | Valkey-backed progress events for the gateway SSE hub |
| `internal/backup`, `internal/stats` | tenant backup export/import, statistics |
| `internal/httpapi`, `internal/grpcapi` | browser and service APIs |
| `internal/app`, `cmd/deployersvc` | wiring and the service binary (serve, `bootstrap`, `version`) |
| `pkg/deployermanifest` | gateway manifest built from the OpenAPI document |
| `pkg/deployerclient` | Go client for the `deployer.v1` API |
| `deploy` | service policy and operations notes |
| `tests/contract` | manifest and API contract checks |
| `ui/` | Vue 3 + FlyonUI federated remote on `@go-tangra/ui` |

## Build and test

You need Go 1.26, Node 22, Docker (for the integration suites and the image),
and a GitHub token with `read:packages` to install `@go-tangra/ui` from GitHub
Packages.

```bash
go build ./... && go vet ./... && go test -race ./...
buf lint
make test-integration                     # -tags integration, TimescaleDB + Valkey via testcontainers
make vuln                                 # govulncheck gate

cd ui
export NODE_AUTH_TOKEN=$(gh auth token)   # ui/.npmrc only references this variable
npm ci && npm run lint && npm run test:unit && npm run build
```

The integration suites (`internal/app`, `internal/repo/repodb`,
`internal/stream/valkeykv`) start their own TimescaleDB and Valkey containers
and skip when Docker is unavailable. The Playwright specs under `ui/tests/e2e`
need a running platform (`E2E_BASE`, default `https://localhost:8443`) and
operator credentials (`E2E_OPERATOR_EMAIL`, `E2E_OPERATOR_PASSWORD`,
optionally `E2E_TOTP_SECRET`); they skip without a password.

## Run locally

The deployer needs the gateway, auth and lcm next to it, so it runs from the
go-tangra platform stack (`deploy/stack` in
[go-tangra/go-tangra](https://github.com/go-tangra/go-tangra)). The stack mounts
its configuration at `/app/deploy/container.yaml` and the development
key-encryption key at `/app/deploy/kek.dev`. `deploy/kek.dev` in this
repository is the same development-only key; it is excluded from the image.

```bash
deployersvc -config deploy/container.yaml             # serve (applies migrations)
deployersvc bootstrap -config deploy/container.yaml   # apply migrations and exit
deployersvc version
```

See `specs/008-deployer-service/quickstart.md` for the end-to-end walkthrough.

## Container image

The image is `ghcr.io/go-tangra/go-tangra-deployer`, built by
`.github/workflows/ci.yaml`. It carries `deployersvc` with the embedded UI remote.

```bash
docker buildx build --secret id=npm_token,env=NODE_AUTH_TOKEN \
  --build-arg APP_VERSION=4.0.0 -t go-tangra-deployer:dev .
docker run --rm go-tangra-deployer:dev version
```

The npm token is mounted only for `npm ci` and never lands in a layer. Key
material under `deploy/` is excluded by `.dockerignore`. Tags `vX.Y.Z` publish
`X.Y.Z`, `X.Y` and `X`; pushes to `main` publish `sha-<short>`. There is no
`latest` tag.

## Versions

v4 is the go-tangra v4 platform rebuild. The v3 line stays on the `v3` branch
and its `v3.x` tags.
