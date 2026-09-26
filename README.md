# go-tangra-deployer

Certificate deployment service for the
[go-tangra v4 platform](https://github.com/go-tangra/go-tangra).

It takes certificates issued by [go-tangra-lcm](https://github.com/go-tangra/go-tangra-lcm)
and installs them on infrastructure targets through pluggable providers (AWS ACM,
Cloudflare, F5 BIG-IP, FortiGate, a generic webhook and a dummy provider for
tests). Deployments run as jobs on a distributed worker pool with leases,
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
