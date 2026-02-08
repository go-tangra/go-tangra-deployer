# go-tangra-deployer

Certificate deployment service that manages automated distribution of TLS certificates to infrastructure targets. Integrates with LCM for event-driven deployments.

## Features

- **Multi-target Deployment** — Deploy certificates to groups of targets with parent/child job hierarchies
- **Provider Abstraction** — Pluggable deployment backends (AWS ACM, F5 BIG-IP, Cloudflare, FortiGate, Webhook)
- **Event-Driven Auto-Deploy** — Listens to LCM certificate events via Redis pub/sub and auto-deploys to matching targets
- **Job Lifecycle** — Async execution with worker pool, retry with exponential backoff, progress tracking
- **Certificate Filtering** — Regex-based matching on issuer, CN, SAN, and organization
- **Verification & Rollback** — Post-deployment verification and rollback support (provider-dependent)
- **Statistics & Audit** — Comprehensive deployment metrics and execution history

## Deployment Providers

| Provider | Description |
|----------|-------------|
| **AWS ACM** | AWS Certificate Manager |
| **F5 BIG-IP** | Load balancer certificate deployment |
| **Cloudflare** | DNS and WAF certificate management |
| **FortiGate** | Fortinet appliance certificate deployment |
| **Webhook** | Generic HTTP webhook for custom integrations |
| **Dummy** | Mock provider for testing |

## gRPC Services

| Service | Port | Purpose |
|---------|------|---------|
| DeploymentService | 9200 | Manual deployments, verify, rollback |
| DeploymentJobService | 9200 | Job management, status tracking, retry |
| DeploymentTargetService | 9200 | Target groups with certificate filter rules |
| TargetConfigurationService | 9200 | Endpoint configuration, credential validation |
| DeployerStatisticsService | 9200 | System-wide and per-tenant metrics |

## Job Workflow

```
Deploy Request → Parent Job (target group)
                  ├── Child Job 1 (endpoint A) → PENDING → PROCESSING → COMPLETED
                  ├── Child Job 2 (endpoint B) → PENDING → PROCESSING → FAILED → RETRYING
                  └── Child Job 3 (endpoint C) → PENDING → PROCESSING → COMPLETED

Parent Status: PARTIAL (some succeeded, some failed)
```

## Configuration

```yaml
deployer:
  events:
    enabled: true
    subscribe_events:
      - "certificate.issued"
      - "renewal.completed"
  jobs:
    worker_count: 5
    max_retries: 3
    retry_delay_seconds: 60
    job_timeout_seconds: 300
```

## Build

```bash
make build-server       # Build binary
make docker             # Build Docker image
make docker-buildx      # Multi-platform build (amd64/arm64)
make test               # Run tests
make ent                # Generate database schema
```

## Docker

```bash
docker run -p 9200:9200 ghcr.io/go-tangra/go-tangra-deployer:latest
```

Runs as non-root user `deployer` (UID 1000). Configuration mounted at `/app/configs`.
