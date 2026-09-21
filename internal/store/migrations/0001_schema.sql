-- +goose Up
CREATE TABLE deployer_configs (
  id                  uuid PRIMARY KEY,
  tenant_id           uuid NOT NULL,
  name                text NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
  description         text NOT NULL DEFAULT '',
  provider_type       text NOT NULL,
  config              jsonb NOT NULL DEFAULT '{}'::jsonb,
  credentials_sealed  bytea,
  status              text NOT NULL DEFAULT 'active' CHECK (status IN ('active','inactive','error')),
  status_message      text NOT NULL DEFAULT '',
  last_deployment_at  timestamptz,
  created_by          text NOT NULL DEFAULT '',
  updated_by          text NOT NULL DEFAULT '',
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name)
);

CREATE TABLE deployer_targets (
  id                  uuid PRIMARY KEY,
  tenant_id           uuid NOT NULL,
  name                text NOT NULL CHECK (length(name) BETWEEN 1 AND 100),
  description         text NOT NULL DEFAULT '',
  auto_deploy         boolean NOT NULL DEFAULT false,
  certificate_filters jsonb NOT NULL DEFAULT '[]'::jsonb,
  config_overrides    jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_by          text NOT NULL DEFAULT '',
  updated_by          text NOT NULL DEFAULT '',
  created_at          timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, name)
);

CREATE TABLE deployer_target_configs_link (
  tenant_id        uuid NOT NULL,
  target_id        uuid NOT NULL REFERENCES deployer_targets(id) ON DELETE CASCADE,
  configuration_id uuid NOT NULL REFERENCES deployer_configs(id) ON DELETE CASCADE,
  PRIMARY KEY (target_id, configuration_id)
);

CREATE TABLE deployer_jobs (
  id                      uuid PRIMARY KEY,
  tenant_id               uuid NOT NULL,
  deployment_target_id    uuid,
  target_configuration_id uuid,
  parent_job_id           uuid,
  certificate_id          text NOT NULL DEFAULT '',
  certificate_serial      text NOT NULL DEFAULT '',
  status                  text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending','processing','completed','failed','cancelled','retrying','partial')),
  status_message          text NOT NULL DEFAULT '',
  progress                int NOT NULL DEFAULT 0,
  retry_count             int NOT NULL DEFAULT 0,
  max_retries             int NOT NULL DEFAULT 3,
  triggered_by            text NOT NULL DEFAULT 'manual' CHECK (triggered_by IN ('manual','event','auto_renewal')),
  result                  jsonb,
  lease_until             timestamptz,
  started_at              timestamptz,
  completed_at            timestamptz,
  next_retry_at           timestamptz,
  created_at              timestamptz NOT NULL DEFAULT now(),
  updated_at              timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX jobs_tenant_status ON deployer_jobs (tenant_id, status, next_retry_at);
CREATE INDEX jobs_due ON deployer_jobs (status, next_retry_at) WHERE status IN ('pending','retrying');
CREATE INDEX jobs_parent ON deployer_jobs (parent_job_id);

CREATE TABLE deployer_history (
  id           uuid PRIMARY KEY,
  tenant_id    uuid NOT NULL,
  job_id       uuid NOT NULL REFERENCES deployer_jobs(id) ON DELETE CASCADE,
  action       text NOT NULL CHECK (action IN ('deploy','verify','rollback')),
  result       text NOT NULL CHECK (result IN ('success','failure','partial')),
  message      text NOT NULL DEFAULT '',
  duration_ms  int NOT NULL DEFAULT 0,
  details      jsonb,
  created_at   timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX history_job ON deployer_history (tenant_id, job_id);

CREATE TABLE deployer_grants (
  id            uuid PRIMARY KEY,
  tenant_id     uuid NOT NULL,
  resource_type text NOT NULL,
  resource_id   text NOT NULL,
  subject_type  text NOT NULL,
  subject_id    text NOT NULL DEFAULT '',
  relation      text NOT NULL,
  granted_by    text,
  granted_at    timestamptz NOT NULL DEFAULT now(),
  expires_at    timestamptz
);
CREATE INDEX grants_resource ON deployer_grants (tenant_id, resource_type, resource_id);

CREATE TABLE deployer_audit_events (
  ts             timestamptz NOT NULL DEFAULT now(),
  tenant_id      uuid NOT NULL,
  event_type     text NOT NULL,
  actor_kind     text NOT NULL,
  actor_id       text NOT NULL DEFAULT '',
  subject_kind   text NOT NULL DEFAULT '',
  subject_id     text NOT NULL DEFAULT '',
  outcome        text NOT NULL,
  reason         text NOT NULL DEFAULT '',
  correlation_id text NOT NULL DEFAULT '',
  details        jsonb
);

-- +goose Down
DROP TABLE IF EXISTS deployer_audit_events;
DROP TABLE IF EXISTS deployer_grants;
DROP TABLE IF EXISTS deployer_history;
DROP TABLE IF EXISTS deployer_jobs;
DROP TABLE IF EXISTS deployer_target_configs_link;
DROP TABLE IF EXISTS deployer_targets;
DROP TABLE IF EXISTS deployer_configs;
