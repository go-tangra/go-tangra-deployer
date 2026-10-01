-- +goose Up
-- Index-backed sorting (go-tangra 032 perf): NOT NULL sort fields order without
-- NULLS LAST, so a plain (tenant_id, col, id) btree serves the jobs list's
-- "created_at DESC, id DESC" default by a backward scan. The 0004 index stored
-- created_at DESC with id ascending, which matches neither direction of the
-- list's tie-breaker.
DROP INDEX IF EXISTS jobs_tenant_created;
CREATE INDEX jobs_tenant_created ON deployer_jobs (tenant_id, created_at, id);

-- +goose Down
DROP INDEX IF EXISTS jobs_tenant_created;
CREATE INDEX jobs_tenant_created ON deployer_jobs (tenant_id, created_at DESC, id);
