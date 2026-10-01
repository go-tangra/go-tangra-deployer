-- +goose Up
-- Server-side list paging and sorting (go-tangra specs/032-server-side-tables):
-- the jobs table and dashboard page a tenant's jobs newest first. The
-- configuration and target lists are small per tenant and their name order is
-- served by the existing (tenant_id, name) unique indexes.
CREATE INDEX IF NOT EXISTS jobs_tenant_created ON deployer_jobs (tenant_id, created_at DESC, id);

-- +goose Down
DROP INDEX IF EXISTS jobs_tenant_created;
