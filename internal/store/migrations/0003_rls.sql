-- +goose Up
-- Per-tenant row-level security. The application role (deployer_app) is
-- NOBYPASSRLS; every statement runs with app.tenant_id set to the caller's
-- tenant. Trusted worker paths (job claiming, cleanup, audit writing) set
-- app.system so the policy admits their cross-tenant access.
-- +goose StatementBegin
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['deployer_configs','deployer_targets','deployer_target_configs_link','deployer_jobs','deployer_history','deployer_grants','deployer_audit_events']
  LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format($p$CREATE POLICY tenant_isolation ON %I USING (tenant_id = current_setting('app.tenant_id', true)::uuid OR current_setting('app.system', true) = 'on') WITH CHECK (tenant_id = current_setting('app.tenant_id', true)::uuid OR current_setting('app.system', true) = 'on')$p$, t);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I TO deployer_app', t);
  END LOOP;
END $$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['deployer_configs','deployer_targets','deployer_target_configs_link','deployer_jobs','deployer_history','deployer_grants','deployer_audit_events']
  LOOP
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format('ALTER TABLE %I DISABLE ROW LEVEL SECURITY', t);
  END LOOP;
END $$;
-- +goose StatementEnd
