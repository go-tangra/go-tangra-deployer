-- +goose Up
-- Status e-mails per target: when a target run (a parent job) finishes, the
-- tenant's owners, admins and deployer administrators are told — on every
-- outcome (always) or only when a deployment failed (failure). Off by default.
ALTER TABLE deployer_targets ADD COLUMN IF NOT EXISTS notify_enabled boolean NOT NULL DEFAULT false;
ALTER TABLE deployer_targets ADD COLUMN IF NOT EXISTS notify_on text NOT NULL DEFAULT 'always';
ALTER TABLE deployer_targets DROP CONSTRAINT IF EXISTS deployer_targets_notify_on_check;
ALTER TABLE deployer_targets ADD CONSTRAINT deployer_targets_notify_on_check CHECK (notify_on IN ('always','failure'));
-- The outcome a parent job's notification was last claimed for: aggregation
-- may run more than once per parent (concurrent children, a retried child),
-- so the notifier claims (parent, status) with a conditional update and only
-- the winner sends — once per distinct outcome.
ALTER TABLE deployer_jobs ADD COLUMN IF NOT EXISTS notified_status text;

-- +goose Down
ALTER TABLE deployer_jobs DROP COLUMN IF EXISTS notified_status;
ALTER TABLE deployer_targets DROP CONSTRAINT IF EXISTS deployer_targets_notify_on_check;
ALTER TABLE deployer_targets DROP COLUMN IF EXISTS notify_on;
ALTER TABLE deployer_targets DROP COLUMN IF EXISTS notify_enabled;
