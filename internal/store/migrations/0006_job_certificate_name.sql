-- +goose Up
-- The deployed certificate's name (common name, else first SAN), recorded when
-- the job fetches it from lcm, so a job is recognisable without looking the
-- certificate id up in lcm. certificate_serial existed but was never filled.
ALTER TABLE deployer_jobs ADD COLUMN IF NOT EXISTS certificate_name text NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE deployer_jobs DROP COLUMN IF EXISTS certificate_name;
