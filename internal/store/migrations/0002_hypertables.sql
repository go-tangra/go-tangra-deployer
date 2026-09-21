-- +goose Up
-- The audit stream is append-only and time-keyed with no primary key, so it
-- becomes a hypertable for time-based retention. deployer_history keeps its uuid
-- primary key (queried by job, not time) and stays a regular table — TimescaleDB
-- requires the partition column in every unique index, which the uuid PK is not.
SELECT create_hypertable('deployer_audit_events', 'ts', if_not_exists => TRUE, migrate_data => TRUE);

-- +goose Down
-- The hypertable is dropped with its table (0001 Down); nothing to revert here.
SELECT 1;
