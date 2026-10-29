-- Every node runs this on startup, so it all has to be safe to re-run.

CREATE TABLE IF NOT EXISTS tenants (
    id          UUID PRIMARY KEY,
    paused      BOOLEAN NOT NULL DEFAULT false,
    max_running INTEGER NOT NULL DEFAULT 100 CHECK (max_running > 0)
);

CREATE TABLE IF NOT EXISTS jobs (
    id             UUID PRIMARY KEY,
    tenant_id      UUID NOT NULL REFERENCES tenants(id),
    type           TEXT NOT NULL,
    payload        JSONB NOT NULL,
    cron           TEXT,
    timezone       TEXT NOT NULL,
    misfire_policy TEXT NOT NULL,
    priority       INTEGER NOT NULL DEFAULT 0,
    max_runtime_ms INTEGER NOT NULL,
    shard          INTEGER NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

CREATE INDEX IF NOT EXISTS jobs_tenant ON jobs (tenant_id, created_at);

-- One row per occurrence. Recurring jobs always have their next run sitting
-- here; claiming it inserts the one after.
CREATE TABLE IF NOT EXISTS runs (
    id              UUID PRIMARY KEY,
    job_id          UUID NOT NULL REFERENCES jobs(id),
    tenant_id       UUID NOT NULL,
    shard           INTEGER NOT NULL,
    priority        INTEGER NOT NULL,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    manual          BOOLEAN NOT NULL DEFAULT false,
    status          TEXT NOT NULL,
    attempt         INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL,
    owner           TEXT,
    fence           BIGINT,
    lease_until     TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    finished_at     TIMESTAMPTZ,
    last_error      TEXT
);

CREATE INDEX IF NOT EXISTS runs_due ON runs (shard, next_attempt_at) WHERE status IN ('PENDING', 'RETRYING');
CREATE INDEX IF NOT EXISTS runs_running ON runs (shard, lease_until) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS runs_running_tenant ON runs (tenant_id) WHERE status = 'RUNNING';
CREATE INDEX IF NOT EXISTS runs_dead ON runs (tenant_id, finished_at) WHERE status = 'DEAD';
CREATE INDEX IF NOT EXISTS runs_job ON runs (job_id, scheduled_at);

-- epoch goes up whenever a shard changes hands. Writes carry the epoch they
-- were claimed under, so a stale owner's writes just don't match.
CREATE TABLE IF NOT EXISTS shard_leases (
    shard      INTEGER PRIMARY KEY,
    owner      TEXT,
    epoch      BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'epoch'
);

INSERT INTO shard_leases (shard) SELECT generate_series(0, 63) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS nodes (
    id           TEXT PRIMARY KEY,
    heartbeat_at TIMESTAMPTZ NOT NULL
);
