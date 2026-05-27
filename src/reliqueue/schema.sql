CREATE TABLE IF NOT EXISTS jobs (
    id UUID PRIMARY KEY,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queued', 'leased', 'completed', 'dead', 'cancelled')),
    idempotency_key TEXT,
    attempt INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    timeout_seconds INTEGER NOT NULL,
    lease_owner TEXT,
    leased_until TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    correlation_id UUID,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS jobs_idempotency_key_uidx
    ON jobs(job_type, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS jobs_runnable_idx
    ON jobs(status, run_at);

CREATE INDEX IF NOT EXISTS jobs_lease_idx
    ON jobs(status, leased_until)
    WHERE status = 'leased';

CREATE INDEX IF NOT EXISTS jobs_finished_idx
    ON jobs(status, finished_at)
    WHERE status IN ('completed', 'dead', 'cancelled');

CREATE TABLE IF NOT EXISTS dead_letter_jobs (
    id UUID PRIMARY KEY,
    original_job_id UUID NOT NULL,
    job_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    attempts INTEGER NOT NULL,
    last_error TEXT,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
