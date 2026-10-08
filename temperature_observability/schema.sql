CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS checkpoints (
    stream TEXT NOT NULL,
    partition INTEGER NOT NULL,
    last_offset BIGINT NOT NULL,
    PRIMARY KEY (stream, partition)
);

CREATE TABLE IF NOT EXISTS measurements (
    experiment_id TEXT NOT NULL REFERENCES experiments(id),
    measurement_id TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('stabilizing', 'running')),
    out_of_range BOOLEAN NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id)
);
CREATE INDEX IF NOT EXISTS measurements_history
    ON measurements (experiment_id, timestamp, measurement_id) WHERE phase = 'running';
CREATE INDEX IF NOT EXISTS measurements_out_of_range
    ON measurements (experiment_id, timestamp, measurement_id)
    WHERE phase = 'running' AND out_of_range;

CREATE TABLE IF NOT EXISTS notification_outbox (
    id BIGSERIAL PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(id),
    measurement_id TEXT NOT NULL,
    notification_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    measured_at DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    lease_token TEXT,
    delivered_at TIMESTAMPTZ,
    latency DOUBLE PRECISION,
    last_status INTEGER,
    UNIQUE (experiment_id, measurement_id, notification_type)
);
CREATE INDEX IF NOT EXISTS notification_pending
    ON notification_outbox (next_attempt_at, id) WHERE status = 'pending';

CREATE TABLE IF NOT EXISTS rejected_events (
    stream TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    records JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, partition, offset_id)
);
