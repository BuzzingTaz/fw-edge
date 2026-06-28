CREATE TABLE measurement_events (
    meas_time       TIMESTAMPTZ NOT NULL,
    user_id         TEXT NOT NULL,
    task_id         UUID NOT NULL,
    service_name    TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    payload         JSONB
);

-- Convert to TimescaleDB hypertable (partitions by time)
SELECT create_hypertable('measurement_events', 'meas_time');

-- Indexes for common query patterns
CREATE INDEX idx_meas_task      ON measurement_events (task_id);
CREATE INDEX idx_meas_type      ON measurement_events (event_type);
CREATE INDEX idx_meas_user      ON measurement_events (user_id);
CREATE INDEX idx_service_name   ON measurement_events (service_name);
CREATE INDEX idx_event          ON measurement_events (event_type);

-- Prevent duplicate inserts from retry logic
ALTER TABLE measurement_events ADD CONSTRAINT uq_task_event_time UNIQUE (task_id, event_type, meas_time);
