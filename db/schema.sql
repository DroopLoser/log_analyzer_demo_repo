CREATE TABLE IF NOT EXISTS logs (
    id              BIGSERIAL PRIMARY KEY,
    service         TEXT NOT NULL,
    level           TEXT NOT NULL,
    message         TEXT NOT NULL,
    ts              TIMESTAMPTZ NOT NULL,
    raw             JSONB,
    kafka_partition INT,
    kafka_offset    BIGINT,
    UNIQUE (kafka_partition, kafka_offset)   -- makes replays safe
);

CREATE INDEX IF NOT EXISTS idx_logs_ts ON logs (ts);
CREATE INDEX IF NOT EXISTS idx_logs_service_level ON logs (service, level, ts);