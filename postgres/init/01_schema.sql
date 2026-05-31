-- Raw telemetry: every reading from every GPU
CREATE TABLE IF NOT EXISTS gpu_raw_telemetry (
    id               BIGSERIAL PRIMARY KEY,
    event_time       TIMESTAMPTZ,
    gpu_id           VARCHAR(20),
    node             VARCHAR(20),
    gpu_model        VARCHAR(50),
    utilization_pct  NUMERIC(6,2),
    temperature_c    NUMERIC(6,2),
    power_draw_w     NUMERIC(8,2),
    mem_bandwidth_gbs NUMERIC(8,2),
    mem_used_pct     NUMERIC(6,2),
    sm_clock_mhz     INTEGER,
    mem_clock_mhz    INTEGER,
    error_code       VARCHAR(30),
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Per-batch analytics aggregated by GPU
CREATE TABLE IF NOT EXISTS gpu_analytics (
    id               BIGSERIAL PRIMARY KEY,
    gpu_id           VARCHAR(20),
    node             VARCHAR(20),
    gpu_model        VARCHAR(50),
    avg_util         NUMERIC(6,2),
    avg_temp         NUMERIC(6,2),
    max_temp         NUMERIC(6,2),
    avg_power        NUMERIC(8,2),
    avg_mem_bw       NUMERIC(8,2),
    avg_mem_util     NUMERIC(6,2),
    util_stddev      NUMERIC(6,2),
    temp_stddev      NUMERIC(6,2),
    reading_count    INTEGER,
    efficiency_score NUMERIC(6,2),
    thermal_throttle BOOLEAN,
    underutilized    BOOLEAN,
    anomaly_flag     BOOLEAN,
    health_status    VARCHAR(20),
    created_at       TIMESTAMPTZ DEFAULT NOW()
);

-- Alerts only (filtered subset for fast dashboard queries)
CREATE TABLE IF NOT EXISTS gpu_alerts (
    id          BIGSERIAL PRIMARY KEY,
    gpu_id      VARCHAR(20),
    node        VARCHAR(20),
    alert_type  VARCHAR(20),
    avg_temp    NUMERIC(6,2),
    avg_util    NUMERIC(6,2),
    max_temp    NUMERIC(6,2),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for dashboard query performance
CREATE INDEX IF NOT EXISTS idx_raw_gpu_id    ON gpu_raw_telemetry(gpu_id);
CREATE INDEX IF NOT EXISTS idx_raw_time      ON gpu_raw_telemetry(event_time DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_gpu ON gpu_analytics(gpu_id);
CREATE INDEX IF NOT EXISTS idx_analytics_ts  ON gpu_analytics(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_ts     ON gpu_alerts(created_at DESC);