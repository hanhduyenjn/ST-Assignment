-- Per-device, per-interface, per-hour-of-day baseline parameters.
-- Written by nightly PSIDriftDetector; read by streaming job as broadcast join.
-- Engine: ReplacingMergeTree(valid_from) — upsert semantics: latest valid_from wins.

CREATE TABLE IF NOT EXISTS network_health.device_baseline_params
(
    device_id                   String,
    interface_name              String,
    hour_of_day                 UInt8,       -- 0–23; enables seasonal baseline
    day_of_week                 UInt8,       -- 0–6
    baseline_mean               Float32,     -- 30-day rolling mean of effective_util_in
    baseline_std                Float32,     -- 30-day rolling std
    iqr_k                       Float32,     -- IQR fence multiplier (default 1.5)
    ewma_alpha                  Float32,     -- fit from autocorrelation
    isolation_score_threshold   Float32,     -- 95th percentile of normal isolation scores
    distilled_rules             String,      -- JSON-encoded decision tree rules from distill()
    valid_from                  DateTime,    -- version column for ReplacingMergeTree
    valid_until                 DateTime
)
ENGINE = ReplacingMergeTree(valid_from)
ORDER BY (device_id, interface_name, hour_of_day, day_of_week)
SETTINGS index_granularity = 8192;
