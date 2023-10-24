CREATE TABLE default.metars_raw
(
    `date` DateTime,
    `added` DateTime MATERIALIZED now(),
    `icao_code` LowCardinality(String),
    `value` String
)
ENGINE = SharedReplacingMergeTree
ORDER BY (icao_code, date);
