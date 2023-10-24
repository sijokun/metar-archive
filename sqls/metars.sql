CREATE TABLE default.metars
(
    `date` DateTime,
    `added` DateTime MATERIALIZED now(),
    `icao_code` LowCardinality(String),
    `raw` String,
    `wind_dir` Nullable(Int32) DEFAULT NULL,
    `wind_speed` Nullable(Int32) DEFAULT NULL,
    `wind_gust` Nullable(Int32) DEFAULT NULL,
    `wind_dir_from` Nullable(Int32) DEFAULT NULL,
    `wind_dir_to` Nullable(Int32) DEFAULT NULL,
    `visibility` Nullable(Int32) DEFAULT NULL,
    `visibility_greater_less` Nullable(FixedString(1)) DEFAULT NULL,
    `condition` Nullable(String) DEFAULT NULL,
    `temperature` Nullable(Int32) DEFAULT NULL,
    `dew_point` Nullable(Int32) DEFAULT NULL,
    `altimeter` Nullable(Int32) DEFAULT NULL,
    `clouds` Nested(
        `type` Nullable(String),
        `ceiling` Nullable(Int32),
        `additional_type` Nullable(String)
        )
)
ENGINE = SharedReplacingMergeTree
ORDER BY (icao_code, date);
