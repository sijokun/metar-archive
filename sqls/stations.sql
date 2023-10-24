CREATE TABLE default.stations
(
    `icao_code` FixedString(4),
    `iata_code` Nullable(FixedString(3)) DEFAULT NULL,
    `name` Nullable(String) DEFAULT NULL,
    `latitude` Nullable(Float32) DEFAULT NULL,
    `longitude` Nullable(Float32) DEFAULT NULL
)
ENGINE = SharedReplacingMergeTree
ORDER BY icao_code;
