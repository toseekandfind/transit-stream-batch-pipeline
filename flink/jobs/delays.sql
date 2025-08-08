-- Flink SQL job to compute 1-minute windowed average delays by route

CREATE TABLE IF NOT EXISTS trip_updates (
    event_type STRING,
    route_id STRING,
    trip_id STRING,
    stop_id STRING,
    delay_seconds INT,
    `timestamp` BIGINT,
    ts AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
    WATERMARK FOR ts AS ts - INTERVAL '30' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gtfs_trip_updates',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-delays',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS delays (
    window_start TIMESTAMP_LTZ(3),
    window_end TIMESTAMP_LTZ(3),
    route_id STRING,
    avg_delay_seconds DOUBLE
) WITH (
    'connector' = 'kafka',
    'topic' = 'delays',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json'
);

INSERT INTO delays
SELECT
    window_start,
    window_end,
    route_id,
    AVG(CAST(delay_seconds AS DOUBLE)) AS avg_delay_seconds
FROM TABLE(
    TUMBLE(TABLE trip_updates, DESCRIPTOR(ts), INTERVAL '1' MINUTE)
)
WHERE delay_seconds IS NOT NULL AND route_id IS NOT NULL
GROUP BY window_start, window_end, route_id;

