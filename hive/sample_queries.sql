/*
    Create external table over Spark Parquet output
*/
CREATE DATABASE IF NOT EXISTS transit;
USE transit;

CREATE EXTERNAL TABLE IF NOT EXISTS route_delay_daily (
    route_id STRING,
    avg_delay_seconds DOUBLE
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION '/opt/warehouse/aggregates/route_delay_daily';

MSCK REPAIR TABLE route_delay_daily;

/* Average delay per route per day */
SELECT
    event_date,
    route_id,
    avg_delay_seconds
FROM route_delay_daily
WHERE event_date >= date_sub(current_date, 7)
ORDER BY event_date DESC, avg_delay_seconds DESC
;

/* Top 5 most delayed routes in last 30 days */
SELECT
    route_id,
    AVG(avg_delay_seconds) AS avg_delay_30d
FROM route_delay_daily
WHERE event_date >= date_sub(current_date, 30)
GROUP BY route_id
ORDER BY avg_delay_30d DESC
LIMIT 5
;

/* On-time performance trend (<= 60s delay) */
SELECT
    event_date,
    AVG(CASE WHEN avg_delay_seconds <= 60 THEN 1 ELSE 0 END) AS on_time_rate
FROM route_delay_daily
GROUP BY event_date
ORDER BY event_date
;

/* Heatmap: average delay by hour of day and route (requires hourly aggregation in future) */
-- Example placeholder if you later store hour_of_day as a column
-- SELECT route_id, hour_of_day, AVG(delay_seconds) AS avg_delay
-- FROM route_delay_hourly
-- WHERE event_date >= date_sub(current_date, 7)
-- GROUP BY route_id, hour_of_day
-- ORDER BY route_id, hour_of_day
-- ;

