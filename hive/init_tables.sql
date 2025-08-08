-- Initialize Hive external tables on startup
-- This script creates the external table over Parquet outputs

CREATE DATABASE IF NOT EXISTS transit_analytics;
USE transit_analytics;

-- External table over daily route delay aggregates
CREATE TABLE IF NOT EXISTS route_delay_daily (
    event_date DATE,
    route_id STRING,
    avg_delay_seconds DOUBLE
)
STORED AS PARQUET
LOCATION '/opt/warehouse/aggregates/route_delay_daily/'
TBLPROPERTIES (
    'parquet.compression'='snappy',
    'description'='Daily route delay aggregations from streaming pipeline'
);

-- Refresh table metadata
MSCK REPAIR TABLE route_delay_daily;

-- Show sample data
SELECT 'Table created successfully' as status;
SELECT COUNT(*) as total_records FROM route_delay_daily;
SELECT * FROM route_delay_daily LIMIT 5;