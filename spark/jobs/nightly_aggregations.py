import os
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, LongType


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def main():
    warehouse_dir = os.environ.get("WAREHOUSE_DIR", "/opt/warehouse")
    kafka_bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "delays")
    gtfs_static_dir = os.environ.get("GTFS_STATIC_DIR", "/opt/data/gtfs_static")

    spark = get_spark("nightly_aggregations")

    # Smoke test: aggregate "today" (UTC) so we don't need to wait until tomorrow
    today = datetime.utcnow().date()
    start_dt = datetime.combine(today, datetime.min.time())
    end_dt = datetime.combine(today + timedelta(days=1), datetime.min.time())

    # Read from Kafka
    df = (
        spark.read.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Parse JSON payload
    payload_schema = (
        StructType()
        .add("window_start", StringType())
        .add("window_end", StringType())
        .add("route_id", StringType())
        .add("avg_delay_seconds", DoubleType())
    )

    parsed = (
        df.select(F.from_json(F.col("value").cast("string"), payload_schema).alias("j"))
        .select("j.*")
        .withColumn("window_start_ts", F.to_timestamp("window_start"))
        .withColumn("event_date", F.to_date("window_start_ts"))
    )

    # Filter to yesterday
    filtered = parsed.where((F.col("event_date") >= F.lit(start_dt.date().isoformat())) & (F.col("event_date") < F.lit(end_dt.date().isoformat())))

    # Optional: join route names from GTFS static if available
    routes_csv = os.path.join(gtfs_static_dir, "routes.txt")
    joined_with_routes = False
    if os.path.exists(routes_csv):
        routes = (
            spark.read.option("header", True).csv(routes_csv)
            .select(
                F.col("route_id").cast("string").alias("r_route_id"),
                F.col("route_short_name"),
                F.col("route_long_name"),
            )
        )
        filtered = filtered.join(routes, filtered.route_id == routes.r_route_id, "left")
        joined_with_routes = True

    # Aggregate: average delay per route per day
    group_cols = ["event_date", "route_id"]
    if joined_with_routes:
        group_cols += ["route_short_name", "route_long_name"]

    agg = filtered.groupBy(*group_cols).agg(
        F.avg("avg_delay_seconds").alias("avg_delay_seconds")
    )

    out_path = os.path.join(warehouse_dir, "aggregates", "route_delay_daily")
    (
        agg.repartition(1)
        .write.mode("append")
        .partitionBy("event_date")
        .parquet(out_path)
    )

    print(f"Wrote daily aggregates to {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()

