### Execution checklist

- Step 1 — Install Docker Desktop: [x]
- Step 2 — Start the stack (`docker compose up -d`): [x]
- Step 3 — Verify UIs (Kafka 8083, Flink 8081, Spark 8082, Airflow 8089, Jupyter 8888): [x]
- Step 4 — Verify streaming data (topics `gtfs_trip_updates`, `delays`; consume a few from `delays`): [ ]
- Step 5 — Trigger batch now (Airflow UI or `spark-submit`): [ ]
- Step 6 — Register Hive external table and run sample queries: [ ]
- Step 7 — View example chart in Jupyter (`notebooks/analysis.ipynb`): [ ]
- Step 8 — Optional: Add GTFS static under `data/gtfs_static/`: [ ]
- Step 9 — Optional: Switch to real feed (set `GTFS_RT_FEED_URL`, `GTFS_API_KEY`): [ ]
- Step 10 — Optional: Confirm Airflow DAG runs daily at 01:00: [ ] - 

