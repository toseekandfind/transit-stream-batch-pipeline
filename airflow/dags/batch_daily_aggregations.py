from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "data",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="batch_daily_aggregations",
    default_args=default_args,
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command=(
            "docker exec tsbp-spark-master /opt/bitnami/spark/bin/spark-submit "
            " --master spark://spark-master:7077 "
            " --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 "
            " /opt/spark/jobs/nightly_aggregations.py"
        ),
    )

    run_spark

