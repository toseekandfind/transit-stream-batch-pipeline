#!/usr/bin/env bash
set -euo pipefail

JAR_DIR=/opt/flink/lib
KAFKA_JAR=flink-sql-connector-kafka-1.17.2.jar
JSON_JAR=flink-json-1.17.2.jar

if [ ! -f "$JAR_DIR/$KAFKA_JAR" ]; then
  echo "Downloading Flink Kafka connector..."
  wget -q -O "$JAR_DIR/$KAFKA_JAR" "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar"
fi

if [ ! -f "$JAR_DIR/$JSON_JAR" ]; then
  echo "Downloading Flink JSON format..."
  wget -q -O "$JAR_DIR/$JSON_JAR" "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.17.2/flink-json-1.17.2.jar"
fi

echo "Submitting PyFlink job..."
/opt/flink/bin/flink run -m flink-jobmanager:8081 -py /opt/flink/jobs/pyflink_delays_job.py

echo "PyFlink job submitted."
wait -n || true

