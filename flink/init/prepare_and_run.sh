#!/usr/bin/env bash
set -euo pipefail

JAR_DIR=/opt/flink/lib
KAFKA_JAR=flink-sql-connector-kafka-1.17.2.jar
RUNTIME_JAR=flink-connector-kafka-1.17.2.jar
JSON_JAR=flink-json-1.17.2.jar
CLIENT_JAR=kafka-clients-3.4.1.jar

echo "Downloading required Flink connectors..."
wget -q -nc -O "$JAR_DIR/$KAFKA_JAR" "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.2/flink-sql-connector-kafka-1.17.2.jar" || true
wget -q -nc -O "$JAR_DIR/$RUNTIME_JAR" "https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.17.2/flink-connector-kafka-1.17.2.jar" || true
wget -q -nc -O "$JAR_DIR/$JSON_JAR" "https://repo1.maven.org/maven2/org/apache/flink/flink-json/1.17.2/flink-json-1.17.2.jar" || true
wget -q -nc -O "$JAR_DIR/$CLIENT_JAR" "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" || true

echo "Waiting for Flink JobManager to be ready..."
sleep 15

echo "Submitting Flink SQL job detached..."
/opt/flink/bin/sql-client.sh -m http://flink-jobmanager:8081 \
  -D execution.attached=false \
  -f /opt/flink/jobs/delays.sql \
  -j /opt/flink/lib/${KAFKA_JAR} \
  -j /opt/flink/lib/${RUNTIME_JAR} \
  -j /opt/flink/lib/${JSON_JAR}

echo "Flink SQL job submitted in detached mode."
sleep infinity

