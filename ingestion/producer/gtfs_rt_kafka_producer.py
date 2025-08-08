import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, Optional

import requests
from confluent_kafka import Producer

try:
    from google.transit import gtfs_realtime_pb2  # type: ignore
    HAS_PROTO = True
except Exception:
    HAS_PROTO = False


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")


def build_kafka_producer(brokers: str) -> Producer:
    conf = {
        "bootstrap.servers": brokers,
        "client.id": "gtfs-rt-producer",
        "enable.idempotence": True,
        "linger.ms": 50,
        "batch.num.messages": 1000,
    }
    return Producer(conf)


def fetch_gtfs_rt(url: str, api_key: Optional[str]) -> Optional[bytes]:
    headers = {}
    if api_key:
        # Common patterns – users may need to adjust for their agency
        headers["x-api-key"] = api_key
        headers["apikey"] = api_key
        headers["Authorization"] = f"Bearer {api_key}"
    try:
        r = requests.get(url, headers=headers, timeout=15)
        r.raise_for_status()
        return r.content
    except Exception as e:
        print(f"Failed to fetch GTFS-RT: {e}")
        return None


def parse_trip_updates(feed_bytes: bytes) -> list[Dict[str, Any]]:
    if not HAS_PROTO:
        return []
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_bytes)
    events: list[Dict[str, Any]] = []
    now_ts = int(time.time())
    for entity in feed.entity:
        if not entity.HasField("trip_update"):
            continue
        tu = entity.trip_update
        route_id = tu.trip.route_id
        trip_id = tu.trip.trip_id
        # Emit one event per stop_time_update if available, else a minimal one
        if tu.stop_time_update:
            for stu in tu.stop_time_update:
                delay_seconds = None
                if stu.HasField("arrival") and stu.arrival.delay:
                    delay_seconds = int(stu.arrival.delay)
                elif stu.HasField("departure") and stu.departure.delay:
                    delay_seconds = int(stu.departure.delay)
                event = {
                    "event_type": "trip_update",
                    "route_id": route_id or None,
                    "trip_id": trip_id or None,
                    "stop_id": stu.stop_id or None,
                    "delay_seconds": delay_seconds,
                    "timestamp": now_ts,
                }
                events.append(event)
        else:
            events.append(
                {
                    "event_type": "trip_update",
                    "route_id": route_id or None,
                    "trip_id": trip_id or None,
                    "stop_id": None,
                    "delay_seconds": None,
                    "timestamp": now_ts,
                }
            )
    return events


def parse_vehicle_positions(feed_bytes: bytes) -> list[Dict[str, Any]]:
    if not HAS_PROTO:
        return []
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(feed_bytes)
    events: list[Dict[str, Any]] = []
    now_ts = int(time.time())
    for entity in feed.entity:
        if not entity.HasField("vehicle"):
            continue
        veh = entity.vehicle
        position = veh.position if veh.HasField("position") else None
        events.append(
            {
                "event_type": "vehicle_position",
                "route_id": veh.trip.route_id or None if veh.HasField("trip") else None,
                "trip_id": veh.trip.trip_id or None if veh.HasField("trip") else None,
                "latitude": getattr(position, "latitude", None) if position else None,
                "longitude": getattr(position, "longitude", None) if position else None,
                "bearing": getattr(position, "bearing", None) if position else None,
                "speed": getattr(position, "speed", None) if position else None,
                "timestamp": now_ts,
            }
        )
    return events


def run_demo(producer: Producer, topic: str) -> None:
    print("Running demo mode: emitting synthetic events...")
    while True:
        ts = int(time.time())
        for route in ["A", "B", "C"]:
            payload = {
                "event_type": "trip_update",
                "route_id": route,
                "trip_id": f"{route}-{ts // 600}",
                "stop_id": "STOP1",
                "delay_seconds": int(((ts // 60) % 15) - 7),
                "timestamp": ts,
            }
            producer.produce(topic, json.dumps(payload).encode("utf-8"), callback=delivery_report)
        producer.flush()
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="GTFS-RT to Kafka producer")
    parser.add_argument("--brokers", default=os.environ.get("KAFKA_BROKERS", "localhost:29092"))
    parser.add_argument("--topic", default=os.environ.get("KAFKA_TOPIC", "gtfs_trip_updates"))
    parser.add_argument("--feed_url", default=os.environ.get("GTFS_RT_FEED_URL"))
    parser.add_argument("--feed_type", choices=["trip_updates", "vehicle_positions"], default=os.environ.get("GTFS_RT_FEED_TYPE", "trip_updates"))
    parser.add_argument("--api_key", default=os.environ.get("GTFS_API_KEY"))
    parser.add_argument("--interval_sec", type=int, default=30)
    parser.add_argument("--demo", action="store_true", help="Run with synthetic data instead of a real feed")
    args = parser.parse_args()

    producer = build_kafka_producer(args.brokers)

    if args.demo or not args.feed_url:
        run_demo(producer, args.topic)
        return

    if not HAS_PROTO:
        print("gtfs-realtime-bindings is not installed; install it or run with --demo")
        sys.exit(1)

    print(f"Starting GTFS-RT ingestion from {args.feed_url} → Kafka topic {args.topic}")
    while True:
        blob = fetch_gtfs_rt(args.feed_url, args.api_key)
        if blob:
            if args.feed_type == "trip_updates":
                events = parse_trip_updates(blob)
            else:
                events = parse_vehicle_positions(blob)

            for evt in events:
                producer.produce(args.topic, json.dumps(evt).encode("utf-8"), callback=delivery_report)
            producer.flush()
            print(f"Emitted {len(events)} events at {datetime.now(timezone.utc).isoformat()}")
        time.sleep(args.interval_sec)


if __name__ == "__main__":
    main()

