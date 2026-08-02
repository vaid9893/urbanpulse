"""
validator.py

Consumes bus_gps and air_quality, validates every message, and routes
failures to urbanpulse.dlq with an error_reason field. Valid messages are
re-published to <topic>.validated so downstream consumers (the Streams
enrichment app, dashboards) only ever see clean data.

Validation rules:
  bus_gps:
    - lat/lon must fall within plausible city bounding box (impossible GPS
      coordinates check)
    - lat/lon must not be null
  air_quality:
    - aqi must not be null (catches the simulated 5% sensor failures from
      the air_quality producer)
    - aqi, if present, must be in range [0, 500] (standard AQI scale;
      anything outside is an out-of-range reading, not a real value)
"""

import json
import logging
import uuid
from datetime import datetime, timezone

from kafka import KafkaConsumer, KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("validator")

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
DLQ_TOPIC = "urbanpulse.dlq"

# City bounding box — same as used by the bus_gps_producer simulator.
LAT_RANGE = (12.90, 13.10)
LON_RANGE = (77.50, 77.70)
AQI_RANGE = (0, 500)


def validate_bus_gps(event: dict) -> str | None:
    """Returns an error_reason string if invalid, else None."""
    lat, lon = event.get("lat"), event.get("lon")
    if lat is None or lon is None:
        return "NULL_COORDINATES"
    if not (LAT_RANGE[0] <= lat <= LAT_RANGE[1]) or not (LON_RANGE[0] <= lon <= LON_RANGE[1]):
        return "IMPOSSIBLE_GPS_COORDINATES"
    if event.get("route_id") is None or event.get("bus_id") is None:
        return "MISSING_IDENTIFIER"
    return None


def validate_air_quality(event: dict) -> str | None:
    aqi = event.get("aqi")
    if aqi is None:
        return "NULL_AQI_VALUE"
    if not (AQI_RANGE[0] <= aqi <= AQI_RANGE[1]):
        return "OUT_OF_RANGE_AQI"
    if event.get("monitor_id") is None:
        return "MISSING_IDENTIFIER"
    return None


VALIDATORS = {
    "bus_gps": validate_bus_gps,
    "air_quality": validate_air_quality,
}


def to_dlq_record(source_topic: str, original_event: dict, error_reason: str) -> dict:
    return {
        "dlq_event_id": str(uuid.uuid4()),
        "source_topic": source_topic,
        "error_reason": error_reason,
        "original_event": original_event,
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }


def run(source_topics=("bus_gps", "air_quality"), run_seconds: int = 300):
    import time

    consumer = KafkaConsumer(
        *source_topics,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="dlq_validator",
        enable_auto_commit=True,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: (
            k if isinstance(k, (bytes, bytearray)) else k.encode("utf-8")
        ) if k else None,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    start = time.time()
    stats = {"valid": 0, "invalid": 0}
    try:
        while time.time() - start < run_seconds:
            records = consumer.poll(timeout_ms=500)
            for tp, messages in records.items():
                topic = tp.topic
                validator = VALIDATORS.get(topic)
                for msg in messages:
                    event = msg.value
                    error_reason = validator(event) if validator else None
                    if error_reason:
                        dlq_record = to_dlq_record(topic, event, error_reason)
                        producer.send(DLQ_TOPIC, key=error_reason, value=dlq_record)
                        stats["invalid"] += 1
                        log.warning("DLQ: %s (%s)", error_reason, topic)
                    else:
                        producer.send(f"{topic}.validated", key=msg.key, value=event)
                        stats["valid"] += 1
            producer.flush()
        log.info("Validation run complete: %s", stats)
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    run()