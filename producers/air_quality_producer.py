"""
air_quality_producer.py

Simulates 600 AQI monitors reporting every ~10 seconds.

At-least-once semantics:
- acks="all" (wait for every in-sync replica to confirm).
- retries with backoff at the producer-client level (network/broker level).
- An explicit application-level retry wrapper on top, because "at-least-once"
  here specifically covers sensor timeouts (the monitor hardware itself
  times out mid-read), which is a different failure mode than a broker
  being unreachable — the client-level `retries` config won't help if the
  *read from the sensor* fails before we ever call producer.send().
- Because this is at-least-once (not exactly-once), duplicate delivery is
  possible on retry after an ack is lost in transit; downstream consumers
  should dedupe on event_id if exact counts matter.

Simulated sensor failure:
5% of readings arrive with aqi=None (sensor timeout/misread). These are
NOT silently dropped and NOT sent as valid readings — they are logged and
still published, but flagged with a `sensor_status` field so downstream
validators (Problem 5's DLQ) can route them to the dead-letter topic
instead of polluting the trend-analysis dataset.
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, KafkaTimeoutError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("air_quality_producer")

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "air_quality"

MONITORS = [f"AQ{n:03d}" for n in range(1, 601)]
NULL_RATE = 0.05          # 5% simulated sensor timeout/failure
SENSOR_READ_TIMEOUT_RATE = 0.03  # 3% simulated hardware read timeout (separate from null AQI)
MAX_APP_LEVEL_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,              # client-level retry for transient broker errors
        retry_backoff_ms=500,
        linger_ms=50,
        compression_type="gzip",
    )


class SensorTimeoutError(Exception):
    """Raised when the simulated hardware read itself fails (not a null AQI)."""


def read_sensor(monitor_id: str) -> dict:
    """Simulates reading a physical AQI monitor. Can raise SensorTimeoutError."""
    if random.random() < SENSOR_READ_TIMEOUT_RATE:
        raise SensorTimeoutError(f"Sensor {monitor_id} timed out on read")

    aqi_value = None if random.random() < NULL_RATE else round(random.uniform(15, 320), 1)

    return {
        "monitor_id": monitor_id,
        "aqi": aqi_value,
        "sensor_status": "OK" if aqi_value is not None else "NULL_READING",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_id": str(uuid.uuid4()),
    }


def send_with_retry(producer: KafkaProducer, monitor_id: str) -> bool:
    """
    Application-level at-least-once wrapper:
    retries the full read+send cycle on sensor timeout, distinct from the
    kafka-python client's own network-level retries.
    Returns True if eventually delivered, False if exhausted retries.
    """
    for attempt in range(1, MAX_APP_LEVEL_RETRIES + 1):
        try:
            payload = read_sensor(monitor_id)
            if payload["aqi"] is None:
                log.warning(
                    "Null AQI reading from %s (event_id=%s) — publishing with "
                    "sensor_status=NULL_READING for downstream DLQ routing.",
                    monitor_id, payload["event_id"],
                )
            future = producer.send(TOPIC, key=monitor_id, value=payload)
            future.get(timeout=10)  # block for ack -> confirms at-least-once delivery
            return True

        except SensorTimeoutError as e:
            log.warning(
                "Attempt %d/%d: %s. Retrying in %.1fs...",
                attempt, MAX_APP_LEVEL_RETRIES, e, RETRY_BACKOFF_SECONDS,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)  # exponential-ish backoff

        except (KafkaTimeoutError, KafkaError) as e:
            log.error(
                "Attempt %d/%d: Kafka send failed for %s: %s. Retrying in %.1fs...",
                attempt, MAX_APP_LEVEL_RETRIES, monitor_id, e, RETRY_BACKOFF_SECONDS,
            )
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    log.error(
        "Giving up on monitor %s after %d attempts — logging as delivery failure.",
        monitor_id, MAX_APP_LEVEL_RETRIES,
    )
    return False


def run(duration_seconds: int = 60, read_interval_seconds: float = 10.0):
    producer = make_producer()
    start = time.time()
    sent, failed, null_readings = 0, 0, 0

    try:
        while time.time() - start < duration_seconds:
            for monitor_id in MONITORS:
                ok = send_with_retry(producer, monitor_id)
                if ok:
                    sent += 1
                else:
                    failed += 1
            log.info(
                "Cycle complete. sent=%d failed=%d (null readings logged separately in send_with_retry)",
                sent, failed,
            )
            time.sleep(read_interval_seconds)
    except KeyboardInterrupt:
        log.info("Stopping producer.")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed. Total sent=%d, total failed=%d", sent, failed)


if __name__ == "__main__":
    run()
