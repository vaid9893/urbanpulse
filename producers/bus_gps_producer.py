"""
bus_gps_producer.py

Simulates 12,000 city buses reporting GPS position every ~5 seconds.

Ordering guarantee:
Kafka guarantees ordering only WITHIN a partition. By keying each message
on route_id, every bus position for a given route always lands on the
same partition, so a consumer reading that partition sees positions for
that route strictly in send order — which is what's needed to reconstruct
a route's path for accident investigation (Problem 1's 24h retention use
case). Buses on different routes are allowed to interleave/reorder
relative to each other, which is fine since they're independent.
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("bus_gps_producer")

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "bus_gps"

# City bounding box for simulated coordinates (swap for real geofence).
LAT_RANGE = (12.90, 13.10)
LON_RANGE = (77.50, 77.70)

ROUTES = [f"R{n:03d}" for n in range(1, 151)]  # 150 active routes
BUSES_PER_ROUTE = 80  # 150 * 80 = 12,000 buses


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",              # wait for all in-sync replicas
        retries=5,
        linger_ms=20,            # small batching window for throughput
        compression_type="gzip",
    )


def simulate_position(route_id: str, bus_id: str) -> dict:
    return {
        "bus_id": bus_id,
        "route_id": route_id,
        "lat": round(random.uniform(*LAT_RANGE), 6),
        "lon": round(random.uniform(*LON_RANGE), 6),
        "speed_kmh": round(random.uniform(0, 55), 1),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_id": str(uuid.uuid4()),
    }


def on_send_error(excp):
    log.error("Failed to deliver bus_gps message: %s", excp)


def run(duration_seconds: int = 60, ping_interval_seconds: float = 5.0):
    producer = make_producer()
    bus_ids = {
        route: [f"{route}-BUS-{i:03d}" for i in range(BUSES_PER_ROUTE)]
        for route in ROUTES
    }

    start = time.time()
    sent = 0
    try:
        while time.time() - start < duration_seconds:
            for route_id, buses in bus_ids.items():
                # Simulate one ping per bus per cycle (in production this
                # would be event-driven per device, not a tight loop).
                for bus_id in buses:
                    payload = simulate_position(route_id, bus_id)
                    future = producer.send(TOPIC, key=route_id, value=payload)
                    future.add_errback(on_send_error)
                    sent += 1
            producer.flush()
            log.info("Cycle complete. Total sent so far: %d", sent)
            time.sleep(ping_interval_seconds)
    except KeyboardInterrupt:
        log.info("Stopping producer.")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed. Total messages sent: %d", sent)


if __name__ == "__main__":
    run()
