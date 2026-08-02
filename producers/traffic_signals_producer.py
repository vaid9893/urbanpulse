"""
traffic_signals_producer.py

Simulates 3,800 signal sensors reporting every ~10 seconds (~380 events/sec
combined). Needed to feed the traffic_signals topic so the priority
consumer demo (HIGH_PRIORITY vs STANDARD_PRIORITY) has real data to show
lag divergence against.
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("traffic_signals_producer")

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "traffic_signals"

SENSORS = [f"SIG{n:04d}" for n in range(1, 3801)]
SIGNAL_STATES = ["RED", "YELLOW", "GREEN"]


def make_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=5,
        linger_ms=20,
        compression_type="gzip",
    )


def simulate_signal(sensor_id: str) -> dict:
    return {
        "sensor_id": sensor_id,
        "state": random.choice(SIGNAL_STATES),
        "queue_length_estimate": random.randint(0, 40),
        "congestion_score": round(random.uniform(0, 1), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_id": str(uuid.uuid4()),
    }


def run(duration_seconds: int = 300, cycle_interval_seconds: float = 10.0):
    producer = make_producer()
    start = time.time()
    sent = 0
    try:
        while time.time() - start < duration_seconds:
            for sensor_id in SENSORS:
                payload = simulate_signal(sensor_id)
                producer.send(TOPIC, key=sensor_id, value=payload)
                sent += 1
            producer.flush()
            log.info("Cycle complete. Total sent so far: %d", sent)
            time.sleep(cycle_interval_seconds)
    except KeyboardInterrupt:
        log.info("Stopping producer.")
    finally:
        producer.flush()
        producer.close()
        log.info("Producer closed. Total messages sent: %d", sent)


if __name__ == "__main__":
    run()
