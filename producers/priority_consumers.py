"""
priority_consumers.py

Implements two independent consumer groups on the traffic_signals topic:

  HIGH_PRIORITY   — 1 consumer, reads ALL 6 partitions.
                    This is the real-time signal-control system: it must
                    process a message and adapt signal timing within the
                    90-second SLA, so it does the absolute minimum work
                    per message (no enrichment, no slow I/O) and commits
                    offsets aggressively.

  STANDARD_PRIORITY — 3 consumers in one group, each gets ~2 partitions
                    (Kafka's rebalance protocol handles the assignment).
                    This is the analytics dashboard: it can tolerate lag
                    because it's not safety-critical, and we simulate a
                    processing slowdown (artificial sleep) to show that
                    even when STANDARD_PRIORITY lag grows into the
                    thousands, HIGH_PRIORITY lag stays near zero because
                    they are fully independent consumer groups — Kafka
                    tracks offsets per (group, partition), so one group
                    falling behind has zero effect on another group's
                    ability to read the same partition.

Run each function in a SEPARATE PROCESS (see bottom of file / README) —
that's what actually proves group independence, rather than just
asserting it in code comments.
"""

import json
import logging
import multiprocessing
import time

from kafka import KafkaConsumer, TopicPartition
from kafka.admin import KafkaAdminClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
TOPIC = "traffic_signals"


def high_priority_consumer(run_seconds: int = 120):
    log = logging.getLogger("HIGH_PRIORITY")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="HIGH_PRIORITY",
        enable_auto_commit=False,   # commit manually right after processing
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=50,
        fetch_max_wait_ms=100,      # low latency polling
    )
    start = time.time()
    processed = 0
    try:
        while time.time() - start < run_seconds:
            records = consumer.poll(timeout_ms=200)
            for tp, messages in records.items():
                for msg in messages:
                    # Minimal, fast work: real-time signal adaptation logic
                    # would trigger here — kept trivial to hit the 90s SLA.
                    processed += 1
            if records:
                consumer.commit()  # commit after each poll batch, immediately
            log.info("processed_total=%d (near-zero lag expected)", processed)
    finally:
        consumer.close()
        log.info("HIGH_PRIORITY consumer stopped. Total processed=%d", processed)


def standard_priority_consumer(consumer_index: int, run_seconds: int = 120,
                                simulate_slowdown_after: int = 20,
                                slowdown_sleep_seconds: float = 2.0):
    log = logging.getLogger(f"STANDARD_PRIORITY-{consumer_index}")
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="STANDARD_PRIORITY",
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        max_poll_records=50,
    )
    start = time.time()
    processed = 0
    try:
        while time.time() - start < run_seconds:
            records = consumer.poll(timeout_ms=500)
            for tp, messages in records.items():
                for msg in messages:
                    processed += 1
                    # Simulated analytics workload (heavier than signal control):
                    # e.g. writing to a dashboard aggregation store.
                    if time.time() - start > simulate_slowdown_after:
                        # Artificial slowdown to demonstrate lag growth.
                        time.sleep(slowdown_sleep_seconds)
            log.info("processed_total=%d", processed)
    finally:
        consumer.close()
        log.info("Consumer %d stopped. Total processed=%d", consumer_index, processed)


def print_consumer_group_lag(bootstrap_servers=BOOTSTRAP_SERVERS):
    """
    Lag demonstration helper: run this periodically (e.g. every 10s in a
    separate terminal) while both groups are consuming, to show
    HIGH_PRIORITY lag staying near 0 while STANDARD_PRIORITY climbs.

    Equivalent CLI command (simplest way to demo this live):
        kafka-consumer-groups --bootstrap-server localhost:9092 \\
            --describe --group HIGH_PRIORITY
        kafka-consumer-groups --bootstrap-server localhost:9092 \\
            --describe --group STANDARD_PRIORITY
    """
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
    for group in ("HIGH_PRIORITY", "STANDARD_PRIORITY"):
        try:
            offsets = admin.list_consumer_group_offsets(group)
            print(f"\n--- {group} ---")
            for tp, meta in offsets.items():
                print(f"  partition={tp.partition} committed_offset={meta.offset}")
        except Exception as e:
            print(f"Could not fetch offsets for {group}: {e}")
    admin.close()


if __name__ == "__main__":
    # Launch: 1 HIGH_PRIORITY consumer + 3 STANDARD_PRIORITY consumers,
    # each in its own process (mirrors how they'd run as separate services).
    procs = [
        multiprocessing.Process(target=high_priority_consumer, kwargs={"run_seconds": 300}),
        multiprocessing.Process(target=standard_priority_consumer,
                                 kwargs={"consumer_index": 1, "run_seconds": 300}),
        multiprocessing.Process(target=standard_priority_consumer,
                                 kwargs={"consumer_index": 2, "run_seconds": 300}),
        multiprocessing.Process(target=standard_priority_consumer,
                                 kwargs={"consumer_index": 3, "run_seconds": 300}),
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()
