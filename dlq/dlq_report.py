"""
dlq_report.py

Consumes urbanpulse.dlq for a fixed 5-minute window and produces a report
of error-type distribution: counts and percentages by error_reason, split
out by source_topic, plus a few sample records per error type for manual
inspection.

Usage:
    python dlq_report.py                # live 5-minute window, from now
    python dlq_report.py --from-earliest  # replay full DLQ history first
"""

import argparse
import json
import time
from collections import Counter, defaultdict

from kafka import KafkaConsumer

BOOTSTRAP_SERVERS = ["localhost:9092", "localhost:9093", "localhost:9094"]
DLQ_TOPIC = "urbanpulse.dlq"
WINDOW_SECONDS = 5 * 60
SAMPLES_PER_ERROR_TYPE = 3


def generate_report(from_earliest: bool = False):
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"dlq_report_{int(time.time())}",  # fresh group each run
        enable_auto_commit=False,
        auto_offset_reset="earliest" if from_earliest else "latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    counts_by_reason = Counter()
    counts_by_topic_reason = Counter()
    samples = defaultdict(list)
    total = 0

    start = time.time()
    print(f"Collecting DLQ records for {WINDOW_SECONDS}s...")
    while time.time() - start < WINDOW_SECONDS:
        try:
            records = consumer.poll(timeout_ms=1000)
        except Exception as e:
            print(f"Transient poll error, retrying: {e}")
            time.sleep(1)
            continue
        for tp, messages in records.items():
            for msg in messages:
                rec = msg.value
                reason = rec.get("error_reason", "UNKNOWN")
                src = rec.get("source_topic", "UNKNOWN")
                counts_by_reason[reason] += 1
                counts_by_topic_reason[(src, reason)] += 1
                total += 1
                if len(samples[reason]) < SAMPLES_PER_ERROR_TYPE:
                    samples[reason].append(rec)

    consumer.close()
    print_report(total, counts_by_reason, counts_by_topic_reason, samples)


def print_report(total, counts_by_reason, counts_by_topic_reason, samples):
    print("\n" + "=" * 60)
    print(f"UrbanPulse DLQ Report — {WINDOW_SECONDS // 60}-minute window")
    print(f"Total DLQ records: {total}")
    print("=" * 60)

    if total == 0:
        print("No DLQ records observed in this window.")
        return

    print("\n-- Error type distribution --")
    for reason, count in counts_by_reason.most_common():
        pct = 100 * count / total
        print(f"  {reason:<30} {count:>6}  ({pct:5.1f}%)")

    print("\n-- Breakdown by source topic --")
    for (src, reason), count in sorted(counts_by_topic_reason.items(), key=lambda x: -x[1]):
        print(f"  {src:<15} {reason:<30} {count:>6}")

    print("\n-- Sample records --")
    for reason, recs in samples.items():
        print(f"\n  [{reason}]")
        for r in recs:
            print(f"    {json.dumps(r, indent=None)[:200]}")

    print("\n" + "=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--from-earliest", action="store_true",
                         help="Replay full DLQ history before the live window.")
    args = parser.parse_args()
    generate_report(from_earliest=args.from_earliest)
