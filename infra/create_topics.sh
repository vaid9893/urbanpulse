#!/usr/bin/env bash
# Creates all four UrbanPulse topics + the DLQ topic with justified
# partition counts and retention policies.
# Run from a machine with access to the cluster, or `docker exec -it kafka-1 bash`.

set -euo pipefail
BOOTSTRAP="kafka-1:9092,kafka-2:9092,kafka-3:9092"
KAFKA_TOPICS="kafka-topics --bootstrap-server ${BOOTSTRAP}"

# --- bus_gps -----------------------------------------------------------
# Rate: ~2,400 events/sec from 12,000 buses (~1 ping/5s per bus).
# Partitions: 12 — sized for throughput (each partition comfortably
# handles ~200 events/sec) AND to give the enrichment Streams app
# (Problem 4) enough parallelism. Keyed by route_id (Problem 2), so
# partition count should also comfortably exceed the number of active
# routes-with-load to avoid hot partitions.
$KAFKA_TOPICS --create --topic bus_gps \
  --partitions 12 --replication-factor 3 \
  --config retention.ms=86400000 \
  --config min.insync.replicas=2
  # 86,400,000 ms = 24 hours.
  # Justification: 24h is enough to replay bus positions for same-day
  # accident investigation (the stated use case) without paying to store
  # GPS pings indefinitely — high-volume, low long-term analytical value
  # once the day's operations are over.

# --- traffic_signals -----------------------------------------------------
# Rate: ~380 events/sec from 3,800 signal sensors (~1 reading/10s per sensor).
# Partitions: 6 — supports the HIGH_PRIORITY (1 consumer reading all
# partitions) and STANDARD_PRIORITY (3 consumers) groups from Problem 3;
# 6 divides evenly by 3 so the standard group balances cleanly, while
# still being small enough for a single high-priority consumer to keep
# up with near-zero lag.
$KAFKA_TOPICS --create --topic traffic_signals \
  --partitions 6 --replication-factor 3 \
  --config retention.ms=604800000 \
  --config min.insync.replicas=2
  # 7 days default retention — enough buffer for the standard/analytics
  # group to fall behind during a slowdown and still catch up without
  # data loss (this is the scenario Problem 3 simulates).

# --- air_quality ---------------------------------------------------------
# Rate: ~60 events/sec from 600 AQI monitors (~1 reading/10s per monitor).
# Partitions: 4 — low volume, so partition count is driven by consumer
# parallelism headroom rather than throughput; 4 is enough to scale out
# the trend-analysis consumers later without needing a topic re-partition.
$KAFKA_TOPICS --create --topic air_quality \
  --partitions 4 --replication-factor 3 \
  --config retention.ms=7776000000 \
  --config min.insync.replicas=2
  # 7,776,000,000 ms = 90 days.
  # Justification: pollution trend analysis (seasonal, monthly) needs a
  # multi-month rolling window; 90 days covers a full quarter for
  # councillor reporting without the storage cost of a full year at
  # this granularity (aggregated/rolled-up data can live longer in
  # TimescaleDB outside Kafka).

# --- smart_meters ----------------------------------------------------------
# Rate: ~1,100 events/sec from 1.1M meters (~1 reading/1000s ≈ 16-17 min
# per meter on average — typical smart-meter interval reporting).
# Partitions: 24 — highest-volume stream and the one with the strictest
# retention (regulatory audits), so it gets the most parallelism for both
# ingestion and the nightly batch-layer aggregation jobs (Lambda batch
# layer, per the architecture decision) that read this topic.
$KAFKA_TOPICS --create --topic smart_meters \
  --partitions 24 --replication-factor 3 \
  --config retention.ms=31536000000 \
  --config min.insync.replicas=2
  # 31,536,000,000 ms = 365 days.
  # Justification: explicitly required for regulatory energy audits,
  # which in most jurisdictions require a full calendar year of
  # consumption records to be producible on demand — this is also the
  # strongest argument in the Lambda-vs-Kappa matrix for the batch
  # layer's deterministic reprocessing capability.

# --- Dead-letter queue (Problem 5) ---------------------------------------
$KAFKA_TOPICS --create --topic urbanpulse.dlq \
  --partitions 3 --replication-factor 3 \
  --config retention.ms=1209600000 \
  --config min.insync.replicas=2
  # 14 days retention — long enough to investigate and reprocess
  # validation failures, short enough not to become an unbounded error sink.

echo "All topics created. Listing:"
$KAFKA_TOPICS --list
echo "Describing partition/retention config:"
for t in bus_gps traffic_signals air_quality smart_meters urbanpulse.dlq; do
  echo "--- $t ---"
  $KAFKA_TOPICS --describe --topic "$t"
done
