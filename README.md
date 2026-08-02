# UrbanPulse — Real-Time City Data Pipeline (Kafka)

## Overview

UrbanPulse ingests four concurrent, heterogeneous city data streams — bus
GPS, traffic signal sensors, air quality monitors, and smart meters — through
a 3-broker Apache Kafka cluster, and demonstrates priority-based consumption,
stream enrichment, and dead-letter handling for a government smart-city
deployment.

**Architecture decision:** Lambda (not Kappa) — driven by the government
reporting/audit mandate, which needs deterministic, re-runnable, sign-off-able
batch computation that a dedicated batch layer provides more reliably than
Kappa's log-replay approach. See `docs/lambda_kappa_evaluation.md` for the
full matrix and reasoning.

## Streams at a glance

| Topic | Source | Rate | Partitions | Retention | Why |
|---|---|---|---|---|---|
| `bus_gps` | 12,000 buses | ~2,400 evt/s | 12 | 24h | Same-day accident-investigation replay |
| `traffic_signals` | 3,800 sensors | ~380 evt/s | 6 | 7d | Buffer for standard-priority group to catch up |
| `air_quality` | 600 monitors | ~60 evt/s | 4 | 90d | Quarterly pollution trend analysis |
| `smart_meters` | 1.1M meters | ~1,100 evt/s | 24 | 365d | Regulatory energy-audit requirement |
| `urbanpulse.dlq` | validation failures | — | 3 | 14d | Investigation window for bad data |

## What each problem statement delivers

**1. Cluster + topics** (`infra/`) — 3-broker Kafka cluster in KRaft mode
with dual listeners (internal Docker network + external `localhost` access
for host-side clients), replication factor 3 / min-isr 2 on every topic for
fault tolerance, and the retention policy above.

**2. Producers** (`producers/`) — `bus_gps_producer.py` keys every message
on `route_id` so Kafka's per-partition ordering guarantees all positions for
a route arrive in send order. `air_quality_producer.py` implements
at-least-once delivery: an application-level retry loop (separate from the
Kafka client's own network-level retries) handles simulated sensor read
timeouts, and a simulated 5% null-AQI rate is logged and still published
(flagged `sensor_status=NULL_READING`) rather than silently dropped, so
downstream validation can route it to the DLQ.

**3. Priority consumers** (`consumers/priority_consumers.py`) — two
independent consumer groups on `traffic_signals`: `HIGH_PRIORITY` (1
consumer, all 6 partitions, minimal per-message work, aggressive commit) vs
`STANDARD_PRIORITY` (3 consumers, simulated processing slowdown after 20s).
Because Kafka tracks offsets per `(group, partition)`, the two groups are
fully independent — verified live: `HIGH_PRIORITY` processed **26,600**
messages in the same window `STANDARD_PRIORITY`'s two active consumers
processed only **50 each**, a >500x gap, with zero cross-impact.

**4. Kafka Streams enrichment** (`streams-app/`) — a KStream-KTable join:
`bus_gps` (KStream) joined against `route_schedule` (KTable, loaded from a
static CSV into a compacted topic) to attach `route_name`, `terminal`, and
`scheduled_arrival_time` to every GPS ping, feeding the eventual real-time
ETA service. Verified working end-to-end with live enriched output.

**5. Dead-letter queue** (`dlq/`) — `validator.py` consumes `bus_gps` and
`air_quality`, checks null/out-of-range AQI and impossible GPS coordinates,
and routes failures to `urbanpulse.dlq` with an `error_reason` field; valid
events are republished to `<topic>.validated`. `dlq_report.py` produces a
5-minute error-distribution report. Verified live: 31 of 600 air_quality
readings (~5.2%) correctly flagged `NULL_AQI_VALUE` and routed to the DLQ.

## Key files

```
infra/docker-compose.yml        3-broker KRaft cluster, dual listeners
infra/create_topics.sh          topic + retention setup with inline justification
producers/bus_gps_producer.py   route_id-keyed, ordering guarantee
producers/air_quality_producer.py   at-least-once retry + null handling
producers/traffic_signals_producer.py   feeds Problem 3's lag demo
consumers/priority_consumers.py HIGH_PRIORITY vs STANDARD_PRIORITY groups
streams-app/.../EnrichmentTopology.java   KStream-KTable join
streams-app/.../RouteScheduleLoader.java  loads CSV into compacted topic
dlq/validator.py                validation + DLQ routing
dlq/dlq_report.py               5-minute error distribution report
docs/RUNBOOK.md                 step-by-step execution guide
```

## Environment notes (Windows-specific fixes applied during setup)

- `kafka-python` → replaced with `kafka-python-ng` (maintained fork; original
  package breaks on Python 3.12+ due to a vendored-`six` import bug)
- `compression_type="snappy"` → changed to `"gzip"` everywhere (snappy needs
  a native C library that isn't available out of the box on Windows)
- Kafka's `KAFKA_ADVERTISED_LISTENERS` split into `INTERNAL` (Docker
  hostnames, for broker-to-broker and `docker exec` clients) and `EXTERNAL`
  (`localhost:9092/9093/9094`, for host-side Python clients) — without this
  split, host clients fail with DNS lookup errors on `kafka-2`/`kafka-3`.
- `CLUSTER_ID` in KRaft mode must be a real base64 UUID (generate via
  `kafka-storage random-uuid`), not an arbitrary string.

## Running it

See `docs/RUNBOOK.md` for the full step-by-step (cluster startup, topic
creation, producers, consumers, Streams app, DLQ pipeline, and how to
capture lag/report evidence for submission).
