# UrbanPulse — Kafka Implementation Runbook

Everything referenced here lives in this project folder:

```
urbanpulse/
├── infra/
│   ├── docker-compose.yml       # 3-broker Kafka cluster (KRaft mode)
│   └── create_topics.sh         # topic creation + retention config
├── producers/
│   ├── bus_gps_producer.py      # keyed by route_id, ordering guarantee
│   └── air_quality_producer.py  # at-least-once, retry, 5% null AQI
├── consumers/
│   └── priority_consumers.py    # HIGH_PRIORITY (1) vs STANDARD_PRIORITY (3)
├── streams-app/                 # Java Kafka Streams — GPS x route_schedule join
│   ├── pom.xml
│   └── src/main/java/com/urbanpulse/
│       ├── EnrichmentTopology.java
│       └── RouteScheduleLoader.java
├── dlq/
│   ├── validator.py             # validates + routes bad events to DLQ
│   └── dlq_report.py            # 5-minute error-distribution report
├── data/route_schedule.csv
└── requirements.txt
```

## Step 1 — Bring up the cluster

```bash
cd infra
docker compose up -d
docker compose ps          # confirm all 3 brokers + kafka-ui are healthy
```

Kafka UI is available at `http://localhost:8080` for a visual view of topics,
partitions, and consumer group lag — useful for Step 4's lag demo.

## Step 2 — Create topics (Problem 1)

```bash
chmod +x create_topics.sh
# run from a host with kafka-topics on PATH, or exec into a broker container:
docker exec -it kafka-1 bash
kafka-topics --bootstrap-server localhost:9092 --list   # sanity check first
exit
./create_topics.sh
```

Also create the compacted `route_schedule` topic needed for Step 5 (Problem 4):

```bash
kafka-topics --bootstrap-server localhost:9092 --create \
  --topic route_schedule --partitions 4 --replication-factor 3 \
  --config cleanup.policy=compact
```

Confirm retention settings landed correctly:

```bash
kafka-configs --bootstrap-server localhost:9092 \
  --entity-type topics --entity-name smart_meters --describe
```

## Step 3 — Run the producers (Problem 2)

```bash
cd ..
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt

python producers/bus_gps_producer.py &
python producers/air_quality_producer.py &
```

Watch the logs: `air_quality_producer.py` will log `WARNING` lines for the
simulated 5% null-AQI readings and for the ~3% sensor-timeout retries — both
are expected, not bugs. To prove per-route ordering, consume `bus_gps` with
`--property print.key=true` and confirm all messages for a given `route_id`
key arrive in non-decreasing timestamp order:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic bus_gps --property print.key=true --from-beginning | grep '"route_id":"R001"'
```

## Step 4 — Priority consumers + lag demo (Problem 3)

```bash
python consumers/priority_consumers.py
```

This spawns 1 HIGH_PRIORITY consumer and 3 STANDARD_PRIORITY consumers as
separate processes. `standard_priority_consumer` starts sleeping
artificially after 20 seconds to simulate a processing slowdown.

While it runs, in another terminal, poll consumer group lag every ~10s:

```bash
watch -n 10 'kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group HIGH_PRIORITY; \
  kafka-consumer-groups --bootstrap-server localhost:9092 \
  --describe --group STANDARD_PRIORITY'
```

Expected result to capture as evidence: `HIGH_PRIORITY` LAG column stays at
or near 0 throughout, while `STANDARD_PRIORITY` LAG climbs steadily once the
simulated slowdown kicks in — because they are separate consumer groups with
independently tracked offsets on the same 6 partitions.

## Step 5 — Kafka Streams enrichment (Problem 4)

```bash
cd streams-app
mvn clean package

# Load the static route_schedule.csv into the compacted topic:
java -cp target/bus-gps-enrichment-1.0.0.jar \
  com.urbanpulse.RouteScheduleLoader ../data/route_schedule.csv

# Start the topology (join bus_gps stream with route_schedule KTable):
java -cp target/bus-gps-enrichment-1.0.0.jar com.urbanpulse.EnrichmentTopology
```

Verify enrichment landed correctly:

```bash
kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic bus_gps_enriched --from-beginning
```

Each record should now contain `route_name`, `terminal`, and
`scheduled_arrival_time` alongside the raw `lat`/`lon`/`speed_kmh`.

## Step 6 — DLQ pattern (Problem 5)

```bash
cd ..
python dlq/validator.py &
```

This consumes `bus_gps` and `air_quality`, validates each event, and routes
failures to `urbanpulse.dlq` with an `error_reason` (`NULL_AQI_VALUE`,
`OUT_OF_RANGE_AQI`, `IMPOSSIBLE_GPS_COORDINATES`, `NULL_COORDINATES`,
`MISSING_IDENTIFIER`). Valid events are republished to
`bus_gps.validated` / `air_quality.validated`.

With the producers and validator running, generate the 5-minute report:

```bash
python dlq/dlq_report.py
```

This prints total DLQ volume, a percentage breakdown by `error_reason`, a
breakdown by source topic, and a few sample records per error type —
submit this console output (or redirect to a file with `> dlq_report.txt`)
as the deliverable.

## Shutdown

```bash
cd infra
docker compose down -v   # -v also clears broker volumes for a clean re-run
```
