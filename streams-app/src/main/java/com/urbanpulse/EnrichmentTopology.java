package com.urbanpulse;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.util.Properties;

/**
 * EnrichmentTopology
 *
 * Joins the high-volume bus_gps stream (KStream, unkeyed events keyed by
 * route_id) with a route_schedule KTable (loaded from the static CSV via
 * RouteScheduleLoader, published to a compacted topic) to produce an
 * enriched stream with scheduled_arrival_time, route_name, and terminal
 * alongside the raw GPS position.
 *
 * This is a KStream-KTable join, not a windowed KStream-KStream join,
 * because route_schedule is reference/lookup data (changes rarely, one
 * current value per route_id) rather than an event stream — the KTable
 * always has the latest schedule row for a given route_id, and every
 * incoming GPS ping is enriched against whatever the current table state is.
 *
 * Output feeds the real-time ETA service: consumers of
 * bus_gps_enriched can now compute
 * eta = scheduled_arrival_time - f(current_position, route)
 * without needing a second lookup against a schedule database.
 */
public class EnrichmentTopology {

    private static final String BUS_GPS_TOPIC = "bus_gps";
    private static final String ROUTE_SCHEDULE_TOPIC = "route_schedule"; // compacted topic, keyed by route_id
    private static final String OUTPUT_TOPIC = "bus_gps_enriched";
    private static final Gson GSON = new Gson();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "urbanpulse-gps-enrichment");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        // At-least 1 replica of internal changelog/state-store topics for
        // fault tolerance consistent with the rest of the cluster.
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 3);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000); // low-latency, matches 90s SLA headroom

        StreamsBuilder builder = new StreamsBuilder();
        Topology topology = buildTopology(builder);

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.start();
    }

    public static Topology buildTopology(StreamsBuilder builder) {
        // route_schedule as a KTable: compacted topic keyed by route_id ->
        // always reflects the current schedule row per route, not history.
        KTable<String, String> routeScheduleTable = builder.table(
                ROUTE_SCHEDULE_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        );

        // bus_gps as a KStream, re-keyed defensively by route_id in case the
        // upstream producer key was ever something else (bus_id, etc.) —
        // the join requires the stream key to match the table key.
        KStream<String, String> busGpsStream = builder.stream(
                BUS_GPS_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
        ).selectKey((key, jsonValue) -> {
            JsonObject obj = GSON.fromJson(jsonValue, JsonObject.class);
            return obj.has("route_id") ? obj.get("route_id").getAsString() : key;
        });

        // KStream-KTable join: for every GPS event, look up the current
        // schedule row for that route_id and merge the fields.
        KStream<String, String> enriched = busGpsStream.join(
                routeScheduleTable,
                EnrichmentTopology::enrich
        );

        enriched.to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        return builder.build();
    }

    private static String enrich(String gpsJson, String scheduleJson) {
        JsonObject gps = GSON.fromJson(gpsJson, JsonObject.class);
        JsonObject schedule = GSON.fromJson(scheduleJson, JsonObject.class);

        JsonObject out = new JsonObject();
        out.addProperty("bus_id", gps.get("bus_id").getAsString());
        out.addProperty("route_id", gps.get("route_id").getAsString());
        out.addProperty("lat", gps.get("lat").getAsDouble());
        out.addProperty("lon", gps.get("lon").getAsDouble());
        out.addProperty("speed_kmh", gps.get("speed_kmh").getAsDouble());
        out.addProperty("timestamp", gps.get("timestamp").getAsString());

        // Enrichment fields from the route_schedule KTable:
        out.addProperty("route_name", schedule.get("route_name").getAsString());
        out.addProperty("terminal", schedule.get("terminal").getAsString());
        out.addProperty("scheduled_arrival_time", schedule.get("scheduled_arrival_time").getAsString());

        return GSON.toJson(out);
    }
}
