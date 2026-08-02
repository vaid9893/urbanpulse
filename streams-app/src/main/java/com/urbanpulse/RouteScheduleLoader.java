package com.urbanpulse;

import com.google.gson.JsonObject;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileReader;
import java.io.Reader;
import java.util.Properties;

/**
 * One-off loader: reads route_schedule.csv and publishes each row to the
 * compacted `route_schedule` topic, keyed by route_id, so
 * EnrichmentTopology can build a KTable from it.
 *
 * The route_schedule topic itself must be created with
 * cleanup.policy=compact (not time-based retention) since it represents
 * "current state per key", not an event log:
 *
 *   kafka-topics --create --topic route_schedule --partitions 4 \
 *     --replication-factor 3 --config cleanup.policy=compact
 */
public class RouteScheduleLoader {

    private static final String TOPIC = "route_schedule";

    public static void main(String[] args) throws Exception {
        String csvPath = args.length > 0 ? args[0] : "data/route_schedule.csv";

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092,localhost:9093,localhost:9094");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             Reader reader = new FileReader(csvPath);
             CSVParser parser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(reader)) {

            int count = 0;
            for (CSVRecord record : parser) {
                String routeId = record.get("route_id");

                JsonObject value = new JsonObject();
                value.addProperty("route_id", routeId);
                value.addProperty("route_name", record.get("route_name"));
                value.addProperty("terminal", record.get("terminal"));
                value.addProperty("scheduled_arrival_time", record.get("scheduled_arrival_time"));

                producer.send(new ProducerRecord<>(TOPIC, routeId, value.toString()));
                count++;
            }
            producer.flush();
            System.out.println("Loaded " + count + " route_schedule rows into topic '" + TOPIC + "'.");
        }
    }
}
