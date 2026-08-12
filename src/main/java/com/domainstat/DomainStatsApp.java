package com.domainstat;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.net.URI;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class DomainStatsApp {

    static String extractRootDomain(String url) {
        try {
            if (url == null || url.isEmpty()) return "unknown";
            if (!url.startsWith("http")) url = "https://" + url;
            URI uri = new URI(url);
            String host = uri.getHost();
            if (host == null) return "unknown";
            String[] parts = host.split("\\.");
            return parts[parts.length - 1];
        } catch (Exception e) {
            return "unknown";
        }
    }

    public static void main(String[] args) throws InterruptedException {

        // Configure Kafka Streams
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "domain-stats-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                  Serdes.String().getClass());
        // Read from the beginning of the topic
        props.put(StreamsConfig.consumerPrefix("auto.offset.reset"), "earliest");

        // Build the topology
        StreamsBuilder builder = new StreamsBuilder();

        // Read from the topic "browser-history"
        KStream<String, String> urls = builder.stream("browser-history");

        KTable<String, Long> domainCounts = urls
            .mapValues(url -> extractRootDomain(url))   // "https://google.com" -> "com"
            .filter((key, domain) -> !domain.equals("unknown"))  // filter invalid domains
            .groupBy((key, domain) -> domain)            // group by domain
            .count(Materialized.as("domain-counts-store")); // count, store in state store

        // Print result to console on each update
        domainCounts
            .toStream()
            .foreach((domain, count) ->
                System.out.println("[UPDATE] " + domain + " -> " + count + " views"));

        // Start
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // shutdown on Ctrl+C
        CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\n=== Stopping streaming... ===");
            printTop5(streams);
            streams.close();
            latch.countDown();
        }));

        System.out.println("=== Domain Stats Kafka Streams started ===");
        System.out.println("Expecting messages from topic 'browser-history'...");
        System.out.println("Press Ctrl+C to stop and see the top-5\n");

        streams.start();
        latch.await();
    }

    // Print top-5 domains from state store
    static void printTop5(KafkaStreams streams) {
        try {
            var store = streams.store(
                StoreQueryParameters.fromNameAndType(
                    "domain-counts-store",
                    org.apache.kafka.streams.state.QueryableStoreTypes.keyValueStore()
                )
            );

            List<Map.Entry<String, Long>> entries = new ArrayList<>();
            store.all().forEachRemaining(kv -> entries.add(Map.entry((String) kv.key, (Long) kv.value)));

            entries.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

            System.out.println("\n========== Top 5 Domains ==========");
            int limit = Math.min(5, entries.size());
            for (int i = 0; i < limit; i++) {
                System.out.printf("%d. .%s — %d views%n",
                    i + 1,
                    entries.get(i).getKey(),
                    entries.get(i).getValue());
            }
            System.out.println("=====================================");
        } catch (Exception e) {
            System.out.println("Error accessing state store: " + e.getMessage());
        }
    }
}