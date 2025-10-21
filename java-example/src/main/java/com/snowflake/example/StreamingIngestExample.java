package com.snowflake.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import com.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Example demonstrating how to use the Snowflake Streaming Ingest SDK.
 * 
 * This example shows how to:
 * 1. Create a Snowflake Streaming Ingest Client
 * 2. Open a channel for data ingestion
 * 3. Ingest rows of data
 * 4. Wait for ingestion to complete
 * 5. Close resources properly
 */
public class StreamingIngestExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String PROFILE_PATH = "profile.json";
    private static final int MAX_ROWS = 100_000;
    private static final int POLL_ATTEMPTS = 30;
    private static final long POLL_INTERVAL_MS = TimeUnit.SECONDS.toMillis(1);

    public static void main(String[] args) {
        try {
            // Load properties from profile.json
            Properties props = new Properties();
            JsonNode jsonNode = MAPPER.readTree(Files.readAllBytes(Paths.get(PROFILE_PATH)));
            jsonNode.fields().forEachRemaining(entry ->
                props.put(entry.getKey(), entry.getValue().asText()));

            // Create Snowflake Streaming Ingest Client using try-with-resources
            try (SnowflakeStreamingIngestClient client = SnowflakeStreamingIngestClientFactory.builder(
                    "MY_CLIENT_" + UUID.randomUUID(),
                    "MY_DATABASE",
                    "MY_SCHEMA",
                    "MY_PIPE")
                    .setProperties(props)
                    .build()) {

                System.out.println("Client created successfully");

                // Open a channel for data ingestion using try-with-resources
                try (SnowflakeStreamingIngestChannel channel = client.openChannel(
                        "MY_CHANNEL_" + UUID.randomUUID(), "0").getChannel()) {

                    System.out.println("Channel opened: " + channel.getName());
                    System.out.println("Ingesting " + MAX_ROWS + " rows...");

                    // Ingest rows
                    for (int i = 1; i <= MAX_ROWS; i++) {
                        String rowId = String.valueOf(i);
                        Map<String, Object> row = Map.of(
                            "c1", i,
                            "c2", rowId,
                            "ts", Instant.now().toEpochMilli() / 1000.0
                        );
                        channel.appendRow(row, rowId);

                        // Print progress every 10,000 rows
                        if (i % 10_000 == 0) {
                            System.out.println("Ingested " + i + " rows...");
                        }
                    }

                    System.out.println("All rows submitted. Waiting for ingestion to complete...");

                    // Wait for ingestion to complete
                    int expectedOffset = MAX_ROWS;
                    long timeoutMillis = POLL_ATTEMPTS * POLL_INTERVAL_MS;
                    
                    try {
                        channel.waitForCommit(
                            token -> token != null && Integer.parseInt(token) >= expectedOffset,
                            java.time.Duration.ofMillis(timeoutMillis)
                        ).get();
                        
                        String latestOffset = channel.getLatestCommittedOffsetToken();
                        System.out.println("Latest offset token: " + latestOffset);
                        System.out.println("All data committed successfully");
                    } catch (java.util.concurrent.ExecutionException e) {
                        throw new RuntimeException("Ingestion failed: " + e.getCause().getMessage(), e);
                    }
                } // Channel automatically closed here

                System.out.println("Data ingestion completed");
            } // Client automatically closed here

        } catch (IOException | InterruptedException e) {
            System.err.println("Error during data ingestion: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
}

