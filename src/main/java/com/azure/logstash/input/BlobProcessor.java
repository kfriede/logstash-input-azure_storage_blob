package com.azure.logstash.input;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobProperties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Streams blob content line-by-line, injects metadata, and produces events.
 *
 * <p>Each non-empty line (or every line if {@code skipEmptyLines} is false)
 * becomes a Logstash event with a {@code message} field and an {@code @metadata}
 * map containing blob provenance information.
 *
 * <p>Processing can be interrupted between lines by the {@code isStopped}
 * supplier. When stopped early, the returned {@link ProcessResult} will have
 * {@code completed=false}.
 *
 * <p>Content is streamed via {@link BlobClient#openInputStream()} and never
 * fully buffered in memory. Line splitting uses {@link BufferedReader#readLine()},
 * which handles {@code \n}, {@code \r}, and {@code \r\n} automatically.
 */
public class BlobProcessor {

    private static final Logger logger = LogManager.getLogger(BlobProcessor.class);

    private final String storageAccount;
    private final String containerName;
    private final boolean skipEmptyLines;

    /**
     * Creates a new BlobProcessor.
     *
     * @param storageAccount the Azure storage account name (for metadata injection)
     * @param containerName  the container name (for metadata injection)
     * @param skipEmptyLines if true, blank lines are silently skipped and do not produce events
     */
    public BlobProcessor(String storageAccount, String containerName, boolean skipEmptyLines) {
        this.storageAccount = storageAccount;
        this.containerName = containerName;
        this.skipEmptyLines = skipEmptyLines;
    }

    /**
     * Processes a blob by streaming its content line-by-line and passing each event
     * to the given consumer.
     *
     * <p>The {@code isStopped} supplier is checked before processing each line.
     * If it returns {@code true}, processing stops immediately and the result
     * is marked as incomplete.
     *
     * <p>Each event is a {@code Map<String, Object>} with:
     * <ul>
     *   <li>{@code message} — the line content</li>
     *   <li>{@code @metadata} — a nested map with blob provenance fields</li>
     * </ul>
     *
     * @param blobClient the Azure blob to read from
     * @param consumer   callback that receives each event map
     * @param isStopped  supplier checked between lines; returns true to abort processing
     * @return a {@link ProcessResult} with event count and completion status
     * @throws IOException if an I/O error occurs while reading the blob stream
     */
    public ProcessResult process(BlobClient blobClient, Consumer<Map<String, Object>> consumer,
                                 Supplier<Boolean> isStopped) throws IOException {
        // Cache properties before the loop to avoid repeated API calls
        BlobProperties properties = blobClient.getProperties();
        String lastModified = properties.getLastModified().toString();
        String blobName = blobClient.getBlobName();

        try (InputStream stream = openStream(blobClient);
             BufferedReader reader = new BufferedReader(
                     new InputStreamReader(stream, StandardCharsets.UTF_8))) {

            String line;
            long lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                if (isStopped.get()) {
                    logger.debug("Stop requested after {} events from blob {}", lineNumber, blobName);
                    return new ProcessResult(lineNumber, false);
                }
                if (skipEmptyLines && line.isEmpty()) {
                    continue;
                }
                lineNumber++;

                Map<String, Object> event = new HashMap<>();
                event.put("message", line);

                Map<String, Object> metadata = new HashMap<>();
                metadata.put("azure_blob_name", blobName);
                metadata.put("azure_blob_container", containerName);
                metadata.put("azure_blob_storage_account", storageAccount);
                metadata.put("azure_blob_line_number", lineNumber);
                metadata.put("azure_blob_last_modified", lastModified);
                event.put("@metadata", metadata);

                consumer.accept(event);
            }
            logger.debug("Completed processing blob {} — {} events", blobName, lineNumber);
            return new ProcessResult(lineNumber, true);
        }
    }

    /**
     * Opens an input stream for the given blob. Package-private for testability —
     * unit tests can override this to supply a {@link java.io.ByteArrayInputStream}
     * without needing to mock {@link BlobClient#openInputStream()} which returns
     * the SDK-internal {@code BlobInputStream} type.
     *
     * @param blobClient the blob to stream
     * @return an InputStream for reading the blob's content
     */
    protected InputStream openStream(BlobClient blobClient) {
        return blobClient.openInputStream();
    }

    /**
     * Result of processing a single blob.
     */
    public static class ProcessResult {
        private final long eventCount;
        private final boolean completed;

        /**
         * @param eventCount number of events emitted
         * @param completed  true if the entire blob was processed; false if stopped early
         */
        public ProcessResult(long eventCount, boolean completed) {
            this.eventCount = eventCount;
            this.completed = completed;
        }

        /**
         * Returns the number of events emitted during processing.
         */
        public long getEventCount() {
            return eventCount;
        }

        /**
         * Returns true if the entire blob was processed to completion.
         */
        public boolean isCompleted() {
            return completed;
        }
    }
}
