package com.azure.logstash.input;

import com.azure.logstash.input.tracking.StateTracker;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Orchestrates poll cycles: lists blobs, filters candidates, claims,
 * processes, and marks results.
 *
 * <p>Each call to {@link #pollOnce(Supplier)} performs a single poll cycle:
 * <ol>
 *   <li>List blobs from the container (optionally filtered by prefix)</li>
 *   <li>Sort by name (lexicographic order for deterministic processing)</li>
 *   <li>Filter via {@link StateTracker#filterCandidates(List)}</li>
 *   <li>For each candidate (up to {@code batchSize}):
 *     <ul>
 *       <li>Check if stopped — if so, break</li>
 *       <li>Attempt to claim via {@link StateTracker#claim(String)}</li>
 *       <li>Process via {@link BlobProcessor#process(BlobClient, Consumer, Supplier)}</li>
 *       <li>Mark completed or failed, then release</li>
 *     </ul>
 *   </li>
 * </ol>
 *
 * <p>The poller does not manage its own thread — callers are responsible
 * for invoking {@code pollOnce()} in a loop with appropriate sleep intervals.
 */
public class BlobPoller {

    private static final Logger logger = LogManager.getLogger(BlobPoller.class);

    private final BlobContainerClient containerClient;
    private final StateTracker stateTracker;
    private final BlobProcessor processor;
    private final Consumer<Map<String, Object>> eventConsumer;
    private final String prefix;
    private final int batchSize;

    /**
     * Creates a new BlobPoller.
     *
     * @param containerClient the Azure container client for listing and accessing blobs
     * @param stateTracker    tracks blob processing state (claim, complete, fail, release)
     * @param processor       streams blob content and produces events
     * @param eventConsumer   callback that receives each event map for Logstash queue
     * @param prefix          blob name prefix filter (empty string means no filter)
     * @param batchSize       maximum number of blobs to process per poll cycle
     */
    public BlobPoller(BlobContainerClient containerClient, StateTracker stateTracker,
                      BlobProcessor processor, Consumer<Map<String, Object>> eventConsumer,
                      String prefix, int batchSize) {
        this.containerClient = containerClient;
        this.stateTracker = stateTracker;
        this.processor = processor;
        this.eventConsumer = eventConsumer;
        this.prefix = prefix;
        this.batchSize = batchSize;
    }

    /**
     * Performs a single poll cycle: list, filter, claim, process, mark.
     *
     * @param isStopped supplier checked between blobs; returns true to abort the cycle
     * @return a summary of the poll cycle results
     */
    public PollCycleSummary pollOnce(Supplier<Boolean> isStopped) {
        long startTime = System.currentTimeMillis();

        int blobsProcessed = 0;
        int blobsFailed = 0;
        int blobsSkipped = 0;
        long eventsProduced = 0;

        // Step 1: List blobs from container with optional prefix
        ListBlobsOptions options = new ListBlobsOptions();
        if (prefix != null && !prefix.isEmpty()) {
            options.setPrefix(prefix);
        }

        List<BlobItem> blobs = containerClient.listBlobs(options, null)
                .stream()
                .collect(Collectors.toList());

        // Step 2: Sort lexicographically by name
        blobs.sort(Comparator.comparing(BlobItem::getName));

        // Step 3: Filter candidates via state tracker
        List<BlobItem> candidates = stateTracker.filterCandidates(blobs);

        logger.debug("Poll cycle: {} blobs listed, {} candidates after filtering",
                blobs.size(), candidates.size());

        // Step 4: Process candidates up to batchSize
        int processed = 0;
        for (BlobItem candidate : candidates) {
            if (processed >= batchSize) {
                break;
            }

            // Check if stopped before processing each blob
            if (isStopped.get()) {
                logger.debug("Stop requested, breaking poll cycle after {} blobs", processed);
                break;
            }

            String blobName = candidate.getName();

            // Try to claim the blob
            if (!stateTracker.claim(blobName)) {
                logger.debug("Could not claim blob {}, skipping", blobName);
                blobsSkipped++;
                continue;
            }

            // Process the blob (claim succeeded)
            try {
                BlobClient blobClient = containerClient.getBlobClient(blobName);
                BlobProcessor.ProcessResult result = processor.process(
                        blobClient, eventConsumer, isStopped);

                if (result.isCompleted()) {
                    stateTracker.markCompleted(blobName);
                    blobsProcessed++;
                    eventsProduced += result.getEventCount();
                } else {
                    // Stopped mid-blob — don't mark completed or failed
                    blobsProcessed++;
                    eventsProduced += result.getEventCount();
                }
            } catch (Exception e) {
                logger.warn("Failed to process blob {}: {}", blobName, e.getMessage());
                stateTracker.markFailed(blobName, e.getMessage());
                blobsFailed++;
            } finally {
                stateTracker.release(blobName);
            }

            processed++;
        }

        long durationMs = System.currentTimeMillis() - startTime;
        logger.debug("Poll cycle complete: {} processed, {} failed, {} skipped, {} events in {}ms",
                blobsProcessed, blobsFailed, blobsSkipped, eventsProduced, durationMs);

        return new PollCycleSummary(blobsProcessed, blobsFailed, blobsSkipped,
                eventsProduced, durationMs);
    }

    /**
     * Summary of a single poll cycle's results.
     */
    public static class PollCycleSummary {
        private final int blobsProcessed;
        private final int blobsFailed;
        private final int blobsSkipped;
        private final long eventsProduced;
        private final long durationMs;

        /**
         * @param blobsProcessed number of blobs successfully processed
         * @param blobsFailed    number of blobs that failed during processing
         * @param blobsSkipped   number of blobs skipped (claim failed)
         * @param eventsProduced total number of events produced across all blobs
         * @param durationMs     wall-clock duration of the poll cycle in milliseconds
         */
        public PollCycleSummary(int blobsProcessed, int blobsFailed, int blobsSkipped,
                                long eventsProduced, long durationMs) {
            this.blobsProcessed = blobsProcessed;
            this.blobsFailed = blobsFailed;
            this.blobsSkipped = blobsSkipped;
            this.eventsProduced = eventsProduced;
            this.durationMs = durationMs;
        }

        /** Returns the number of blobs successfully processed. */
        public int getBlobsProcessed() {
            return blobsProcessed;
        }

        /** Returns the number of blobs that failed during processing. */
        public int getBlobsFailed() {
            return blobsFailed;
        }

        /** Returns the number of blobs skipped (could not be claimed). */
        public int getBlobsSkipped() {
            return blobsSkipped;
        }

        /** Returns the total number of events produced across all blobs. */
        public long getEventsProduced() {
            return eventsProduced;
        }

        /** Returns the wall-clock duration of the poll cycle in milliseconds. */
        public long getDurationMs() {
            return durationMs;
        }
    }
}
