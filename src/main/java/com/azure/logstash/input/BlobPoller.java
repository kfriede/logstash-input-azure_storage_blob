package com.azure.logstash.input;

import com.azure.core.http.rest.PagedResponse;
import com.azure.logstash.input.tracking.StateTracker;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Orchestrates poll cycles: lists blobs, filters candidates, claims,
 * processes, and marks results.
 *
 * <p>Each call to {@link #pollOnce(Supplier)} performs a single poll cycle:
 * <ol>
 *   <li>List blobs from the container page-by-page (optionally filtered by prefix),
 *       bounding memory to one page at a time instead of materializing the full listing</li>
 *   <li>Filter each page via {@link StateTracker#filterCandidates(List)}</li>
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
 * <p>Azure Blob Storage returns blobs in lexicographic order natively, so no
 * explicit sorting is needed.
 *
 * <p>The poller does not manage its own thread — callers are responsible
 * for invoking {@code pollOnce()} in a loop with appropriate sleep intervals.
 */
public class BlobPoller {

    private static final Logger logger = LogManager.getLogger(BlobPoller.class);

    /** Maximum number of blobs per API page. Bounds memory per poll cycle. */
    private static final int LISTING_PAGE_SIZE = 5000;

    private final BlobContainerClient containerClient;
    private final StateTracker stateTracker;
    private final BlobProcessor processor;
    private final Consumer<Map<String, Object>> eventConsumer;
    private final String prefix;
    private final int batchSize;
    private final int concurrency;
    private final ExecutorService executor;

    /**
     * Creates a new BlobPoller with the specified concurrency level.
     *
     * @param containerClient the Azure container client for listing and accessing blobs
     * @param stateTracker    tracks blob processing state (claim, complete, fail, release)
     * @param processor       streams blob content and produces events
     * @param eventConsumer   callback that receives each event map for Logstash queue
     * @param prefix          blob name prefix filter (empty string means no filter)
     * @param batchSize       maximum number of blobs to process per poll cycle
     * @param concurrency     number of worker threads for parallel blob processing
     */
    public BlobPoller(BlobContainerClient containerClient, StateTracker stateTracker,
                      BlobProcessor processor, Consumer<Map<String, Object>> eventConsumer,
                      String prefix, int batchSize, int concurrency) {
        this.containerClient = containerClient;
        this.stateTracker = stateTracker;
        this.processor = processor;
        this.eventConsumer = eventConsumer;
        this.prefix = prefix;
        this.batchSize = batchSize;
        this.concurrency = concurrency;
        this.executor = Executors.newFixedThreadPool(concurrency, r -> {
            Thread t = new Thread(r);
            t.setName("azure-blob-worker-" + t.getId());
            t.setDaemon(true);
            return t;
        });
    }

    /**
     * Creates a new BlobPoller with default concurrency of 1 (sequential processing).
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
        this(containerClient, stateTracker, processor, eventConsumer, prefix, batchSize, 1);
    }

    /**
     * Performs a single poll cycle: list, filter, claim, process, mark.
     *
     * @param isStopped supplier checked between blobs; returns true to abort the cycle
     * @return a summary of the poll cycle results
     */
    public PollCycleSummary pollOnce(Supplier<Boolean> isStopped) {
        long startTime = System.currentTimeMillis();

        // Phase 1: Discovery — list, filter, claim (sequential)
        List<String> claimedBlobs = new ArrayList<>();
        int blobsSkipped = 0;

        ListBlobsOptions options = new ListBlobsOptions();
        if (prefix != null && !prefix.isEmpty()) {
            options.setPrefix(prefix);
        }
        options.setMaxResultsPerPage(LISTING_PAGE_SIZE);

        for (PagedResponse<BlobItem> page :
                containerClient.listBlobs(options, null).iterableByPage()) {

            if (claimedBlobs.size() >= batchSize || isStopped.get()) {
                break;
            }

            List<BlobItem> candidates = stateTracker.filterCandidates(page.getValue());

            logger.debug("Page: {} blobs listed, {} candidates after filtering",
                    page.getValue().size(), candidates.size());

            for (BlobItem candidate : candidates) {
                if (claimedBlobs.size() >= batchSize || isStopped.get()) {
                    break;
                }

                String blobName = candidate.getName();
                if (stateTracker.claim(blobName)) {
                    claimedBlobs.add(blobName);
                } else {
                    logger.debug("Could not claim blob {}, skipping", blobName);
                    blobsSkipped++;
                }
            }
        }

        // Phase 2: Processing — process, mark, release (parallel)
        if (claimedBlobs.isEmpty()) {
            long durationMs = System.currentTimeMillis() - startTime;
            return new PollCycleSummary(0, 0, blobsSkipped, 0, durationMs);
        }

        // Build tasks
        List<Callable<BlobResult>> tasks = new ArrayList<>();
        for (String blobName : claimedBlobs) {
            tasks.add(() -> processBlob(blobName, isStopped));
        }

        // Execute and collect
        int blobsProcessed = 0;
        int blobsFailed = 0;
        long eventsProduced = 0;

        try {
            List<Future<BlobResult>> futures = executor.invokeAll(tasks);
            for (Future<BlobResult> future : futures) {
                try {
                    BlobResult result = future.get();
                    if (result.success) {
                        blobsProcessed++;
                    } else {
                        blobsFailed++;
                    }
                    eventsProduced += result.events;
                } catch (ExecutionException e) {
                    // Should not happen — processBlob catches all exceptions
                    logger.error("Unexpected execution error", e);
                    blobsFailed++;
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Poll cycle interrupted during processing");
            blobsFailed = claimedBlobs.size() - blobsProcessed;
        }

        long durationMs = System.currentTimeMillis() - startTime;
        logger.debug("Poll cycle complete: {} processed, {} failed, {} skipped, {} events in {}ms",
                blobsProcessed, blobsFailed, blobsSkipped, eventsProduced, durationMs);

        return new PollCycleSummary(blobsProcessed, blobsFailed, blobsSkipped,
                eventsProduced, durationMs);
    }

    /**
     * Shuts down the worker thread pool. Safe to call multiple times.
     */
    public void close() {
        executor.shutdownNow();
        try {
            if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("BlobPoller thread pool did not terminate within 5 seconds");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for BlobPoller thread pool shutdown");
        }
    }

    /**
     * Processes a single claimed blob: stream content, mark result, release.
     * Called from worker threads. Never throws — all exceptions are caught.
     */
    private BlobResult processBlob(String blobName, Supplier<Boolean> isStopped) {
        try {
            BlobClient blobClient = containerClient.getBlobClient(blobName);
            BlobProcessor.ProcessResult result = processor.process(
                    blobClient, eventConsumer, isStopped);

            if (result.isCompleted()) {
                if (stateTracker.wasLeaseRenewalFailed(blobName)) {
                    logger.warn("Lease renewal failed for blob '{}' during processing — "
                            + "marking as failed to prevent duplicates", blobName);
                    stateTracker.markFailed(blobName, "lease renewal failed during processing");
                    return new BlobResult(false, result.getEventCount());
                } else {
                    stateTracker.markCompleted(blobName);
                    return new BlobResult(true, result.getEventCount());
                }
            } else {
                stateTracker.markFailed(blobName, "interrupted");
                return new BlobResult(false, result.getEventCount());
            }
        } catch (Exception e) {
            logger.warn("Failed to process blob {}: {}", blobName, e.getMessage());
            stateTracker.markFailed(blobName, e.getMessage());
            return new BlobResult(false, 0);
        } finally {
            stateTracker.release(blobName);
        }
    }

    /** Result of processing a single blob. */
    private static class BlobResult {
        final boolean success;
        final long events;

        BlobResult(boolean success, long events) {
            this.success = success;
            this.events = events;
        }
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
