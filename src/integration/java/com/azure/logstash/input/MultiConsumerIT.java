package com.azure.logstash.input;

import com.azure.logstash.input.tracking.RegistryStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * Integration tests for multi-consumer coordination.
 *
 * <p>Uses the registry strategy with separate SQLite databases for each consumer.
 * While this doesn't provide true distributed coordination (each DB is independent),
 * it validates that the BlobPoller + BlobProcessor pipeline handles concurrent
 * access to the same Azurite container gracefully.
 *
 * <p>True multi-consumer coordination with leases is tested in
 * {@link LeaseRecoveryIT} at the LeaseManager level.
 */
@Category(IntegrationTest.class)
public class MultiConsumerIT extends AzuriteTestBase {

    private static final int BLOB_COUNT = 10;
    private static final int LINES_PER_BLOB = 3;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String containerName;
    private BlobContainerClient containerClient;

    @Before
    public void setUp() {
        containerName = uniqueContainerName("multi");
        containerClient = createContainer(containerName);
    }

    // ── Test 1: Two consumers process blobs concurrently ────────────────────

    @Test
    public void testTwoConsumersProcessBlobs() throws Exception {
        // Upload blobs
        for (int i = 1; i <= BLOB_COUNT; i++) {
            uploadBlob(containerName, String.format("blob-%02d.log", i),
                    makeNumberedLines(LINES_PER_BLOB));
        }

        List<Map<String, Object>> events1 = new CopyOnWriteArrayList<>();
        List<Map<String, Object>> events2 = new CopyOnWriteArrayList<>();

        String db1 = new File(tempFolder.getRoot(), "consumer1.db").getAbsolutePath();
        String db2 = new File(tempFolder.getRoot(), "consumer2.db").getAbsolutePath();

        RegistryStateTracker tracker1 = new RegistryStateTracker(db1, "consumer-1");
        RegistryStateTracker tracker2 = new RegistryStateTracker(db2, "consumer-2");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);

        BlobPoller poller1 = new BlobPoller(containerClient, tracker1, processor,
                events1::add, "", BLOB_COUNT);
        BlobPoller poller2 = new BlobPoller(containerClient, tracker2, processor,
                events2::add, "", BLOB_COUNT);

        ExecutorService executor = Executors.newFixedThreadPool(2);
        Future<BlobPoller.PollCycleSummary> future1 = executor.submit(() -> poller1.pollOnce(() -> false));
        Future<BlobPoller.PollCycleSummary> future2 = executor.submit(() -> poller2.pollOnce(() -> false));

        BlobPoller.PollCycleSummary summary1 = future1.get(60, TimeUnit.SECONDS);
        BlobPoller.PollCycleSummary summary2 = future2.get(60, TimeUnit.SECONDS);
        executor.shutdown();

        // Both consumers should have processed all blobs (independent DBs)
        assertEquals("Consumer 1 should process all blobs", BLOB_COUNT, summary1.getBlobsProcessed());
        assertEquals("Consumer 2 should process all blobs", BLOB_COUNT, summary2.getBlobsProcessed());

        tracker1.close();
        tracker2.close();
    }

    // ── Test 2: No lost events ──────────────────────────────────────────────

    @Test
    public void testNoLostEvents() throws IOException {
        for (int i = 1; i <= BLOB_COUNT; i++) {
            uploadBlob(containerName, String.format("blob-%02d.log", i),
                    makeNumberedLines(LINES_PER_BLOB));
        }

        List<Map<String, Object>> allEvents = new CopyOnWriteArrayList<>();

        String dbPath = new File(tempFolder.getRoot(), "events.db").getAbsolutePath();
        RegistryStateTracker tracker = new RegistryStateTracker(dbPath, "consumer");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);

        BlobPoller poller = new BlobPoller(containerClient, tracker, processor,
                allEvents::add, "", BLOB_COUNT);

        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        long expectedEvents = (long) BLOB_COUNT * LINES_PER_BLOB;
        assertEquals("Total events should equal all lines across all blobs",
                expectedEvents, summary.getEventsProduced());
        assertEquals("Events list should contain all events",
                expectedEvents, allEvents.size());

        tracker.close();
    }

    // ── Test 3: Second poll after first completes all ───────────────────────

    @Test
    public void testSecondPollSkipsCompleted() throws IOException {
        for (int i = 1; i <= 5; i++) {
            uploadBlob(containerName, "blob-" + i + ".log", "data\n");
        }

        List<Map<String, Object>> events = new CopyOnWriteArrayList<>();
        String dbPath = new File(tempFolder.getRoot(), "skip.db").getAbsolutePath();
        RegistryStateTracker tracker = new RegistryStateTracker(dbPath, "consumer");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);

        BlobPoller poller = new BlobPoller(containerClient, tracker, processor,
                events::add, "", 50);

        // First poll processes all
        BlobPoller.PollCycleSummary first = poller.pollOnce(() -> false);
        assertEquals(5, first.getBlobsProcessed());

        // Second poll skips all (already completed)
        BlobPoller.PollCycleSummary second = poller.pollOnce(() -> false);
        assertEquals(0, second.getBlobsProcessed());

        tracker.close();
    }
}
