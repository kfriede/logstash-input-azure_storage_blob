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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Integration tests for edge cases and error handling.
 * Uses registry strategy to avoid Azurite tag/lease limitations.
 */
@Category(IntegrationTest.class)
public class EdgeCaseIT extends AzuriteTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;
    private String dbPath;

    @Before
    public void setUp() throws IOException {
        containerName = uniqueContainerName("edge");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
        dbPath = new File(tempFolder.getRoot(), "edge.db").getAbsolutePath();
    }

    private BlobPoller createPoller() {
        RegistryStateTracker stateTracker = new RegistryStateTracker(dbPath, "test-host");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, "", 50);
    }

    // ── Test 1: Binary blob handled gracefully ──────────────────────────────

    @Test
    public void testBinaryBlobHandledGracefully() {
        // Binary content: Java's UTF-8 decoder with BufferedReader replaces
        // malformed bytes with replacement characters, so this won't crash.
        byte[] binaryData = new byte[256];
        for (int i = 0; i < 256; i++) {
            binaryData[i] = (byte) i;
        }
        uploadBlobBytes(containerName, "binary.bin", binaryData);

        BlobPoller poller = createPoller();
        // This should not throw
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        // Blob should be either processed or failed — either way, no crash
        assertTrue("Should have processed or failed the blob",
                summary.getBlobsProcessed() + summary.getBlobsFailed() > 0);
    }

    // ── Test 2: Zero-byte blob completed ────────────────────────────────────

    @Test
    public void testZeroByteBlobCompleted() {
        uploadBlob(containerName, "zero.log", "");

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Zero-byte blob should be processed", 1, summary.getBlobsProcessed());
        assertEquals("No events from zero-byte blob", 0, summary.getEventsProduced());
        assertEquals("No failures expected", 0, summary.getBlobsFailed());
    }

    // ── Test 3: Large blob streaming ────────────────────────────────────────

    @Test
    public void testLargeBlobStreaming() {
        // Generate ~1MB of content (~10000 lines of ~100 chars each)
        int lineCount = 10_000;
        StringBuilder sb = new StringBuilder(lineCount * 110);
        for (int i = 1; i <= lineCount; i++) {
            sb.append(String.format("line-%06d: %s%n", i,
                    "abcdefghijklmnopqrstuvwxyz0123456789abcdefghijklmnopqrstuvwxyz0123456789abcdef"));
        }
        String content = sb.toString();
        uploadBlob(containerName, "large.log", content);

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Should process 1 blob", 1, summary.getBlobsProcessed());
        assertEquals("Should produce all lines as events",
                lineCount, summary.getEventsProduced());
        assertEquals(lineCount, events.size());
    }

    // ── Test 4: Graceful shutdown mid-blob ──────────────────────────────────

    @Test
    public void testGracefulShutdownMidBlob() throws IOException {
        // Upload a blob with many lines
        int lineCount = 100;
        uploadBlob(containerName, "shutdown.log", makeNumberedLines(lineCount));

        AtomicBoolean stopped = new AtomicBoolean(false);

        String shutdownDb = new File(tempFolder.getRoot(), "shutdown.db").getAbsolutePath();
        RegistryStateTracker stateTracker = new RegistryStateTracker(shutdownDb, "test-host");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);

        // Use a consumer that signals stop after 10 events
        List<Map<String, Object>> shutdownEvents = new CopyOnWriteArrayList<>();
        BlobPoller poller = new BlobPoller(containerClient, stateTracker, processor,
                event -> {
                    shutdownEvents.add(event);
                    if (shutdownEvents.size() >= 10) {
                        stopped.set(true);
                    }
                }, "", 50);

        BlobPoller.PollCycleSummary summary = poller.pollOnce(stopped::get);

        // Should have stopped early with partial events
        assertTrue("Should have produced some events", shutdownEvents.size() >= 10);
        assertTrue("Should not have produced all events",
                shutdownEvents.size() < lineCount);
    }

    // ── Test 5: Blob deleted externally before processing ───────────────────

    @Test
    public void testBlobDeletedExternally() throws IOException {
        // Upload two blobs, then delete one before processing
        uploadBlob(containerName, "normal.log", "normal-data\n");
        uploadBlob(containerName, "deleted.log", "data\n");

        // Delete the second blob
        serviceClient.getBlobContainerClient(containerName)
                .getBlobClient("deleted.log")
                .delete();

        String deleteDb = new File(tempFolder.getRoot(), "deleted.db").getAbsolutePath();
        RegistryStateTracker stateTracker = new RegistryStateTracker(deleteDb, "test-host");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);

        List<Map<String, Object>> deleteEvents = new CopyOnWriteArrayList<>();
        BlobPoller poller = new BlobPoller(containerClient, stateTracker, processor,
                deleteEvents::add, "", 50);

        // The poller should handle the missing blob gracefully
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        // The normal blob should have been processed
        // The deleted blob might not appear in listing at all (already gone)
        assertTrue("At least the normal blob should be processed",
                summary.getBlobsProcessed() >= 1);

        boolean hasNormalEvent = deleteEvents.stream()
                .anyMatch(e -> "normal-data".equals(e.get("message")));
        assertTrue("Normal blob should have produced events", hasNormalEvent);
    }
}
