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

import static org.junit.Assert.*;

/**
 * Integration tests for basic end-to-end blob processing against Azurite.
 *
 * <p>Uses the registry strategy to avoid Azurite limitations with blob tags
 * and leases. The focus here is on BlobPoller + BlobProcessor behavior: correct
 * event production, prefix filtering, batch limits, and metadata injection.
 */
@Category(IntegrationTest.class)
public class BasicProcessingIT extends AzuriteTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;
    private String dbPath;

    @Before
    public void setUp() throws IOException {
        containerName = uniqueContainerName("basic");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
        dbPath = new File(tempFolder.getRoot(), "basic.db").getAbsolutePath();
    }

    /**
     * Creates a BlobPoller using registry strategy (avoids Azurite tag/lease issues).
     */
    private BlobPoller createPoller(String prefix, int batchSize) {
        RegistryStateTracker stateTracker = new RegistryStateTracker(dbPath, "test-processor");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, prefix, batchSize);
    }

    private BlobPoller createPoller() {
        return createPoller("", 50);
    }

    // ── Test 1: Single blob produces correct events ─────────────────────────

    @Test
    public void testSingleBlobProducesCorrectEvents() {
        uploadBlob(containerName, "test.log", makeLines(
                "alpha", "bravo", "charlie", "delta", "echo"));

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Should produce 5 events", 5, summary.getEventsProduced());
        assertEquals("Should process 1 blob", 1, summary.getBlobsProcessed());
        assertEquals(5, events.size());
        assertEquals("alpha", events.get(0).get("message"));
        assertEquals("bravo", events.get(1).get("message"));
        assertEquals("charlie", events.get(2).get("message"));
        assertEquals("delta", events.get(3).get("message"));
        assertEquals("echo", events.get(4).get("message"));
    }

    // ── Test 2: Multiple blobs processed in order ───────────────────────────

    @Test
    public void testMultipleBlobsProcessedInOrder() {
        // Upload out of order — poller should sort lexicographically
        uploadBlob(containerName, "c.log", "charlie\n");
        uploadBlob(containerName, "a.log", "alpha\n");
        uploadBlob(containerName, "b.log", "bravo\n");

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(3, summary.getBlobsProcessed());
        assertEquals(3, events.size());
        // Events should arrive in order a, b, c
        assertEquals("alpha", events.get(0).get("message"));
        assertEquals("bravo", events.get(1).get("message"));
        assertEquals("charlie", events.get(2).get("message"));
    }

    // ── Test 3: Prefix filtering ────────────────────────────────────────────

    @Test
    public void testPrefixFiltering() {
        uploadBlob(containerName, "logs/a.log", "log-line\n");
        uploadBlob(containerName, "data/b.log", "data-line\n");

        BlobPoller poller = createPoller("logs/", 50);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Should process only 1 blob", 1, summary.getBlobsProcessed());
        assertEquals(1, events.size());
        assertEquals("log-line", events.get(0).get("message"));
    }

    // ── Test 4: Batch size limits ───────────────────────────────────────────

    @Test
    public void testBatchSizeLimits() {
        // Upload 20 blobs
        for (int i = 1; i <= 20; i++) {
            uploadBlob(containerName, String.format("blob-%02d.log", i),
                    "line-" + i + "\n");
        }

        BlobPoller poller = createPoller("", 3);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Should process at most 3 blobs", 3, summary.getBlobsProcessed());
        assertEquals(3, events.size());
    }

    // ── Test 5: Empty container, no errors ──────────────────────────────────

    @Test
    public void testEmptyContainerNoErrors() {
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(0, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());
        assertEquals(0, summary.getEventsProduced());
        assertTrue(events.isEmpty());
    }

    // ── Test 6: Empty (zero-byte) blob, no errors ───────────────────────────

    @Test
    public void testEmptyBlobNoErrors() {
        uploadBlob(containerName, "empty.log", "");

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());
        assertEquals(0, summary.getEventsProduced());
    }

    // ── Test 7: Blob metadata correct ───────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    public void testBlobMetadataCorrect() {
        uploadBlob(containerName, "meta-test.log", "hello\n");

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        assertEquals(1, events.size());
        Map<String, Object> event = events.get(0);
        assertEquals("hello", event.get("message"));

        Map<String, Object> metadata = (Map<String, Object>) event.get("@metadata");
        assertNotNull("@metadata should be present", metadata);
        assertEquals("meta-test.log", metadata.get("azure_blob_name"));
        assertEquals(containerName, metadata.get("azure_blob_container"));
        assertEquals(AZURITE_ACCOUNT, metadata.get("azure_blob_storage_account"));
        assertEquals(1L, metadata.get("azure_blob_line_number"));
        assertNotNull("last_modified should be set", metadata.get("azure_blob_last_modified"));
    }
}
