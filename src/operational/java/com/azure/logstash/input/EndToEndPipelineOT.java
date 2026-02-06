package com.azure.logstash.input;

import com.azure.logstash.input.tracking.ContainerStateTracker;
import com.azure.logstash.input.tracking.RegistryStateTracker;
import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * End-to-end pipeline operational tests against Azure Government.
 *
 * <p>Validates the full processing pipeline for each tracking strategy:
 * upload blobs, run the poller, verify all events are produced and state
 * is correctly recorded.
 */
@Category(OperationalTest.class)
public class EndToEndPipelineOT extends AzureGovTestBase {

    private static final int BLOB_COUNT = 5;
    private static final int LINES_PER_BLOB = 3;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private BlobContainerClient incomingClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        incomingClient = serviceClient.getBlobContainerClient("incoming");
        events = new CopyOnWriteArrayList<>();

        // Clean up all containers
        cleanContainer("incoming");
        cleanContainer("archive");
        cleanContainer("errors");
    }

    private void uploadTestBlobs() {
        for (int i = 1; i <= BLOB_COUNT; i++) {
            uploadBlob("incoming", String.format("e2e-%02d.log", i),
                    makeNumberedLines(LINES_PER_BLOB));
        }
    }

    // ── Test 1: End-to-end with tags strategy ───────────────────────────────

    @Test
    public void testEndToEndTagsStrategy() {
        uploadTestBlobs();

        TagStateTracker tracker = new TagStateTracker(incomingClient, 15, 10, "e2e-host");
        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        BlobPoller poller = new BlobPoller(incomingClient, tracker, processor,
                events::add, "", BLOB_COUNT);

        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);
        tracker.close();

        // Verify all blobs processed
        assertEquals(BLOB_COUNT, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());

        // Verify all events
        long expectedEvents = (long) BLOB_COUNT * LINES_PER_BLOB;
        assertEquals(expectedEvents, summary.getEventsProduced());
        assertEquals(expectedEvents, events.size());

        // Verify all blobs tagged as completed
        for (int i = 1; i <= BLOB_COUNT; i++) {
            Map<String, String> tags = getBlobTags("incoming", String.format("e2e-%02d.log", i));
            assertEquals("completed", tags.get("logstash_status"));
        }

        // Verify event metadata
        for (Map<String, Object> event : events) {
            assertNotNull(event.get("message"));
            @SuppressWarnings("unchecked")
            Map<String, Object> metadata = (Map<String, Object>) event.get("@metadata");
            assertNotNull(metadata);
            assertEquals(accountName, metadata.get("azure_blob_storage_account"));
            assertEquals("incoming", metadata.get("azure_blob_container"));
            assertNotNull(metadata.get("azure_blob_name"));
            assertNotNull(metadata.get("azure_blob_line_number"));
            assertNotNull(metadata.get("azure_blob_last_modified"));
        }
    }

    // ── Test 2: End-to-end with container strategy ──────────────────────────

    @Test
    public void testEndToEndContainerStrategy() {
        uploadTestBlobs();

        ContainerStateTracker tracker = new ContainerStateTracker(
                serviceClient, "incoming", "archive", "errors",
                15, 10, "e2e-host");
        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        BlobPoller poller = new BlobPoller(incomingClient, tracker, processor,
                events::add, "", BLOB_COUNT);

        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);
        tracker.close();

        // Verify all blobs processed
        assertEquals(BLOB_COUNT, summary.getBlobsProcessed());

        // Verify all events
        long expectedEvents = (long) BLOB_COUNT * LINES_PER_BLOB;
        assertEquals(expectedEvents, summary.getEventsProduced());

        // Verify all blobs moved to archive
        List<String> archiveBlobs = listBlobNames("archive");
        assertEquals(BLOB_COUNT, archiveBlobs.size());
        for (int i = 1; i <= BLOB_COUNT; i++) {
            assertTrue("Archive should contain blob " + i,
                    archiveBlobs.contains(String.format("e2e-%02d.log", i)));
        }

        // Verify incoming is empty
        List<String> incomingBlobs = listBlobNames("incoming");
        assertEquals("Incoming should be empty", 0, incomingBlobs.size());
    }

    // ── Test 3: End-to-end with registry strategy ───────────────────────────

    @Test
    public void testEndToEndRegistryStrategy() throws Exception {
        uploadTestBlobs();

        String dbPath = new File(tempFolder.getRoot(), "e2e-registry.db").getAbsolutePath();
        RegistryStateTracker tracker = new RegistryStateTracker(dbPath, "e2e-host");
        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        BlobPoller poller = new BlobPoller(incomingClient, tracker, processor,
                events::add, "", BLOB_COUNT);

        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        // Verify all blobs processed
        assertEquals(BLOB_COUNT, summary.getBlobsProcessed());

        // Verify all events
        long expectedEvents = (long) BLOB_COUNT * LINES_PER_BLOB;
        assertEquals(expectedEvents, summary.getEventsProduced());
        assertEquals(expectedEvents, events.size());

        // Second poll should find nothing new to process
        events.clear();
        BlobPoller.PollCycleSummary second = new BlobPoller(incomingClient, tracker, processor,
                events::add, "", BLOB_COUNT).pollOnce(() -> false);
        assertEquals("Second poll should process 0 blobs", 0, second.getBlobsProcessed());
        assertEquals(0, events.size());

        tracker.close();
    }
}
