package com.azure.logstash.input;

import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Integration tests for the tag-based state tracking strategy against Azurite.
 *
 * <p>Note: Azurite does not fully support blob index tags on leased blobs
 * (returns 412 on setTags with an active lease). These tests exercise tag
 * operations directly without leases to verify tag read/write behavior, and
 * use a no-lease TagStateTracker variant where possible.
 */
@Category(IntegrationTest.class)
public class TagsStrategyIT extends AzuriteTestBase {

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        containerName = uniqueContainerName("tags");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
    }

    /**
     * Creates a TagStateTracker that uses a no-op lease manager (no real leases)
     * so that tag operations work on Azurite without 412 errors.
     */
    private TagStateTracker createNoLeaseTracker() {
        return new TagStateTracker(containerClient, 15, 10, "test-host",
                blobClient -> new LeaseManager(
                        new com.azure.storage.blob.specialized.BlobLeaseClientBuilder()
                                .blobClient(blobClient).buildClient(),
                        15, 10, () -> {}) {
                    @Override
                    public String acquireLease() {
                        // Return a fake lease ID — skip actual lease acquisition
                        return "fake-lease-id";
                    }
                    @Override
                    public void releaseLease() {
                        // No-op
                    }
                    @Override
                    public void startRenewal() {
                        // No-op
                    }
                    @Override
                    public void stopRenewal() {
                        // No-op
                    }
                });
    }

    private BlobPoller createPoller() {
        TagStateTracker stateTracker = createNoLeaseTracker();
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, "", 50);
    }

    // ── Test 1: New blob gets processing then completed tags ────────────────

    @Test
    public void testNewBlobGetsProcessingThenCompletedTags() {
        uploadBlob(containerName, "tag-test.log", "hello\n");

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        // After processing, the blob should be tagged as completed
        Map<String, String> tags = getBlobTags(containerName, "tag-test.log");
        assertEquals("completed", tags.get("logstash_status"));
        assertNotNull("logstash_completed should be set", tags.get("logstash_completed"));
        assertEquals("test-host", tags.get("logstash_processor"));
    }

    // ── Test 2: Completed blob not reprocessed ──────────────────────────────

    @Test
    public void testCompletedBlobNotReprocessed() {
        uploadBlob(containerName, "completed.log", "data\n");

        BlobPoller poller = createPoller();

        // First poll: processes the blob
        BlobPoller.PollCycleSummary first = poller.pollOnce(() -> false);
        assertEquals(1, first.getBlobsProcessed());
        assertEquals(1, events.size());

        // Second poll: blob should be skipped (already completed)
        BlobPoller.PollCycleSummary second = poller.pollOnce(() -> false);
        assertEquals(0, second.getBlobsProcessed());
        assertEquals(1, events.size()); // no new events
    }

    // ── Test 3: Failed blob is reprocessed ──────────────────────────────────

    @Test
    public void testFailedBlobReprocessed() {
        uploadBlob(containerName, "will-fail.log", "good-data\n");

        // Manually set the blob tags to 'failed' to simulate a previous failure
        Map<String, String> failedTags = new HashMap<>();
        failedTags.put("logstash_status", "failed");
        failedTags.put("logstash_error", "previous error");
        setBlobTags(containerName, "will-fail.log", failedTags);

        // Poll should pick up the failed blob for reprocessing
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Failed blob should be reprocessed", 1, summary.getBlobsProcessed());
        assertEquals(1, events.size());
        assertEquals("good-data", events.get(0).get("message"));

        // After reprocessing, it should be completed
        Map<String, String> tags = getBlobTags(containerName, "will-fail.log");
        assertEquals("completed", tags.get("logstash_status"));
    }

    // ── Test 4: User tags preserved ─────────────────────────────────────────

    @Test
    public void testUserTagsPreserved() {
        uploadBlob(containerName, "user-tags.log", "data\n");

        // Set a user tag before processing
        Map<String, String> userTags = new HashMap<>();
        userTags.put("team", "ops");
        setBlobTags(containerName, "user-tags.log", userTags);

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        // Verify user tag is still present
        Map<String, String> tags = getBlobTags(containerName, "user-tags.log");
        assertEquals("User tag should be preserved", "ops", tags.get("team"));
        assertEquals("completed", tags.get("logstash_status"));
    }

    // ── Test 5: Processor tag set ───────────────────────────────────────────

    @Test
    public void testProcessorTagSet() {
        uploadBlob(containerName, "proc-tag.log", "data\n");

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        Map<String, String> tags = getBlobTags(containerName, "proc-tag.log");
        assertEquals("test-host", tags.get("logstash_processor"));
    }

    // ── Test 6: Timestamps are ISO 8601 ─────────────────────────────────────

    @Test
    public void testTimestampsAreISO8601() {
        uploadBlob(containerName, "ts-test.log", "data\n");

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        Map<String, String> tags = getBlobTags(containerName, "ts-test.log");
        String completedTs = tags.get("logstash_completed");
        assertNotNull("logstash_completed should be present", completedTs);

        // ISO 8601 instant format: contains 'T' separator and 'Z' suffix
        assertTrue("Timestamp should contain 'T': " + completedTs, completedTs.contains("T"));
        assertTrue("Timestamp should end with 'Z': " + completedTs, completedTs.endsWith("Z"));

        // Verify it parses as a valid Instant
        Instant parsed = Instant.parse(completedTs);
        assertNotNull("Should parse as a valid Instant", parsed);
    }
}
