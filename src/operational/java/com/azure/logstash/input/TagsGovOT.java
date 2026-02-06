package com.azure.logstash.input;

import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Operational tests for tag-based state tracking against Azure Government.
 *
 * <p>Validates that blob index tags work correctly in Azure Government,
 * including set/get operations, user tag preservation, tag lifecycle
 * transitions, and the 10-tag limit.
 */
@Category(OperationalTest.class)
public class TagsGovOT extends AzureGovTestBase {

    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        containerClient = serviceClient.getBlobContainerClient("incoming");
        events = new CopyOnWriteArrayList<>();
        // Clean up any leftover blobs from previous tests (breaks leases first)
        cleanContainer("incoming");
    }

    private TagStateTracker createTracker() {
        return new TagStateTracker(containerClient, 15, 10, "test-host");
    }

    private BlobPoller createPoller() {
        TagStateTracker tracker = createTracker();
        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        return new BlobPoller(containerClient, tracker, processor,
                events::add, "", 50);
    }

    // ── Test 1: Tag operations work in gov ──────────────────────────────────

    @Test
    public void testTagOperationsWorkInGov() {
        uploadBlob("incoming", "tag-ops.log", "hello\n");

        // Set tags
        Map<String, String> tags = new HashMap<>();
        tags.put("env", "test");
        tags.put("team", "ops");
        setBlobTags("incoming", "tag-ops.log", tags);

        // Get tags
        Map<String, String> retrieved = getBlobTags("incoming", "tag-ops.log");
        assertEquals("test", retrieved.get("env"));
        assertEquals("ops", retrieved.get("team"));

        // Update tags
        tags.put("version", "2");
        setBlobTags("incoming", "tag-ops.log", tags);
        retrieved = getBlobTags("incoming", "tag-ops.log");
        assertEquals("2", retrieved.get("version"));
    }

    // ── Test 2: Full claim/complete lifecycle ────────────────────────────────

    @Test
    public void testConditionalTagUpdate412() {
        uploadBlob("incoming", "cond-tag.log", "data\n");

        TagStateTracker tracker = createTracker();

        // Claim should succeed
        boolean claimed = tracker.claim("cond-tag.log");
        assertTrue("Should be able to claim new blob", claimed);

        // Tags should show processing
        Map<String, String> tags = getBlobTags("incoming", "cond-tag.log");
        assertEquals("processing", tags.get("logstash_status"));

        // Mark completed then release
        tracker.markCompleted("cond-tag.log");
        tracker.release("cond-tag.log");

        tags = getBlobTags("incoming", "cond-tag.log");
        assertEquals("completed", tags.get("logstash_status"));
    }

    // ── Test 3: User tag preservation ───────────────────────────────────────

    @Test
    public void testUserTagPreservation() {
        uploadBlob("incoming", "user-tag.log", "data\n");

        // Set user tags before processing
        Map<String, String> userTags = new HashMap<>();
        userTags.put("team", "security");
        userTags.put("source", "firewall");
        setBlobTags("incoming", "user-tag.log", userTags);

        // Process with tags strategy
        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        // Verify user tags are preserved alongside logstash tags
        Map<String, String> tags = getBlobTags("incoming", "user-tag.log");
        assertEquals("security", tags.get("team"));
        assertEquals("firewall", tags.get("source"));
        assertEquals("completed", tags.get("logstash_status"));
    }

    // ── Test 4: Full tags lifecycle ─────────────────────────────────────────

    @Test
    public void testFullTagsLifecycle() {
        // Upload 3 blobs
        for (int i = 1; i <= 3; i++) {
            uploadBlob("incoming", "lifecycle-" + i + ".log", "line-" + i + "\n");
        }

        // Process all with tags strategy
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("All 3 blobs should be processed", 3, summary.getBlobsProcessed());

        // Verify all are tagged as completed
        for (int i = 1; i <= 3; i++) {
            Map<String, String> tags = getBlobTags("incoming", "lifecycle-" + i + ".log");
            assertEquals("Blob " + i + " should be completed",
                    "completed", tags.get("logstash_status"));
            assertNotNull("Blob " + i + " should have logstash_completed timestamp",
                    tags.get("logstash_completed"));
            assertEquals("test-host", tags.get("logstash_processor"));
        }

        // Verify events
        assertEquals(3, events.size());

        // Second poll should process zero (all completed)
        events.clear();
        BlobPoller.PollCycleSummary second = createPoller().pollOnce(() -> false);
        assertEquals("Second poll should process 0 blobs", 0, second.getBlobsProcessed());
        assertEquals(0, events.size());
    }

    // ── Test 5: Failed blob tags ────────────────────────────────────────────

    @Test
    public void testFailedBlobTags() {
        uploadBlob("incoming", "fail-tag.log", "data\n");

        TagStateTracker tracker = createTracker();

        // Claim the blob
        assertTrue("Should be able to claim", tracker.claim("fail-tag.log"));

        // Mark as failed
        tracker.markFailed("fail-tag.log", "simulated processing error");
        tracker.release("fail-tag.log");

        // Verify tags
        Map<String, String> tags = getBlobTags("incoming", "fail-tag.log");
        assertEquals("failed", tags.get("logstash_status"));
        assertEquals("simulated processing error", tags.get("logstash_error"));
        assertEquals("test-host", tags.get("logstash_processor"));
    }

    // ── Test 6: Ten tag limit (5 user + 5 logstash = 10) ───────────────────

    @Test
    public void testTenTagLimit() {
        uploadBlob("incoming", "ten-tags.log", "data\n");

        // Set 5 user tags
        Map<String, String> userTags = new HashMap<>();
        for (int i = 1; i <= 5; i++) {
            userTags.put("user_tag_" + i, "value_" + i);
        }
        setBlobTags("incoming", "ten-tags.log", userTags);

        // Process with tags strategy (adds up to 5 logstash tags)
        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        // Verify all tags are present (5 user + logstash tags = up to 10)
        Map<String, String> tags = getBlobTags("incoming", "ten-tags.log");

        // User tags preserved
        for (int i = 1; i <= 5; i++) {
            assertEquals("value_" + i, tags.get("user_tag_" + i));
        }

        // Logstash tags present
        assertEquals("completed", tags.get("logstash_status"));
        assertNotNull(tags.get("logstash_completed"));
        assertNotNull(tags.get("logstash_processor"));

        // Total should not exceed 10
        assertTrue("Total tags should be <= 10, got " + tags.size(), tags.size() <= 10);
    }
}
