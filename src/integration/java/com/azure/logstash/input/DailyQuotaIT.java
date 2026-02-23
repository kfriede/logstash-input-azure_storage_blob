package com.azure.logstash.input;

import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Integration tests for daily quota enforcement against Azurite.
 */
@Category(IntegrationTest.class)
public class DailyQuotaIT extends AzuriteTestBase {

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        containerName = uniqueContainerName("quota");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
    }

    private BlobPoller createPoller(long dailyQuotaBytes) {
        TagStateTracker stateTracker = new TagStateTracker(
                containerClient, 15, 10, "test-host");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, Collections.emptyList(), Collections.emptyList(),
                50, 1, dailyQuotaBytes);
    }

    // ── Test 1: Quota stops processing after limit ──────────────────────

    @Test
    public void testQuotaStopsProcessingAfterLimit() {
        // Upload 3 blobs, each ~11 bytes
        uploadBlob(containerName, "a.log", "aaaaaaaaaa\n"); // 11 bytes
        uploadBlob(containerName, "b.log", "bbbbbbbbbb\n"); // 11 bytes
        uploadBlob(containerName, "c.log", "cccccccccc\n"); // 11 bytes

        // Set quota to 25 bytes — should process 2 blobs (~22 bytes), skip third
        BlobPoller poller = createPoller(25L);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Should process 2 blobs before hitting quota", 2, summary.getBlobsProcessed());
        assertTrue("Quota should be reached", summary.isQuotaReached());

        // Second poll: quota already met from first cycle's completed tags
        BlobPoller poller2 = createPoller(25L);
        BlobPoller.PollCycleSummary summary2 = poller2.pollOnce(() -> false);

        assertEquals("No more blobs should be processed", 0, summary2.getBlobsProcessed());
        assertTrue("Quota should still be reached", summary2.isQuotaReached());
    }

    // ── Test 2: Quota disabled processes all blobs ──────────────────────

    @Test
    public void testQuotaDisabledProcessesAll() {
        uploadBlob(containerName, "a.log", "aaaaaaaaaa\n");
        uploadBlob(containerName, "b.log", "bbbbbbbbbb\n");
        uploadBlob(containerName, "c.log", "cccccccccc\n");

        BlobPoller poller = createPoller(0L); // disabled
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("All 3 blobs should be processed", 3, summary.getBlobsProcessed());
        assertFalse("Quota should not be reached", summary.isQuotaReached());
    }

    // ── Test 3: Multi-cycle — second cycle sees first cycle's tags ──────

    @Test
    public void testMultiCycleQuotaAccumulation() {
        // Upload 2 blobs
        uploadBlob(containerName, "a.log", "aaaaaaaaaa\n"); // ~11 bytes
        uploadBlob(containerName, "b.log", "bbbbbbbbbb\n"); // ~11 bytes

        // Quota of 50 bytes — enough for both
        BlobPoller poller = createPoller(50L);
        BlobPoller.PollCycleSummary first = poller.pollOnce(() -> false);
        assertEquals(2, first.getBlobsProcessed());

        // Upload a third blob
        uploadBlob(containerName, "c.log", "cccccccccc\n"); // ~11 bytes

        // Second cycle: ~22 bytes already used, 11 more → ~33, still under 50
        BlobPoller poller2 = createPoller(50L);
        BlobPoller.PollCycleSummary second = poller2.pollOnce(() -> false);
        assertEquals("Third blob should be processed (under quota)", 1, second.getBlobsProcessed());
    }
}
