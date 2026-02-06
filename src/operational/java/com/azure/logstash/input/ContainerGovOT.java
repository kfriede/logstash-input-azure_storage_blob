package com.azure.logstash.input;

import com.azure.logstash.input.tracking.ContainerStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Operational tests for container-based state tracking against Azure Government.
 *
 * <p>Validates that blobs are correctly moved between incoming, archive, and
 * error containers when using the container state tracking strategy.
 */
@Category(OperationalTest.class)
public class ContainerGovOT extends AzureGovTestBase {

    private BlobContainerClient incomingClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        incomingClient = serviceClient.getBlobContainerClient("incoming");
        events = new CopyOnWriteArrayList<>();

        // Clean up all containers before each test
        cleanContainer("incoming");
        cleanContainer("archive");
        cleanContainer("errors");
    }

    private BlobPoller createPoller() {
        ContainerStateTracker tracker = new ContainerStateTracker(
                serviceClient, "incoming", "archive", "errors",
                15, 10, "test-host");
        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        return new BlobPoller(incomingClient, tracker, processor,
                events::add, "", 50);
    }

    // ── Test 1: Cross-container copy (process -> archive) ───────────────────

    @Test
    public void testCrossContainerCopy() {
        String content = "line1\nline2\nline3\n";
        uploadBlob("incoming", "container-test.log", content);

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(3, summary.getEventsProduced());

        // Blob should be in archive with identical content
        List<String> archiveBlobs = listBlobNames("archive");
        assertTrue("Blob should be in archive", archiveBlobs.contains("container-test.log"));

        String archiveContent = getBlobContent("archive", "container-test.log");
        assertEquals("Content should be identical in archive", content, archiveContent);

        // Blob should no longer be in incoming
        List<String> incomingBlobs = listBlobNames("incoming");
        assertFalse("Blob should not be in incoming after processing",
                incomingBlobs.contains("container-test.log"));
    }

    // ── Test 2: Error container routing ─────────────────────────────────────

    @Test
    public void testErrorContainerRouting() {
        uploadBlob("incoming", "error-route.log", "data\n");

        ContainerStateTracker tracker = new ContainerStateTracker(
                serviceClient, "incoming", "archive", "errors",
                15, 10, "test-host");

        // Claim and then mark failed
        assertTrue(tracker.claim("error-route.log"));
        tracker.markFailed("error-route.log", "simulated failure");

        // Blob should be in errors container
        List<String> errorBlobs = listBlobNames("errors");
        assertTrue("Blob should be in errors container", errorBlobs.contains("error-route.log"));

        // Blob should not be in incoming
        List<String> incomingBlobs = listBlobNames("incoming");
        assertFalse("Blob should not be in incoming after failure",
                incomingBlobs.contains("error-route.log"));

        // Content should be preserved
        String content = getBlobContent("errors", "error-route.log");
        assertEquals("data\n", content);
    }

    // ── Test 3: Missing archive container routes blob to errors ─────────────

    @Test
    public void testMissingArchiveContainerRoutesToErrors() {
        uploadBlob("incoming", "missing-archive.log", "data\n");

        // Use a non-existent archive container name
        ContainerStateTracker tracker = new ContainerStateTracker(
                serviceClient, "incoming", "nonexistent-archive-container", "errors",
                15, 10, "test-host");

        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        BlobPoller poller = new BlobPoller(incomingClient, tracker, processor,
                events::add, "", 50);

        // With per-blob existence checks in filterCandidates, a non-existent archive
        // container no longer causes an immediate error (exists() returns false for
        // blobs in missing containers). Instead, the blob passes filtering, gets
        // processed, then markCompleted fails (copy to non-existent container).
        // BlobPoller catches this and routes the blob to the error container.
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals("Blob should be counted as failed", 1, summary.getBlobsFailed());
        assertEquals("No blobs should be marked processed", 0, summary.getBlobsProcessed());

        // The blob should be in the error container (routed there by markFailed)
        List<String> errorBlobs = listBlobNames("errors");
        assertTrue("Blob should be routed to error container",
                errorBlobs.contains("missing-archive.log"));

        // The blob should not remain in incoming
        List<String> incomingBlobs = listBlobNames("incoming");
        assertFalse("Blob should not be in incoming after error routing",
                incomingBlobs.contains("missing-archive.log"));
    }
}
