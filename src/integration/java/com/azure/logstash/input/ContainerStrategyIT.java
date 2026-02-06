package com.azure.logstash.input;

import com.azure.logstash.input.tracking.ContainerStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Integration tests for the container-based state tracking strategy against Azurite.
 *
 * <p>Uses three containers: incoming, archive, and errors. Uses a no-op lease
 * manager factory to avoid Azurite lease-related limitations in copyAndDelete.
 */
@Category(IntegrationTest.class)
public class ContainerStrategyIT extends AzuriteTestBase {

    private String incomingName;
    private String archiveName;
    private String errorsName;
    private BlobContainerClient incomingClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        String suffix = uniqueContainerName("ctr");
        incomingName = suffix + "-in";
        archiveName = suffix + "-arc";
        errorsName = suffix + "-err";
        incomingClient = createContainer(incomingName);
        createContainer(archiveName);
        createContainer(errorsName);
        events = new CopyOnWriteArrayList<>();
    }

    /**
     * Creates a ContainerStateTracker with a no-op lease manager so that
     * blob copy/delete operations work against Azurite without lease conflicts.
     */
    private ContainerStateTracker createNoLeaseTracker() {
        return new ContainerStateTracker(serviceClient, incomingName, archiveName, errorsName,
                15, 10, "test-host",
                blobClient -> new LeaseManager(
                        new com.azure.storage.blob.specialized.BlobLeaseClientBuilder()
                                .blobClient(blobClient).buildClient(),
                        15, 10, () -> {}) {
                    @Override
                    public String acquireLease() {
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
        return createPoller("");
    }

    private BlobPoller createPoller(String prefix) {
        ContainerStateTracker stateTracker = createNoLeaseTracker();
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, incomingName, true);
        return new BlobPoller(incomingClient, stateTracker, processor,
                events::add, prefix, 50);
    }

    // ── Test 1: Completed blob moved to archive ─────────────────────────────

    @Test
    public void testCompletedBlobMovedToArchive() {
        uploadBlob(incomingName, "good.log", "success\n");

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());

        // Blob should be in archive, not in incoming
        List<String> archiveBlobs = listBlobNames(archiveName);
        List<String> incomingBlobs = listBlobNames(incomingName);
        assertTrue("Blob should be in archive", archiveBlobs.contains("good.log"));
        assertFalse("Blob should not be in incoming", incomingBlobs.contains("good.log"));
    }

    // ── Test 2: Failed blob moved to error container ────────────────────────

    @Test
    public void testFailedBlobMovedToErrorContainer() {
        // Binary data processed with UTF-8 won't actually crash BlobProcessor.
        // Instead, we test the container move mechanism by verifying that if the
        // BlobPoller encounters an exception, the blob goes to the errors container.
        // Since standard binary data is silently decoded, we test the archive path
        // and verify the errors container works by using the tracker directly.
        uploadBlob(incomingName, "test-err.log", "data\n");

        ContainerStateTracker tracker = createNoLeaseTracker();
        tracker.claim("test-err.log");
        tracker.markFailed("test-err.log", "simulated error");

        List<String> errorBlobs = listBlobNames(errorsName);
        List<String> incomingBlobs = listBlobNames(incomingName);
        assertTrue("Blob should be in errors", errorBlobs.contains("test-err.log"));
        assertFalse("Blob should not be in incoming", incomingBlobs.contains("test-err.log"));
    }

    // ── Test 3: Archive blob content matches original ───────────────────────

    @Test
    public void testArchiveBlobContentMatches() {
        String content = "line1\nline2\nline3\n";
        uploadBlob(incomingName, "verify.log", content);

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        String archivedContent = getBlobContent(archiveName, "verify.log");
        assertEquals("Archive content should match original", content, archivedContent);
    }

    // ── Test 4: Duplicate detection after crash ─────────────────────────────

    @Test
    public void testDuplicateDetectionAfterCrash() {
        // Simulate crash recovery: blob exists in both incoming and archive
        String content = "data\n";
        uploadBlob(incomingName, "crashed.log", content);
        uploadBlob(archiveName, "crashed.log", content);

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        // Blob should be skipped since it exists in archive
        assertEquals("Blob should not be processed (duplicate)", 0, summary.getBlobsProcessed());
        assertTrue(events.isEmpty());
    }

    // ── Test 5: Blob path preserved ─────────────────────────────────────────

    @Test
    public void testBlobPathPreserved() {
        uploadBlob(incomingName, "nested/dir/deep/file.log", "nested-data\n");

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        List<String> archiveBlobs = listBlobNames(archiveName);
        assertTrue("Nested path should be preserved in archive",
                archiveBlobs.contains("nested/dir/deep/file.log"));
    }
}
