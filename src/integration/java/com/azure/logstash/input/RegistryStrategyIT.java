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
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Integration tests for the registry (SQLite) state tracking strategy.
 * Uses a temp SQLite database and real Azurite for blob content.
 */
@Category(IntegrationTest.class)
public class RegistryStrategyIT extends AzuriteTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;
    private String dbPath;

    @Before
    public void setUp() throws IOException {
        containerName = uniqueContainerName("reg");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
        dbPath = new File(tempFolder.getRoot(), "test-registry.db").getAbsolutePath();
    }

    private BlobPoller createPoller(String registryPath) {
        RegistryStateTracker stateTracker = new RegistryStateTracker(
                registryPath, "test-host");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, "", 50);
    }

    private BlobPoller createPoller() {
        return createPoller(dbPath);
    }

    /**
     * Helper to query the registry DB status for a blob.
     */
    private String queryBlobStatus(String registryPath, String blobName) throws SQLException {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + registryPath);
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT status FROM blobs WHERE blob_name = ?")) {
            ps.setString(1, blobName);
            ResultSet rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getString("status");
            }
            return null;
        }
    }

    // ── Test 1: Completed blob recorded in DB ───────────────────────────────

    @Test
    public void testCompletedBlobRecordedInDB() throws SQLException {
        uploadBlob(containerName, "recorded.log", "data\n");

        BlobPoller poller = createPoller();
        poller.pollOnce(() -> false);

        assertEquals(1, events.size());
        String status = queryBlobStatus(dbPath, "recorded.log");
        assertEquals("completed", status);
    }

    // ── Test 2: Completed blob not reprocessed ──────────────────────────────

    @Test
    public void testCompletedBlobNotReprocessed() {
        uploadBlob(containerName, "once.log", "data\n");

        BlobPoller poller = createPoller();

        // First poll: processes the blob
        BlobPoller.PollCycleSummary first = poller.pollOnce(() -> false);
        assertEquals(1, first.getBlobsProcessed());
        assertEquals(1, events.size());

        // Second poll: blob should be skipped
        BlobPoller.PollCycleSummary second = poller.pollOnce(() -> false);
        assertEquals(0, second.getBlobsProcessed());
        assertEquals(1, events.size()); // no new events
    }

    // ── Test 3: Failed blob recorded in DB ──────────────────────────────────

    @Test
    public void testFailedBlobRecordedInDB() throws Exception {
        // We cannot easily force a failure through BlobProcessor since it uses
        // UTF-8 CharsetDecoder which replaces malformed bytes.
        // Instead, manually simulate a failed state through the tracker directly.
        RegistryStateTracker tracker = new RegistryStateTracker(dbPath, "test-host");
        tracker.claim("manual-fail.log");
        tracker.markFailed("manual-fail.log", "test error");
        tracker.close();

        String status = queryBlobStatus(dbPath, "manual-fail.log");
        assertEquals("failed", status);
    }

    // ── Test 4: DB survives restart ─────────────────────────────────────────

    @Test
    public void testDBSurvivesRestart() {
        uploadBlob(containerName, "persistent.log", "data\n");

        // First session: process the blob
        BlobPoller poller1 = createPoller();
        poller1.pollOnce(() -> false);
        assertEquals(1, events.size());

        // Second session: new poller with SAME db path — blob should be skipped
        events.clear();
        BlobPoller poller2 = createPoller(dbPath);
        BlobPoller.PollCycleSummary summary = poller2.pollOnce(() -> false);
        assertEquals("Blob should be skipped after restart", 0, summary.getBlobsProcessed());
        assertTrue("No new events expected", events.isEmpty());
    }

    // ── Test 5: Ephemeral DB reprocesses all ────────────────────────────────

    @Test
    public void testEphemeralDBReprocessesAll() throws IOException {
        uploadBlob(containerName, "ephemeral.log", "data\n");

        // First session
        BlobPoller poller1 = createPoller();
        poller1.pollOnce(() -> false);
        assertEquals(1, events.size());

        // Second session with a DIFFERENT db path — blob should be reprocessed
        events.clear();
        String newDbPath = new File(tempFolder.getRoot(), "new-registry.db").getAbsolutePath();
        BlobPoller poller2 = createPoller(newDbPath);
        BlobPoller.PollCycleSummary summary = poller2.pollOnce(() -> false);
        assertEquals("Blob should be reprocessed with new DB", 1, summary.getBlobsProcessed());
        assertEquals(1, events.size());
    }
}
