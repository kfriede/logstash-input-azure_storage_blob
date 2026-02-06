package com.azure.logstash.input.unit;

import com.azure.logstash.input.tracking.RegistryStateTracker;
import com.azure.storage.blob.models.BlobItem;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.*;

/**
 * Unit tests for RegistryStateTracker — SQLite-backed state tracking
 * for single-replica deployments.
 */
public class RegistryStateTrackerTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String dbPath;
    private RegistryStateTracker tracker;

    @Before
    public void setUp() throws Exception {
        File dbFile = tempFolder.newFile("test-registry.db");
        // Delete so RegistryStateTracker can create it fresh
        dbFile.delete();
        dbPath = dbFile.getAbsolutePath();
        tracker = new RegistryStateTracker(dbPath, "test-processor");
    }

    @After
    public void tearDown() {
        if (tracker != null) {
            tracker.close();
        }
    }

    // -----------------------------------------------------------------------
    // 1. Schema is created on init — blobs table exists
    // -----------------------------------------------------------------------
    @Test
    public void testSchemaCreatedOnInit() throws Exception {
        // Connect directly and verify the blobs table exists
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT name FROM sqlite_master WHERE type='table' AND name='blobs'")) {
            assertTrue("blobs table should exist after init", rs.next());
            assertEquals("blobs", rs.getString("name"));
        }
    }

    // -----------------------------------------------------------------------
    // 2. filterCandidates excludes completed blobs
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesExcludesCompleted() throws Exception {
        // Insert a completed record directly
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "INSERT INTO blobs (blob_name, status, completed_at) "
                            + "VALUES ('completed-blob', 'completed', '2024-01-01T00:00:00Z')");
        }

        BlobItem completedBlob = new BlobItem().setName("completed-blob");
        BlobItem newBlob = new BlobItem().setName("new-blob");
        List<BlobItem> candidates = tracker.filterCandidates(Arrays.asList(completedBlob, newBlob));

        assertEquals("Only new-blob should be returned", 1, candidates.size());
        assertEquals("new-blob", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 3. filterCandidates includes new (unknown) blobs
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesIncludesNew() {
        BlobItem newBlob = new BlobItem().setName("brand-new-blob");
        List<BlobItem> candidates = tracker.filterCandidates(Collections.singletonList(newBlob));

        assertEquals("New blob should be included", 1, candidates.size());
        assertEquals("brand-new-blob", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 4. filterCandidates includes failed blobs (for reprocessing)
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesIncludesFailed() throws Exception {
        // Insert a failed record directly
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(
                    "INSERT INTO blobs (blob_name, status, error) "
                            + "VALUES ('failed-blob', 'failed', 'something went wrong')");
        }

        BlobItem failedBlob = new BlobItem().setName("failed-blob");
        List<BlobItem> candidates = tracker.filterCandidates(Collections.singletonList(failedBlob));

        assertEquals("Failed blob should be included for reprocessing", 1, candidates.size());
        assertEquals("failed-blob", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 5. claim inserts a processing record
    // -----------------------------------------------------------------------
    @Test
    public void testClaimInsertsProcessingRecord() throws Exception {
        tracker.claim("my-blob");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT status, processor FROM blobs WHERE blob_name='my-blob'")) {
            assertTrue("Record should exist after claim", rs.next());
            assertEquals("processing", rs.getString("status"));
            assertEquals("test-processor", rs.getString("processor"));
        }
    }

    // -----------------------------------------------------------------------
    // 6. claim returns true for a new blob
    // -----------------------------------------------------------------------
    @Test
    public void testClaimReturnsTrueForNewBlob() {
        boolean result = tracker.claim("unclaimed-blob");
        assertTrue("claim should return true for a new blob", result);
    }

    // -----------------------------------------------------------------------
    // 7. claim returns false for an already-processing blob
    // -----------------------------------------------------------------------
    @Test
    public void testClaimReturnsFalseForAlreadyProcessingBlob() {
        boolean first = tracker.claim("dup-blob");
        boolean second = tracker.claim("dup-blob");

        assertTrue("First claim should succeed", first);
        assertFalse("Second claim on same blob should fail", second);
    }

    // -----------------------------------------------------------------------
    // 8. markCompleted updates status and sets completed_at
    // -----------------------------------------------------------------------
    @Test
    public void testMarkCompletedUpdatesStatus() throws Exception {
        tracker.claim("complete-me");
        tracker.markCompleted("complete-me");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT status, completed_at FROM blobs WHERE blob_name='complete-me'")) {
            assertTrue("Record should exist", rs.next());
            assertEquals("completed", rs.getString("status"));
            assertNotNull("completed_at should be set", rs.getString("completed_at"));
        }
    }

    // -----------------------------------------------------------------------
    // 9. markFailed updates status and records error
    // -----------------------------------------------------------------------
    @Test
    public void testMarkFailedUpdatesStatus() throws Exception {
        tracker.claim("fail-me");
        tracker.markFailed("fail-me", "disk full");

        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT status, error FROM blobs WHERE blob_name='fail-me'")) {
            assertTrue("Record should exist", rs.next());
            assertEquals("failed", rs.getString("status"));
            assertEquals("disk full", rs.getString("error"));
        }
    }

    // -----------------------------------------------------------------------
    // 10. release deletes processing record (allows re-claim)
    // -----------------------------------------------------------------------
    @Test
    public void testReleaseDeletesProcessingRecord() throws Exception {
        tracker.claim("release-me");
        tracker.release("release-me");

        // Blob should no longer be in the DB
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + dbPath);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT COUNT(*) AS cnt FROM blobs WHERE blob_name='release-me'")) {
            assertEquals("Record should be deleted after release", 0, rs.getInt("cnt"));
        }

        // Should be able to re-claim
        boolean reClaim = tracker.claim("release-me");
        assertTrue("Should be able to re-claim after release", reClaim);
    }

    // -----------------------------------------------------------------------
    // 11. close closes the connection — subsequent operations throw
    // -----------------------------------------------------------------------
    @Test
    public void testCloseClosesConnection() {
        tracker.close();
        try {
            tracker.claim("after-close");
            fail("Should have thrown after close");
        } catch (RuntimeException e) {
            // Expected — the underlying SQLException is wrapped in a RuntimeException
            assertTrue("Should wrap a SQLException",
                    e.getCause() instanceof SQLException
                            || e.getMessage().contains("closed")
                            || e.getMessage().contains("Connection"));
        } finally {
            // Prevent double-close in tearDown
            tracker = null;
        }
    }
}
