package com.azure.logstash.input.tracking;

import com.azure.storage.blob.models.BlobItem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * SQLite-backed state tracker for single-replica deployments.
 *
 * <p>This strategy stores blob processing state in a local SQLite database,
 * requiring only Azure Data Reader permissions. It is NOT safe for multi-replica
 * deployments because each replica maintains its own independent database.
 *
 * <p>State transitions:
 * <ul>
 *   <li>(new) → processing: via {@link #claim(String)}</li>
 *   <li>processing → completed: via {@link #markCompleted(String)}</li>
 *   <li>processing → failed: via {@link #markFailed(String, String)}</li>
 *   <li>processing → (deleted): via {@link #release(String)}, allows re-claim</li>
 *   <li>failed → processing: implicit via {@link #filterCandidates} + re-claim</li>
 * </ul>
 */
public class RegistryStateTracker implements StateTracker {

    private static final Logger logger = LogManager.getLogger(RegistryStateTracker.class);

    private static final String CREATE_TABLE_SQL =
            "CREATE TABLE IF NOT EXISTS blobs ("
                    + "blob_name TEXT PRIMARY KEY, "
                    + "status TEXT NOT NULL, "
                    + "started_at TEXT, "
                    + "completed_at TEXT, "
                    + "error TEXT, "
                    + "processor TEXT"
                    + ")";

    private static final String CREATE_INDEX_SQL =
            "CREATE INDEX IF NOT EXISTS idx_status ON blobs(status)";

    private static final String SELECT_COMPLETED_SQL =
            "SELECT blob_name FROM blobs WHERE status = 'completed'";

    private static final String INSERT_CLAIM_SQL =
            "INSERT OR IGNORE INTO blobs (blob_name, status, started_at, processor) "
                    + "VALUES (?, 'processing', ?, ?)";

    private static final String UPDATE_COMPLETED_SQL =
            "UPDATE blobs SET status='completed', completed_at=? WHERE blob_name=?";

    private static final String UPDATE_FAILED_SQL =
            "UPDATE blobs SET status='failed', error=? WHERE blob_name=?";

    private static final String DELETE_PROCESSING_SQL =
            "DELETE FROM blobs WHERE blob_name=? AND status='processing'";

    private final Connection connection;
    private final String processorName;

    /**
     * Creates a new RegistryStateTracker backed by a SQLite database at the given path.
     *
     * @param registryPath  path to the SQLite database file (created if it does not exist)
     * @param processorName identifier for this processor instance (stored in the processor column)
     */
    public RegistryStateTracker(String registryPath, String processorName) {
        this.processorName = processorName;
        try {
            this.connection = DriverManager.getConnection("jdbc:sqlite:" + registryPath);
            initSchema();
            logger.info("Registry state tracker initialized at {}", registryPath);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize SQLite registry at " + registryPath, e);
        }
    }

    private void initSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL);
            stmt.execute(CREATE_INDEX_SQL);
        }
    }

    /**
     * Returns blobs whose names are NOT in the completed set. Failed blobs ARE included
     * as candidates for reprocessing.
     */
    @Override
    public List<BlobItem> filterCandidates(List<BlobItem> blobs) {
        Set<String> completed = new HashSet<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(SELECT_COMPLETED_SQL)) {
            while (rs.next()) {
                completed.add(rs.getString("blob_name"));
            }
        } catch (SQLException e) {
            throw new RuntimeException("Failed to query completed blobs", e);
        }

        List<BlobItem> candidates = new ArrayList<>();
        for (BlobItem blob : blobs) {
            if (!completed.contains(blob.getName())) {
                candidates.add(blob);
            }
        }
        return candidates;
    }

    /**
     * Attempts to claim a blob for processing by inserting a record with status 'processing'.
     * Uses INSERT OR IGNORE so that if the blob already exists (in any state), the insert
     * is silently skipped and the method returns false.
     *
     * @return true if the blob was successfully claimed, false if it was already present
     */
    @Override
    public boolean claim(String blobName) {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_CLAIM_SQL)) {
            ps.setString(1, blobName);
            ps.setString(2, Instant.now().toString());
            ps.setString(3, processorName);
            int rowsAffected = ps.executeUpdate();
            return rowsAffected > 0;
        } catch (SQLException e) {
            throw new RuntimeException("Failed to claim blob: " + blobName, e);
        }
    }

    @Override
    public void markCompleted(String blobName) {
        try (PreparedStatement ps = connection.prepareStatement(UPDATE_COMPLETED_SQL)) {
            ps.setString(1, Instant.now().toString());
            ps.setString(2, blobName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob completed: " + blobName, e);
        }
    }

    @Override
    public void markFailed(String blobName, String error) {
        try (PreparedStatement ps = connection.prepareStatement(UPDATE_FAILED_SQL)) {
            ps.setString(1, error);
            ps.setString(2, blobName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to mark blob failed: " + blobName, e);
        }
    }

    /**
     * Releases a claim by deleting the record, but only if it is still in 'processing' status.
     * This allows the blob to be re-discovered and re-claimed on the next poll cycle.
     */
    @Override
    public void release(String blobName) {
        try (PreparedStatement ps = connection.prepareStatement(DELETE_PROCESSING_SQL)) {
            ps.setString(1, blobName);
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to release blob: " + blobName, e);
        }
    }

    @Override
    public void close() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
                logger.info("Registry state tracker closed");
            }
        } catch (SQLException e) {
            logger.warn("Error closing registry connection", e);
        }
    }
}
