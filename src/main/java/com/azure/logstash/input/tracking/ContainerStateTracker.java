package com.azure.logstash.input.tracking;

import com.azure.core.util.polling.SyncPoller;
import com.azure.logstash.input.LeaseManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Container-based state tracker for multi-replica deployments.
 *
 * <p>State is implicit in which container a blob resides in. Blobs start in
 * the "incoming" container, are moved to "archive" on success, or "errors"
 * on failure. Uses {@link LeaseManager} for distributed coordination so that
 * multiple replicas can safely process blobs from the same incoming container.
 *
 * <p>Requires Azure Storage Data Contributor RBAC role because blob copy and
 * delete operations need that permission level.
 *
 * <p>State transitions:
 * <ul>
 *   <li>incoming → archive: via {@link #markCompleted(String)}</li>
 *   <li>incoming → errors: via {@link #markFailed(String, String)}</li>
 * </ul>
 *
 * <p>Crash recovery: if a blob exists in both incoming and archive, it was
 * already fully processed — {@link #filterCandidates} skips it. The copy
 * completed but the delete from incoming did not complete before the crash.
 */
public class ContainerStateTracker implements StateTracker {

    private static final Logger logger = LogManager.getLogger(ContainerStateTracker.class);

    private final BlobContainerClient incomingContainerClient;
    private final BlobContainerClient archiveContainerClient;
    private final BlobContainerClient errorContainerClient;
    private final int leaseDurationSeconds;
    private final int renewalIntervalSeconds;
    private final String processorName;
    private final Function<BlobClient, LeaseManager> leaseManagerFactory;
    private final Map<String, LeaseManager> activeLeases = new ConcurrentHashMap<>();

    /**
     * Public constructor — creates LeaseManagers internally via the BlobLeaseClientBuilder.
     *
     * @param serviceClient        the blob service client for accessing containers
     * @param incomingContainer    name of the container where new blobs arrive
     * @param archiveContainer     name of the container where completed blobs are moved
     * @param errorContainer       name of the container where failed blobs are moved
     * @param leaseDurationSeconds lease duration in seconds (15-60)
     * @param renewalIntervalSeconds how often to renew leases, in seconds
     * @param processorName        identifier for this processor instance (hostname/pod name)
     */
    public ContainerStateTracker(BlobServiceClient serviceClient, String incomingContainer,
                                  String archiveContainer, String errorContainer,
                                  int leaseDurationSeconds, int renewalIntervalSeconds,
                                  String processorName) {
        this(serviceClient, incomingContainer, archiveContainer, errorContainer,
                leaseDurationSeconds, renewalIntervalSeconds, processorName,
                blobClient -> new LeaseManager(blobClient, leaseDurationSeconds,
                        renewalIntervalSeconds, () -> {}));
    }

    /**
     * Test constructor — accepts a factory to create LeaseManagers so tests can
     * inject mocks without building real BlobLeaseClients. Production code should
     * use {@link #ContainerStateTracker(BlobServiceClient, String, String, String, int, int, String)}.
     *
     * @param serviceClient          the blob service client for accessing containers
     * @param incomingContainer      name of the incoming container
     * @param archiveContainer       name of the archive container
     * @param errorContainer         name of the error container
     * @param leaseDurationSeconds   lease duration in seconds
     * @param renewalIntervalSeconds how often to renew leases, in seconds
     * @param processorName          identifier for this processor instance
     * @param leaseManagerFactory    factory function that creates a LeaseManager for a BlobClient
     */
    public ContainerStateTracker(BlobServiceClient serviceClient, String incomingContainer,
                                  String archiveContainer, String errorContainer,
                                  int leaseDurationSeconds, int renewalIntervalSeconds,
                                  String processorName,
                                  Function<BlobClient, LeaseManager> leaseManagerFactory) {
        this.incomingContainerClient = serviceClient.getBlobContainerClient(incomingContainer);
        this.archiveContainerClient = serviceClient.getBlobContainerClient(archiveContainer);
        this.errorContainerClient = serviceClient.getBlobContainerClient(errorContainer);
        this.leaseDurationSeconds = leaseDurationSeconds;
        this.renewalIntervalSeconds = renewalIntervalSeconds;
        this.processorName = processorName;
        this.leaseManagerFactory = leaseManagerFactory;
        logger.info("Container state tracker initialized for processor '{}': "
                + "incoming='{}', archive='{}', errors='{}'",
                processorName, incomingContainer, archiveContainer, errorContainer);
    }

    /**
     * Filters blobs to include only those eligible for processing.
     *
     * <p>Lists all blobs in the archive container and builds a set of their names.
     * Returns only blobs whose names are NOT in the archive set. This handles the
     * crash recovery case: if a blob exists in both incoming and archive, the copy
     * completed but the delete did not — the blob was already processed, so skip it.
     */
    @Override
    public List<BlobItem> filterCandidates(List<BlobItem> blobs) {
        Set<String> archivedNames = new HashSet<>();
        for (BlobItem archived : archiveContainerClient.listBlobs()) {
            archivedNames.add(archived.getName());
        }

        List<BlobItem> candidates = new ArrayList<>();
        for (BlobItem blob : blobs) {
            if (archivedNames.contains(blob.getName())) {
                logger.debug("Excluding blob '{}' — already exists in archive container",
                        blob.getName());
            } else {
                candidates.add(blob);
            }
        }
        return candidates;
    }

    /**
     * Claims a blob for processing by acquiring a lease on it in the incoming container.
     *
     * <p>Steps:
     * <ol>
     *   <li>Create a LeaseManager for the blob in the incoming container</li>
     *   <li>Acquire a lease — if 409 (already leased), return false</li>
     *   <li>Start lease renewal</li>
     *   <li>Store the LeaseManager in the active leases map</li>
     * </ol>
     *
     * @return true if the claim succeeded, false if the blob is already leased
     */
    @Override
    public boolean claim(String blobName) {
        BlobClient blobClient = incomingContainerClient.getBlobClient(blobName);
        LeaseManager lease = leaseManagerFactory.apply(blobClient);

        String leaseId = lease.acquireLease();
        if (leaseId == null) {
            logger.debug("Could not acquire lease for blob '{}' (409 conflict)", blobName);
            return false;
        }

        lease.startRenewal();
        activeLeases.put(blobName, lease);
        logger.debug("Claimed blob '{}' with lease '{}'", blobName, leaseId);
        return true;
    }

    /**
     * Marks a blob as successfully completed by copying it to the archive container
     * and then deleting it from the incoming container.
     *
     * <p>Steps:
     * <ol>
     *   <li>Get source blob URL from the incoming container</li>
     *   <li>Start async copy to archive container via {@code beginCopy}</li>
     *   <li>Poll until copy completes</li>
     *   <li>Delete the blob from the incoming container</li>
     *   <li>Stop lease renewal and release the lease</li>
     * </ol>
     *
     * <p>If the copy fails, the blob remains in incoming so it can be retried.
     * The exception is propagated to the caller.
     */
    @Override
    public void markCompleted(String blobName) {
        copyAndDelete(blobName, archiveContainerClient, "archive");
    }

    /**
     * Marks a blob as failed by copying it to the error container and then
     * deleting it from the incoming container.
     *
     * <p>Same copy-then-delete logic as {@link #markCompleted(String)}, but the
     * destination is the error container instead of archive.
     */
    @Override
    public void markFailed(String blobName, String error) {
        logger.debug("Marking blob '{}' as failed: {}", blobName, error);
        copyAndDelete(blobName, errorContainerClient, "errors");
    }

    /**
     * Releases a claim on a blob by stopping the lease renewal and releasing the lease.
     * Removes the LeaseManager from the active leases map.
     */
    @Override
    public void release(String blobName) {
        LeaseManager lease = activeLeases.remove(blobName);
        if (lease != null) {
            lease.stopRenewal();
            lease.releaseLease();
            logger.debug("Released lease for blob '{}'", blobName);
        } else {
            logger.warn("No active lease found for blob '{}' during release", blobName);
        }
    }

    /**
     * Releases all active leases and clears the active leases map.
     */
    @Override
    public void close() {
        for (Map.Entry<String, LeaseManager> entry : activeLeases.entrySet()) {
            try {
                entry.getValue().stopRenewal();
                entry.getValue().releaseLease();
                logger.debug("Released lease for blob '{}' during close", entry.getKey());
            } catch (Exception e) {
                logger.warn("Error releasing lease for blob '{}' during close: {}",
                        entry.getKey(), e.getMessage());
            }
        }
        activeLeases.clear();
        logger.info("Container state tracker closed");
    }

    // -----------------------------------------------------------------------
    // Internal: copy blob to destination container, then delete from incoming
    // -----------------------------------------------------------------------
    private void copyAndDelete(String blobName, BlobContainerClient destinationClient,
                                String destinationName) {
        BlobClient sourceBlobClient = incomingContainerClient.getBlobClient(blobName);
        String sourceUrl = sourceBlobClient.getBlobUrl();

        BlobClient destBlobClient = destinationClient.getBlobClient(blobName);

        // Start server-side copy and wait for completion
        SyncPoller<BlobCopyInfo, Void> poller = destBlobClient.beginCopy(sourceUrl, null);
        poller.waitForCompletion();
        logger.debug("Copied blob '{}' to {} container", blobName, destinationName);

        // Release the lease before deleting from incoming
        LeaseManager lease = activeLeases.remove(blobName);
        if (lease != null) {
            lease.stopRenewal();
            lease.releaseLease();
            logger.debug("Released lease for blob '{}' before delete", blobName);
        }

        // Delete from incoming — only after copy is confirmed complete and lease released
        sourceBlobClient.delete();
        logger.debug("Deleted blob '{}' from incoming container after move to {}",
                blobName, destinationName);
    }
}
