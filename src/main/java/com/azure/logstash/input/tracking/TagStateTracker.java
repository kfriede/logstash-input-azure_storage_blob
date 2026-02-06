package com.azure.logstash.input.tracking;

import com.azure.logstash.input.LeaseManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.options.BlobSetTagsOptions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Blob index tag-based state tracker for multi-replica deployments.
 *
 * <p>Uses Azure Blob Index Tags to store processing state on each blob and
 * {@link LeaseManager} for distributed coordination. Requires Azure Storage
 * Data Owner RBAC role because blob tag operations need that permission level.
 *
 * <p>Tag keys used (5 of 10 available per blob):
 * <ul>
 *   <li>{@code logstash_status} — "processing", "completed", "failed"</li>
 *   <li>{@code logstash_processor} — pod/hostname identifier</li>
 *   <li>{@code logstash_started} — ISO 8601 timestamp when processing began</li>
 *   <li>{@code logstash_completed} — ISO 8601 timestamp when processing finished</li>
 *   <li>{@code logstash_error} — truncated error message (max 256 chars)</li>
 * </ul>
 *
 * <p>State transitions:
 * <ul>
 *   <li>(new) → processing: via {@link #claim(String)}</li>
 *   <li>processing → completed: via {@link #markCompleted(String)}</li>
 *   <li>processing → failed: via {@link #markFailed(String, String)}</li>
 *   <li>failed → processing: implicit via {@link #filterCandidates} + re-claim</li>
 * </ul>
 */
public class TagStateTracker implements StateTracker {

    private static final Logger logger = LogManager.getLogger(TagStateTracker.class);

    static final String TAG_STATUS = "logstash_status";
    static final String TAG_PROCESSOR = "logstash_processor";
    static final String TAG_STARTED = "logstash_started";
    static final String TAG_COMPLETED = "logstash_completed";
    static final String TAG_ERROR = "logstash_error";

    static final String STATUS_PROCESSING = "processing";
    static final String STATUS_COMPLETED = "completed";
    static final String STATUS_FAILED = "failed";

    static final int MAX_ERROR_LENGTH = 256;

    private final BlobContainerClient containerClient;
    private final int leaseDurationSeconds;
    private final int renewalIntervalSeconds;
    private final String processorName;
    private final Function<BlobClient, LeaseManager> leaseManagerFactory;
    private final Map<String, LeaseManager> activeLeases = new ConcurrentHashMap<>();
    private final Set<String> compromisedLeases = ConcurrentHashMap.newKeySet();

    /**
     * Public constructor — creates LeaseManagers internally via the BlobLeaseClientBuilder.
     *
     * @param containerClient      the container client for accessing blobs
     * @param leaseDurationSeconds lease duration in seconds (15-60)
     * @param renewalIntervalSeconds how often to renew leases, in seconds
     * @param processorName        identifier for this processor instance (hostname/pod name)
     */
    public TagStateTracker(BlobContainerClient containerClient, int leaseDurationSeconds,
                           int renewalIntervalSeconds, String processorName) {
        this.containerClient = containerClient;
        this.leaseDurationSeconds = leaseDurationSeconds;
        this.renewalIntervalSeconds = renewalIntervalSeconds;
        this.processorName = processorName;
        this.leaseManagerFactory = blobClient -> {
            final String name = blobClient.getBlobName();
            return new LeaseManager(blobClient, leaseDurationSeconds,
                    renewalIntervalSeconds, () -> compromisedLeases.add(name));
        };
        logger.info("Tag state tracker initialized for processor '{}'", processorName);
    }

    /**
     * Test constructor — accepts a factory to create LeaseManagers so tests can
     * inject mocks without building real BlobLeaseClients. Production code should
     * use {@link #TagStateTracker(BlobContainerClient, int, int, String)}.
     *
     * @param containerClient        the container client for accessing blobs
     * @param leaseDurationSeconds   lease duration in seconds
     * @param renewalIntervalSeconds how often to renew leases, in seconds
     * @param processorName          identifier for this processor instance
     * @param leaseManagerFactory    factory function that creates a LeaseManager for a BlobClient
     */
    public TagStateTracker(BlobContainerClient containerClient, int leaseDurationSeconds,
                           int renewalIntervalSeconds, String processorName,
                           Function<BlobClient, LeaseManager> leaseManagerFactory) {
        this.containerClient = containerClient;
        this.leaseDurationSeconds = leaseDurationSeconds;
        this.renewalIntervalSeconds = renewalIntervalSeconds;
        this.processorName = processorName;
        this.leaseManagerFactory = leaseManagerFactory;
        logger.info("Tag state tracker initialized for processor '{}'", processorName);
    }

    /**
     * Filters blobs to include only those eligible for processing.
     *
     * <p>For each blob, retrieves its tags and checks the {@code logstash_status} tag:
     * <ul>
     *   <li>Excludes blobs with status "processing" or "completed"</li>
     *   <li>Includes blobs with no logstash_status tag (new blobs)</li>
     *   <li>Includes blobs with status "failed" (for reprocessing)</li>
     * </ul>
     */
    @Override
    public List<BlobItem> filterCandidates(List<BlobItem> blobs) {
        List<BlobItem> candidates = new ArrayList<>();
        for (BlobItem blob : blobs) {
            try {
                BlobClient blobClient = containerClient.getBlobClient(blob.getName());
                Map<String, String> tags = blobClient.getTags();
                String status = tags.get(TAG_STATUS);

                if (status == null || STATUS_FAILED.equals(status)) {
                    candidates.add(blob);
                } else {
                    logger.debug("Excluding blob '{}' with status '{}'",
                            blob.getName(), status);
                }
            } catch (BlobStorageException e) {
                logger.warn("Failed to read tags for blob '{}', skipping: {}",
                        blob.getName(), e.getMessage());
            }
        }
        return candidates;
    }

    /**
     * Claims a blob for processing by acquiring a lease and setting index tags.
     *
     * <p>Steps:
     * <ol>
     *   <li>Create a LeaseManager for the blob</li>
     *   <li>Acquire a lease — if 409 (already leased), return false</li>
     *   <li>Read existing tags to preserve user-defined tags</li>
     *   <li>Merge plugin tags: status=processing, processor, started timestamp</li>
     *   <li>Set the merged tags on the blob</li>
     *   <li>Start lease renewal</li>
     *   <li>Store the LeaseManager in the active leases map</li>
     * </ol>
     *
     * @return true if the claim succeeded, false if the blob is already leased or
     *         a precondition failure (412) occurred during tag update
     */
    @Override
    public boolean claim(String blobName) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        LeaseManager lease = leaseManagerFactory.apply(blobClient);

        // Step 1: Acquire the lease
        String leaseId = lease.acquireLease();
        if (leaseId == null) {
            logger.debug("Could not acquire lease for blob '{}' (409 conflict)", blobName);
            return false;
        }

        try {
            // Step 2: Read existing tags to preserve user tags
            Map<String, String> existingTags = blobClient.getTags();
            Map<String, String> mergedTags = new HashMap<>(existingTags);

            // Step 3: Merge plugin tags
            mergedTags.put(TAG_STATUS, STATUS_PROCESSING);
            mergedTags.put(TAG_PROCESSOR, processorName);
            mergedTags.put(TAG_STARTED, Instant.now().toString());

            // Step 4: Set merged tags with lease condition
            setTagsWithLease(blobClient, mergedTags, leaseId);

        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 412) {
                logger.debug("Precondition failed (412) setting tags for blob '{}', "
                        + "releasing lease", blobName);
                lease.releaseLease();
                return false;
            }
            // Unexpected error — release lease and rethrow
            lease.releaseLease();
            throw e;
        }

        // Step 5: Start lease renewal and store in map
        lease.startRenewal();
        activeLeases.put(blobName, lease);
        logger.debug("Claimed blob '{}' with lease '{}'", blobName, leaseId);
        return true;
    }

    /**
     * Marks a blob as successfully completed by updating its index tags.
     *
     * <p>Sets {@code logstash_status=completed} and {@code logstash_completed} timestamp.
     * Removes {@code logstash_error} and {@code logstash_started} if present.
     * Preserves all user-defined tags.
     */
    @Override
    public void markCompleted(String blobName) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        Map<String, String> existingTags = blobClient.getTags();
        Map<String, String> mergedTags = new HashMap<>(existingTags);

        mergedTags.put(TAG_STATUS, STATUS_COMPLETED);
        mergedTags.put(TAG_COMPLETED, Instant.now().toString());
        mergedTags.put(TAG_PROCESSOR, processorName);
        mergedTags.remove(TAG_STARTED);
        mergedTags.remove(TAG_ERROR);

        LeaseManager lease = activeLeases.get(blobName);
        String leaseId = (lease != null) ? lease.getLeaseId() : null;
        setTagsWithLease(blobClient, mergedTags, leaseId);
        logger.debug("Marked blob '{}' as completed", blobName);
    }

    /**
     * Marks a blob as failed by updating its index tags with the error message.
     *
     * <p>Sets {@code logstash_status=failed} and {@code logstash_error} (truncated to 256 chars).
     * Preserves all user-defined tags.
     */
    @Override
    public void markFailed(String blobName, String error) {
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        Map<String, String> existingTags = blobClient.getTags();
        Map<String, String> mergedTags = new HashMap<>(existingTags);

        String truncatedError = error != null ? error : "unknown";
        // Azure Blob Index Tag values only allow: alphanumeric, space, +, -, ., :, =, _
        // Replace any disallowed characters with underscore, then truncate.
        truncatedError = truncatedError.replaceAll("[^a-zA-Z0-9 +\\-./:=_]", "_");
        if (truncatedError.length() > MAX_ERROR_LENGTH) {
            truncatedError = truncatedError.substring(0, MAX_ERROR_LENGTH);
        }

        mergedTags.put(TAG_STATUS, STATUS_FAILED);
        mergedTags.put(TAG_ERROR, truncatedError);
        mergedTags.put(TAG_PROCESSOR, processorName);

        LeaseManager lease = activeLeases.get(blobName);
        String leaseId = (lease != null) ? lease.getLeaseId() : null;
        setTagsWithLease(blobClient, mergedTags, leaseId);
        logger.debug("Marked blob '{}' as failed: {}", blobName, truncatedError);
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

    @Override
    public boolean wasLeaseRenewalFailed(String blobName) {
        return compromisedLeases.remove(blobName);
    }

    /**
     * Sets tags on a blob, including the lease ID in the request conditions if provided.
     * Azure Storage requires the lease ID to be specified when modifying tags on a leased blob.
     */
    private void setTagsWithLease(BlobClient blobClient, Map<String, String> tags, String leaseId) {
        if (leaseId != null) {
            BlobSetTagsOptions options = new BlobSetTagsOptions(tags);
            options.setRequestConditions(new BlobRequestConditions().setLeaseId(leaseId));
            blobClient.setTagsWithResponse(options, null, null);
        } else {
            blobClient.setTags(tags);
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
        compromisedLeases.clear();
        logger.info("Tag state tracker closed");
    }
}
