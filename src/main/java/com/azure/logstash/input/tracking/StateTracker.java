package com.azure.logstash.input.tracking;

import com.azure.storage.blob.models.BlobItem;
import java.util.List;

/**
 * Tracks which blobs have been processed, are in-progress, or failed.
 *
 * <p>Three implementations exist (ordered by preference):
 * <ol>
 *   <li>{@code TagStateTracker} — blob index tags, needs Data Owner, multi-replica safe</li>
 *   <li>{@code ContainerStateTracker} — move blobs between containers, needs Data Contributor, multi-replica safe</li>
 *   <li>{@code RegistryStateTracker} — local SQLite, needs Data Reader only, single-replica only</li>
 * </ol>
 */
public interface StateTracker {

    /**
     * Filters a list of discovered blobs, returning only those that are candidates
     * for processing (i.e., not already completed).
     *
     * @param blobs the full list of blobs discovered during a poll cycle
     * @return blobs that should be considered for processing
     */
    List<BlobItem> filterCandidates(List<BlobItem> blobs);

    /**
     * Attempts to claim a blob for processing. Returns true if the claim succeeded
     * (i.e., the blob was not already claimed by another processor).
     *
     * @param blobName the name of the blob to claim
     * @return true if the claim succeeded, false if the blob is already claimed
     */
    boolean claim(String blobName);

    /**
     * Marks a blob as successfully completed.
     *
     * @param blobName the name of the blob that was processed
     */
    void markCompleted(String blobName);

    /**
     * Marks a blob as failed with an error message. Failed blobs may be
     * retried on a subsequent poll cycle.
     *
     * @param blobName the name of the blob that failed
     * @param error    a description of the error
     */
    void markFailed(String blobName, String error);

    /**
     * Releases a claim on a blob without marking it completed or failed.
     * This allows the blob to be re-claimed on the next poll cycle (e.g.,
     * during graceful shutdown or lease loss).
     *
     * @param blobName the name of the blob to release
     */
    void release(String blobName);

    /**
     * Releases any resources held by this tracker (e.g., database connections).
     */
    void close();
}
