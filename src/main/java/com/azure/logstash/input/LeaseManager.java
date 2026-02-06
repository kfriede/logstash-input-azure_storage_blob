package com.azure.logstash.input;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Manages Azure Blob leases for distributed coordination.
 *
 * <p>Used by TagStateTracker and ContainerStateTracker to ensure that only
 * one replica processes a blob at a time. Provides lease acquisition, renewal
 * (via a background scheduled thread), and release.
 *
 * <p>Lease renewal runs on a single-thread scheduled executor. If a renewal
 * fails, the configured {@code onRenewalFailure} callback is invoked and
 * the renewal loop stops automatically.
 */
public class LeaseManager {

    private static final Logger logger = LogManager.getLogger(LeaseManager.class);

    private final BlobLeaseClient leaseClient;
    private final int leaseDurationSeconds;
    private final int renewalIntervalSeconds;
    private final Runnable onRenewalFailure;

    private volatile String leaseId;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> renewalFuture;

    /**
     * Public constructor — creates the BlobLeaseClient internally via the builder.
     *
     * @param blobClient             the blob to lease
     * @param leaseDurationSeconds   lease duration in seconds (15-60, or -1 for infinite)
     * @param renewalIntervalSeconds how often to renew the lease, in seconds
     * @param onRenewalFailure       callback invoked when a lease renewal fails
     */
    public LeaseManager(BlobClient blobClient, int leaseDurationSeconds,
                        int renewalIntervalSeconds, Runnable onRenewalFailure) {
        this(new BlobLeaseClientBuilder().blobClient(blobClient).buildClient(),
             leaseDurationSeconds, renewalIntervalSeconds, onRenewalFailure);
    }

    /**
     * Test constructor — accepts a BlobLeaseClient directly so tests can inject mocks.
     *
     * <p>Production code should use the {@link #LeaseManager(BlobClient, int, int, Runnable)}
     * constructor instead.</p>
     *
     * @param leaseClient            the pre-built lease client (mockable)
     * @param leaseDurationSeconds   lease duration in seconds
     * @param renewalIntervalSeconds how often to renew the lease, in seconds
     * @param onRenewalFailure       callback invoked when a lease renewal fails
     */
    public LeaseManager(BlobLeaseClient leaseClient, int leaseDurationSeconds,
                        int renewalIntervalSeconds, Runnable onRenewalFailure) {
        this.leaseClient = leaseClient;
        this.leaseDurationSeconds = leaseDurationSeconds;
        this.renewalIntervalSeconds = renewalIntervalSeconds;
        this.onRenewalFailure = onRenewalFailure;
    }

    /**
     * Attempts to acquire a lease on the blob.
     *
     * @return the lease ID on success, or null if the blob is already leased (409 Conflict)
     */
    public String acquireLease() {
        try {
            String id = leaseClient.acquireLease(leaseDurationSeconds);
            this.leaseId = id;
            logger.debug("Acquired lease: {}", id);
            return id;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 409) {
                logger.debug("Blob already leased (409 Conflict)");
                return null;
            }
            throw e;
        }
    }

    /**
     * Renews the current lease. Throws on failure — the caller decides what to do.
     */
    public void renewLease() {
        leaseClient.renewLease();
        logger.debug("Renewed lease: {}", leaseId);
    }

    /**
     * Releases the current lease. Catches 409 (lease already expired/released) silently.
     */
    public void releaseLease() {
        try {
            leaseClient.releaseLease();
            logger.debug("Released lease: {}", leaseId);
            this.leaseId = null;
        } catch (BlobStorageException e) {
            if (e.getStatusCode() == 409) {
                logger.debug("Lease already expired or released (409)");
                this.leaseId = null;
                return;
            }
            throw e;
        }
    }

    /**
     * Starts a background thread that renews the lease at a fixed rate.
     * On renewal failure, calls the {@code onRenewalFailure} callback and
     * stops further renewals.
     */
    public void startRenewal() {
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "lease-renewal");
            t.setDaemon(true);
            return t;
        });
        renewalFuture = scheduler.scheduleAtFixedRate(() -> {
            try {
                renewLease();
            } catch (Exception e) {
                logger.warn("Lease renewal failed, invoking failure callback", e);
                try {
                    onRenewalFailure.run();
                } catch (Exception callbackEx) {
                    logger.error("onRenewalFailure callback threw an exception", callbackEx);
                }
                // Stop further renewals after failure
                stopRenewal();
            }
        }, renewalIntervalSeconds, renewalIntervalSeconds, TimeUnit.SECONDS);
    }

    /**
     * Stops the background lease renewal thread.
     */
    public void stopRenewal() {
        if (renewalFuture != null) {
            renewalFuture.cancel(false);
            renewalFuture = null;
        }
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }
    }

    /**
     * Returns the current lease ID, or null if no lease is held.
     */
    public String getLeaseId() {
        return leaseId;
    }
}
