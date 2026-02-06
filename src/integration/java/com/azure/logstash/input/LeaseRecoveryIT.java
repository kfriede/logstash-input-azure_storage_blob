package com.azure.logstash.input;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import com.azure.storage.blob.specialized.BlobLeaseClientBuilder;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

/**
 * Integration tests for lease expiry and renewal behavior against Azurite.
 */
@Category(IntegrationTest.class)
public class LeaseRecoveryIT extends AzuriteTestBase {

    private String containerName;
    private BlobContainerClient containerClient;

    @Before
    public void setUp() {
        containerName = uniqueContainerName("lease");
        containerClient = createContainer(containerName);
    }

    // ── Test 1: Lease expiry allows reclaim ─────────────────────────────────

    @Test
    public void testLeaseExpiryAllowsReclaim() throws Exception {
        uploadBlob(containerName, "leased.log", "data\n");

        BlobClient blobClient = containerClient.getBlobClient("leased.log");

        // First acquirer: acquire with minimum duration (15 seconds)
        LeaseManager lease1 = new LeaseManager(blobClient, 15, 10, () -> {});
        String leaseId1 = lease1.acquireLease();
        assertNotNull("First lease should succeed", leaseId1);

        // Do NOT release or renew — let it expire
        // Second acquirer should initially fail
        LeaseManager lease2 = new LeaseManager(blobClient, 15, 10, () -> {});
        String leaseId2 = lease2.acquireLease();
        assertNull("Second lease should fail while first is active", leaseId2);

        // Wait for the lease to expire (15 seconds + buffer)
        Thread.sleep(16_000);

        // Now the second acquirer should succeed
        String leaseId3 = lease2.acquireLease();
        assertNotNull("Lease should succeed after expiry", leaseId3);

        // Clean up
        lease2.releaseLease();
    }

    // ── Test 2: Lease renewal failure invokes callback ──────────────────────

    @Test
    public void testLeaseRenewalFailureInvokesCallback() throws Exception {
        uploadBlob(containerName, "callback.log", "data\n");

        BlobClient blobClient = containerClient.getBlobClient("callback.log");

        // Track whether the failure callback is invoked
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        // Create LeaseManager with a short renewal interval (2s)
        LeaseManager lease = new LeaseManager(blobClient, 15, 2,
                () -> callbackInvoked.set(true));
        String leaseId = lease.acquireLease();
        assertNotNull("Lease should succeed", leaseId);
        lease.startRenewal();

        // Break the lease externally so the next renewal attempt fails
        BlobLeaseClient breakClient = new BlobLeaseClientBuilder()
                .blobClient(blobClient)
                .buildClient();
        breakClient.breakLease();

        // Wait for renewal to fail (2s interval + buffer)
        Thread.sleep(4_000);

        assertTrue("Failure callback should have been invoked after lease break",
                callbackInvoked.get());

        // Clean up
        lease.stopRenewal();
    }

    // ── Test 3: Lease renewal keeps claim ───────────────────────────────────

    @Test
    public void testLeaseRenewalKeepsClaim() throws Exception {
        uploadBlob(containerName, "renewed.log", "data\n");

        BlobClient blobClient = containerClient.getBlobClient("renewed.log");

        // Acquire lease with 15s duration and 10s renewal
        LeaseManager lease1 = new LeaseManager(blobClient, 15, 10, () -> {});
        String leaseId1 = lease1.acquireLease();
        assertNotNull("First lease should succeed", leaseId1);
        lease1.startRenewal();

        // Wait past the original 15s expiry time
        Thread.sleep(17_000);

        // The lease should still be active due to renewal
        LeaseManager lease2 = new LeaseManager(blobClient, 15, 10, () -> {});
        String leaseId2 = lease2.acquireLease();
        assertNull("Second lease should fail because renewal keeps first alive", leaseId2);

        // Clean up
        lease1.stopRenewal();
        lease1.releaseLease();
    }
}
