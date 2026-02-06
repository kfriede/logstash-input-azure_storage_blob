package com.azure.logstash.input.unit;

import com.azure.core.http.HttpResponse;
import com.azure.logstash.input.LeaseManager;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.specialized.BlobLeaseClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Unit tests for LeaseManager — manages Azure Blob leases for
 * distributed coordination (used by Tags and Container strategies).
 */
public class LeaseManagerTest {

    private BlobLeaseClient leaseClient;
    private LeaseManager leaseManager;

    @Before
    public void setUp() {
        leaseClient = mock(BlobLeaseClient.class);
    }

    @After
    public void tearDown() {
        if (leaseManager != null) {
            leaseManager.stopRenewal();
        }
    }

    // -----------------------------------------------------------------------
    // Helper: create a mock BlobStorageException with a specific status code
    // -----------------------------------------------------------------------
    private BlobStorageException createBlobStorageException(int statusCode) {
        HttpResponse mockResponse = mock(HttpResponse.class);
        when(mockResponse.getStatusCode()).thenReturn(statusCode);
        BlobStorageException bse = mock(BlobStorageException.class);
        when(bse.getStatusCode()).thenReturn(statusCode);
        return bse;
    }

    // -----------------------------------------------------------------------
    // 1. acquireLease succeeds — returns lease ID
    // -----------------------------------------------------------------------
    @Test
    public void testAcquireLeaseSuccess() {
        when(leaseClient.acquireLease(30)).thenReturn("lease-id-123");
        leaseManager = new LeaseManager(leaseClient, 30, 10, () -> {});

        String leaseId = leaseManager.acquireLease();

        assertEquals("lease-id-123", leaseId);
        assertEquals("lease-id-123", leaseManager.getLeaseId());
        verify(leaseClient).acquireLease(30);
    }

    // -----------------------------------------------------------------------
    // 2. acquireLease conflict 409 — returns null
    // -----------------------------------------------------------------------
    @Test
    public void testAcquireLeaseConflict409() {
        BlobStorageException bse = createBlobStorageException(409);
        doThrow(bse).when(leaseClient).acquireLease(anyInt());
        leaseManager = new LeaseManager(leaseClient, 30, 10, () -> {});

        String leaseId = leaseManager.acquireLease();

        assertNull("Should return null on 409 conflict", leaseId);
        assertNull("getLeaseId should be null after failed acquire", leaseManager.getLeaseId());
    }

    // -----------------------------------------------------------------------
    // 3. renewLease succeeds — no exception
    // -----------------------------------------------------------------------
    @Test
    public void testRenewLeaseSuccess() {
        when(leaseClient.acquireLease(30)).thenReturn("lease-id-123");
        leaseManager = new LeaseManager(leaseClient, 30, 10, () -> {});
        leaseManager.acquireLease();

        // Should not throw
        leaseManager.renewLease();
        verify(leaseClient).renewLease();
    }

    // -----------------------------------------------------------------------
    // 4. renewLease failure throws — exception propagated
    // -----------------------------------------------------------------------
    @Test(expected = BlobStorageException.class)
    public void testRenewLeaseFailureThrows() {
        BlobStorageException bse = createBlobStorageException(404);
        doThrow(bse).when(leaseClient).renewLease();
        leaseManager = new LeaseManager(leaseClient, 30, 10, () -> {});

        leaseManager.renewLease();
    }

    // -----------------------------------------------------------------------
    // 5. releaseLease succeeds — no exception
    // -----------------------------------------------------------------------
    @Test
    public void testReleaseLeaseSuccess() {
        leaseManager = new LeaseManager(leaseClient, 30, 10, () -> {});

        // Should not throw
        leaseManager.releaseLease();
        verify(leaseClient).releaseLease();
    }

    // -----------------------------------------------------------------------
    // 6. releaseLease already expired (409) — swallowed silently
    // -----------------------------------------------------------------------
    @Test
    public void testReleaseLeaseAlreadyExpired() {
        BlobStorageException bse = createBlobStorageException(409);
        doThrow(bse).when(leaseClient).releaseLease();
        leaseManager = new LeaseManager(leaseClient, 30, 10, () -> {});

        // Should NOT throw — 409 is swallowed silently
        leaseManager.releaseLease();
        verify(leaseClient).releaseLease();
    }

    // -----------------------------------------------------------------------
    // 7. startRenewal schedules renewal — renewLease called at least once
    // -----------------------------------------------------------------------
    @Test
    public void testStartRenewalSchedulesRenewal() throws Exception {
        when(leaseClient.acquireLease(30)).thenReturn("lease-id-123");
        // Use a very short renewal interval (1 second) for testing
        leaseManager = new LeaseManager(leaseClient, 30, 1, () -> {});
        leaseManager.acquireLease();

        leaseManager.startRenewal();

        // Wait enough time for at least one renewal to fire
        Thread.sleep(2500);

        verify(leaseClient, atLeastOnce()).renewLease();
    }

    // -----------------------------------------------------------------------
    // 8. stopRenewal cancels thread — no more renewals after stop
    // -----------------------------------------------------------------------
    @Test
    public void testStopRenewalCancelsThread() throws Exception {
        when(leaseClient.acquireLease(30)).thenReturn("lease-id-123");
        leaseManager = new LeaseManager(leaseClient, 30, 1, () -> {});
        leaseManager.acquireLease();

        leaseManager.startRenewal();
        // Let one renewal happen
        Thread.sleep(1500);
        leaseManager.stopRenewal();

        // Record how many times renewLease was called
        int callsAfterStop = mockingDetails(leaseClient).getInvocations().stream()
                .filter(inv -> inv.getMethod().getName().equals("renewLease"))
                .mapToInt(inv -> 1)
                .sum();

        // Wait to see if any more calls happen
        Thread.sleep(2000);

        int callsLater = mockingDetails(leaseClient).getInvocations().stream()
                .filter(inv -> inv.getMethod().getName().equals("renewLease"))
                .mapToInt(inv -> 1)
                .sum();

        assertEquals("No additional renewals should happen after stop", callsAfterStop, callsLater);
    }

    // -----------------------------------------------------------------------
    // 9. Renewal failure calls callback
    // -----------------------------------------------------------------------
    @Test
    public void testRenewalFailureCallsCallback() throws Exception {
        BlobStorageException bse = createBlobStorageException(404);
        doThrow(bse).when(leaseClient).renewLease();

        CountDownLatch callbackLatch = new CountDownLatch(1);
        AtomicBoolean callbackInvoked = new AtomicBoolean(false);

        leaseManager = new LeaseManager(leaseClient, 30, 1, () -> {
            callbackInvoked.set(true);
            callbackLatch.countDown();
        });

        leaseManager.startRenewal();

        // Wait for the callback to be invoked
        boolean reached = callbackLatch.await(5, TimeUnit.SECONDS);
        assertTrue("Callback should have been invoked within 5 seconds", reached);
        assertTrue("onRenewalFailure callback should have been called", callbackInvoked.get());
    }
}
