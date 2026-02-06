package com.azure.logstash.input.unit;

import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.IterableStream;
import com.azure.core.util.polling.PollResponse;
import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.SyncPoller;
import com.azure.logstash.input.LeaseManager;
import com.azure.logstash.input.tracking.ContainerStateTracker;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobStorageException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for ContainerStateTracker — container-based state tracking
 * that moves blobs between incoming, archive, and error containers.
 * Uses LeaseManager for distributed coordination.
 */
public class ContainerStateTrackerTest {

    private BlobServiceClient serviceClient;
    private BlobContainerClient incomingContainerClient;
    private BlobContainerClient archiveContainerClient;
    private BlobContainerClient errorContainerClient;
    private BlobClient incomingBlobClient;
    private BlobClient archiveBlobClient;
    private BlobClient errorBlobClient;
    private LeaseManager leaseManager;
    private Function<BlobClient, LeaseManager> leaseManagerFactory;
    private ContainerStateTracker tracker;

    @Before
    public void setUp() {
        serviceClient = mock(BlobServiceClient.class);
        incomingContainerClient = mock(BlobContainerClient.class);
        archiveContainerClient = mock(BlobContainerClient.class);
        errorContainerClient = mock(BlobContainerClient.class);
        incomingBlobClient = mock(BlobClient.class);
        archiveBlobClient = mock(BlobClient.class);
        errorBlobClient = mock(BlobClient.class);
        leaseManager = mock(LeaseManager.class);
        leaseManagerFactory = bc -> leaseManager;

        when(serviceClient.getBlobContainerClient("incoming")).thenReturn(incomingContainerClient);
        when(serviceClient.getBlobContainerClient("archive")).thenReturn(archiveContainerClient);
        when(serviceClient.getBlobContainerClient("errors")).thenReturn(errorContainerClient);

        when(incomingContainerClient.getBlobClient(anyString())).thenReturn(incomingBlobClient);
        when(archiveContainerClient.getBlobClient(anyString())).thenReturn(archiveBlobClient);
        when(errorContainerClient.getBlobClient(anyString())).thenReturn(errorBlobClient);

        tracker = new ContainerStateTracker(serviceClient, "incoming", "archive",
                "errors", 30, 10, "test-processor", leaseManagerFactory);
    }

    @After
    public void tearDown() {
        if (tracker != null) {
            tracker.close();
        }
    }

    // -----------------------------------------------------------------------
    // Helper: create a mock PagedIterable that yields specific BlobItems
    // -----------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private com.azure.storage.blob.models.BlobItem blobItem(String name) {
        return new BlobItem().setName(name);
    }

    @SuppressWarnings("unchecked")
    private PagedIterable<BlobItem> mockPagedIterable(BlobItem... items) {
        List<BlobItem> itemList = Arrays.asList(items);
        PagedResponse<BlobItem> pagedResponse = mock(PagedResponse.class);
        when(pagedResponse.getValue()).thenReturn(itemList);
        when(pagedResponse.getElements()).thenReturn(new IterableStream<>(itemList));
        when(pagedResponse.getContinuationToken()).thenReturn(null);
        when(pagedResponse.getStatusCode()).thenReturn(200);
        return new PagedIterable<>(() -> pagedResponse);
    }

    @SuppressWarnings("unchecked")
    private SyncPoller<BlobCopyInfo, Void> mockSyncPoller() {
        SyncPoller<BlobCopyInfo, Void> poller = mock(SyncPoller.class);
        PollResponse<BlobCopyInfo> pollResponse = mock(PollResponse.class);
        when(pollResponse.getStatus()).thenReturn(LongRunningOperationStatus.SUCCESSFULLY_COMPLETED);
        when(poller.waitForCompletion()).thenReturn(pollResponse);
        return poller;
    }

    // -----------------------------------------------------------------------
    // 1. filterCandidates excludes blobs already in archive
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesExcludesAlreadyInArchive() {
        // "a.log" exists in both incoming and archive
        BlobItem archiveBlob = blobItem("a.log");
        PagedIterable<BlobItem> archiveList = mockPagedIterable(archiveBlob);
        when(archiveContainerClient.listBlobs()).thenReturn(archiveList);

        BlobItem incomingBlob = blobItem("a.log");
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(incomingBlob));

        assertTrue("Blob already in archive should be excluded", candidates.isEmpty());
    }

    // -----------------------------------------------------------------------
    // 2. filterCandidates includes new blobs not in archive
    // -----------------------------------------------------------------------
    @Test
    public void testFilterCandidatesIncludesNew() {
        // archive is empty
        PagedIterable<BlobItem> emptyArchive = mockPagedIterable();
        when(archiveContainerClient.listBlobs()).thenReturn(emptyArchive);

        BlobItem incomingBlob = blobItem("b.log");
        List<BlobItem> candidates = tracker.filterCandidates(
                Collections.singletonList(incomingBlob));

        assertEquals("New blob should be included", 1, candidates.size());
        assertEquals("b.log", candidates.get(0).getName());
    }

    // -----------------------------------------------------------------------
    // 3. claim acquires lease and returns true
    // -----------------------------------------------------------------------
    @Test
    public void testClaimAcquiresLease() {
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");

        boolean result = tracker.claim("test-blob");

        assertTrue("claim should return true on success", result);
        verify(leaseManager).acquireLease();
        verify(leaseManager).startRenewal();
    }

    // -----------------------------------------------------------------------
    // 4. claim returns false on 409 (already leased)
    // -----------------------------------------------------------------------
    @Test
    public void testClaimReturnsFalseOn409() {
        when(leaseManager.acquireLease()).thenReturn(null);

        boolean result = tracker.claim("test-blob");

        assertFalse("claim should return false when lease cannot be acquired", result);
        verify(leaseManager, never()).startRenewal();
    }

    // -----------------------------------------------------------------------
    // 5. markCompleted copies to archive then deletes from incoming
    // -----------------------------------------------------------------------
    @Test
    public void testMarkCompletedCopiesThenDeletes() {
        // First claim the blob
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        tracker.claim("test-blob");

        // Set up copy mocks
        String sourceUrl = "https://account.blob.core.windows.net/incoming/test-blob";
        when(incomingBlobClient.getBlobUrl()).thenReturn(sourceUrl);

        SyncPoller<BlobCopyInfo, Void> poller = mockSyncPoller();
        when(archiveBlobClient.beginCopy(eq(sourceUrl), isNull())).thenReturn(poller);

        tracker.markCompleted("test-blob");

        // Verify order: beginCopy to archive, then release lease, then delete from incoming
        InOrder inOrder = inOrder(archiveBlobClient, poller, leaseManager, incomingBlobClient);
        inOrder.verify(archiveBlobClient).beginCopy(eq(sourceUrl), isNull());
        inOrder.verify(poller).waitForCompletion();
        inOrder.verify(leaseManager).stopRenewal();
        inOrder.verify(leaseManager).releaseLease();
        inOrder.verify(incomingBlobClient).delete();
    }

    // -----------------------------------------------------------------------
    // 6. markCompleted preserves blob path (including subdirectories)
    // -----------------------------------------------------------------------
    @Test
    public void testMarkCompletedPreservesPath() {
        String nestedBlobName = "logs/2026/server.log";

        // Set up specific blob clients for this path
        BlobClient nestedIncomingBlobClient = mock(BlobClient.class);
        BlobClient nestedArchiveBlobClient = mock(BlobClient.class);
        when(incomingContainerClient.getBlobClient(nestedBlobName)).thenReturn(nestedIncomingBlobClient);
        when(archiveContainerClient.getBlobClient(nestedBlobName)).thenReturn(nestedArchiveBlobClient);

        // Claim with the nested path
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        tracker.claim(nestedBlobName);

        String sourceUrl = "https://account.blob.core.windows.net/incoming/logs/2026/server.log";
        when(nestedIncomingBlobClient.getBlobUrl()).thenReturn(sourceUrl);

        SyncPoller<BlobCopyInfo, Void> poller = mockSyncPoller();
        when(nestedArchiveBlobClient.beginCopy(eq(sourceUrl), isNull())).thenReturn(poller);

        tracker.markCompleted(nestedBlobName);

        // Verify that archive container is called with the same nested path
        verify(archiveContainerClient).getBlobClient(nestedBlobName);
        verify(nestedArchiveBlobClient).beginCopy(eq(sourceUrl), isNull());
    }

    // -----------------------------------------------------------------------
    // 7. markFailed copies to error container then deletes from incoming
    // -----------------------------------------------------------------------
    @Test
    public void testMarkFailedCopiesToErrorContainer() {
        // First claim the blob
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        tracker.claim("test-blob");

        // Set up copy mocks for error container
        String sourceUrl = "https://account.blob.core.windows.net/incoming/test-blob";
        when(incomingBlobClient.getBlobUrl()).thenReturn(sourceUrl);

        SyncPoller<BlobCopyInfo, Void> poller = mockSyncPoller();
        when(errorBlobClient.beginCopy(eq(sourceUrl), isNull())).thenReturn(poller);

        tracker.markFailed("test-blob", "something went wrong");

        // Verify copy to error container and delete from incoming
        verify(errorBlobClient).beginCopy(eq(sourceUrl), isNull());
        verify(poller).waitForCompletion();
        verify(incomingBlobClient).delete();
        verify(leaseManager).stopRenewal();
        verify(leaseManager).releaseLease();
    }

    // -----------------------------------------------------------------------
    // 8. copy failure leaves blob in incoming (delete NOT called)
    // -----------------------------------------------------------------------
    @Test
    public void testCopyFailureLeavesBlobInIncoming() {
        // First claim the blob
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        tracker.claim("test-blob");

        // Set up copy to throw
        String sourceUrl = "https://account.blob.core.windows.net/incoming/test-blob";
        when(incomingBlobClient.getBlobUrl()).thenReturn(sourceUrl);

        BlobStorageException copyException = mock(BlobStorageException.class);
        when(copyException.getStatusCode()).thenReturn(500);
        when(copyException.getMessage()).thenReturn("Copy failed");
        when(archiveBlobClient.beginCopy(eq(sourceUrl), isNull())).thenThrow(copyException);

        try {
            tracker.markCompleted("test-blob");
            fail("Should have thrown BlobStorageException");
        } catch (BlobStorageException e) {
            // Expected
        }

        // Verify delete was NOT called — blob stays in incoming
        verify(incomingBlobClient, never()).delete();
    }

    // -----------------------------------------------------------------------
    // 9. release stops renewal and releases lease
    // -----------------------------------------------------------------------
    @Test
    public void testReleaseLease() {
        // First claim so we have a lease manager in the map
        when(leaseManager.acquireLease()).thenReturn("lease-id-123");
        tracker.claim("test-blob");

        tracker.release("test-blob");

        verify(leaseManager).stopRenewal();
        verify(leaseManager).releaseLease();
    }
}
