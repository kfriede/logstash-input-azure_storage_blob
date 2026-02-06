package com.azure.logstash.input.unit;

import com.azure.logstash.input.BlobPoller;
import com.azure.logstash.input.BlobProcessor;
import com.azure.logstash.input.tracking.StateTracker;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.http.rest.PagedResponse;
import com.azure.core.util.IterableStream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for BlobPoller — orchestrates poll cycles by listing blobs,
 * filtering candidates, claiming, processing, and marking results.
 *
 * <p>All Azure SDK types (BlobContainerClient, BlobClient, PagedIterable)
 * and plugin components (StateTracker, BlobProcessor) are mocked.
 */
public class BlobPollerTest {

    private BlobContainerClient containerClient;
    private StateTracker stateTracker;
    private BlobProcessor processor;
    private Consumer<Map<String, Object>> eventConsumer;
    private Supplier<Boolean> notStopped;

    @Before
    public void setUp() {
        containerClient = mock(BlobContainerClient.class);
        stateTracker = mock(StateTracker.class);
        processor = mock(BlobProcessor.class);
        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);
        eventConsumer = consumer;
        notStopped = () -> false;
    }

    // -----------------------------------------------------------------------
    // Helper: create BlobItems with given names
    // -----------------------------------------------------------------------
    private List<BlobItem> createBlobItems(String... names) {
        List<BlobItem> items = new ArrayList<>();
        for (String name : names) {
            items.add(new BlobItem().setName(name));
        }
        return items;
    }

    // -----------------------------------------------------------------------
    // Helper: mock containerClient.listBlobs to return given BlobItems
    // -----------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private void mockListBlobs(List<BlobItem> items) {
        PagedIterable<BlobItem> pagedIterable = mock(PagedIterable.class);

        PagedResponse<BlobItem> page = mock(PagedResponse.class);
        when(page.getValue()).thenReturn(items);

        when(pagedIterable.iterableByPage())
                .thenReturn(new IterableStream<>(Collections.singletonList(page)));

        when(containerClient.listBlobs(any(ListBlobsOptions.class), any()))
                .thenReturn(pagedIterable);
    }

    // -----------------------------------------------------------------------
    // Helper: create a BlobPoller with given prefix and batch size
    // -----------------------------------------------------------------------
    private BlobPoller createPoller(String prefix, int batchSize) {
        return new BlobPoller(containerClient, stateTracker, processor,
                eventConsumer, prefix, batchSize);
    }

    // -----------------------------------------------------------------------
    // Helper: setup default process mock (returns 5 events, completed)
    // -----------------------------------------------------------------------
    private void mockProcessSuccess(long events) throws IOException {
        when(processor.process(any(BlobClient.class), any(Consumer.class), any(Supplier.class)))
                .thenReturn(new BlobProcessor.ProcessResult(events, true));
    }

    // -----------------------------------------------------------------------
    // 1. testPollCycleListsBlobsAndProcesses — 3 blobs, all candidates, all claimed → 3 processed
    // -----------------------------------------------------------------------
    @Test
    public void testPollCycleListsBlobsAndProcesses() throws IOException {
        List<BlobItem> blobs = createBlobItems("a.log", "b.log", "c.log");
        mockListBlobs(blobs);

        // All blobs pass filter
        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        // All blobs claimed successfully
        when(stateTracker.claim(anyString())).thenReturn(true);
        // Each blob processes 5 events
        mockProcessSuccess(5);
        // Mock getBlobClient for each blob
        when(containerClient.getBlobClient(anyString())).thenReturn(mock(BlobClient.class));

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(3, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());
        assertEquals(0, summary.getBlobsSkipped());
        assertEquals(15, summary.getEventsProduced());
        verify(stateTracker, times(3)).claim(anyString());
        verify(stateTracker, times(3)).markCompleted(anyString());
        verify(stateTracker, times(3)).release(anyString());
    }

    // -----------------------------------------------------------------------
    // 2. testBatchSizeLimits — 20 blobs, batchSize=5 → only 5 processed
    // -----------------------------------------------------------------------
    @Test
    public void testBatchSizeLimits() throws IOException {
        List<BlobItem> blobs = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            blobs.add(new BlobItem().setName(String.format("blob-%02d.log", i)));
        }
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim(anyString())).thenReturn(true);
        mockProcessSuccess(3);
        when(containerClient.getBlobClient(anyString())).thenReturn(mock(BlobClient.class));

        BlobPoller poller = createPoller("", 5);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(5, summary.getBlobsProcessed());
        assertEquals(15, summary.getEventsProduced());
        // Only 5 blobs should have been claimed
        verify(stateTracker, times(5)).claim(anyString());
    }

    // -----------------------------------------------------------------------
    // 3. testPrefixFiltering — verify listBlobs called with prefix option
    // -----------------------------------------------------------------------
    @Test
    public void testPrefixFiltering() throws IOException {
        mockListBlobs(Collections.emptyList());
        when(stateTracker.filterCandidates(any())).thenReturn(Collections.emptyList());

        BlobPoller poller = createPoller("logs/2024/", 10);
        poller.pollOnce(notStopped);

        ArgumentCaptor<ListBlobsOptions> optionsCaptor =
                ArgumentCaptor.forClass(ListBlobsOptions.class);
        verify(containerClient).listBlobs(optionsCaptor.capture(), any());
        assertEquals("logs/2024/", optionsCaptor.getValue().getPrefix());
    }

    // -----------------------------------------------------------------------
    // 4. testSkipUnclaimableBlobs — claim returns false for blob 2 → skipped, others processed
    // -----------------------------------------------------------------------
    @Test
    public void testSkipUnclaimableBlobs() throws IOException {
        List<BlobItem> blobs = createBlobItems("a.log", "b.log", "c.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        // Blob b.log cannot be claimed
        when(stateTracker.claim("a.log")).thenReturn(true);
        when(stateTracker.claim("b.log")).thenReturn(false);
        when(stateTracker.claim("c.log")).thenReturn(true);
        mockProcessSuccess(5);
        when(containerClient.getBlobClient(anyString())).thenReturn(mock(BlobClient.class));

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(2, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());
        assertEquals(1, summary.getBlobsSkipped());
        assertEquals(10, summary.getEventsProduced());
        // markCompleted only for claimed blobs
        verify(stateTracker, times(2)).markCompleted(anyString());
        // release only for claimed blobs
        verify(stateTracker, times(2)).release(anyString());
    }

    // -----------------------------------------------------------------------
    // 5. testFailedBlobMarkedFailed — processor.process throws RuntimeException → markFailed called
    // -----------------------------------------------------------------------
    @Test
    public void testFailedBlobMarkedFailed() throws IOException {
        List<BlobItem> blobs = createBlobItems("fail.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim("fail.log")).thenReturn(true);
        when(containerClient.getBlobClient("fail.log")).thenReturn(mock(BlobClient.class));

        // Processor throws RuntimeException
        when(processor.process(any(BlobClient.class), any(Consumer.class), any(Supplier.class)))
                .thenThrow(new RuntimeException("disk full"));

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(0, summary.getBlobsProcessed());
        assertEquals(1, summary.getBlobsFailed());
        verify(stateTracker).markFailed("fail.log", "disk full");
        // release should still be called (finally block)
        verify(stateTracker).release("fail.log");
    }

    // -----------------------------------------------------------------------
    // 6. testCompletedBlobMarkedCompleted — successful processing → markCompleted called
    // -----------------------------------------------------------------------
    @Test
    public void testCompletedBlobMarkedCompleted() throws IOException {
        List<BlobItem> blobs = createBlobItems("success.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim("success.log")).thenReturn(true);
        mockProcessSuccess(10);
        when(containerClient.getBlobClient("success.log")).thenReturn(mock(BlobClient.class));

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(1, summary.getBlobsProcessed());
        verify(stateTracker).markCompleted("success.log");
        verify(stateTracker).release("success.log");
    }

    // -----------------------------------------------------------------------
    // 7. testStopBetweenBlobs — isStopped returns true after blob 1 of 3 → only 1 processed
    // -----------------------------------------------------------------------
    @Test
    public void testStopBetweenBlobs() throws IOException {
        List<BlobItem> blobs = createBlobItems("a.log", "b.log", "c.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim(anyString())).thenReturn(true);
        mockProcessSuccess(5);
        when(containerClient.getBlobClient(anyString())).thenReturn(mock(BlobClient.class));

        // isStopped: false for page-level check and first blob, then true
        // The paginated poller checks isStopped at the page level and before each blob
        AtomicInteger checkCount = new AtomicInteger(0);
        Supplier<Boolean> isStopped = () -> checkCount.incrementAndGet() > 2;

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(isStopped);

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(5, summary.getEventsProduced());
        verify(stateTracker, times(1)).claim(anyString());
    }

    // -----------------------------------------------------------------------
    // 8. testStoppedMidBlobMarkedFailed — blob stopped mid-processing → markFailed with "interrupted"
    // -----------------------------------------------------------------------
    @Test
    public void testStoppedMidBlobMarkedFailed() throws IOException {
        List<BlobItem> blobs = createBlobItems("mid-stop.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim("mid-stop.log")).thenReturn(true);
        when(containerClient.getBlobClient("mid-stop.log")).thenReturn(mock(BlobClient.class));

        // Processor returns incomplete (stopped mid-blob): isCompleted = false, 3 events produced
        when(processor.process(any(BlobClient.class), any(Consumer.class), any(Supplier.class)))
                .thenReturn(new BlobProcessor.ProcessResult(3, false));

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        // Should be counted as failed, not processed
        assertEquals(0, summary.getBlobsProcessed());
        assertEquals(1, summary.getBlobsFailed());
        assertEquals(3, summary.getEventsProduced());
        // markFailed should be called with "interrupted"
        verify(stateTracker).markFailed("mid-stop.log", "interrupted");
        verify(stateTracker, never()).markCompleted(anyString());
        // release should still be called (finally block)
        verify(stateTracker).release("mid-stop.log");
    }

    // -----------------------------------------------------------------------
    // 9. testEmptyContainerNoop — no blobs → summary all zeros, no errors
    // -----------------------------------------------------------------------
    @Test
    public void testEmptyContainerNoop() throws IOException {
        mockListBlobs(Collections.emptyList());
        when(stateTracker.filterCandidates(any())).thenReturn(Collections.emptyList());

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(0, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());
        assertEquals(0, summary.getBlobsSkipped());
        assertEquals(0, summary.getEventsProduced());
        verify(stateTracker, never()).claim(anyString());
        verify(processor, never()).process(any(), any(), any());
    }

    // -----------------------------------------------------------------------
    // 9. testProcessedInApiReturnOrder — blobs processed in the order the API returns them
    // -----------------------------------------------------------------------
    @Test
    public void testProcessedInApiReturnOrder() throws IOException {
        // API returns blobs in this order (Azure returns lexicographically)
        List<BlobItem> blobs = createBlobItems("a.log", "b.log", "c.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(stateTracker.claim(anyString())).thenReturn(true);
        mockProcessSuccess(1);
        when(containerClient.getBlobClient(anyString())).thenReturn(mock(BlobClient.class));

        BlobPoller poller = createPoller("", 10);
        poller.pollOnce(notStopped);

        // Verify processing follows API return order
        InOrder inOrder = inOrder(stateTracker);
        inOrder.verify(stateTracker).claim("a.log");
        inOrder.verify(stateTracker).claim("b.log");
        inOrder.verify(stateTracker).claim("c.log");
    }

    // -----------------------------------------------------------------------
    // 10. testLeaseRenewalFailureMarksFailedInsteadOfCompleted — wasLeaseRenewalFailed returns true
    // -----------------------------------------------------------------------
    @Test
    public void testLeaseRenewalFailureMarksFailedInsteadOfCompleted() throws IOException {
        List<BlobItem> blobs = createBlobItems("compromised.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim("compromised.log")).thenReturn(true);
        mockProcessSuccess(10);
        when(containerClient.getBlobClient("compromised.log")).thenReturn(mock(BlobClient.class));

        // Simulate lease renewal failure during processing
        when(stateTracker.wasLeaseRenewalFailed("compromised.log")).thenReturn(true);

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        // Should be counted as failed, not processed
        assertEquals(0, summary.getBlobsProcessed());
        assertEquals(1, summary.getBlobsFailed());
        assertEquals(10, summary.getEventsProduced());
        // markFailed should be called, NOT markCompleted
        verify(stateTracker).markFailed("compromised.log", "lease renewal failed during processing");
        verify(stateTracker, never()).markCompleted(anyString());
        // release should still be called (finally block)
        verify(stateTracker).release("compromised.log");
    }

    // -----------------------------------------------------------------------
    // 11. testNoLeaseRenewalFailureMarksCompleted — wasLeaseRenewalFailed returns false
    // -----------------------------------------------------------------------
    @Test
    public void testNoLeaseRenewalFailureMarksCompleted() throws IOException {
        List<BlobItem> blobs = createBlobItems("healthy.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim("healthy.log")).thenReturn(true);
        mockProcessSuccess(7);
        when(containerClient.getBlobClient("healthy.log")).thenReturn(mock(BlobClient.class));

        // No lease renewal failure
        when(stateTracker.wasLeaseRenewalFailed("healthy.log")).thenReturn(false);

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(0, summary.getBlobsFailed());
        assertEquals(7, summary.getEventsProduced());
        verify(stateTracker).markCompleted("healthy.log");
        verify(stateTracker, never()).markFailed(anyString(), anyString());
        verify(stateTracker).release("healthy.log");
    }

    // -----------------------------------------------------------------------
    // 12. testPollCycleReturnsSummary — verify summary fields: processed, failed, skipped, events, duration > 0
    // -----------------------------------------------------------------------
    @Test
    public void testPollCycleReturnsSummary() throws IOException {
        // Setup: 3 blobs - 1 succeeds (5 events), 1 fails, 1 skipped
        List<BlobItem> blobs = createBlobItems("a.log", "b.log", "c.log");
        mockListBlobs(blobs);

        when(stateTracker.filterCandidates(any())).thenReturn(blobs);
        when(stateTracker.claim("a.log")).thenReturn(true);
        when(stateTracker.claim("b.log")).thenReturn(false); // skipped
        when(stateTracker.claim("c.log")).thenReturn(true);

        when(containerClient.getBlobClient(anyString())).thenReturn(mock(BlobClient.class));

        // a.log processes successfully with 5 events
        // c.log throws exception
        when(processor.process(any(BlobClient.class), any(Consumer.class), any(Supplier.class)))
                .thenReturn(new BlobProcessor.ProcessResult(5, true))
                .thenThrow(new RuntimeException("error"));

        BlobPoller poller = createPoller("", 10);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(notStopped);

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(1, summary.getBlobsFailed());
        assertEquals(1, summary.getBlobsSkipped());
        assertEquals(5, summary.getEventsProduced());
        assertTrue("Duration should be >= 0", summary.getDurationMs() >= 0);
    }
}
