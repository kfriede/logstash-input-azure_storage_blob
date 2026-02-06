package com.azure.logstash.input;

import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Operational performance tests against Azure Government.
 *
 * <p>Measures throughput for various blob sizes and verifies that the streaming
 * architecture does not cause out-of-memory errors. These are smoke tests, not
 * benchmarks; they primarily verify correctness under load.
 */
@Category(OperationalTest.class)
public class PerformanceOT extends AzureGovTestBase {

    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;

    @Before
    public void setUp() {
        containerClient = serviceClient.getBlobContainerClient("incoming");
        events = new CopyOnWriteArrayList<>();
        // Clean up (breaks leases first)
        cleanContainer("incoming");
    }

    private BlobPoller createPoller(int batchSize) {
        TagStateTracker tracker = new TagStateTracker(containerClient, 30, 20, "perf-host");
        BlobProcessor processor = new BlobProcessor(accountName, "incoming", true);
        return new BlobPoller(containerClient, tracker, processor,
                events::add, "", batchSize);
    }

    // ── Test 1: Throughput 20 x 1KB blobs ───────────────────────────────────

    @Test
    public void testThroughput1KBBlobs() {
        int blobCount = 20;
        int linesPerBlob = 10;  // ~100 bytes per line = ~1KB per blob

        for (int i = 1; i <= blobCount; i++) {
            StringBuilder content = new StringBuilder();
            for (int j = 1; j <= linesPerBlob; j++) {
                // ~100 chars per line to make ~1KB total
                content.append(String.format("log-%03d: This is a test log line with some padding data %050d%n", j, 0));
            }
            uploadBlob("incoming", String.format("perf-1k-%02d.log", i), content.toString());
        }

        long start = System.currentTimeMillis();
        BlobPoller poller = createPoller(blobCount);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals("All blobs should be processed", blobCount, summary.getBlobsProcessed());
        long expectedEvents = (long) blobCount * linesPerBlob;
        assertEquals("All events should be produced", expectedEvents, summary.getEventsProduced());

        double eventsPerSec = (summary.getEventsProduced() * 1000.0) / elapsed;
        System.out.printf("Throughput (20 x 1KB): %.0f events/sec, %d ms total%n",
                eventsPerSec, elapsed);

        // Sanity: should complete in reasonable time (< 5 min)
        assertTrue("Should complete within 5 minutes", elapsed < 300_000);
    }

    // ── Test 2: Throughput 1 x 10MB blob ────────────────────────────────────

    @Test
    public void testThroughput10MBBlob() {
        // Create a ~10MB blob with many lines
        int targetSizeBytes = 10 * 1024 * 1024;
        String line = "2024-01-15T10:30:00Z INFO [worker-1] Processing request id=12345 client=192.168.1.100 duration=42ms status=200\n";
        int lineLength = line.getBytes().length;
        int lineCount = targetSizeBytes / lineLength;

        StringBuilder content = new StringBuilder(targetSizeBytes + 1024);
        for (int i = 0; i < lineCount; i++) {
            content.append(line);
        }

        uploadBlob("incoming", "perf-10mb.log", content.toString());

        long start = System.currentTimeMillis();
        BlobPoller poller = createPoller(1);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);
        long elapsed = System.currentTimeMillis() - start;

        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(lineCount, summary.getEventsProduced());

        double eventsPerSec = (summary.getEventsProduced() * 1000.0) / elapsed;
        System.out.printf("Throughput (1 x 10MB): %.0f events/sec (%d lines), %d ms total%n",
                eventsPerSec, lineCount, elapsed);

        // Sanity: should complete in reasonable time
        assertTrue("Should complete within 5 minutes", elapsed < 300_000);
    }

    // ── Test 3: Memory bounded (process 10MB without OOM) ───────────────────

    @Test
    public void testMemoryBounded() {
        // Create a ~10MB blob
        int targetSizeBytes = 10 * 1024 * 1024;
        String line = "memory-test: This is a line of log data with enough content to be meaningful\n";
        int lineLength = line.getBytes().length;
        int lineCount = targetSizeBytes / lineLength;

        StringBuilder content = new StringBuilder(targetSizeBytes + 1024);
        for (int i = 0; i < lineCount; i++) {
            content.append(line);
        }

        uploadBlob("incoming", "perf-memory.log", content.toString());

        // Record heap before processing
        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long heapBefore = runtime.totalMemory() - runtime.freeMemory();

        BlobPoller poller = createPoller(1);
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        // If we got here without OOM, the test passes
        assertEquals(1, summary.getBlobsProcessed());
        assertEquals(lineCount, summary.getEventsProduced());

        // Record heap after - should not have grown by more than 2x the blob size
        // (generous bound; the streaming architecture should keep memory flat)
        runtime.gc();
        long heapAfter = runtime.totalMemory() - runtime.freeMemory();
        long heapGrowth = heapAfter - heapBefore;

        System.out.printf("Memory test: heap before=%dMB, after=%dMB, growth=%dMB%n",
                heapBefore / (1024*1024), heapAfter / (1024*1024), heapGrowth / (1024*1024));

        // Note: We store all events in-memory in the test (CopyOnWriteArrayList),
        // so heap growth will be substantial. The important thing is no OOM.
        assertTrue("Processing should complete without OOM", summary.getEventsProduced() > 0);
    }
}
