package com.azure.logstash.input;

import com.azure.logstash.input.tracking.RegistryStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.Assert.*;

/**
 * Integration tests for plugin metrics and health state tracking.
 * Verifies that metrics are correctly updated after processing real blobs.
 * Uses registry strategy to avoid Azurite tag/lease limitations.
 */
@Category(IntegrationTest.class)
public class MetricsIT extends AzuriteTestBase {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String containerName;
    private BlobContainerClient containerClient;
    private List<Map<String, Object>> events;
    private String dbPath;

    @Before
    public void setUp() throws IOException {
        containerName = uniqueContainerName("metrics");
        containerClient = createContainer(containerName);
        events = new CopyOnWriteArrayList<>();
        dbPath = new File(tempFolder.getRoot(), "metrics.db").getAbsolutePath();
    }

    private BlobPoller createPoller() {
        RegistryStateTracker stateTracker = new RegistryStateTracker(dbPath, "test-host");
        BlobProcessor processor = new BlobProcessor(AZURITE_ACCOUNT, containerName, true);
        return new BlobPoller(containerClient, stateTracker, processor,
                events::add, "", 50);
    }

    // ── Test 1: Blobs processed counter increments ──────────────────────────

    @Test
    public void testBlobsProcessedCounterIncrements() {
        for (int i = 1; i <= 3; i++) {
            uploadBlob(containerName, "blob-" + i + ".log", "data-" + i + "\n");
        }

        PluginMetrics metrics = new PluginMetrics();
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        // Simulate how AzureBlobStorageInput updates metrics
        for (int i = 0; i < summary.getBlobsProcessed(); i++) {
            metrics.incrementBlobsProcessed();
        }

        assertEquals("Blobs processed counter should be 3",
                3, metrics.getBlobsProcessed());
    }

    // ── Test 2: Blobs failed counter increments ─────────────────────────────

    @Test
    public void testBlobsFailedCounterIncrements() {
        uploadBlob(containerName, "ok.log", "data\n");

        PluginMetrics metrics = new PluginMetrics();
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        for (int i = 0; i < summary.getBlobsFailed(); i++) {
            metrics.incrementBlobsFailed();
        }

        // Process succeeded, so failed should be 0
        assertEquals(0, metrics.getBlobsFailed());

        // Manually simulate a failure to verify counter works
        metrics.incrementBlobsFailed();
        assertEquals("Counter should increment", 1, metrics.getBlobsFailed());
    }

    // ── Test 3: Events produced counter ─────────────────────────────────────

    @Test
    public void testEventsProducedCounter() {
        int blobCount = 3;
        int linesPerBlob = 10;

        for (int i = 1; i <= blobCount; i++) {
            uploadBlob(containerName, "blob-" + i + ".log",
                    makeNumberedLines(linesPerBlob));
        }

        PluginMetrics metrics = new PluginMetrics();
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        metrics.addEventsProduced(summary.getEventsProduced());

        long expected = (long) blobCount * linesPerBlob;
        assertEquals("Events produced should be " + expected,
                expected, metrics.getEventsProduced());
        assertEquals(expected, events.size());
    }

    // ── Test 4: Poll cycle duration recorded ────────────────────────────────

    @Test
    public void testPollCycleDurationRecorded() {
        uploadBlob(containerName, "timing.log", "data\n");

        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        PluginMetrics metrics = new PluginMetrics();
        metrics.setPollCycleDuration(summary.getDurationMs() / 1000.0);

        assertTrue("Poll cycle duration should be > 0",
                metrics.getPollCycleDuration() > 0);
        assertTrue("Duration should be recorded in seconds",
                summary.getDurationMs() > 0);
    }

    // ── Test 5: Health state transitions ────────────────────────────────────

    @Test
    public void testHealthStateTransitions() {
        HealthState healthState = new HealthState();
        assertEquals("Initial state should be STARTING",
                HealthState.State.STARTING, healthState.getState());

        // Process a blob successfully
        uploadBlob(containerName, "health.log", "data\n");
        BlobPoller poller = createPoller();
        BlobPoller.PollCycleSummary summary = poller.pollOnce(() -> false);

        healthState.recordPollResult(summary.getBlobsProcessed(), summary.getBlobsFailed());

        assertEquals("After successful processing, state should be HEALTHY",
                HealthState.State.HEALTHY, healthState.getState());
    }
}
