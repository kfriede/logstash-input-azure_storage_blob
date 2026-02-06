package com.azure.logstash.input.unit;

import co.elastic.logstash.api.Configuration;
import co.elastic.logstash.api.PluginConfigSpec;
import com.azure.logstash.input.AzureBlobStorageInput;
import com.azure.logstash.input.BlobPoller;
import com.azure.logstash.input.BlobProcessor;
import com.azure.logstash.input.HealthState;
import com.azure.logstash.input.PluginMetrics;
import com.azure.logstash.input.config.PluginConfig;
import com.azure.logstash.input.tracking.StateTracker;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import org.logstash.plugins.ConfigurationImpl;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for AzureBlobStorageInput — the main plugin class that wires
 * all components together and runs the poll loop.
 *
 * <p>Uses a package-private constructor to inject mock dependencies, avoiding
 * real Azure SDK calls. Tests focus on behavioral aspects: config schema,
 * start/stop lifecycle, metrics updates, and health state transitions.
 */
public class AzureBlobStorageInputTest {

    private BlobServiceClient serviceClient;
    private BlobContainerClient containerClient;
    private StateTracker stateTracker;
    private BlobProcessor processor;
    private BlobPoller poller;

    private Configuration minimalConfig() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        return new ConfigurationImpl(raw);
    }

    private Configuration registryConfig() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("tracking_strategy", "registry");
        return new ConfigurationImpl(raw);
    }

    @Before
    public void setUp() {
        serviceClient = mock(BlobServiceClient.class);
        containerClient = mock(BlobContainerClient.class);
        stateTracker = mock(StateTracker.class);
        processor = mock(BlobProcessor.class);
        poller = mock(BlobPoller.class);

        when(serviceClient.getBlobContainerClient(anyString())).thenReturn(containerClient);
    }

    // -----------------------------------------------------------------------
    // 1. configSchema returns ALL_CONFIGS
    // -----------------------------------------------------------------------
    @Test
    public void testConfigSchemaReturnsAllConfigs() {
        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", minimalConfig(), null,
                serviceClient, stateTracker, processor, poller);

        Collection<PluginConfigSpec<?>> schema = input.configSchema();
        assertEquals(PluginConfig.ALL_CONFIGS, schema);
    }

    // -----------------------------------------------------------------------
    // 2. Constructor with valid config does not throw
    // -----------------------------------------------------------------------
    @Test
    public void testConstructorWithValidConfigDoesNotThrow() {
        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", minimalConfig(), null,
                serviceClient, stateTracker, processor, poller);

        assertNotNull(input);
        assertEquals("test-id", input.getId());
    }

    // -----------------------------------------------------------------------
    // 3. start() runs poll loop and calls pollOnce
    // -----------------------------------------------------------------------
    @Test
    public void testStartRunsPollLoop() throws Exception {
        // pollOnce returns a summary, then on second call we stop the plugin
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(2, 0, 0, 10, 100);
        when(poller.pollOnce(any())).thenReturn(summary);

        // Use a short poll interval
        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        // Start in a separate thread
        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        // Wait a bit for at least one poll cycle
        Thread.sleep(500);

        // Stop the plugin
        input.stop();
        input.awaitStop();

        // Verify pollOnce was called at least once
        verify(poller, atLeastOnce()).pollOnce(any());

        pluginThread.join(2000);
        assertFalse("Plugin thread should have exited", pluginThread.isAlive());
    }

    // -----------------------------------------------------------------------
    // 4. stop() sets stopped flag and poll loop exits
    // -----------------------------------------------------------------------
    @Test
    public void testStopSetsStoppedFlag() throws Exception {
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(0, 0, 0, 0, 50);
        when(poller.pollOnce(any())).thenReturn(summary);

        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        Thread.sleep(200);
        input.stop();

        // awaitStop should return within a reasonable time
        boolean completed = pluginThread.isAlive();
        pluginThread.join(3000);
        assertFalse("Plugin thread should have exited after stop()", pluginThread.isAlive());
    }

    // -----------------------------------------------------------------------
    // 5. awaitStop() blocks until start() completes
    // -----------------------------------------------------------------------
    @Test
    public void testAwaitStopBlocksUntilDone() throws Exception {
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(0, 0, 0, 0, 10);
        when(poller.pollOnce(any())).thenReturn(summary);

        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        // awaitStop should not return before start is called (it blocks)
        CountDownLatch awaitDone = new CountDownLatch(1);
        Thread awaitThread = new Thread(() -> {
            try {
                input.awaitStop();
                awaitDone.countDown();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        awaitThread.start();

        // Verify awaitStop is still blocking
        assertFalse("awaitStop should block before start completes",
                awaitDone.await(300, TimeUnit.MILLISECONDS));

        // Now start the plugin and stop it
        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        Thread.sleep(200);
        input.stop();

        // awaitStop should now return
        assertTrue("awaitStop should return after stop",
                awaitDone.await(3, TimeUnit.SECONDS));

        pluginThread.join(2000);
    }

    // -----------------------------------------------------------------------
    // 6. Poll cycle updates metrics correctly
    // -----------------------------------------------------------------------
    @Test
    public void testPollCycleUpdatesMetrics() throws Exception {
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(3, 1, 2, 50, 200);
        // Return summary once, then stop
        AtomicInteger callCount = new AtomicInteger(0);
        when(poller.pollOnce(any())).thenAnswer(invocation -> {
            if (callCount.incrementAndGet() > 1) {
                // Give time for stop signal to propagate
                Thread.sleep(100);
            }
            return summary;
        });

        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        // Wait for at least one poll cycle
        Thread.sleep(500);
        input.stop();
        input.awaitStop();
        pluginThread.join(2000);

        // Verify metrics were updated
        PluginMetrics metrics = input.getMetrics();
        assertNotNull("Metrics should be available", metrics);
        assertTrue("Blobs processed should be > 0", metrics.getBlobsProcessed() > 0);
        assertTrue("Blobs failed should be > 0", metrics.getBlobsFailed() > 0);
        assertTrue("Blobs skipped should be > 0", metrics.getBlobsSkipped() > 0);
        assertTrue("Events produced should be > 0", metrics.getEventsProduced() > 0);
    }

    // -----------------------------------------------------------------------
    // 7. Poll cycle updates health state
    // -----------------------------------------------------------------------
    @Test
    public void testPollCycleUpdatesHealthState() throws Exception {
        // Summary with successes and no failures -> HEALTHY
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(5, 0, 0, 25, 100);
        when(poller.pollOnce(any())).thenReturn(summary);

        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        // Health state starts as STARTING
        assertEquals(HealthState.State.STARTING, input.getHealthState().getState());

        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        Thread.sleep(500);
        input.stop();
        input.awaitStop();
        pluginThread.join(2000);

        // After successful poll cycles, health should be HEALTHY
        assertEquals(HealthState.State.HEALTHY, input.getHealthState().getState());
    }

    // -----------------------------------------------------------------------
    // 8. State tracker close() called on stop
    // -----------------------------------------------------------------------
    @Test
    public void testStateTrackerClosedOnStop() throws Exception {
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(0, 0, 0, 0, 10);
        when(poller.pollOnce(any())).thenReturn(summary);

        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        Thread.sleep(200);
        input.stop();
        input.awaitStop();
        pluginThread.join(2000);

        verify(stateTracker).close();
    }

    // -----------------------------------------------------------------------
    // 9. Degraded health on mixed results
    // -----------------------------------------------------------------------
    @Test
    public void testDegradedHealthOnMixedResults() throws Exception {
        // Summary with both successes and failures -> DEGRADED
        BlobPoller.PollCycleSummary summary =
                new BlobPoller.PollCycleSummary(2, 1, 0, 10, 100);
        when(poller.pollOnce(any())).thenReturn(summary);

        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "testaccount");
        raw.put("container", "testcontainer");
        raw.put("poll_interval", 1L);
        Configuration config = new ConfigurationImpl(raw);

        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "test-id", config, null,
                serviceClient, stateTracker, processor, poller);

        @SuppressWarnings("unchecked")
        Consumer<Map<String, Object>> consumer = mock(Consumer.class);

        Thread pluginThread = new Thread(() -> input.start(consumer));
        pluginThread.start();

        Thread.sleep(500);
        input.stop();
        input.awaitStop();
        pluginThread.join(2000);

        assertEquals(HealthState.State.DEGRADED, input.getHealthState().getState());
    }

    // -----------------------------------------------------------------------
    // 10. getId returns the plugin id
    // -----------------------------------------------------------------------
    @Test
    public void testGetIdReturnsPluginId() {
        AzureBlobStorageInput input = new AzureBlobStorageInput(
                "my-plugin-id", minimalConfig(), null,
                serviceClient, stateTracker, processor, poller);

        assertEquals("my-plugin-id", input.getId());
    }
}
