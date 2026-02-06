package com.azure.logstash.input.unit;

import com.azure.logstash.input.PluginMetrics;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for PluginMetrics — verifies counter/gauge tracking for plugin telemetry.
 */
public class PluginMetricsTest {

    // -----------------------------------------------------------------------
    // 1. incrementBlobsProcessed — increment, verify value
    // -----------------------------------------------------------------------
    @Test
    public void testIncrementBlobsProcessed() {
        PluginMetrics metrics = new PluginMetrics();
        assertEquals(0, metrics.getBlobsProcessed());
        metrics.incrementBlobsProcessed();
        assertEquals(1, metrics.getBlobsProcessed());
        metrics.incrementBlobsProcessed();
        assertEquals(2, metrics.getBlobsProcessed());
    }

    // -----------------------------------------------------------------------
    // 2. incrementBlobsFailed — increment, verify value
    // -----------------------------------------------------------------------
    @Test
    public void testIncrementBlobsFailed() {
        PluginMetrics metrics = new PluginMetrics();
        assertEquals(0, metrics.getBlobsFailed());
        metrics.incrementBlobsFailed();
        assertEquals(1, metrics.getBlobsFailed());
        metrics.incrementBlobsFailed();
        assertEquals(2, metrics.getBlobsFailed());
    }

    // -----------------------------------------------------------------------
    // 3. incrementBlobsSkipped — increment, verify value
    // -----------------------------------------------------------------------
    @Test
    public void testIncrementBlobsSkipped() {
        PluginMetrics metrics = new PluginMetrics();
        assertEquals(0, metrics.getBlobsSkipped());
        metrics.incrementBlobsSkipped();
        assertEquals(1, metrics.getBlobsSkipped());
        metrics.incrementBlobsSkipped();
        assertEquals(2, metrics.getBlobsSkipped());
    }

    // -----------------------------------------------------------------------
    // 4. addEventsProduced — add 100, add 50, verify 150
    // -----------------------------------------------------------------------
    @Test
    public void testAddEventsProduced() {
        PluginMetrics metrics = new PluginMetrics();
        assertEquals(0, metrics.getEventsProduced());
        metrics.addEventsProduced(100);
        assertEquals(100, metrics.getEventsProduced());
        metrics.addEventsProduced(50);
        assertEquals(150, metrics.getEventsProduced());
    }

    // -----------------------------------------------------------------------
    // 5. setPollCycleDuration — set 12.5, verify
    // -----------------------------------------------------------------------
    @Test
    public void testSetPollCycleDuration() {
        PluginMetrics metrics = new PluginMetrics();
        assertEquals(0.0, metrics.getPollCycleDuration(), 0.001);
        metrics.setPollCycleDuration(12.5);
        assertEquals(12.5, metrics.getPollCycleDuration(), 0.001);
    }

    // -----------------------------------------------------------------------
    // 6. setActiveLeaseCount — set 3, verify
    // -----------------------------------------------------------------------
    @Test
    public void testSetActiveLeaseCount() {
        PluginMetrics metrics = new PluginMetrics();
        assertEquals(0, metrics.getActiveLeaseCount());
        metrics.setActiveLeaseCount(3);
        assertEquals(3, metrics.getActiveLeaseCount());
    }

}
