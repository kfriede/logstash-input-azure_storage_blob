package com.azure.logstash.input;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple counter/gauge wrapper for plugin telemetry metrics.
 *
 * <p>Uses {@link AtomicLong} and {@link AtomicInteger} counters internally
 * (standalone mode) so the class is thread-safe without external synchronization.
 * In a future integration phase this can be adapted to delegate to Logstash's
 * built-in metric API when a real Logstash context is available.</p>
 */
public class PluginMetrics {

    private final AtomicLong blobsProcessed = new AtomicLong();
    private final AtomicLong blobsFailed = new AtomicLong();
    private final AtomicLong blobsSkipped = new AtomicLong();
    private final AtomicLong eventsProduced = new AtomicLong();
    private volatile double pollCycleDuration;
    private final AtomicInteger activeLeaseCount = new AtomicInteger();
    private final AtomicLong dailyQuotaBytesUsed = new AtomicLong();
    private volatile long dailyQuotaBytesLimit;
    private volatile boolean dailyQuotaReached;

    // -- Mutators -----------------------------------------------------------

    public void incrementBlobsProcessed() {
        blobsProcessed.incrementAndGet();
    }

    public void incrementBlobsFailed() {
        blobsFailed.incrementAndGet();
    }

    public void incrementBlobsSkipped() {
        blobsSkipped.incrementAndGet();
    }

    public void addEventsProduced(long count) {
        eventsProduced.addAndGet(count);
    }

    public void setPollCycleDuration(double seconds) {
        this.pollCycleDuration = seconds;
    }

    public void setActiveLeaseCount(int count) {
        activeLeaseCount.set(count);
    }

    public void setDailyQuotaBytesUsed(long bytes) {
        dailyQuotaBytesUsed.set(bytes);
    }

    public void setDailyQuotaBytesLimit(long bytes) {
        this.dailyQuotaBytesLimit = bytes;
    }

    public void setDailyQuotaReached(boolean reached) {
        this.dailyQuotaReached = reached;
    }

    // -- Accessors ----------------------------------------------------------

    public long getBlobsProcessed() {
        return blobsProcessed.get();
    }

    public long getBlobsFailed() {
        return blobsFailed.get();
    }

    public long getBlobsSkipped() {
        return blobsSkipped.get();
    }

    public long getEventsProduced() {
        return eventsProduced.get();
    }

    public double getPollCycleDuration() {
        return pollCycleDuration;
    }

    public int getActiveLeaseCount() {
        return activeLeaseCount.get();
    }

    public long getDailyQuotaBytesUsed() {
        return dailyQuotaBytesUsed.get();
    }

    public long getDailyQuotaBytesLimit() {
        return dailyQuotaBytesLimit;
    }

    public boolean isDailyQuotaReached() {
        return dailyQuotaReached;
    }
}
