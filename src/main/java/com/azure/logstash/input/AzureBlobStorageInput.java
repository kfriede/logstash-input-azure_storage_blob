package com.azure.logstash.input;

import co.elastic.logstash.api.*;
import com.azure.logstash.input.config.ConfigValidator;
import com.azure.logstash.input.config.PluginConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

@LogstashPlugin(name = "azure_storage_blob")
public class AzureBlobStorageInput implements Input {

    private static final Logger logger = LogManager.getLogger(AzureBlobStorageInput.class);

    private final String id;
    private final CountDownLatch done = new CountDownLatch(1);
    private volatile boolean stopped;

    // ── Config values ──────────────────────────────────────────────────────
    private final String storageAccount;
    private final String container;
    private final String authMethod;
    private final String connectionString;
    private final String storageKey;
    private final String cloud;
    private final String blobEndpoint;
    private final String trackingStrategy;
    private final String archiveContainer;
    private final String errorContainer;
    private final String registryPath;
    private final long pollInterval;
    private final String prefix;
    private final long blobBatchSize;
    private final boolean skipEmptyLines;
    private final long leaseDuration;
    private final long leaseRenewal;

    public AzureBlobStorageInput(String id, Configuration config, Context context) {
        this.id = id;

        // Read all config values
        this.storageAccount = config.get(PluginConfig.STORAGE_ACCOUNT);
        this.container = config.get(PluginConfig.CONTAINER);
        this.authMethod = config.get(PluginConfig.AUTH_METHOD);
        this.connectionString = config.get(PluginConfig.CONNECTION_STRING);
        this.storageKey = config.get(PluginConfig.STORAGE_KEY);
        this.cloud = config.get(PluginConfig.CLOUD);
        this.blobEndpoint = config.get(PluginConfig.BLOB_ENDPOINT);
        this.trackingStrategy = config.get(PluginConfig.TRACKING_STRATEGY);
        this.archiveContainer = config.get(PluginConfig.ARCHIVE_CONTAINER);
        this.errorContainer = config.get(PluginConfig.ERROR_CONTAINER);
        this.registryPath = config.get(PluginConfig.REGISTRY_PATH);
        this.pollInterval = config.get(PluginConfig.POLL_INTERVAL);
        this.prefix = config.get(PluginConfig.PREFIX);
        this.blobBatchSize = config.get(PluginConfig.BLOB_BATCH_SIZE);
        this.skipEmptyLines = config.get(PluginConfig.SKIP_EMPTY_LINES);
        this.leaseDuration = config.get(PluginConfig.LEASE_DURATION);
        this.leaseRenewal = config.get(PluginConfig.LEASE_RENEWAL);

        // Validate and log warnings
        List<String> warnings = ConfigValidator.validate(config);
        for (String warning : warnings) {
            logger.warn(warning);
        }
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        try { while (!stopped) { Thread.sleep(1000); } }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        finally { stopped = true; done.countDown(); }
    }

    @Override public void stop() { stopped = true; }
    @Override public void awaitStop() throws InterruptedException { done.await(); }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return PluginConfig.ALL_CONFIGS;
    }

    @Override public String getId() { return id; }
}
