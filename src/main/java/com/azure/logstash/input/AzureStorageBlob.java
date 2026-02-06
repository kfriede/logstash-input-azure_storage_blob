package com.azure.logstash.input;

import co.elastic.logstash.api.*;
import com.azure.logstash.input.auth.CredentialFactory;
import com.azure.logstash.input.config.ConfigValidator;
import com.azure.logstash.input.config.PluginConfig;
import com.azure.logstash.input.tracking.ContainerStateTracker;
import com.azure.logstash.input.tracking.RegistryStateTracker;
import com.azure.logstash.input.tracking.StateTracker;
import com.azure.logstash.input.tracking.TagStateTracker;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

/**
 * Logstash input plugin that reads blobs from Azure Blob Storage.
 *
 * <p>Polls a container for new blobs, streams their content line-by-line,
 * and pushes events into the Logstash pipeline. Three state-tracking
 * strategies control which blobs are picked up and how completion is
 * recorded (tags, container moves, or a local SQLite registry).
 *
 * <p>All Azure SDK operations are performed through the first-party
 * {@code azure-storage-blob} and {@code azure-identity} Java SDKs,
 * running natively on the JVM.
 */
@LogstashPlugin(name = "azure_storage_blob")
public class AzureStorageBlob implements Input {

    private static final Logger logger = LogManager.getLogger(AzureStorageBlob.class);

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

    // ── Wired components ───────────────────────────────────────────────────
    private final BlobContainerClient containerClient;
    private final StateTracker stateTracker;
    private final BlobProcessor processor;
    private final BlobPoller poller;
    private final PluginMetrics metrics;
    private final HealthState healthState;

    /**
     * Public constructor for Logstash runtime. Builds all dependencies from config.
     */
    public AzureStorageBlob(String id, Configuration config, Context context) {
        this(id, config, context, null, null, null, null);
    }

    /**
     * Constructor for testing with injected dependencies.
     *
     * <p>When any dependency parameter is non-null, it is used directly
     * instead of being created from configuration. This allows unit tests
     * to inject mocks without requiring real Azure SDK connections.
     *
     * <p><b>Visible for testing only</b> — production code should use the
     * three-argument constructor.
     *
     * @param id            the plugin instance id
     * @param config        Logstash configuration
     * @param context       Logstash context (may be null in tests)
     * @param serviceClient pre-built BlobServiceClient, or null to create from config
     * @param stateTracker  pre-built StateTracker, or null to create from config
     * @param processor     pre-built BlobProcessor, or null to create from config
     * @param poller        pre-built BlobPoller, or null to create in start()
     */
    public AzureStorageBlob(String id, Configuration config, Context context,
                           BlobServiceClient serviceClient, StateTracker stateTracker,
                           BlobProcessor processor, BlobPoller poller) {
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

        // Resolve hostname for processor identification
        String hostname = resolveHostname();

        // Initialize metrics and health state
        this.metrics = new PluginMetrics();
        this.healthState = new HealthState();

        // Build or inject components
        if (serviceClient != null) {
            // Test path: use injected dependencies directly
            this.containerClient = serviceClient.getBlobContainerClient(this.container);
            this.stateTracker = stateTracker;
            this.processor = processor;
            this.poller = poller;
        } else {
            // Production path: build from config
            CredentialFactory credentialFactory = new CredentialFactory(
                    this.authMethod, this.storageAccount, this.connectionString,
                    this.storageKey, this.cloud, this.blobEndpoint);

            BlobServiceClient builtServiceClient = credentialFactory.buildBlobServiceClient();
            this.containerClient = builtServiceClient.getBlobContainerClient(this.container);

            this.stateTracker = createStateTracker(builtServiceClient, hostname);
            this.processor = new BlobProcessor(this.storageAccount, this.container, this.skipEmptyLines);
            this.poller = null; // Will be created in start() with the consumer
        }

        // Log config summary
        logger.info("Azure Blob Storage input plugin initialized: "
                        + "storage_account={}, container={}, tracking_strategy={}, "
                        + "poll_interval={}s, prefix='{}', blob_batch_size={}, "
                        + "auth_method={}, cloud={}, hostname={}",
                this.storageAccount, this.container, this.trackingStrategy,
                this.pollInterval, this.prefix, this.blobBatchSize,
                this.authMethod, this.cloud.isEmpty() ? "(auto-detect)" : this.cloud,
                hostname);
    }

    @Override
    public void start(Consumer<Map<String, Object>> consumer) {
        // Create BlobPoller if not injected (production path)
        BlobPoller activePoller = this.poller;
        if (activePoller == null) {
            activePoller = new BlobPoller(containerClient, stateTracker, processor,
                    consumer, prefix, (int) blobBatchSize);
        }

        try {
            while (!stopped) {
                BlobPoller.PollCycleSummary summary = activePoller.pollOnce(() -> stopped);

                // Update metrics
                for (int i = 0; i < summary.getBlobsProcessed(); i++) {
                    metrics.incrementBlobsProcessed();
                }
                for (int i = 0; i < summary.getBlobsFailed(); i++) {
                    metrics.incrementBlobsFailed();
                }
                for (int i = 0; i < summary.getBlobsSkipped(); i++) {
                    metrics.incrementBlobsSkipped();
                }
                metrics.addEventsProduced(summary.getEventsProduced());
                metrics.setPollCycleDuration(summary.getDurationMs() / 1000.0);

                // Update health state
                healthState.recordPollResult(
                        summary.getBlobsProcessed(), summary.getBlobsFailed());

                // Log summary
                logger.info("Poll cycle complete: {} blobs processed, {} failed, "
                                + "{} skipped, {} events produced in {}ms. "
                                + "Next poll in {}s. Health: {}",
                        summary.getBlobsProcessed(), summary.getBlobsFailed(),
                        summary.getBlobsSkipped(), summary.getEventsProduced(),
                        summary.getDurationMs(), pollInterval,
                        healthState.getState());

                // Sleep between poll cycles
                if (!stopped) {
                    Thread.sleep(pollInterval * 1000L);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info("Poll loop interrupted, shutting down");
        } finally {
            stopped = true;
            if (stateTracker != null) {
                stateTracker.close();
            }
            done.countDown();
        }
    }

    @Override
    public void stop() {
        stopped = true;
    }

    @Override
    public void awaitStop() throws InterruptedException {
        done.await();
    }

    @Override
    public Collection<PluginConfigSpec<?>> configSchema() {
        return PluginConfig.ALL_CONFIGS;
    }

    @Override
    public String getId() {
        return id;
    }

    // ── Package-private accessors for testing ──────────────────────────────

    /**
     * Returns the plugin metrics. Visible for testing.
     */
    public PluginMetrics getMetrics() {
        return metrics;
    }

    /**
     * Returns the health state. Visible for testing.
     */
    public HealthState getHealthState() {
        return healthState;
    }

    // ── Private helpers ────────────────────────────────────────────────────

    /**
     * Creates the appropriate StateTracker based on the configured tracking strategy.
     */
    private StateTracker createStateTracker(BlobServiceClient builtServiceClient, String hostname) {
        switch (trackingStrategy) {
            case "tags":
                return new TagStateTracker(containerClient,
                        (int) leaseDuration, (int) leaseRenewal, hostname);
            case "container":
                return new ContainerStateTracker(builtServiceClient,
                        container, archiveContainer, errorContainer,
                        (int) leaseDuration, (int) leaseRenewal, hostname);
            case "registry":
                logger.warn("Registry strategy uses local SQLite — single-replica only. "
                        + "Do not run multiple instances with this strategy.");
                return new RegistryStateTracker(registryPath, hostname);
            default:
                throw new IllegalArgumentException(
                        "Unknown tracking_strategy: " + trackingStrategy);
        }
    }

    /**
     * Resolves the local hostname for use as the processor name.
     * Falls back to "unknown" if hostname resolution fails.
     */
    private static String resolveHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            logger.warn("Could not resolve hostname, using 'unknown': {}", e.getMessage());
            return "unknown";
        }
    }
}
