package com.azure.logstash.input.config;

import co.elastic.logstash.api.PluginConfigSpec;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Defines all configuration settings for the azure_storage_blob input plugin.
 *
 * <p>Each setting is exposed as a {@code public static final} constant so that
 * both the plugin class and the validator can reference them without magic strings.
 */
public final class PluginConfig {

    private PluginConfig() {
        // utility class
    }

    // ── Connection (required) ──────────────────────────────────────────────

    /** Azure Storage account name. Required, no default. */
    public static final PluginConfigSpec<String> STORAGE_ACCOUNT =
            PluginConfigSpec.requiredStringSetting("storage_account");

    /** Blob container name to read from. Required, no default. */
    public static final PluginConfigSpec<String> CONTAINER =
            PluginConfigSpec.requiredStringSetting("container");

    // ── Authentication ─────────────────────────────────────────────────────

    /** Auth method: "default", "connection_string", or "shared_key". */
    public static final PluginConfigSpec<String> AUTH_METHOD =
            PluginConfigSpec.stringSetting("auth_method", "default");

    /** Azure Storage connection string (used when auth_method=connection_string). */
    public static final PluginConfigSpec<String> CONNECTION_STRING =
            PluginConfigSpec.stringSetting("connection_string", "");

    /** Storage account access key (used when auth_method=shared_key). */
    public static final PluginConfigSpec<String> STORAGE_KEY =
            PluginConfigSpec.stringSetting("storage_key", "");

    // ── Azure Environment ──────────────────────────────────────────────────

    /** Cloud environment: "", "AzureCloud", "AzureUSGovernment", "AzureChinaCloud". Empty = auto-detect. */
    public static final PluginConfigSpec<String> CLOUD =
            PluginConfigSpec.stringSetting("cloud", "");

    /** Blob endpoint override for Azurite, private endpoints, or unlisted sovereign clouds. */
    public static final PluginConfigSpec<String> BLOB_ENDPOINT =
            PluginConfigSpec.stringSetting("blob_endpoint", "");

    // ── State Tracking ─────────────────────────────────────────────────────

    /** Tracking strategy: "tags", "container", or "registry". */
    public static final PluginConfigSpec<String> TRACKING_STRATEGY =
            PluginConfigSpec.stringSetting("tracking_strategy", "tags");

    /** Container name for archived (completed) blobs. Used by container strategy. */
    public static final PluginConfigSpec<String> ARCHIVE_CONTAINER =
            PluginConfigSpec.stringSetting("archive_container", "archive");

    /** Container name for failed blobs. Used by container strategy. */
    public static final PluginConfigSpec<String> ERROR_CONTAINER =
            PluginConfigSpec.stringSetting("error_container", "errors");

    /** Path to SQLite registry database. Used by registry strategy. */
    public static final PluginConfigSpec<String> REGISTRY_PATH =
            PluginConfigSpec.stringSetting("registry_path", "/data/registry.db");

    // ── Polling ────────────────────────────────────────────────────────────

    /** Seconds between poll cycles. */
    public static final PluginConfigSpec<Long> POLL_INTERVAL =
            PluginConfigSpec.numSetting("poll_interval", 30);

    /** Only process blobs whose name starts with this prefix. */
    public static final PluginConfigSpec<String> PREFIX =
            PluginConfigSpec.stringSetting("prefix", "");

    // ── Processing ─────────────────────────────────────────────────────────

    /** Maximum number of blobs to process per poll cycle. */
    public static final PluginConfigSpec<Long> BLOB_BATCH_SIZE =
            PluginConfigSpec.numSetting("blob_batch_size", 10);

    /** Number of blobs to process in parallel within a poll cycle. */
    public static final PluginConfigSpec<Long> BLOB_CONCURRENCY =
            PluginConfigSpec.numSetting("blob_concurrency", 1);

    /** Whether to skip empty lines when processing blob content. */
    public static final PluginConfigSpec<Boolean> SKIP_EMPTY_LINES =
            PluginConfigSpec.booleanSetting("skip_empty_lines", true);

    // ── Lease Coordination ─────────────────────────────────────────────────

    /** Lease duration in seconds. Azure requires 15-60. Used by tags and container strategies. */
    public static final PluginConfigSpec<Long> LEASE_DURATION =
            PluginConfigSpec.numSetting("lease_duration", 30);

    /** Lease renewal interval in seconds. Should be less than lease_duration. */
    public static final PluginConfigSpec<Long> LEASE_RENEWAL =
            PluginConfigSpec.numSetting("lease_renewal", 20);

    // ── Aggregate ──────────────────────────────────────────────────────────

    /** All configuration specs, for use in {@code configSchema()}. */
    public static final Collection<PluginConfigSpec<?>> ALL_CONFIGS =
            Collections.unmodifiableList(Arrays.asList(
                    STORAGE_ACCOUNT,
                    CONTAINER,
                    AUTH_METHOD,
                    CONNECTION_STRING,
                    STORAGE_KEY,
                    CLOUD,
                    BLOB_ENDPOINT,
                    TRACKING_STRATEGY,
                    ARCHIVE_CONTAINER,
                    ERROR_CONTAINER,
                    REGISTRY_PATH,
                    POLL_INTERVAL,
                    PREFIX,
                    BLOB_BATCH_SIZE,
                    BLOB_CONCURRENCY,
                    SKIP_EMPTY_LINES,
                    LEASE_DURATION,
                    LEASE_RENEWAL
            ));
}
