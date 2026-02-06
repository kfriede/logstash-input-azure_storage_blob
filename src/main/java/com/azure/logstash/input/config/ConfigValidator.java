package com.azure.logstash.input.config;

import co.elastic.logstash.api.Configuration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates plugin configuration at startup.
 *
 * <p>{@link #validate(Configuration)} throws {@link IllegalArgumentException} for
 * hard errors (invalid enum values, missing credentials, out-of-range values) and
 * returns a list of warning strings for soft mismatches (settings that will be
 * ignored given the current tracking strategy).
 */
public final class ConfigValidator {

    private static final Set<String> VALID_AUTH_METHODS =
            new HashSet<>(Arrays.asList("default", "connection_string", "shared_key"));

    private static final Set<String> VALID_CLOUDS =
            new HashSet<>(Arrays.asList("", "AzureCloud", "AzureUSGovernment", "AzureChinaCloud"));

    private static final Set<String> VALID_TRACKING_STRATEGIES =
            new HashSet<>(Arrays.asList("tags", "container", "registry"));

    private ConfigValidator() {
        // utility class
    }

    /**
     * Validates the given configuration.
     *
     * @param config the Logstash {@link Configuration} to validate
     * @return a list of warning messages (may be empty); never null
     * @throws IllegalArgumentException if any hard validation error is found
     */
    public static List<String> validate(Configuration config) {
        List<String> warnings = new ArrayList<>();

        // ── Required fields ────────────────────────────────────────────────
        if (!config.contains(PluginConfig.STORAGE_ACCOUNT)) {
            throw new IllegalArgumentException("'storage_account' is required");
        }
        if (!config.contains(PluginConfig.CONTAINER)) {
            throw new IllegalArgumentException("'container' is required");
        }

        // ── Enum validation ────────────────────────────────────────────────
        String authMethod = config.get(PluginConfig.AUTH_METHOD);
        if (!VALID_AUTH_METHODS.contains(authMethod)) {
            throw new IllegalArgumentException(
                    "Invalid auth_method '" + authMethod + "'. "
                            + "Valid values: default, connection_string, shared_key");
        }

        String cloud = config.get(PluginConfig.CLOUD);
        if (!VALID_CLOUDS.contains(cloud)) {
            throw new IllegalArgumentException(
                    "Invalid cloud '" + cloud + "'. "
                            + "Valid values: (empty), AzureCloud, AzureUSGovernment, AzureChinaCloud");
        }

        String strategy = config.get(PluginConfig.TRACKING_STRATEGY);
        if (!VALID_TRACKING_STRATEGIES.contains(strategy)) {
            throw new IllegalArgumentException(
                    "Invalid tracking_strategy '" + strategy + "'. "
                            + "Valid values: tags, container, registry");
        }

        // ── Range validation ───────────────────────────────────────────────
        long leaseDuration = config.get(PluginConfig.LEASE_DURATION);
        if (leaseDuration < 15 || leaseDuration > 60) {
            throw new IllegalArgumentException(
                    "lease_duration must be between 15 and 60 seconds (Azure requirement), got " + leaseDuration);
        }

        long pollInterval = config.get(PluginConfig.POLL_INTERVAL);
        if (pollInterval < 1) {
            throw new IllegalArgumentException(
                    "poll_interval must be at least 1 second, got " + pollInterval);
        }

        // ── Credential requirements ────────────────────────────────────────
        if ("connection_string".equals(authMethod)) {
            String connStr = config.get(PluginConfig.CONNECTION_STRING);
            if (connStr == null || connStr.isEmpty()) {
                throw new IllegalArgumentException(
                        "connection_string is required when auth_method is 'connection_string'");
            }
        }

        if ("shared_key".equals(authMethod)) {
            String storageKey = config.get(PluginConfig.STORAGE_KEY);
            if (storageKey == null || storageKey.isEmpty()) {
                throw new IllegalArgumentException(
                        "storage_key is required when auth_method is 'shared_key'");
            }
        }

        // ── Strategy-mismatch warnings ─────────────────────────────────────
        if (!"container".equals(strategy) && config.contains(PluginConfig.ARCHIVE_CONTAINER)) {
            warnings.add("archive_container is configured but will be ignored because "
                    + "tracking_strategy is '" + strategy + "'");
        }

        if (!"container".equals(strategy) && config.contains(PluginConfig.ERROR_CONTAINER)) {
            warnings.add("error_container is configured but will be ignored because "
                    + "tracking_strategy is '" + strategy + "'");
        }

        if (!"registry".equals(strategy) && config.contains(PluginConfig.REGISTRY_PATH)) {
            warnings.add("registry_path is configured but will be ignored because "
                    + "tracking_strategy is '" + strategy + "'");
        }

        if ("registry".equals(strategy) && config.contains(PluginConfig.LEASE_DURATION)) {
            warnings.add("Lease coordination is not used with registry strategy");
        }

        return warnings;
    }
}
