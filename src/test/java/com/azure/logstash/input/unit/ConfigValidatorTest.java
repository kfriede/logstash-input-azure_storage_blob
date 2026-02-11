package com.azure.logstash.input.unit;

import com.azure.logstash.input.config.ConfigValidator;
import com.azure.logstash.input.config.PluginConfig;
import co.elastic.logstash.api.Configuration;
import org.logstash.plugins.ConfigurationImpl;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Unit tests for ConfigValidator — validates plugin configuration at startup.
 */
public class ConfigValidatorTest {

    /**
     * Helper to build a Configuration from a map of raw settings.
     */
    private Configuration configFrom(Map<String, Object> raw) {
        return new ConfigurationImpl(raw);
    }

    /**
     * Helper to build a minimal valid config with only required fields.
     */
    private Map<String, Object> minimalConfig() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "myaccount");
        raw.put("container", "mycontainer");
        return raw;
    }

    // -----------------------------------------------------------------------
    // 1. Minimal valid config — no warnings, no exception
    // -----------------------------------------------------------------------
    @Test
    public void testMinimalConfigValid() {
        Configuration config = configFrom(minimalConfig());
        List<String> warnings = ConfigValidator.validate(config);
        assertTrue("Minimal config should produce no warnings", warnings.isEmpty());
    }

    // -----------------------------------------------------------------------
    // 2. Missing storage_account throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testMissingStorageAccountThrows() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("container", "mycontainer");
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 3. Missing container throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testMissingContainerThrows() {
        Map<String, Object> raw = new HashMap<>();
        raw.put("storage_account", "myaccount");
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 4. archive_container set with tags strategy => warn
    // -----------------------------------------------------------------------
    @Test
    public void testArchiveContainerIgnoredForTagsStrategy() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "tags");
        raw.put("archive_container", "custom-archive");
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        assertEquals(1, warnings.size());
        assertTrue(warnings.get(0).contains("archive_container"));
        assertTrue(warnings.get(0).contains("tags"));
    }

    // -----------------------------------------------------------------------
    // 5. archive_container with container strategy => no warn
    // -----------------------------------------------------------------------
    @Test
    public void testArchiveContainerUsedForContainerStrategy() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "container");
        raw.put("archive_container", "custom-archive");
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        assertTrue("No warning expected for archive_container with container strategy",
                warnings.stream().noneMatch(w -> w.contains("archive_container")));
    }

    // -----------------------------------------------------------------------
    // 6. error_container set with registry strategy => warn
    // -----------------------------------------------------------------------
    @Test
    public void testErrorContainerIgnoredForRegistryStrategy() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "registry");
        raw.put("error_container", "custom-errors");
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("error_container")));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("registry")));
    }

    // -----------------------------------------------------------------------
    // 7. registry_path set with tags strategy => warn
    // -----------------------------------------------------------------------
    @Test
    public void testRegistryPathIgnoredForTagsStrategy() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "tags");
        raw.put("registry_path", "/custom/path.db");
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("registry_path")));
        assertTrue(warnings.stream().anyMatch(w -> w.contains("tags")));
    }

    // -----------------------------------------------------------------------
    // 8. lease_duration with registry strategy => warn about lease coordination
    // -----------------------------------------------------------------------
    @Test
    public void testLeaseDurationIgnoredForRegistryStrategy() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "registry");
        raw.put("lease_duration", 30L);
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        assertTrue(warnings.stream().anyMatch(
                w -> w.contains("Lease coordination is not used with registry strategy")));
    }

    // -----------------------------------------------------------------------
    // 9. Invalid auth_method throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidAuthMethodThrows() {
        Map<String, Object> raw = minimalConfig();
        raw.put("auth_method", "oauth2");
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 10. Invalid cloud throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCloudThrows() {
        Map<String, Object> raw = minimalConfig();
        raw.put("cloud", "AzureGermanyCloud");
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 11. Invalid tracking_strategy throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidTrackingStrategyThrows() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "s3");
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 12. lease_duration out of range throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testLeaseDurationOutOfRange() {
        Map<String, Object> raw = minimalConfig();
        raw.put("lease_duration", 5L);
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 13. connection_string required when auth_method=connection_string
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testConnectionStringRequiredWhenAuthMethodConnectionString() {
        Map<String, Object> raw = minimalConfig();
        raw.put("auth_method", "connection_string");
        // connection_string not set => should throw
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 14. storage_key required when auth_method=shared_key
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testSharedKeyRequiredWhenAuthMethodSharedKey() {
        Map<String, Object> raw = minimalConfig();
        raw.put("auth_method", "shared_key");
        // storage_key not set => should throw
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 15. poll_interval of zero throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testPollIntervalZeroThrows() {
        Map<String, Object> raw = minimalConfig();
        raw.put("poll_interval", 0L);
        ConfigValidator.validate(configFrom(raw));
    }

    // -----------------------------------------------------------------------
    // 16. poll_interval of 1 is valid
    // -----------------------------------------------------------------------
    @Test
    public void testPollIntervalOneIsValid() {
        Map<String, Object> raw = minimalConfig();
        raw.put("poll_interval", 1L);
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        // Should not throw — 1 is valid
        assertNotNull(warnings);
    }

    // -----------------------------------------------------------------------
    // 17. blob_concurrency with registry strategy => warn and force to 1
    // -----------------------------------------------------------------------
    @Test
    public void testBlobConcurrencyIgnoredForRegistryStrategy() {
        Map<String, Object> raw = minimalConfig();
        raw.put("tracking_strategy", "registry");
        raw.put("blob_concurrency", 4L);
        List<String> warnings = ConfigValidator.validate(configFrom(raw));
        assertTrue(warnings.stream().anyMatch(
                w -> w.contains("blob_concurrency") && w.contains("registry")));
    }

    // -----------------------------------------------------------------------
    // 18. blob_concurrency of zero throws
    // -----------------------------------------------------------------------
    @Test(expected = IllegalArgumentException.class)
    public void testBlobConcurrencyZeroThrows() {
        Map<String, Object> raw = minimalConfig();
        raw.put("blob_concurrency", 0L);
        ConfigValidator.validate(configFrom(raw));
    }
}
