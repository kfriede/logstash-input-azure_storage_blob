package com.azure.logstash.input;

import com.azure.logstash.input.auth.CredentialFactory;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.BlobStorageException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.*;

/**
 * Operational tests for authentication against real Azure Government endpoints.
 *
 * <p>Validates that all supported auth methods work against a real gov storage
 * account, and that cloud auto-detection and explicit cloud configuration
 * produce the correct endpoints and authority hosts.
 */
@Category(OperationalTest.class)
public class AuthenticationOT extends AzureGovTestBase {

    // ── Test 1: DefaultAzureCredential authenticates to gov ──────────────────

    @Test
    public void testDefaultCredentialAuthenticates() throws Exception {
        BlobServiceClient dacClient = buildBlobServiceClientWithDefaultCredential();
        // RBAC role propagation on a new storage account can take up to a few
        // minutes in Azure Government. Retry with backoff if we get 403.
        int maxAttempts = 6;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                dacClient.getBlobContainerClient("incoming").listBlobs().forEach(b -> {});
                return; // success
            } catch (BlobStorageException e) {
                if (e.getStatusCode() == 403 && attempt < maxAttempts) {
                    System.out.println("RBAC not yet propagated (attempt " + attempt
                            + "/" + maxAttempts + "), waiting 10s...");
                    Thread.sleep(10000);
                } else {
                    throw e;
                }
            }
        }
    }

    // ── Test 2: Explicit AzureUSGovernment cloud config ─────────────────────

    @Test
    public void testExplicitGovCloudConfig() {
        CredentialFactory factory = new CredentialFactory(
                "connection_string", accountName, connectionString, "",
                "AzureUSGovernment", "");

        String endpoint = factory.getEndpoint();
        assertTrue("Endpoint should contain usgovcloudapi.net: " + endpoint,
                endpoint.contains("usgovcloudapi.net"));
        assertEquals("https://login.microsoftonline.us", factory.getAuthorityHost());
    }

    // ── Test 3: Connection string auth ──────────────────────────────────────

    @Test
    public void testConnectionStringAuth() {
        CredentialFactory factory = new CredentialFactory(
                "connection_string", accountName, connectionString, "",
                "AzureUSGovernment", "");

        BlobServiceClient client = factory.buildBlobServiceClient();
        // Should list blobs successfully
        client.getBlobContainerClient("incoming").listBlobs().forEach(b -> {});
    }

    // ── Test 4: Shared key auth ─────────────────────────────────────────────

    @Test
    public void testSharedKeyAuth() {
        CredentialFactory factory = new CredentialFactory(
                "shared_key", accountName, "", storageKey,
                "AzureUSGovernment", "");

        BlobServiceClient client = factory.buildBlobServiceClient();
        // Should list blobs successfully
        client.getBlobContainerClient("incoming").listBlobs().forEach(b -> {});
    }

    // ── Test 5: Wrong cloud config fails ────────────────────────────────────

    @Test
    public void testWrongCloudConfigFails() {
        CredentialFactory factory = new CredentialFactory(
                "default", accountName, "", "",
                "AzureCloud", "");

        BlobServiceClient client = factory.buildBlobServiceClient();
        try {
            // Attempting to list blobs with AzureCloud endpoint against a gov
            // account should fail (wrong DNS suffix)
            client.getBlobContainerClient("incoming").listBlobs().forEach(b -> {});
            fail("Should have thrown an exception when using AzureCloud config against gov account");
        } catch (Exception e) {
            // Expected: the endpoint blob.core.windows.net won't resolve
            // to the gov storage account
            assertNotNull("Exception should have a message", e.getMessage());
        }
    }

    // ── Test 6: Cloud auto-detection for gov ────────────────────────────────

    @Test
    public void testAutoDetectedCloud() {
        // Simulate AZURE_AUTHORITY_HOST env var pointing to gov
        CredentialFactory factory = new CredentialFactory(
                "default", accountName, "", "",
                "", "",
                varName -> {
                    if ("AZURE_AUTHORITY_HOST".equals(varName)) {
                        return "https://login.microsoftonline.us/";
                    }
                    return null;
                });

        assertEquals("AzureUSGovernment", factory.getResolvedCloud());
        assertTrue("Endpoint should contain usgovcloudapi.net",
                factory.getEndpoint().contains("usgovcloudapi.net"));
        assertEquals("https://login.microsoftonline.us", factory.getAuthorityHost());
    }
}
