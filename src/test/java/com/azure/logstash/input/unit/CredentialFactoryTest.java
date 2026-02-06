package com.azure.logstash.input.unit;

import com.azure.identity.DefaultAzureCredential;
import com.azure.logstash.input.auth.CredentialFactory;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for CredentialFactory — builds Azure credentials and BlobServiceClient
 * with cloud auto-detection from AZURE_AUTHORITY_HOST.
 */
public class CredentialFactoryTest {

    // Well-known Azurite connection string for local testing
    private static final String AZURITE_CONNECTION_STRING =
            "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;"
                    + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/"
                    + "K1SZFPTOtr/KBHBeksoGMGw==;"
                    + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    // -----------------------------------------------------------------------
    // 1. auth_method=default → buildCredential() returns DefaultAzureCredential
    // -----------------------------------------------------------------------
    @Test
    public void testDefaultCredentialCreatesDefaultAzureCredential() {
        CredentialFactory factory = new CredentialFactory(
                "default", "testaccount", null, null,
                "AzureCloud", null);
        Object credential = factory.buildCredential();
        assertNotNull("Default auth should produce a credential", credential);
        assertTrue("Should be DefaultAzureCredential",
                credential instanceof DefaultAzureCredential);
    }

    // -----------------------------------------------------------------------
    // 2. auth_method=connection_string → buildCredential() returns null
    // -----------------------------------------------------------------------
    @Test
    public void testConnectionStringReturnsNullCredential() {
        CredentialFactory factory = new CredentialFactory(
                "connection_string", "testaccount",
                AZURITE_CONNECTION_STRING, null,
                "AzureCloud", null);
        Object credential = factory.buildCredential();
        assertNull("connection_string auth should return null credential", credential);
    }

    // -----------------------------------------------------------------------
    // 3. auth_method=shared_key → returns StorageSharedKeyCredential
    // -----------------------------------------------------------------------
    @Test
    public void testSharedKeyReturnsStorageSharedKeyCredential() {
        CredentialFactory factory = new CredentialFactory(
                "shared_key", "testaccount", null,
                "dGVzdGtleQ==",  // base64 "testkey"
                "AzureCloud", null);
        Object credential = factory.buildCredential();
        assertNotNull("Shared key auth should produce a credential", credential);
        assertTrue("Should be StorageSharedKeyCredential",
                credential instanceof StorageSharedKeyCredential);
    }

    // -----------------------------------------------------------------------
    // 4. cloud=AzureCloud → endpoint is https://test.blob.core.windows.net
    // -----------------------------------------------------------------------
    @Test
    public void testAzureCloudEndpoint() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "AzureCloud", null);
        assertEquals("https://test.blob.core.windows.net", factory.getEndpoint());
    }

    // -----------------------------------------------------------------------
    // 5. cloud=AzureUSGovernment → endpoint for Gov
    // -----------------------------------------------------------------------
    @Test
    public void testAzureGovEndpoint() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "AzureUSGovernment", null);
        assertEquals("https://test.blob.core.usgovcloudapi.net", factory.getEndpoint());
    }

    // -----------------------------------------------------------------------
    // 6. cloud=AzureChinaCloud → endpoint for China
    // -----------------------------------------------------------------------
    @Test
    public void testAzureChinaEndpoint() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "AzureChinaCloud", null);
        assertEquals("https://test.blob.core.chinacloudapi.cn", factory.getEndpoint());
    }

    // -----------------------------------------------------------------------
    // 7. blobEndpoint overrides cloud endpoint
    // -----------------------------------------------------------------------
    @Test
    public void testBlobEndpointOverridesCloudEndpoint() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "AzureCloud", "http://127.0.0.1:10000/devstoreaccount1");
        assertEquals("http://127.0.0.1:10000/devstoreaccount1", factory.getEndpoint());
    }

    // -----------------------------------------------------------------------
    // 8. Auto-detect Gov from AZURE_AUTHORITY_HOST env var
    // -----------------------------------------------------------------------
    @Test
    public void testAutoDetectGovFromEnvVar() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "", null,
                key -> "AZURE_AUTHORITY_HOST".equals(key)
                        ? "https://login.microsoftonline.us"
                        : null);
        assertEquals("AzureUSGovernment", factory.getResolvedCloud());
    }

    // -----------------------------------------------------------------------
    // 9. Auto-detect China from env var with trailing slash
    // -----------------------------------------------------------------------
    @Test
    public void testAutoDetectChinaFromEnvVar() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "", null,
                key -> "AZURE_AUTHORITY_HOST".equals(key)
                        ? "https://login.chinacloudapi.cn/"
                        : null);
        assertEquals("AzureChinaCloud", factory.getResolvedCloud());
    }

    // -----------------------------------------------------------------------
    // 10. Auto-detect Commercial from env var
    // -----------------------------------------------------------------------
    @Test
    public void testAutoDetectCommercialFromEnvVar() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "", null,
                key -> "AZURE_AUTHORITY_HOST".equals(key)
                        ? "https://login.microsoftonline.com"
                        : null);
        assertEquals("AzureCloud", factory.getResolvedCloud());
    }

    // -----------------------------------------------------------------------
    // 11. Explicit cloud takes precedence over env var
    // -----------------------------------------------------------------------
    @Test
    public void testNoAutoDetectWhenCloudExplicit() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "AzureUSGovernment", null,
                key -> "AZURE_AUTHORITY_HOST".equals(key)
                        ? "https://login.microsoftonline.com"
                        : null);
        assertEquals("AzureUSGovernment", factory.getResolvedCloud());
    }

    // -----------------------------------------------------------------------
    // 12. Default to AzureCloud when no env var
    // -----------------------------------------------------------------------
    @Test
    public void testDefaultCloudWhenNoEnvVar() {
        CredentialFactory factory = new CredentialFactory(
                "default", "test", null, null,
                "", null,
                key -> null);
        assertEquals("AzureCloud", factory.getResolvedCloud());
    }

    // -----------------------------------------------------------------------
    // 13. Build BlobServiceClient with default credential (non-null, no exception)
    // -----------------------------------------------------------------------
    @Test
    public void testBuildBlobServiceClientWithDefaultCredential() {
        // Use HTTPS endpoint — the Azure SDK rejects bearer tokens over HTTP
        CredentialFactory factory = new CredentialFactory(
                "default", "devstoreaccount1", null, null,
                "AzureCloud", "https://devstoreaccount1.blob.core.windows.net");
        BlobServiceClient client = factory.buildBlobServiceClient();
        assertNotNull("BlobServiceClient should be non-null", client);
    }

    // -----------------------------------------------------------------------
    // 14. Build BlobServiceClient with connection string
    // -----------------------------------------------------------------------
    @Test
    public void testBuildBlobServiceClientWithConnectionString() {
        CredentialFactory factory = new CredentialFactory(
                "connection_string", "devstoreaccount1",
                AZURITE_CONNECTION_STRING, null,
                "AzureCloud", null);
        BlobServiceClient client = factory.buildBlobServiceClient();
        assertNotNull("BlobServiceClient should be non-null", client);
    }
}
