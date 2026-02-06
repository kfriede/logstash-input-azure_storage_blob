package com.azure.logstash.input;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import org.junit.After;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Base class for all integration tests running against Azurite.
 *
 * <p>Provides a shared {@link BlobServiceClient} and helper methods for
 * container and blob operations. Cleans up all test containers after each test.
 */
public abstract class AzuriteTestBase {

    protected static final String AZURITE_ENDPOINT = "http://127.0.0.1:10000";
    protected static final String AZURITE_ACCOUNT = "devstoreaccount1";
    protected static final String AZURITE_CONNECTION_STRING =
            "DefaultEndpointsProtocol=http;"
            + "AccountName=devstoreaccount1;"
            + "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;"
            + "BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;";

    protected static BlobServiceClient serviceClient;

    /** Tracks all containers created during a test so they can be cleaned up. */
    private final Set<String> testContainers = new HashSet<>();

    @BeforeClass
    public static void verifyAzuriteAndCreateClient() {
        // Verify Azurite is reachable
        try {
            HttpURLConnection connection =
                    (HttpURLConnection) new URL(AZURITE_ENDPOINT).openConnection();
            connection.setConnectTimeout(3000);
            connection.setReadTimeout(3000);
            connection.setRequestMethod("GET");
            connection.connect();
            // Any response (even 400) means Azurite is running
            connection.getResponseCode();
            connection.disconnect();
        } catch (IOException e) {
            assumeTrue("Azurite is not running at " + AZURITE_ENDPOINT
                    + ". Start it with: docker run -p 10000:10000 "
                    + "mcr.microsoft.com/azure-storage/azurite", false);
        }

        serviceClient = new BlobServiceClientBuilder()
                .connectionString(AZURITE_CONNECTION_STRING)
                .buildClient();
    }

    @After
    public void cleanupContainers() {
        for (String containerName : testContainers) {
            try {
                BlobContainerClient cc = serviceClient.getBlobContainerClient(containerName);
                if (cc.exists()) {
                    // Delete all blobs first (some may have leases)
                    for (BlobItem item : cc.listBlobs()) {
                        try {
                            BlobClient bc = cc.getBlobClient(item.getName());
                            try {
                                // Break any existing lease before deleting
                                bc.getBlockBlobClient()
                                  .getCustomerProvidedKeyClient(null);
                                new com.azure.storage.blob.specialized.BlobLeaseClientBuilder()
                                        .blobClient(bc)
                                        .buildClient()
                                        .breakLease();
                            } catch (Exception ignored) {
                                // No lease to break
                            }
                            bc.delete();
                        } catch (Exception ignored) {
                            // Best effort cleanup
                        }
                    }
                    cc.delete();
                }
            } catch (Exception ignored) {
                // Best effort cleanup
            }
        }
        testContainers.clear();
    }

    // ── Helper methods ──────────────────────────────────────────────────────

    /**
     * Creates a container and registers it for cleanup.
     */
    protected BlobContainerClient createContainer(String name) {
        testContainers.add(name);
        BlobContainerClient cc = serviceClient.getBlobContainerClient(name);
        if (!cc.exists()) {
            cc.create();
        }
        return cc;
    }

    /**
     * Registers a container for cleanup without creating it.
     */
    protected void trackContainer(String name) {
        testContainers.add(name);
    }

    /**
     * Deletes a container.
     */
    protected void deleteContainer(String name) {
        BlobContainerClient cc = serviceClient.getBlobContainerClient(name);
        if (cc.exists()) {
            cc.delete();
        }
    }

    /**
     * Uploads a blob with the given text content.
     */
    protected void uploadBlob(String containerName, String blobName, String content) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        bc.upload(new ByteArrayInputStream(data), data.length, true);
    }

    /**
     * Uploads a blob with raw byte content.
     */
    protected void uploadBlobBytes(String containerName, String blobName, byte[] content) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        bc.upload(new ByteArrayInputStream(content), content.length, true);
    }

    /**
     * Lists blob names in a container.
     */
    protected List<String> listBlobNames(String containerName) {
        BlobContainerClient cc = serviceClient.getBlobContainerClient(containerName);
        return cc.listBlobs().stream()
                .map(BlobItem::getName)
                .collect(Collectors.toList());
    }

    /**
     * Gets the text content of a blob.
     */
    protected String getBlobContent(String containerName, String blobName) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        bc.downloadStream(out);
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * Gets the tags of a blob.
     */
    protected Map<String, String> getBlobTags(String containerName, String blobName) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        return bc.getTags();
    }

    /**
     * Sets tags on a blob.
     */
    protected void setBlobTags(String containerName, String blobName, Map<String, String> tags) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        bc.setTags(tags);
    }

    /**
     * Generates a unique container name using the test class name and a random suffix.
     */
    protected String uniqueContainerName(String suffix) {
        // Azure container names: 3-63 chars, lowercase, alphanumeric + dash
        String name = "it-" + suffix + "-" + System.nanoTime() % 100000;
        return name.toLowerCase().replaceAll("[^a-z0-9-]", "");
    }

    /**
     * Creates multi-line content for testing.
     */
    protected String makeLines(String... lines) {
        return String.join("\n", lines) + "\n";
    }

    /**
     * Creates numbered lines for testing.
     */
    protected String makeNumberedLines(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            sb.append("line-").append(i).append("\n");
        }
        return sb.toString();
    }
}
