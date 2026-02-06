package com.azure.logstash.input;

import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * Base class for all operational tests running against real Azure Government
 * storage accounts.
 *
 * <p>Uses lazy initialization with a static lock so that the storage account is
 * created exactly once across all test classes in the same JVM, regardless of
 * the order in which test classes execute.
 *
 * <p>A JVM shutdown hook ensures the storage account is always deleted when the
 * test process exits, even if tests fail or are interrupted.
 *
 * <p>Provides helper methods for blob upload/download/tag operations using
 * the Azure SDK via connection string.
 */
public abstract class AzureGovTestBase {

    protected static final String RESOURCE_GROUP = "rg-dev-logstash-plugin-playground";
    protected static final String LOCATION = "usgovvirginia";
    protected static final String SUBSCRIPTION_ID = "af16aa5c-8bae-4e4d-8996-56d2e65f75f4";
    protected static final String GOV_AUTHORITY_HOST = "https://login.microsoftonline.us";
    protected static final String GOV_STORAGE_SUFFIX = "blob.core.usgovcloudapi.net";

    private static final Object INIT_LOCK = new Object();
    private static volatile boolean initialized = false;

    protected static String accountName;
    protected static String connectionString;
    protected static String storageKey;
    protected static BlobServiceClient serviceClient;

    @BeforeClass
    public static void ensureStorageAccount() throws Exception {
        synchronized (INIT_LOCK) {
            if (initialized) {
                return;
            }

            // Step 1: Generate unique storage account name (max 24 chars)
            String suffix = randomLowerAlpha(8);
            accountName = "stplugin" + suffix;

            try {
                // Step 2: Create storage account
                System.out.println("Creating storage account: " + accountName);
                runAzCommand("az", "storage", "account", "create",
                        "--name", accountName,
                        "--resource-group", RESOURCE_GROUP,
                        "--location", LOCATION,
                        "--sku", "Standard_LRS",
                        "--kind", "StorageV2",
                        "--allow-blob-public-access", "false");

                // Step 3: Get connection string
                connectionString = runAzCommand("az", "storage", "account", "show-connection-string",
                        "--name", accountName,
                        "--resource-group", RESOURCE_GROUP,
                        "--query", "connectionString",
                        "-o", "tsv");

                // Step 4: Get storage key
                storageKey = runAzCommand("az", "storage", "account", "keys", "list",
                        "--account-name", accountName,
                        "--resource-group", RESOURCE_GROUP,
                        "--query", "[0].value",
                        "-o", "tsv");

                // Step 5: Create containers
                for (String container : new String[]{"incoming", "archive", "errors"}) {
                    runAzCommand("az", "storage", "container", "create",
                            "--name", container,
                            "--account-name", accountName,
                            "--connection-string", connectionString);
                }

                // Step 6: Get current identity's principal ID.
                // Try signed-in-user first (works for user accounts),
                // then fall back to MSI identity from RG role assignments.
                String objectId;
                String principalType;
                try {
                    objectId = runAzCommand("az", "ad", "signed-in-user", "show",
                            "--query", "id", "-o", "tsv");
                    principalType = "User";
                } catch (Exception e) {
                    // Not a user login (likely MSI). Get the Owner SP from RG.
                    objectId = runAzCommand("az", "role", "assignment", "list",
                            "--resource-group", RESOURCE_GROUP,
                            "--query",
                            "[?principalType=='ServicePrincipal' && roleDefinitionName=='Owner'].principalId | [0]",
                            "-o", "tsv");
                    principalType = "ServicePrincipal";
                }
                System.out.println("Identity: " + objectId + " (" + principalType + ")");

                // Step 7: Assign Storage Blob Data Owner role on the new storage account
                String scope = "/subscriptions/" + SUBSCRIPTION_ID
                        + "/resourceGroups/" + RESOURCE_GROUP
                        + "/providers/Microsoft.Storage/storageAccounts/" + accountName;
                runAzCommand("az", "role", "assignment", "create",
                        "--role", "Storage Blob Data Owner",
                        "--assignee-object-id", objectId,
                        "--assignee-principal-type", principalType,
                        "--scope", scope);

                // Step 8: Wait for RBAC propagation (Azure Gov can take 60s+)
                System.out.println("Waiting 60s for RBAC propagation...");
                Thread.sleep(60000);

                // Build the SDK client from connection string
                serviceClient = new BlobServiceClientBuilder()
                        .connectionString(connectionString)
                        .buildClient();

                System.out.println("Storage account " + accountName + " ready.");

            } catch (Exception e) {
                // If setup fails, ensure cleanup
                cleanupStorageAccount();
                throw e;
            }

            // Register shutdown hook to always clean up
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("Shutdown hook: deleting storage account " + accountName);
                cleanupStorageAccount();
            }, "storage-account-cleanup"));

            initialized = true;
        }
    }

    private static void cleanupStorageAccount() {
        if (accountName != null) {
            try {
                runAzCommand("az", "storage", "account", "delete",
                        "--name", accountName,
                        "--resource-group", RESOURCE_GROUP,
                        "--yes");
                System.out.println("Deleted storage account: " + accountName);
            } catch (Exception e) {
                System.err.println("WARNING: Failed to delete storage account "
                        + accountName + ": " + e.getMessage());
            }
        }
    }

    // ── Helper methods ──────────────────────────────────────────────────────

    /**
     * Uploads a text blob to the specified container.
     */
    protected static void uploadBlob(String containerName, String blobName, String content) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        bc.upload(new ByteArrayInputStream(data), data.length, true);
    }

    /**
     * Uploads a blob with raw byte content.
     */
    protected static void uploadBlobBytes(String containerName, String blobName, byte[] content) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        bc.upload(new ByteArrayInputStream(content), content.length, true);
    }

    /**
     * Gets the text content of a blob.
     */
    protected static String getBlobContent(String containerName, String blobName) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        bc.downloadStream(out);
        return out.toString(StandardCharsets.UTF_8);
    }

    /**
     * Gets the tags of a blob.
     */
    protected static Map<String, String> getBlobTags(String containerName, String blobName) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        return bc.getTags();
    }

    /**
     * Sets tags on a blob.
     */
    protected static void setBlobTags(String containerName, String blobName, Map<String, String> tags) {
        BlobClient bc = serviceClient.getBlobContainerClient(containerName)
                .getBlobClient(blobName);
        bc.setTags(tags);
    }

    /**
     * Lists blob names in a container.
     */
    protected static List<String> listBlobNames(String containerName) {
        BlobContainerClient cc = serviceClient.getBlobContainerClient(containerName);
        return cc.listBlobs().stream()
                .map(BlobItem::getName)
                .collect(Collectors.toList());
    }

    /**
     * Builds a BlobServiceClient using DefaultAzureCredential with gov authority host.
     */
    protected static BlobServiceClient buildBlobServiceClientWithDefaultCredential() {
        String endpoint = "https://" + accountName + "." + GOV_STORAGE_SUFFIX;
        return new BlobServiceClientBuilder()
                .endpoint(endpoint)
                .credential(new DefaultAzureCredentialBuilder()
                        .authorityHost(GOV_AUTHORITY_HOST)
                        .build())
                .buildClient();
    }

    /**
     * Cleans all blobs from a container, breaking any active leases first.
     */
    protected static void cleanContainer(String name) {
        BlobContainerClient cc = serviceClient.getBlobContainerClient(name);
        if (cc.exists()) {
            cc.listBlobs().forEach(blob -> {
                try {
                    try {
                        new com.azure.storage.blob.specialized.BlobLeaseClientBuilder()
                                .blobClient(cc.getBlobClient(blob.getName()))
                                .buildClient()
                                .breakLease();
                    } catch (Exception ignored) {}
                    cc.getBlobClient(blob.getName()).delete();
                } catch (Exception ignored) {}
            });
        }
    }

    /**
     * Creates numbered lines for testing.
     */
    protected static String makeNumberedLines(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 1; i <= count; i++) {
            sb.append("line-").append(i).append("\n");
        }
        return sb.toString();
    }

    /**
     * Generates a random lowercase alphabetic string of the given length.
     */
    private static String randomLowerAlpha(int length) {
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('a' + random.nextInt(26)));
        }
        return sb.toString();
    }

    /**
     * Runs an az CLI command and returns its stdout output.
     * Throws RuntimeException if the command exits with non-zero status.
     */
    protected static String runAzCommand(String... args) throws Exception {
        ProcessBuilder pb = new ProcessBuilder(args);
        pb.redirectErrorStream(true);
        Process p = pb.start();
        String output = new String(p.getInputStream().readAllBytes()).trim();
        int exitCode = p.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("az command failed (exit " + exitCode + "): " + output);
        }
        return output;
    }
}
